package execution

import (
	"chronos/observability"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"chronos/database"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidOperationRequest = errors.New(
		"invalid operation request",
	)

	ErrOperationInProgress = errors.New(
		"operation is already processing",
	)

	ErrOperationOutcomeUnknown = errors.New(
		"operation outcome is unknown",
	)

	ErrOperationFailed = errors.New(
		"operation failed",
	)

	ErrProviderNotConfigured = errors.New(
		"provider is not configured",
	)
)

type OperationService struct {
	pool      *pgxpool.Pool
	providers map[string]Provider
	metrics   *observability.Metrics
}

// operationPersistenceContext allows a short database update
// to finish even if the worker's execution context is cancelled.
func operationPersistenceContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.WithoutCancel(parent),
		5*time.Second,
	)
}

func NewOperationService(pool *pgxpool.Pool, providers map[string]Provider, metrics ...*observability.Metrics) (*OperationService, error) {

	if pool == nil {
		return nil, errors.New("database pool is required")
	}

	if len(providers) == 0 {
		return nil, errors.New("at least one provider is required")
	}

	providerCopy := make(map[string]Provider, len(providers))

	for name, provider := range providers {
		name = strings.TrimSpace(name)

		if name == "" || provider == nil {
			return nil, errors.New(
				"provider name and implementation are required",
			)
		}
		providerCopy[name] = provider
	}

	service := &OperationService{
		pool:      pool,
		providers: providerCopy,
	}

	if len(metrics) > 0 {
		service.metrics = metrics[0]
	}

	return service, nil

}

func validateOperationRequest(
	request OperationRequest,
) error {
	if request.WorkflowTaskID <= 0 {
		return fmt.Errorf(
			"%w: invalid workflow task ID",
			ErrInvalidOperationRequest,
		)
	}

	if strings.TrimSpace(request.OperationName) == "" {
		return fmt.Errorf(
			"%w: operation name is required",
			ErrInvalidOperationRequest,
		)
	}

	if strings.TrimSpace(request.Provider) == "" {
		return fmt.Errorf(
			"%w: provider is required",
			ErrInvalidOperationRequest,
		)
	}

	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return fmt.Errorf(
			"%w: idempotency key is required",
			ErrInvalidOperationRequest,
		)
	}

	if utf8.RuneCountInString(request.IdempotencyKey) > 255 {
		return fmt.Errorf(
			"%w: idempotency key exceeds 255 characters",
			ErrInvalidOperationRequest,
		)
	}

	if request.FencingToken < 0 {
		return fmt.Errorf(
			"%w: fencing token cannot be negative",
			ErrInvalidOperationRequest,
		)
	}

	return nil
}

func hashOperationPayload(payload []byte) []byte {
	hash := sha256.Sum256(payload)

	return hash[:]
}

func (s *OperationService) Execute(ctx context.Context, request OperationRequest) (OperationResult, error) {

	if s == nil || s.pool == nil {
		return OperationResult{}, errors.New(
			"operation service is not initialized",
		)
	}

	if err := validateOperationRequest(request); err != nil {
		return OperationResult{}, err
	}

	provider, ok := s.providers[request.Provider]
	if !ok {
		return OperationResult{}, fmt.Errorf(
			"%w: %s",
			ErrProviderNotConfigured,
			request.Provider,
		)
	}

	requestHash := hashOperationPayload(request.Payload)

	operation, created, err := database.ReserveExecutionOperation(
		ctx, s.pool,
		database.ExecutionOperationInput{
			WorkflowTaskID: request.WorkflowTaskID,
			OperationName:  request.OperationName,
			Provider:       request.Provider,
			IdempotencyKey: request.IdempotencyKey,
			RequestHash:    requestHash,
			FencingToken:   request.FencingToken,
		},
	)

	if err != nil {
		return OperationResult{}, fmt.Errorf(
			"reserve execution operation: %w",
			err,
		)
	}

	// An existing operation must be inspected. before calling rovider again

	if !created {
		return s.handleExistingOperation(
			ctx,
			operation,
			request,
			provider,
		)
	}

	providerStarted := time.Now()

	providerResult, providerErr := provider.Execute(
		ctx,
		request.IdempotencyKey,
		request.Payload,
	)

	if s.metrics != nil {
		s.metrics.ExternalOperationDuration.Observe(
			time.Since(providerStarted).Seconds(),
		)
	}

	if providerErr != nil {
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			providerErr.Error(),
		)
	}

	switch providerResult.Outcome {

	case ProviderSucceeded:

		providerReference := providerResult.ProviderReference

		persistCtx, cancel := operationPersistenceContext(ctx)
		defer cancel()

		err := database.CompleteExecutionOperation(
			persistCtx,
			s.pool,
			operation.ID,
			request.FencingToken,
			&providerReference,
			providerResult.Data,
		)
		if err != nil {
			unknownErr := s.markUnknown(
				ctx,
				operation.ID,
				request.FencingToken,
				"provider succeeded but result persistence failed",
			)

			return OperationResult{}, errors.Join(
				fmt.Errorf(
					"provider reported success but Chronos could not persist the result: %w",
					err,
				),
				unknownErr,
			)
		}

		return OperationResult{
			ProviderReference: providerResult.ProviderReference,
			Data:              providerResult.Data,
			Replayed:          false,
		}, nil

	case ProviderFailed:
		lastError := "provider reported a definitive failure"

		if len(providerResult.Data) > 0 {
			lastError += ": " + string(providerResult.Data)
		}

		persistCtx, cancel := operationPersistenceContext(ctx)
		defer cancel()

		err := database.FailExecutionOperation(
			persistCtx,
			s.pool,
			operation.ID,
			request.FencingToken,
			lastError,
		)
		if err != nil {
			return OperationResult{}, fmt.Errorf(
				"provider reported failure but Chronos could not persist it: %w",
				err,
			)
		}

		return OperationResult{}, fmt.Errorf(
			"%w: %s",
			ErrOperationFailed,
			lastError,
		)

	case ProviderPending:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			"provider reports that the operation is still pending",
		)

	case ProviderUnknown:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			"provider could not establish the operation outcome",
		)

	case ProviderNotFound:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			"provider did not find the operation after execution",
		)

	default:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			fmt.Sprintf(
				"provider returned unsupported outcome: %q",
				providerResult.Outcome,
			),
		)
	}
}

func (s *OperationService) handleExistingOperation(
	ctx context.Context,
	operation database.ExecutionOperation,
	request OperationRequest,
	provider Provider,
) (OperationResult, error) {

	fencingToken := request.FencingToken

	// Reject an older worker.
	if operation.FencingToken > fencingToken {
		return OperationResult{},
			database.ErrExecutionOperationStaleToken
	}

	// Transfer an unfinished operation to the new worker.
	if operation.FencingToken < fencingToken &&
		(operation.Status == "processing" ||
			operation.Status == "outcome_unknown") {

		updatedOperation, err := database.TakeOverExecutionOperation(
			ctx,
			s.pool,
			operation.ID,
			fencingToken,
		)
		if err != nil {
			return OperationResult{}, fmt.Errorf(
				"take over execution operation: %w",
				err,
			)
		}

		operation = updatedOperation
	}

	switch operation.Status {

	case "succeeded":
		return OperationResult{
			ProviderReference: stringValue(operation.ProviderReference),
			Data:              operation.Result,
			Replayed:          true,
		}, nil

	case "failed":
		lastError := "operation previously failed"

		if operation.LastError != nil {
			lastError = *operation.LastError
		}

		return OperationResult{}, fmt.Errorf(
			"%w: %s",
			ErrOperationFailed,
			lastError,
		)

	case "processing":
		return OperationResult{}, ErrOperationInProgress

	case "outcome_unknown":
		return s.reconcileUnknownOperation(
			ctx,
			operation,
			request,
			provider,
		)

	default:
		return OperationResult{}, fmt.Errorf(
			"%w: unsupported operation status %q",
			database.ErrExecutionOperationInvalidState,
			operation.Status,
		)
	}
}

func (s *OperationService) reconcileUnknownOperation(
	ctx context.Context,
	operation database.ExecutionOperation,
	request OperationRequest,
	provider Provider,
) (OperationResult, error) {

	// Ask the provider about the original operation.
	// Never generate a new key during reconciliation.
	providerStarted := time.Now()

	providerResult, err := provider.Lookup(
		ctx,
		request.IdempotencyKey,
	)

	if s.metrics != nil {
		s.metrics.ExternalOperationDuration.Observe(
			time.Since(providerStarted).Seconds(),
		)
	}

	if err != nil {
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			"provider lookup failed: "+err.Error(),
		)
	}

	switch providerResult.Outcome {

	case ProviderSucceeded:
		// The provider confirms that the original operation
		// succeeded. Save its result without executing again.
		providerReference := providerResult.ProviderReference

		err := database.CompleteExecutionOperation(
			ctx,
			s.pool,
			operation.ID,
			request.FencingToken,
			&providerReference,
			providerResult.Data,
		)
		if err != nil {
			return OperationResult{}, fmt.Errorf(
				"provider confirmed success but result persistence failed: %w",
				err,
			)
		}

		return OperationResult{
			ProviderReference: providerReference,
			Data:              providerResult.Data,
			Replayed:          true,
		}, nil

	case ProviderFailed:
		// The provider has established a definitive failure.
		lastError := "provider confirmed operation failure"

		if len(providerResult.Data) > 0 {
			lastError += ": " + string(providerResult.Data)
		}

		persistCtx, cancel := operationPersistenceContext(ctx)
		defer cancel()

		err := database.FailExecutionOperation(
			persistCtx,
			s.pool,
			operation.ID,
			request.FencingToken,
			lastError,
		)
		if err != nil {
			return OperationResult{}, fmt.Errorf(
				"provider confirmed failure but persistence failed: %w",
				err,
			)
		}

		return OperationResult{}, fmt.Errorf(
			"%w: %s",
			ErrOperationFailed,
			lastError,
		)

	case ProviderPending:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			"provider reports operation is still pending",
		)

	case ProviderNotFound:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			"provider lookup did not find the operation; safe retry is not established",
		)

	case ProviderUnknown:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			"provider could not establish the operation outcome",
		)

	default:
		return OperationResult{}, s.markUnknown(
			ctx,
			operation.ID,
			request.FencingToken,
			fmt.Sprintf(
				"provider returned unsupported lookup outcome: %q",
				providerResult.Outcome,
			),
		)
	}
}

// markUnknown persists an uncertain provider outcome.
func (s *OperationService) markUnknown(
	ctx context.Context,
	operationID int64,
	fencingToken int64,
	reason string,
) error {
	persistCtx, cancel := operationPersistenceContext(ctx)
	defer cancel()

	err := database.MarkExecutionOutcomeUnknown(
		persistCtx,
		s.pool,
		operationID,
		fencingToken,
		reason,
	)
	if err != nil {
		return errors.Join(
			ErrOperationOutcomeUnknown,
			fmt.Errorf(
				"failed to persist unknown outcome: %w",
				err,
			),
		)
	}

	return fmt.Errorf(
		"%w: %s",
		ErrOperationOutcomeUnknown,
		reason,
	)
}

// stringValue safely converts a nullable string pointer
// into a normal string.
func stringValue(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}
