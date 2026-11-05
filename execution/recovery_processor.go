package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"chronos/database"

	"github.com/jackc/pgx/v5/pgxpool"
)

type RecoveryProcessor struct {
	pool          *pgxpool.Pool
	providers     map[string]Provider
	workerID      string
	leaseDuration time.Duration
	maxAttempts   int
}

func NewRecoveryProcessor(pool *pgxpool.Pool, providers map[string]Provider, workerID string, leaseDuration time.Duration, maxAttempts int) (*RecoveryProcessor, error) {

	if pool == nil {
		return nil, errors.New("database pool is required")
	}

	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("recovery worker ID is required")
	}

	if leaseDuration < time.Millisecond {
		return nil, errors.New("recovery lease duration must be at least one millisecond")
	}

	if maxAttempts <= 0 {
		return nil, errors.New("maximum recovery attempts must be positive")
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

	return &RecoveryProcessor{
		pool:          pool,
		providers:     providerCopy,
		workerID:      workerID,
		leaseDuration: leaseDuration,
		maxAttempts:   maxAttempts,
	}, nil
}

// RunOnce claims and processes at most one unresolved operation.
//
// Recovery always uses Provider.Lookup. It never calls Provider.Execute.

func (p *RecoveryProcessor) RunOnce(
	ctx context.Context,
) (bool, error) {

	if ctx == nil {
		return false, errors.New("context is required")
	}

	operation, claimed, err := database.ClaimNextExecutionOperationRecovery(
		ctx,
		p.pool, p.workerID, p.leaseDuration, p.maxAttempts,
	)

	if err != nil {
		return false, fmt.Errorf(
			"claim execution operation recovery: %w",
			err,
		)
	}

	if !claimed {
		return false, nil
	}

	provider, exists := p.providers[operation.Provider]
	if !exists {
		return true, p.reschedule(
			ctx,
			operation,
			fmt.Sprintf(
				"recovery provider %q is not registered",
				operation.Provider,
			),
		)
	}

	providerResult, lookupErr := provider.Lookup(
		ctx,
		operation.IdempotencyKey,
	)

	if lookupErr != nil {
		return true, p.reschedule(
			ctx,
			operation,
			fmt.Sprintf("provider lookup failed: %v", lookupErr),
		)
	}

	switch providerResult.Outcome {
	case ProviderSucceeded:
		return true, p.complete(
			ctx,
			operation,
			providerResult,
		)

	case ProviderFailed:
		return true, p.fail(
			ctx,
			operation,
			"provider confirmed operation failure",
		)

	case ProviderPending:
		return true, p.reschedule(
			ctx,
			operation,
			"provider reports operation is still pending",
		)

	case ProviderNotFound:
		return true, p.reschedule(
			ctx,
			operation,
			"provider could not find the original operation",
		)

	case ProviderUnknown:
		return true, p.reschedule(
			ctx,
			operation,
			"provider could not determine the operation outcome",
		)

	default:
		return true, p.reschedule(
			ctx,
			operation,
			fmt.Sprintf(
				"provider returned unsupported outcome %q",
				providerResult.Outcome,
			),
		)
	}
}

func (p *RecoveryProcessor) complete(
	ctx context.Context,
	operation database.ExecutionOperation,
	result ProviderResult,
) error {
	persistCtx, cancel := operationPersistenceContext(ctx)
	defer cancel()

	_, err := database.CompleteExecutionOperationRecovery(
		persistCtx,
		p.pool,
		operation.ID,
		p.workerID,
		operation.RecoveryFencingToken,
		&result.ProviderReference,
		result.Data,
	)
	if err != nil {
		return fmt.Errorf(
			"persist recovered operation success: %w",
			err,
		)
	}

	return nil
}

func (p *RecoveryProcessor) fail(
	ctx context.Context,
	operation database.ExecutionOperation,
	lastError string,
) error {
	persistCtx, cancel := operationPersistenceContext(ctx)
	defer cancel()

	_, err := database.FailExecutionOperationRecovery(
		persistCtx,
		p.pool,
		operation.ID,
		p.workerID,
		operation.RecoveryFencingToken,
		lastError,
	)
	if err != nil {
		return fmt.Errorf(
			"persist recovered operation failure: %w",
			err,
		)
	}

	return nil
}

func (p *RecoveryProcessor) reschedule(
	ctx context.Context,
	operation database.ExecutionOperation,
	lastError string,
) error {
	persistCtx, cancel := operationPersistenceContext(ctx)
	defer cancel()

	_, err := database.RescheduleExecutionOperationRecovery(
		persistCtx,
		p.pool,
		operation.ID,
		p.workerID,
		operation.RecoveryFencingToken,
		lastError,
		RecoveryDelay(operation.RecoveryAttemptCount),
		p.maxAttempts,
	)
	if err != nil {
		return fmt.Errorf(
			"reschedule recovered operation: %w",
			err,
		)
	}

	return nil
}
