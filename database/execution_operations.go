package database

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrExecutionOperationConflict = errors.New(
		"execution operation already exists with different parameters",
	)

	ErrExecutionOperationNotFound = errors.New(
		"execution operation not found",
	)

	ErrExecutionOperationStaleToken = errors.New(
		"stale execution operation fencing token",
	)

	ErrExecutionOperationInvalidState = errors.New(
		"invalid execution operation state",
	)

	ErrInvalidExecutionOperation = errors.New(
		"invalid execution operation input",
	)
)

// ExecutionOperation represents one logical external side effect
// performed by a workflow task.
type ExecutionOperation struct {
	ID                   int64
	WorkflowTaskID       int64
	OperationName        string
	Provider             string
	IdempotencyKey       string
	RequestHash          []byte
	Status               string
	FencingToken         int64
	ProviderReference    *string
	Result               []byte
	LastError            *string
	AttemptCount         int
	CreatedAt            time.Time
	UpdatedAt            time.Time
	CompletedAt          *time.Time
	RecoveryAttemptCount int
	NextRecoveryAt       *time.Time
	RecoveryWorkerID     *string
	RecoveryLeaseUntil   *time.Time
	RecoveryFencingToken int64
}

// ExecutionOperationInput contains the information required
// to reserve a new logical execution operation.
type ExecutionOperationInput struct {
	WorkflowTaskID int64
	OperationName  string
	Provider       string
	IdempotencyKey string
	RequestHash    []byte
	FencingToken   int64
}

const executionOperationColumns = `
	id,
	workflow_task_id,
	operation_name,
	provider,
	idempotency_key,
	request_hash,
	status,
	fencing_token,
	provider_reference,
	result,
	last_error,
	attempt_count,
	created_at,
	updated_at,
	completed_at,
	recovery_attempt_count,
	next_recovery_at,
	recovery_worker_id,
	recovery_lease_until,
	recovery_fencing_token
`

// scanExecutionOperation converts a PostgreSQL row into an ExecutionOperation.
func scanExecutionOperation(row pgx.Row) (ExecutionOperation, error) {
	var operation ExecutionOperation

	var providerReference sql.NullString
	var lastError sql.NullString
	var completedAt sql.NullTime
	var nextRecoveryAt sql.NullTime
	var recoveryWorkerID sql.NullString
	var recoveryLeaseUntil sql.NullTime

	err := row.Scan(
		&operation.ID,
		&operation.WorkflowTaskID,
		&operation.OperationName,
		&operation.Provider,
		&operation.IdempotencyKey,
		&operation.RequestHash,
		&operation.Status,
		&operation.FencingToken,
		&providerReference,
		&operation.Result,
		&lastError,
		&operation.AttemptCount,
		&operation.CreatedAt,
		&operation.UpdatedAt,
		&completedAt,
		&operation.RecoveryAttemptCount,
		&nextRecoveryAt,
		&recoveryWorkerID,
		&recoveryLeaseUntil,
		&operation.RecoveryFencingToken,
	)

	if err != nil {
		return ExecutionOperation{}, err
	}

	if providerReference.Valid {
		value := providerReference.String
		operation.ProviderReference = &value
	}

	if lastError.Valid {
		value := lastError.String
		operation.LastError = &value
	}

	if completedAt.Valid {
		value := completedAt.Time
		operation.CompletedAt = &value
	}

	if nextRecoveryAt.Valid {
		value := nextRecoveryAt.Time
		operation.NextRecoveryAt = &value
	}

	if recoveryWorkerID.Valid {
		value := recoveryWorkerID.String
		operation.RecoveryWorkerID = &value
	}

	if recoveryLeaseUntil.Valid {
		value := recoveryLeaseUntil.Time
		operation.RecoveryLeaseUntil = &value
	}

	return operation, nil
}

// validateExecutionOperationInput validates the reservation parameters.

func validateExecutionOperationInput(pool *pgxpool.Pool, input ExecutionOperationInput) error {
	if pool == nil ||
		input.WorkflowTaskID <= 0 ||
		strings.TrimSpace(input.OperationName) == "" ||
		strings.TrimSpace(input.Provider) == "" ||
		strings.TrimSpace(input.IdempotencyKey) == "" ||
		len(input.IdempotencyKey) > 255 ||
		len(input.RequestHash) != 32 ||
		input.FencingToken < 0 {

		return ErrInvalidExecutionOperation
	}

	return nil
}

func lockWorkflowTaskJob(
	ctx context.Context,
	tx pgx.Tx,
	workflowTaskID int64,
) (int64, string, error) {

	var fencingToken int64
	var status string

	err := tx.QueryRow(
		ctx,
		`SELECT
			j.fencing_token,
			j.status
		 FROM workflow_tasks wt
		 JOIN jobs j
		   ON j.id = wt.job_id
		 WHERE wt.id = $1
		 FOR UPDATE OF wt, j`,
		workflowTaskID,
	).Scan(
		&fencingToken,
		&status,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", fmt.Errorf(
			"%w: workflow task or linked job not found",
			ErrInvalidExecutionOperation,
		)
	}

	if err != nil {
		return 0, "", fmt.Errorf(
			"lock workflow task job: %w",
			err,
		)
	}

	return fencingToken, status, nil
}

// ReserveExecutionOperation creates a new execution operation.
// If the logical operation already exists with identical parameters,
// it returns the existing record without creating another one.
// created is true only when a new database record was inserted.

func ReserveExecutionOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	input ExecutionOperationInput,
) (operation ExecutionOperation, created bool, err error) {

	if err := validateExecutionOperationInput(pool, input); err != nil {
		return ExecutionOperation{}, false, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return ExecutionOperation{}, false, fmt.Errorf(
			"begin execution reservation: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Step 1: Lock the linked workflow task and job.
	currentToken, jobStatus, err := lockWorkflowTaskJob(
		ctx,
		tx,
		input.WorkflowTaskID,
	)

	if err != nil {
		return ExecutionOperation{}, false, err
	}

	// Step 2: Verify actual job ownership.
	if currentToken != input.FencingToken {
		return ExecutionOperation{}, false,
			ErrExecutionOperationStaleToken
	}

	if jobStatus != "Running" {
		return ExecutionOperation{}, false, fmt.Errorf(
			"%w: linked job is not running",
			ErrExecutionOperationInvalidState,
		)
	}

	// Step 3: Attempt to reserve the logical operation.
	operation, err = scanExecutionOperation(
		tx.QueryRow(
			ctx,
			`INSERT INTO execution_operations (
				workflow_task_id,
				operation_name,
				provider,
				idempotency_key,
				request_hash,
				status,
				fencing_token
			)
			VALUES ($1, $2, $3, $4, $5, 'processing', $6)
			ON CONFLICT DO NOTHING
			RETURNING `+executionOperationColumns,
			input.WorkflowTaskID,
			input.OperationName,
			input.Provider,
			input.IdempotencyKey,
			input.RequestHash,
			input.FencingToken,
		),
	)

	// Step 4: New operation successfully reserved.
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return ExecutionOperation{}, false, fmt.Errorf(
				"commit execution reservation: %w",
				err,
			)
		}

		return operation, true, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return ExecutionOperation{}, false, fmt.Errorf(
			"reserve execution operation: %w",
			err,
		)
	}

	// Step 5: Check whether the operation already exists
	// for this workflow task.
	operation, err = scanExecutionOperation(
		tx.QueryRow(
			ctx,
			`SELECT `+executionOperationColumns+`
			 FROM execution_operations
			 WHERE workflow_task_id = $1
			   AND operation_name = $2`,
			input.WorkflowTaskID,
			input.OperationName,
		),
	)

	if err == nil {
		if operation.Provider != input.Provider ||
			operation.IdempotencyKey != input.IdempotencyKey ||
			!bytes.Equal(operation.RequestHash, input.RequestHash) {

			return ExecutionOperation{}, false,
				ErrExecutionOperationConflict
		}

		if err := tx.Commit(ctx); err != nil {
			return ExecutionOperation{}, false, fmt.Errorf(
				"commit existing execution lookup: %w",
				err,
			)
		}

		return operation, false, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return ExecutionOperation{}, false, fmt.Errorf(
			"retrieve existing task operation: %w",
			err,
		)
	}

	// Step 6: Check whether another task already owns
	// this provider idempotency key.
	_, err = scanExecutionOperation(
		tx.QueryRow(
			ctx,
			`SELECT `+executionOperationColumns+`
			 FROM execution_operations
			 WHERE provider = $1
			   AND idempotency_key = $2`,
			input.Provider,
			input.IdempotencyKey,
		),
	)

	if err == nil {
		return ExecutionOperation{}, false,
			ErrExecutionOperationConflict
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return ExecutionOperation{}, false, fmt.Errorf(
			"check provider idempotency key: %w",
			err,
		)
	}

	return ExecutionOperation{}, false, fmt.Errorf(
		"execution reservation conflict without matching record",
	)
}

func FailExecutionOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	fencingToken int64,
	lastError string,
) error {

	tx, operation, err := beginOwnedExecutionOperation(
		ctx,
		pool,
		operationID,
		fencingToken,
	)
	if err != nil {
		return err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if operation.Status != "processing" &&
		operation.Status != "outcome_unknown" {

		return fmt.Errorf(
			"%w: cannot fail operation with status %s",
			ErrExecutionOperationInvalidState,
			operation.Status,
		)
	}

	tag, err := tx.Exec(
		ctx,
		`UPDATE execution_operations
		 SET status = 'failed',
		     last_error = $3,
		     completed_at = NOW(),
		     updated_at = NOW()
		 WHERE id = $1
		   AND fencing_token = $2
		   AND status IN ('processing', 'outcome_unknown')`,
		operationID,
		fencingToken,
		lastError,
	)

	if err != nil {
		return fmt.Errorf(
			"fail execution operation %d: %w",
			operationID,
			err,
		)
	}

	if tag.RowsAffected() != 1 {
		return ErrExecutionOperationInvalidState
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit execution failure: %w",
			err,
		)
	}

	return nil
}

func MarkExecutionOutcomeUnknown(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	fencingToken int64,
	lastError string,
) error {

	tx, operation, err := beginOwnedExecutionOperation(
		ctx,
		pool,
		operationID,
		fencingToken,
	)
	if err != nil {
		return err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if operation.Status != "processing" &&
		operation.Status != "outcome_unknown" {

		return fmt.Errorf(
			"%w: cannot mark operation unknown with status %s",
			ErrExecutionOperationInvalidState,
			operation.Status,
		)
	}

	// Repeating this call for an already unknown operation
	// should not create another execution attempt.
	if operation.Status == "outcome_unknown" &&
		operation.LastError != nil &&
		*operation.LastError == lastError &&
		operation.NextRecoveryAt != nil {
		return nil
	}

	tag, err := tx.Exec(
		ctx,
		`UPDATE execution_operations
SET status = 'outcome_unknown',
    last_error = $3,
    completed_at = NULL,
    next_recovery_at = COALESCE(next_recovery_at, NOW()),
    recovery_worker_id = NULL,
    recovery_lease_until = NULL,
    updated_at = NOW()
WHERE id = $1
  AND fencing_token = $2
  AND status IN ('processing', 'outcome_unknown')`,
		operationID,
		fencingToken,
		lastError,
	)

	if err != nil {
		return fmt.Errorf(
			"mark execution outcome unknown %d: %w",
			operationID,
			err,
		)
	}

	if tag.RowsAffected() != 1 {
		return ErrExecutionOperationInvalidState
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit unknown execution outcome: %w",
			err,
		)
	}

	return nil
}

func inspectExecutionOperationState(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	fencingToken int64,
) error {

	var storedToken int64
	var storedStatus string

	err := pool.QueryRow(
		ctx,
		`SELECT fencing_token, status
		 FROM execution_operations
		 WHERE id = $1`,
		operationID,
	).Scan(
		&storedToken,
		&storedStatus,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return ErrExecutionOperationNotFound
	}

	if err != nil {
		return fmt.Errorf(
			"inspect execution operation: %w",
			err,
		)
	}

	if storedToken != fencingToken {
		return ErrExecutionOperationStaleToken
	}

	return fmt.Errorf(
		"%w: operation currently has status %s",
		ErrExecutionOperationInvalidState,
		storedStatus,
	)
}

// update the fencing token of other worker when original worker crash

// TakeOverExecutionOperation transfers ownership to a worker
// that currently owns the linked running job.
//
// A takeover changes the execution status to outcome_unknown,
// because the previous worker may have completed an external
// side effect before crashing.
func TakeOverExecutionOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	newFencingToken int64,
) (ExecutionOperation, error) {

	if pool == nil || operationID <= 0 || newFencingToken <= 0 {
		return ExecutionOperation{}, ErrInvalidExecutionOperation
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return ExecutionOperation{}, fmt.Errorf(
			"begin execution takeover: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	operation, currentJobToken, jobStatus, err :=
		lockExecutionOperationAndJob(ctx, tx, operationID)

	if err != nil {
		return ExecutionOperation{}, err
	}

	// The supplied token must belong to the current job owner.
	if currentJobToken != newFencingToken {
		return ExecutionOperation{}, ErrExecutionOperationStaleToken
	}

	if jobStatus != "Running" {
		return ExecutionOperation{}, fmt.Errorf(
			"%w: linked job is not running",
			ErrExecutionOperationInvalidState,
		)
	}

	if operation.Status != "processing" &&
		operation.Status != "outcome_unknown" {

		return ExecutionOperation{}, fmt.Errorf(
			"%w: cannot take over operation with status %s",
			ErrExecutionOperationInvalidState,
			operation.Status,
		)
	}

	if operation.FencingToken > newFencingToken {
		return ExecutionOperation{}, ErrExecutionOperationStaleToken
	}

	// Repeated invocation by the same owner is idempotent.
	if operation.FencingToken == newFencingToken {
		if err := tx.Commit(ctx); err != nil {
			return ExecutionOperation{}, fmt.Errorf(
				"commit existing execution ownership: %w",
				err,
			)
		}

		return operation, nil
	}

	// Transfer ownership, but preserve uncertainty.
	operation, err = scanExecutionOperation(
		tx.QueryRow(
			ctx,
			`UPDATE execution_operations
			 SET status = 'outcome_unknown',
			     fencing_token = $2,
			     attempt_count = attempt_count + 1,
			     updated_at = NOW()
			 WHERE id = $1
			 RETURNING `+executionOperationColumns,
			operationID,
			newFencingToken,
		),
	)

	if err != nil {
		return ExecutionOperation{}, fmt.Errorf(
			"update execution ownership: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return ExecutionOperation{}, fmt.Errorf(
			"commit execution takeover: %w",
			err,
		)
	}

	return operation, nil
}

// lockExecutionOperationAndJob locks the current job first,
// verifies its ownership state, and then locks the operation.
//
// All execution-operation state transitions should use this
// consistent lock order.
func lockExecutionOperationAndJob(
	ctx context.Context,
	tx pgx.Tx,
	operationID int64,
) (ExecutionOperation, int64, string, error) {

	var currentJobToken int64
	var currentJobStatus string

	err := tx.QueryRow(
		ctx,
		`SELECT
			j.fencing_token,
			j.status
		 FROM execution_operations eo
		 JOIN workflow_tasks wt
		   ON wt.id = eo.workflow_task_id
		 JOIN jobs j
		   ON j.id = wt.job_id
		 WHERE eo.id = $1
		 FOR UPDATE OF j`,
		operationID,
	).Scan(
		&currentJobToken,
		&currentJobStatus,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool

		checkErr := tx.QueryRow(
			ctx,
			`SELECT EXISTS (
				SELECT 1
				FROM execution_operations
				WHERE id = $1
			)`,
			operationID,
		).Scan(&exists)

		if checkErr != nil {
			return ExecutionOperation{}, 0, "",
				fmt.Errorf("check execution operation: %w", checkErr)
		}

		if !exists {
			return ExecutionOperation{}, 0, "",
				ErrExecutionOperationNotFound
		}

		return ExecutionOperation{}, 0, "",
			fmt.Errorf(
				"%w: workflow task has no linked job",
				ErrExecutionOperationInvalidState,
			)
	}

	if err != nil {
		return ExecutionOperation{}, 0, "",
			fmt.Errorf("lock current job ownership: %w", err)
	}

	operation, err := scanExecutionOperation(
		tx.QueryRow(
			ctx,
			`SELECT `+executionOperationColumns+`
			 FROM execution_operations
			 WHERE id = $1
			 FOR UPDATE`,
			operationID,
		),
	)

	if err != nil {
		return ExecutionOperation{}, 0, "",
			fmt.Errorf("lock execution operation: %w", err)
	}

	return operation, currentJobToken, currentJobStatus, nil
}

// GetExecutionOperation retrieves an execution operation by ID.
func GetExecutionOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int64,
) (ExecutionOperation, error) {

	if pool == nil || id <= 0 {
		return ExecutionOperation{}, ErrInvalidExecutionOperation
	}

	operation, err := scanExecutionOperation(
		pool.QueryRow(
			ctx,
			`SELECT `+executionOperationColumns+`
			 FROM execution_operations
			 WHERE id = $1`,
			id,
		),
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return ExecutionOperation{}, ErrExecutionOperationNotFound
	}

	if err != nil {
		return ExecutionOperation{}, fmt.Errorf(
			"get execution operation %d: %w",
			id,
			err,
		)
	}

	return operation, nil
}

// CompleteExecutionOperation records a successful external operation.
// The current job owner and execution fencing token must both match.
func CompleteExecutionOperation(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	fencingToken int64,
	providerReference *string,
	result []byte,
) error {

	tx, operation, err := beginOwnedExecutionOperation(
		ctx,
		pool,
		operationID,
		fencingToken,
	)
	if err != nil {
		return err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if operation.Status != "processing" &&
		operation.Status != "outcome_unknown" {

		return fmt.Errorf(
			"%w: cannot complete operation with status %s",
			ErrExecutionOperationInvalidState,
			operation.Status,
		)
	}

	tag, err := tx.Exec(
		ctx,
		`UPDATE execution_operations
		 SET status = 'succeeded',
		     provider_reference = $3,
		     result = $4,
		     last_error = NULL,
		     completed_at = NOW(),
		     updated_at = NOW()
		 WHERE id = $1
		   AND fencing_token = $2
		   AND status IN ('processing', 'outcome_unknown')`,
		operationID,
		fencingToken,
		providerReference,
		result,
	)

	if err != nil {
		return fmt.Errorf(
			"complete execution operation %d: %w",
			operationID,
			err,
		)
	}

	if tag.RowsAffected() != 1 {
		return ErrExecutionOperationInvalidState
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit execution completion: %w",
			err,
		)
	}

	return nil
}

func beginOwnedExecutionOperation(ctx context.Context, pool *pgxpool.Pool, operationID int64, fencingToken int64) (pgx.Tx, ExecutionOperation, error) {

	if pool == nil || operationID <= 0 || fencingToken < 0 {
		return nil, ExecutionOperation{},
			ErrInvalidExecutionOperation
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, ExecutionOperation{}, fmt.Errorf(
			"begin execution operation transition: %w",
			err,
		)
	}

	operation, currentJobToken, jobStatus, err :=
		lockExecutionOperationAndJob(
			ctx,
			tx,
			operationID,
		)

	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, ExecutionOperation{}, err
	}

	// The supplied token must match both the current job and the execution operation.

	if currentJobToken != fencingToken ||
		operation.FencingToken != fencingToken {

		_ = tx.Rollback(ctx)

		return nil, ExecutionOperation{},
			ErrExecutionOperationStaleToken
	}

	if jobStatus != "Running" {
		_ = tx.Rollback(ctx)

		return nil, ExecutionOperation{}, fmt.Errorf(
			"%w: linked job is not running",
			ErrExecutionOperationInvalidState,
		)
	}

	return tx, operation, nil
}
