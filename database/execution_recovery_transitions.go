package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func validateRecoveryOwnership(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	workerID string,
	recoveryToken int64,
) error {
	var valid bool

	err := pool.QueryRow(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM execution_operations
			WHERE id = $1
			  AND status IN ('processing', 'outcome_unknown')
			  AND recovery_worker_id = $2
			  AND recovery_fencing_token = $3
			  AND recovery_lease_until > NOW()
		)`,
		operationID,
		workerID,
		recoveryToken,
	).Scan(&valid)

	if err != nil {
		return fmt.Errorf("validate recovery ownership: %w", err)
	}

	if !valid {
		return ErrExecutionOperationStaleToken
	}

	return nil
}

func CompleteExecutionOperationRecovery(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	workerID string,
	recoveryToken int64,
	providerReference *string,
	result []byte,
) (ExecutionOperation, error) {
	if pool == nil ||
		operationID <= 0 ||
		strings.TrimSpace(workerID) == "" ||
		recoveryToken <= 0 {
		return ExecutionOperation{}, ErrInvalidExecutionOperation
	}

	operation, err := scanExecutionOperation(
		pool.QueryRow(
			ctx,
			`UPDATE execution_operations
             SET status = 'succeeded',
                 provider_reference = $4,
                 result = $5,
                 last_error = NULL,
                 completed_at = NOW(),
                 next_recovery_at = NULL,
                 recovery_worker_id = NULL,
                 recovery_lease_until = NULL,
                 updated_at = NOW()
             WHERE id = $1
               AND status IN ('processing', 'outcome_unknown')
               AND recovery_worker_id = $2
               AND recovery_fencing_token = $3
               AND recovery_lease_until > NOW()
             RETURNING `+executionOperationColumns,
			operationID,
			workerID,
			recoveryToken,
			providerReference,
			result,
		),
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ExecutionOperation{}, ErrExecutionOperationStaleToken
		}

		return ExecutionOperation{}, fmt.Errorf(
			"complete execution operation recovery: %w",
			err,
		)
	}

	return operation, nil
}

func FailExecutionOperationRecovery(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	workerID string,
	recoveryToken int64,
	lastError string,
) (ExecutionOperation, error) {
	if pool == nil ||
		operationID <= 0 ||
		strings.TrimSpace(workerID) == "" ||
		recoveryToken <= 0 ||
		strings.TrimSpace(lastError) == "" {
		return ExecutionOperation{}, ErrInvalidExecutionOperation
	}

	operation, err := scanExecutionOperation(
		pool.QueryRow(
			ctx,
			`UPDATE execution_operations
             SET status = 'failed',
                 last_error = $4,
                 completed_at = NOW(),
                 next_recovery_at = NULL,
                 recovery_worker_id = NULL,
                 recovery_lease_until = NULL,
                 updated_at = NOW()
             WHERE id = $1
               AND status IN ('processing', 'outcome_unknown')
               AND recovery_worker_id = $2
               AND recovery_fencing_token = $3
               AND recovery_lease_until > NOW()
             RETURNING `+executionOperationColumns,
			operationID,
			workerID,
			recoveryToken,
			lastError,
		),
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ExecutionOperation{}, ErrExecutionOperationStaleToken
		}

		return ExecutionOperation{}, fmt.Errorf(
			"fail execution operation recovery: %w",
			err,
		)
	}

	return operation, nil
}

// When the provider returns pending, not_found, unknown, or a temporary lookup error, we must not mark the operation failed. We schedule another lookup using exponential backoff.

func RescheduleExecutionOperationRecovery(
	ctx context.Context,
	pool *pgxpool.Pool,
	operationID int64,
	workerID string,
	recoveryToken int64,
	lastError string,
	delay time.Duration,
	maxAttempts int,
) (ExecutionOperation, error) {
	if pool == nil ||
		operationID <= 0 ||
		strings.TrimSpace(workerID) == "" ||
		recoveryToken <= 0 ||
		strings.TrimSpace(lastError) == "" ||
		delay < 0 ||
		maxAttempts <= 0 {
		return ExecutionOperation{}, ErrInvalidExecutionOperation
	}

	operation, err := scanExecutionOperation(
		pool.QueryRow(
			ctx,
			`UPDATE execution_operations
             SET status = 'outcome_unknown',
                 last_error = $4,
                 completed_at = NULL,
                 next_recovery_at = CASE
                     WHEN recovery_attempt_count < $6
                     THEN NOW() + ($5 * INTERVAL '1 millisecond')
                     ELSE NULL
                 END,
                 recovery_worker_id = NULL,
                 recovery_lease_until = NULL,
                 updated_at = NOW()
             WHERE id = $1
               AND status IN ('processing', 'outcome_unknown')
               AND recovery_worker_id = $2
               AND recovery_fencing_token = $3
               AND recovery_lease_until > NOW()
             RETURNING `+executionOperationColumns,
			operationID,
			workerID,
			recoveryToken,
			lastError,
			delay.Milliseconds(),
			maxAttempts,
		),
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ExecutionOperation{}, ErrExecutionOperationStaleToken
		}

		return ExecutionOperation{}, fmt.Errorf(
			"reschedule execution operation recovery: %w",
			err,
		)
	}

	return operation, nil
}
