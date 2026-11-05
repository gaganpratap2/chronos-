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

func ClaimNextExecutionOperationRecovery(ctx context.Context, pool *pgxpool.Pool, workerID string, leaseDuration time.Duration, maxAttempts int) (ExecutionOperation, bool, error) {

	if pool == nil ||
		strings.TrimSpace(workerID) == "" ||
		leaseDuration < time.Millisecond ||
		maxAttempts <= 0 {
		return ExecutionOperation{}, false,
			ErrInvalidExecutionOperation
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return ExecutionOperation{}, false, fmt.Errorf(
			"begin recovery claim: %w", err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var operationID int64

	// Step 1: Find and lock the linked Failed job first.
	err = tx.QueryRow(
		ctx,
		`SELECT eo.id
		 FROM jobs j
		 JOIN workflow_tasks wt ON wt.job_id = j.id
		 JOIN execution_operations eo
		   ON eo.workflow_task_id = wt.id
		 WHERE j.status = 'Failed'
		   AND eo.status IN ('processing', 'outcome_unknown')
		   AND eo.next_recovery_at IS NOT NULL
		   AND eo.next_recovery_at <= NOW()
		   AND eo.recovery_attempt_count < $1
		   AND (
		     eo.recovery_worker_id IS NULL
		     OR eo.recovery_lease_until <= NOW()
		   )
			 ORDER BY eo.next_recovery_at, eo.updated_at, eo.id
		 LIMIT 1
		 FOR UPDATE OF j SKIP LOCKED`,
		maxAttempts,
	).Scan(&operationID)

	if errors.Is(err, pgx.ErrNoRows) {
		return ExecutionOperation{}, false, nil
	}

	if err != nil {
		return ExecutionOperation{}, false, fmt.Errorf(
			"find due recovery operation: %w", err,
		)
	}

	// Step 2: Lock and claim the operation.
	operation, err := scanExecutionOperation(
		tx.QueryRow(
			ctx,
			`UPDATE execution_operations
			 SET recovery_worker_id = $2,
			     recovery_lease_until =
			       NOW() + ($3 * INTERVAL '1 millisecond'),
			     recovery_attempt_count =
			       recovery_attempt_count + 1,
			     recovery_fencing_token =
			       recovery_fencing_token + 1,
			     updated_at = NOW()
			 WHERE id = $1
			   AND status IN ('processing', 'outcome_unknown')
			   AND recovery_attempt_count < $4
			   AND next_recovery_at <= NOW()
			   AND (
			     recovery_worker_id IS NULL
			     OR recovery_lease_until <= NOW()
			   )
			 RETURNING `+executionOperationColumns,
			operationID,
			workerID,
			leaseDuration.Milliseconds(),
			maxAttempts,
		),
	)

	if err != nil {
		return ExecutionOperation{}, false, fmt.Errorf(
			"claim recovery operation: %w", err,
		)
	}

	// Step 3: Commit ownership before any provider network call.
	if err := tx.Commit(ctx); err != nil {
		return ExecutionOperation{}, false, fmt.Errorf(
			"commit recovery claim: %w", err,
		)
	}

	return operation, true, nil
}
