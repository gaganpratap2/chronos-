package database

import (
	"context"
	"errors"
	"fmt"

	"chronos/job"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DispatchedTask struct {
	TaskID     int64
	WorkflowID int64
	JobID      int64
	TaskName   string
}

func ClaimTaskAndCreateJob(
	ctx context.Context,
	pool *pgxpool.Pool,
) (*DispatchedTask, error) {
	if pool == nil {
		return nil, errors.New("database pool is nil")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"begin task dispatch transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	task, err := claimTaskAndCreateJobTx(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf(
			"commit task dispatch: %w",
			err,
		)
	}

	return task, nil
}

func claimTaskAndCreateJobTx(
	ctx context.Context,
	tx pgx.Tx,
) (*DispatchedTask, error) {
	var task DispatchedTask

	err := tx.QueryRow(
		ctx,
		`SELECT
			t.id,
			t.workflow_id,
			t.name
		FROM workflow_tasks t
		WHERE t.status = 'Pending'
		  AND NOT EXISTS (
			SELECT 1
			FROM task_dependencies d
			JOIN workflow_tasks dependency
			  ON dependency.id = d.depends_on_task_id
			WHERE d.task_id = t.id
			  AND dependency.status <> 'Completed'
		  )
		ORDER BY t.id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
		`,
	).Scan(
		&task.TaskID,
		&task.WorkflowID,
		&task.TaskName,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf(
			"find runnable task: %w",
			err,
		)
	}

	jobID, err := NextJobID(ctx, tx)
	if err != nil {
		return nil, err
	}

	executionJob, err := job.NewJob(
		jobID,
		task.TaskName,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create execution job: %w",
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO jobs (
			id,
			name,
			status,
			retry_count,
			max_retries,
			next_retry_at
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		`,
		executionJob.ID,
		executionJob.Name,
		executionJob.Status,
		executionJob.RetryCount,
		executionJob.MaxRetries,
		executionJob.NextRetryAt,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"save execution job: %w",
			err,
		)
	}

	tag, err := tx.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET
			status = 'Ready',
			job_id = $1,
			updated_at = NOW()
		WHERE id = $2
		  AND status = 'Pending'
		`,
		executionJob.ID,
		task.TaskID,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"attach execution job to task: %w",
			err,
		)
	}

	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf(
			"task %d was not updated",
			task.TaskID,
		)
	}

	task.JobID = int64(executionJob.ID)

	return &task, nil
}
