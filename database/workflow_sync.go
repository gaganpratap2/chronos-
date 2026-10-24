package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func SyncTaskWithJob(ctx context.Context, pool *pgxpool.Pool, jobID int64) error {
	var (
		taskID    int64
		jobState  string
		taskState string
	)

	err := pool.QueryRow(
		ctx,
		`SELECT 
			t.id ,
			j.status ,
			t.status
		FROM workflow_tasks t
		JOIN jobs j 
			ON j.id = t.job_id
		WHERE t.job_id = $1
		`,
		jobID,
	).Scan(
		&taskID,
		&jobState,
		&taskState,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf(
			"no workflow task found for job %d",
			jobID,
		)
	}

	if err != nil {
		return fmt.Errorf(
			"find task for job %d: %w",
			jobID,
			err,
		)
	}

	switch jobState {
	case "Running":
		if taskState == "Ready" {
			_, err = pool.Exec(
				ctx,
				`
			UPDATE workflow_tasks
			SET status = 'Running',
			    updated_at = NOW()
			WHERE id = $1
			  AND status = 'Ready'
			`,
				taskID,
			)
		}

	case "Completed":
		if taskState == "Ready" || taskState == "Running" {
			result, updateErr := pool.Exec(ctx, `
            UPDATE workflow_tasks
            SET status = 'Completed',
                updated_at = NOW()
            WHERE id = $1
        `, taskID)

			if updateErr != nil {
				err = updateErr
				break
			}

			if result.RowsAffected() != 1 {
				err = fmt.Errorf(
					"expected to update task %d, affected %d rows",
					taskID,
					result.RowsAffected(),
				)
			}
		}

	case "Failed":
		if taskState == "Ready" || taskState == "Running" {
			_, err = pool.Exec(
				ctx,
				`
			UPDATE workflow_tasks
			SET status = 'Failed',
			    updated_at = NOW()
			WHERE id = $1
			  AND status IN ('Ready', 'Running')
			`,
				taskID,
			)
		}
	}

	if err != nil {
		return fmt.Errorf(
			"sync task %d with job %d: %w",
			taskID,
			jobID,
			err,
		)
	}

	return nil
}

func SyncWorkflowTasksWithJobs(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowID int64,
) error {
	rows, err := pool.Query(
		ctx,
		`
		SELECT job_id
		FROM workflow_tasks
		WHERE workflow_id = $1
		  AND job_id IS NOT NULL
		`,
		workflowID,
	)
	if err != nil {
		return fmt.Errorf(
			"find jobs for workflow %d: %w",
			workflowID,
			err,
		)
	}
	defer rows.Close()

	for rows.Next() {
		var jobID int64

		if err := rows.Scan(&jobID); err != nil {
			return fmt.Errorf(
				"scan workflow %d job ID: %w",
				workflowID,
				err,
			)
		}

		if err := SyncTaskWithJob(
			ctx,
			pool,
			jobID,
		); err != nil {
			return fmt.Errorf(
				"sync workflow %d job %d: %w",
				workflowID,
				jobID,
				err,
			)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf(
			"iterate workflow %d jobs: %w",
			workflowID,
			err,
		)
	}

	return nil
}

func ReconcileWorkflow(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowID int64,
) error {
	if err := SyncWorkflowTasksWithJobs(
		ctx,
		pool,
		workflowID,
	); err != nil {
		return fmt.Errorf(
			"sync workflow %d tasks: %w",
			workflowID,
			err,
		)
	}

	if err := PropagateBlockedTasks(
		ctx,
		pool,
		workflowID,
	); err != nil {
		return fmt.Errorf(
			"propagate workflow %d failures: %w",
			workflowID,
			err,
		)
	}

	if err := UpdateWorkflowStatus(
		ctx,
		pool,
		workflowID,
	); err != nil {
		return fmt.Errorf(
			"update workflow %d status: %w",
			workflowID,
			err,
		)
	}

	return nil
}
