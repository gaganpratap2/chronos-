package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNoWorkflowTaskForJob = errors.New(
		"no workflow task found for job",
	)

	ErrMultipleWorkflowTasksForJob = errors.New(
		"multiple workflow tasks found for job",
	)
)

// GetWorkflowTaskIDByJobID returns the workflow task linked
// to a particular execution job.
//
// A job can belong to one workflow task, or it can be a
// standalone job with no workflow task.
func GetWorkflowTaskIDByJobID(
	ctx context.Context,
	pool *pgxpool.Pool,
	jobID int,
) (int64, error) {
	if pool == nil {
		return 0, errors.New("database pool is nil")
	}

	if jobID <= 0 {
		return 0, errors.New("job ID must be positive")
	}

	rows, err := pool.Query(
		ctx,
		`
		SELECT id
		FROM workflow_tasks
		WHERE job_id = $1
		ORDER BY id
		LIMIT 2
		`,
		jobID,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"query workflow task for job %d: %w",
			jobID,
			err,
		)
	}
	defer rows.Close()

	// No workflow task is linked to this job.
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf(
				"read workflow task for job %d: %w",
				jobID,
				err,
			)
		}

		return 0, fmt.Errorf(
			"%w: job ID %d",
			ErrNoWorkflowTaskForJob,
			jobID,
		)
	}

	var taskID int64

	if err := rows.Scan(&taskID); err != nil {
		return 0, fmt.Errorf(
			"scan workflow task for job %d: %w",
			jobID,
			err,
		)
	}

	// One job must not silently resolve to multiple tasks.
	if rows.Next() {
		return 0, fmt.Errorf(
			"%w: job ID %d",
			ErrMultipleWorkflowTasksForJob,
			jobID,
		)
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf(
			"read workflow task for job %d: %w",
			jobID,
			err,
		)
	}

	return taskID, nil
}
