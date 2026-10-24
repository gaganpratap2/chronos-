package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func AreTaskDependenciesCompleted(
	ctx context.Context,
	pool *pgxpool.Pool,
	taskID int64,
) (bool, error) {
	var ready bool

	err := pool.QueryRow(
		ctx,
		`
		SELECT NOT EXISTS (
			SELECT 1
			FROM task_dependencies d
			JOIN workflow_tasks dependency
				ON dependency.id = d.depends_on_task_id
			WHERE d.task_id = $1
			  AND dependency.status <> 'Completed'
		)
		`,
		taskID,
	).Scan(&ready)

	if err != nil {
		return false, fmt.Errorf(
			"check task %d dependencies: %w",
			taskID,
			err,
		)
	}

	return ready, nil
}
