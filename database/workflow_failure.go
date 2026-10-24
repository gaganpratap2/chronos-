package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func PropagateBlockedTasks(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowID int64,
) error {
	_, err := pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks t
		SET status = 'Blocked',
		    updated_at = NOW()
		WHERE t.workflow_id = $1
		  AND t.status = 'Pending'
		  AND EXISTS (
		      SELECT 1
		      FROM task_dependencies d
		      JOIN workflow_tasks dependency
		        ON dependency.id = d.depends_on_task_id
		      WHERE d.task_id = t.id
		        AND dependency.workflow_id = $1
		        AND dependency.status IN ('Failed', 'Blocked')
		  )
		`,
		workflowID,
	)

	if err != nil {
		return fmt.Errorf(
			"propagate blocked tasks for workflow %d: %w",
			workflowID,
			err,
		)
	}

	return nil
}
