package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RunnableTask struct {
	ID         int64
	WorkflowID int64
	Name       string
}

func GetRunnableTasks(
	ctx context.Context,
	pool *pgxpool.Pool,
) ([]RunnableTask, error) {

	rows, err := pool.Query(
		ctx,
		`
		SELECT
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
		`,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get runnable tasks: %w",
			err,
		)
	}
	defer rows.Close()

	tasks := make([]RunnableTask, 0)

	for rows.Next() {
		var task RunnableTask

		if err := rows.Scan(
			&task.ID,
			&task.WorkflowID,
			&task.Name,
		); err != nil {
			return nil, fmt.Errorf(
				"scan runnable task: %w",
				err,
			)
		}

		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate runnable tasks: %w",
			err,
		)
	}

	return tasks, nil
}

func ClaimRunnableTask(ctx context.Context, pool *pgxpool.Pool) (*RunnableTask, error) {

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"begin task claim transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var task RunnableTask

	err = tx.QueryRow(
		ctx,
		`
	SELECT
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
		&task.ID,
		&task.WorkflowID,
		&task.Name,
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

	tag, err := tx.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET 
			status = 'Ready',
			updated_at = NOW()
		WHERE id = $1
			AND status = 'Pending'
		`,
		task.ID,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"claim task %d: %w",
			task.ID,
			err,
		)
	}

	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf(
			"claim task %d: task was already claimed",
			task.ID,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf(
			"commit task claim: %w",
			err,
		)
	}

	return &task, nil
}
