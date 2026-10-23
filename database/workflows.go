package database

import (
	"context"
	"fmt"

	"chronos/workflow"

	"github.com/jackc/pgx/v5/pgxpool"
)

func SaveWorkflow(
	ctx context.Context,
	pool *pgxpool.Pool,
	w workflow.Workflow,
) error {
	_, err := pool.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		w.ID,
		w.Name,
		w.Status,
	)
	if err != nil {
		return fmt.Errorf("save workflow %d: %w", w.ID, err)
	}

	return nil
}

func GetWorkflow(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int64,
) (workflow.Workflow, error) {
	var w workflow.Workflow

	err := pool.QueryRow(
		ctx,
		`SELECT id, name, status
		 FROM workflows
		 WHERE id = $1`,
		id,
	).Scan(
		&w.ID,
		&w.Name,
		&w.Status,
	)
	if err != nil {
		return workflow.Workflow{}, fmt.Errorf(
			"get workflow %d: %w",
			id,
			err,
		)
	}

	return w, nil
}

func SaveTask(
	ctx context.Context,
	pool *pgxpool.Pool,
	t workflow.Task,
) error {
	_, err := pool.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id,
    		workflow_id,
    		job_id,
    		name,
    		status,
    		created_at,
    		updated_at
		)
		VALUES ($1, $2, $3, $4 , $5, NOW() , NOW())`,
		t.ID,
		t.WorkflowID,
		t.JobID,
		t.Name,
		t.Status,
	)
	if err != nil {
		return fmt.Errorf(
			"save task %d: %w",
			t.ID,
			err,
		)
	}

	return nil
}

func GetTask(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int64,
) (workflow.Task, error) {
	var task workflow.Task

	err := pool.QueryRow(
		ctx,
		`SELECT id, workflow_id, name, status
		 FROM workflow_tasks
		 WHERE id = $1`,
		id,
	).Scan(
		&task.ID,
		&task.WorkflowID,
		&task.Name,
		&task.Status,
	)
	if err != nil {
		return workflow.Task{}, fmt.Errorf(
			"get task %d: %w",
			id,
			err,
		)
	}

	return task, nil
}

func SaveDependency(
	ctx context.Context,
	pool *pgxpool.Pool,
	dependency workflow.Dependency,
) error {
	_, err := pool.Exec(
		ctx,
		`INSERT INTO task_dependencies (
			task_id,
			depends_on_task_id
		)
		VALUES ($1, $2)`,
		dependency.TaskID,
		dependency.DependsOnTaskID,
	)
	if err != nil {
		return fmt.Errorf(
			"save dependency %d -> %d: %w",
			dependency.DependsOnTaskID,
			dependency.TaskID,
			err,
		)
	}

	return nil
}

func GetWorkflowTasks(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowID int64,
) ([]workflow.Task, error) {
	rows, err := pool.Query(
		ctx,
		`SELECT id, workflow_id, name, status
		 FROM workflow_tasks
		 WHERE workflow_id = $1
		 ORDER BY id`,
		workflowID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get tasks for workflow %d: %w",
			workflowID,
			err,
		)
	}
	defer rows.Close()

	tasks := make([]workflow.Task, 0)

	for rows.Next() {
		var task workflow.Task

		if err := rows.Scan(
			&task.ID,
			&task.WorkflowID,
			&task.Name,
			&task.Status,
		); err != nil {
			return nil, fmt.Errorf(
				"scan task for workflow %d: %w",
				workflowID,
				err,
			)
		}

		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate tasks for workflow %d: %w",
			workflowID,
			err,
		)
	}

	return tasks, nil
}

func GetWorkflowDependencies(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowID int64,
) ([]workflow.Dependency, error) {
	rows, err := pool.Query(
		ctx,
		`SELECT
			td.task_id,
			td.depends_on_task_id
		 FROM task_dependencies td
		 JOIN workflow_tasks t
			 ON t.id = td.task_id
		 WHERE t.workflow_id = $1
		 ORDER BY td.task_id, td.depends_on_task_id`,
		workflowID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get dependencies for workflow %d: %w",
			workflowID,
			err,
		)
	}
	defer rows.Close()

	dependencies := make([]workflow.Dependency, 0)

	for rows.Next() {
		var dependency workflow.Dependency

		if err := rows.Scan(
			&dependency.TaskID,
			&dependency.DependsOnTaskID,
		); err != nil {
			return nil, fmt.Errorf(
				"scan dependency for workflow %d: %w",
				workflowID,
				err,
			)
		}

		dependencies = append(
			dependencies,
			dependency,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate dependencies for workflow %d: %w",
			workflowID,
			err,
		)
	}

	return dependencies, nil
}
