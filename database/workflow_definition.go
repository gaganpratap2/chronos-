package database

import (
	"context"
	"fmt"

	"chronos/workflow"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkflowDefinition struct {
	Workflow     workflow.Workflow
	Tasks        []workflow.Task
	Dependencies []workflow.Dependency
}

func CreateWorkflow(
	ctx context.Context,
	pool *pgxpool.Pool,
	definition WorkflowDefinition,
) error {

	if err := ValidateWorkflowDefinition(definition); err != nil {
		return fmt.Errorf("validate workflow definition: %w", err)
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin workflow transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflows (
			id,
			name,
			status
		)
		VALUES ($1, $2, $3)`,
		definition.Workflow.ID,
		definition.Workflow.Name,
		definition.Workflow.Status,
	)
	if err != nil {
		return fmt.Errorf(
			"insert workflow %d: %w",
			definition.Workflow.ID,
			err,
		)
	}

	for _, task := range definition.Tasks {
		_, err = tx.Exec(
			ctx,
			`INSERT INTO workflow_tasks (
				id,
				workflow_id,
				name,
				status
			)
			VALUES ($1, $2, $3, $4)`,
			task.ID,
			task.WorkflowID,
			task.Name,
			task.Status,
		)
		if err != nil {
			return fmt.Errorf(
				"insert task %d: %w",
				task.ID,
				err,
			)
		}
	}

	for _, dependency := range definition.Dependencies {
		_, err = tx.Exec(
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
				"insert dependency %d -> %d: %w",
				dependency.DependsOnTaskID,
				dependency.TaskID,
				err,
			)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit workflow %d: %w",
			definition.Workflow.ID,
			err,
		)
	}

	return nil
}

func ValidateWorkflowDefinition(definition WorkflowDefinition) error {
	if definition.Workflow.ID <= 0 {
		return workflow.ErrInvalidWorkflowID
	}

	if definition.Workflow.Name == "" {
		return workflow.ErrInvalidWorkflowName
	}

	taskIDs := make(map[int64]workflow.Task)

	for _, task := range definition.Tasks {
		if task.ID <= 0 {
			return workflow.ErrInvalidTaskID
		}
		if task.WorkflowID != definition.Workflow.ID {
			return fmt.Errorf(
				"task %d belongs to workflow %d, expected workflow %d",
				task.ID,
				task.WorkflowID,
				definition.Workflow.ID,
			)
		}

		if task.Name == "" {
			return workflow.ErrInvalidTaskName
		}

		if _, exists := taskIDs[task.ID]; exists {
			return fmt.Errorf(
				"duplicate task ID %d",
				task.ID,
			)
		}
		taskIDs[task.ID] = task
	}

	for _, dependency := range definition.Dependencies {
		if _, exists := taskIDs[dependency.TaskID]; !exists {
			return fmt.Errorf(
				"dependency task %d does not exist",
				dependency.TaskID,
			)
		}

		if _, exists := taskIDs[dependency.DependsOnTaskID]; !exists {
			return fmt.Errorf(
				"dependency task %d does not exist",
				dependency.DependsOnTaskID,
			)
		}

		if dependency.TaskID == dependency.DependsOnTaskID {
			return workflow.ErrSelfDependency
		}
	}

	if workflow.HasCycle(
		definition.Tasks,
		definition.Dependencies,
	) {
		return workflow.ErrCycleDetected
	}

	return nil
}
