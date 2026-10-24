package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func UpdateWorkflowStatus(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowID int64,
) error {
	var (
		currentStatus string
	)

	err := pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflows
		WHERE id = $1
		`,
		workflowID,
	).Scan(&currentStatus)

	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf(
			"workflow %d not found",
			workflowID,
		)
	}

	if err != nil {
		return fmt.Errorf(
			"read workflow %d status: %w",
			workflowID,
			err,
		)
	}

	var (
		totalTasks     int
		completedTasks int
		failedTasks    int
		blockedTasks   int
		runningTasks   int
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'Completed'),
			COUNT(*) FILTER (WHERE status = 'Failed'),
			COUNT(*) FILTER (WHERE status = 'Blocked'),
			COUNT(*) FILTER (WHERE status = 'Running')
		FROM workflow_tasks
		WHERE workflow_id = $1
		`,
		workflowID,
	).Scan(
		&totalTasks,
		&completedTasks,
		&failedTasks,
		&blockedTasks,
		&runningTasks,
	)

	if err != nil {
		return fmt.Errorf(
			"read workflow %d task states: %w",
			workflowID,
			err,
		)
	}

	var nextStatus string

	switch {
	case failedTasks > 0 || blockedTasks > 0:
		nextStatus = "Failed"

	case totalTasks > 0 && completedTasks == totalTasks:
		nextStatus = "Completed"

	case runningTasks > 0 || completedTasks > 0:
		nextStatus = "Running"

	default:
		nextStatus = "Pending"
	}

	if currentStatus == nextStatus {
		return nil
	}

	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflows
		SET status = $1,
		    updated_at = NOW()
		WHERE id = $2
		`,
		nextStatus,
		workflowID,
	)

	if err != nil {
		return fmt.Errorf(
			"update workflow %d status: %w",
			workflowID,
			err,
		)
	}

	return nil
}
