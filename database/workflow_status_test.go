package database

import (
	"context"
	// "errors"
	"testing"
	"time"

	// "github.com/jackc/pgx/v5"

	// "chronos/workflow"

	"github.com/joho/godotenv"
)

func TestUpdateWorkflowStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const workflowID = int64(999601)

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (id, name, status)
		VALUES ($1, $2, 'Pending')
		`,
		workflowID,
		"workflow-status-test",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status
		)
		VALUES
			(999602, $1, 'task-a', 'Completed'),
			(999603, $1, 'task-b', 'Completed')
		`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("create tasks: %v", err)
	}

	err = UpdateWorkflowStatus(
		ctx,
		pool,
		workflowID,
	)
	if err != nil {
		t.Fatalf("update workflow status: %v", err)
	}

	var status string

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflows
		WHERE id = $1
		`,
		workflowID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read workflow status: %v", err)
	}

	if status != "Completed" {
		t.Fatalf(
			"expected Completed, got %q",
			status,
		)
	}

	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET status = 'Running'
		WHERE id = 999603
		`,
	)
	if err != nil {
		t.Fatalf("update task: %v", err)
	}

	err = UpdateWorkflowStatus(
		ctx,
		pool,
		workflowID,
	)
	if err != nil {
		t.Fatalf("update running workflow: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflows
		WHERE id = $1
		`,
		workflowID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read running workflow: %v", err)
	}

	if status != "Running" {
		t.Fatalf(
			"expected Running, got %q",
			status,
		)
	}

	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET status = 'Failed'
		WHERE id = 999603
		`,
	)
	if err != nil {
		t.Fatalf("fail task: %v", err)
	}

	err = UpdateWorkflowStatus(
		ctx,
		pool,
		workflowID,
	)
	if err != nil {
		t.Fatalf("update failed workflow: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflows
		WHERE id = $1
		`,
		workflowID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read failed workflow: %v", err)
	}

	if status != "Failed" {
		t.Fatalf(
			"expected Failed, got %q",
			status,
		)
	}

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup workflow: %v", err)
	}
}
