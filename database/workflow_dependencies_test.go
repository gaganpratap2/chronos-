package database

import (
	"context"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func TestAreTaskDependenciesCompleted(t *testing.T) {
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

	const (
		workflowID = int64(999701)
		taskA      = int64(999702)
		taskB      = int64(999703)
	)

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
		VALUES ($1, $2, 'Running')
		`,
		workflowID,
		"dependency-test",
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
			($1, $3, 'task-a', 'Pending'),
			($2, $3, 'task-b', 'Pending')
		`,
		taskA,
		taskB,
		workflowID,
	)
	if err != nil {
		t.Fatalf("create tasks: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO task_dependencies (
			task_id,
			depends_on_task_id
		)
		VALUES ($1, $2)
		`,
		taskB,
		taskA,
	)
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	ready, err := AreTaskDependenciesCompleted(
		ctx,
		pool,
		taskB,
	)
	if err != nil {
		t.Fatalf("check dependencies: %v", err)
	}

	if ready {
		t.Fatal("task B should not be ready while task A is pending")
	}

	_, err = pool.Exec(
		ctx,
		`
		UPDATE workflow_tasks
		SET status = 'Completed'
		WHERE id = $1
		`,
		taskA,
	)
	if err != nil {
		t.Fatalf("complete task A: %v", err)
	}

	ready, err = AreTaskDependenciesCompleted(
		ctx,
		pool,
		taskB,
	)
	if err != nil {
		t.Fatalf("check dependencies after completion: %v", err)
	}

	if !ready {
		t.Fatal("task B should be ready after task A completes")
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
