package database

import (
	"context"
	"testing"

	"github.com/joho/godotenv"
)

func TestPropagateBlockedTasks(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	ctx := context.Background()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const (
		workflowID int64 = 999920
		taskA      int64 = 999921
		taskB      int64 = 999922
		taskC      int64 = 999923
	)

	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (id, name, status)
		VALUES ($1, 'failure-propagation-test', 'Running')
		`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)

	tasks := []struct {
		id     int64
		name   string
		status string
	}{
		{taskA, "A", "Completed"},
		{taskB, "B", "Failed"},
		{taskC, "C", "Pending"},
	}

	for _, task := range tasks {
		_, err = pool.Exec(
			ctx,
			`
			INSERT INTO workflow_tasks (
				id,
				workflow_id,
				name,
				status
			)
			VALUES ($1, $2, $3, $4)
			`,
			task.id,
			workflowID,
			task.name,
			task.status,
		)
		if err != nil {
			t.Fatalf(
				"insert task %s: %v",
				task.name,
				err,
			)
		}
	}

	// C depends on B.
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO task_dependencies (
			task_id,
			depends_on_task_id
		)
		VALUES ($1, $2)
		`,
		taskC,
		taskB,
	)
	if err != nil {
		t.Fatalf("insert dependency: %v", err)
	}

	if err := PropagateBlockedTasks(
		ctx,
		pool,
		workflowID,
	); err != nil {
		t.Fatalf(
			"propagate blocked tasks: %v",
			err,
		)
	}

	var status string

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskC,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read task C status: %v", err)
	}

	if status != "Blocked" {
		t.Fatalf(
			"expected task C to be Blocked, got %s",
			status,
		)
	}
}
