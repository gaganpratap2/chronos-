package worker

import (
	"context"
	"testing"
	"time"

	"chronos/database"

	"github.com/joho/godotenv"
)

func TestWorkflowJobCanBeClaimedByWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}

	defer pool.Close()

	const (
    	workflowID = int64(999801)
    	taskID     = int64(999802)
	)

	_, err = pool.Exec(
		ctx, `DELETE FROM workflows WHERE id = $1`, workflowID,
	)

	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (id, name, status)
		VALUES ($1, $2, 'Pending')
		`,
		workflowID,
		"worker-integration-test",
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
		VALUES ($1, $2, $3, 'Pending')
		`,
		taskID,
		workflowID,
		"worker-test-task",
	)

	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	dispatched, err := database.ClaimTaskAndCreateJob(
		ctx,
		pool,
	)
	if err != nil {
		t.Fatalf("dispatch task: %v", err)
	}

	if dispatched == nil {
		t.Fatal("expected task to be dispatched")
	}

	if dispatched.TaskID != taskID {
		t.Fatalf(
			"expected task ID %d, got %d",
			taskID,
			dispatched.TaskID,
		)
	}

	if dispatched.JobID <= 0 {
		t.Fatalf(
			"expected valid job ID, got %d",
			dispatched.JobID,
		)
	}

	var jobStatus string

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM jobs
		WHERE id = $1
		`,
		dispatched.JobID,
	).Scan(&jobStatus)
	if err != nil {
		t.Fatalf("read job: %v", err)
	}

	if jobStatus != "Pending" {
		t.Fatalf(
			"expected job status Pending, got %q",
			jobStatus,
		)
	}

	var taskStatus string

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskID,
	).Scan(&taskStatus)
	if err != nil {
		t.Fatalf("read task: %v", err)
	}

	if taskStatus != "Ready" {
		t.Fatalf(
			"expected task status Ready, got %q",
			taskStatus,
		)
	}

	_, err = pool.Exec(
		ctx,
		`DELETE FROM jobs WHERE id = $1`,
		dispatched.JobID,
	)
	if err != nil {
		t.Fatalf("cleanup job: %v", err)
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
