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

func TestSyncTaskWithJob(t *testing.T) {
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
		workflowID = int64(999501)
		taskID     = int64(999502)
		jobID      = 999503
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
		VALUES ($1, $2, 'Pending')
		`,
		workflowID,
		"sync-test",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO jobs (
			id,
			name,
			status,
			retry_count,
			max_retries
		)
		VALUES ($1, $2, 'Running', 0, 3)
		`,
		jobID,
		"sync-job",
	)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflow_tasks (
			id,
			workflow_id,
			name,
			status,
			job_id
		)
		VALUES ($1, $2, $3, 'Ready', $4)
		`,
		taskID,
		workflowID,
		"sync-task",
		jobID,
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	err = SyncTaskWithJob(ctx, pool, jobID)
	if err != nil {
		t.Fatalf("sync running job: %v", err)
	}

	var status string

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read task status: %v", err)
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
		UPDATE jobs
		SET status = 'Completed'
		WHERE id = $1
		`,
		jobID,
	)
	if err != nil {
		t.Fatalf("complete job: %v", err)
	}

	err = SyncTaskWithJob(ctx, pool, jobID)
	if err != nil {
		t.Fatalf("sync completed job: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read completed task: %v", err)
	}

	if status != "Completed" {
		t.Fatalf(
			"expected Completed, got %q",
			status,
		)
	}

	_, err = pool.Exec(
		ctx,
		`DELETE FROM jobs WHERE id = $1`,
		jobID,
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
