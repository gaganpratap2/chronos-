package database

import (
	"context"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func TestClaimTaskAndCreateJob(t *testing.T) {
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
		workflowID = int64(999101)
		taskID     = int64(999102)
	)

	// Cleanup before test.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	// Create workflow.
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (
			id,
			name,
			status
		)
		VALUES ($1, $2, 'Pending')
		`,
		workflowID,
		"dispatch-test-workflow",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	// Create task.
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
		"dispatch-test-task",
	)
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	// Claim task and create execution job atomically.
	dispatched, err := ClaimTaskAndCreateJob(
		ctx,
		pool,
	)
	if err != nil {
		t.Fatalf(
			"claim task and create job: %v",
			err,
		)
	}

	if dispatched == nil {
		t.Fatal("expected dispatched task, got nil")
	}

	if dispatched.TaskID != taskID {
		t.Fatalf(
			"expected task ID %d, got %d",
			taskID,
			dispatched.TaskID,
		)
	}

	if dispatched.WorkflowID != workflowID {
		t.Fatalf(
			"expected workflow ID %d, got %d",
			workflowID,
			dispatched.WorkflowID,
		)
	}

	if dispatched.JobID <= 0 {
		t.Fatalf(
			"expected valid job ID, got %d",
			dispatched.JobID,
		)
	}

	if dispatched.TaskName != "dispatch-test-task" {
		t.Fatalf(
			"expected task name %q, got %q",
			"dispatch-test-task",
			dispatched.TaskName,
		)
	}

	// Verify task state.
	var (
		taskStatus string
		jobID      *int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT status, job_id
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskID,
	).Scan(
		&taskStatus,
		&jobID,
	)

	if err != nil {
		t.Fatalf("read task: %v", err)
	}

	if taskStatus != "Ready" {
		t.Fatalf(
			"expected task status Ready, got %q",
			taskStatus,
		)
	}

	if jobID == nil {
		t.Fatal("expected task to have job_id")
	}

	if *jobID != int64(dispatched.JobID) {
		t.Fatalf(
			"expected job ID %d, got %d",
			dispatched.JobID,
			*jobID,
		)
	}

	// Verify execution job.
	var (
		jobName   string
		jobStatus string
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT name, status
		FROM jobs
		WHERE id = $1
		`,
		dispatched.JobID,
	).Scan(
		&jobName,
		&jobStatus,
	)

	if err != nil {
		t.Fatalf("read execution job: %v", err)
	}

	if jobName != "dispatch-test-task" {
		t.Fatalf(
			"expected job name %q, got %q",
			"dispatch-test-task",
			jobName,
		)
	}

	if jobStatus != "Pending" {
		t.Fatalf(
			"expected job status Pending, got %q",
			jobStatus,
		)
	}

	// Cleanup workflow.
	// ON DELETE CASCADE removes the task.
	// Delete the job separately.
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

func TestClaimTaskAndCreateJobConcurrently(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const (
		workflowID = int64(999201)
		taskID1    = int64(999202)
		taskID2    = int64(999203)
	)

	// Cleanup before test.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	// Create workflow.
	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO workflows (
			id,
			name,
			status
		)
		VALUES ($1, $2, 'Pending')
		`,
		workflowID,
		"concurrent-dispatch-test",
	)
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	// Create two runnable tasks.
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
			($1, $2, 'task-one', 'Pending'),
			($3, $2, 'task-two', 'Pending')
		`,
		taskID1,
		workflowID,
		taskID2,
	)
	if err != nil {
		t.Fatalf("create tasks: %v", err)
	}

	type result struct {
		task *DispatchedTask
		err  error
	}

	results := make(chan result, 2)

	// Simulate two scheduler instances.
	go func() {
		task, err := ClaimTaskAndCreateJob(
			ctx,
			pool,
		)

		results <- result{
			task: task,
			err:  err,
		}
	}()

	go func() {
		task, err := ClaimTaskAndCreateJob(
			ctx,
			pool,
		)

		results <- result{
			task: task,
			err:  err,
		}
	}()

	first := <-results
	second := <-results

	if first.err != nil {
		t.Fatalf(
			"first scheduler failed: %v",
			first.err,
		)
	}

	if second.err != nil {
		t.Fatalf(
			"second scheduler failed: %v",
			second.err,
		)
	}

	if first.task == nil {
		t.Fatal("first scheduler returned nil task")
	}

	if second.task == nil {
		t.Fatal("second scheduler returned nil task")
	}

	// The two schedulers must receive different tasks.
	if first.task.TaskID == second.task.TaskID {
		t.Fatalf(
			"both schedulers claimed task %d",
			first.task.TaskID,
		)
	}

	// They must also receive different execution jobs.
	if first.task.JobID == second.task.JobID {
		t.Fatalf(
			"both schedulers created job %d",
			first.task.JobID,
		)
	}

	// Verify both tasks are Ready and have different job IDs.
	rows, err := pool.Query(
		ctx,
		`
		SELECT id, status, job_id
		FROM workflow_tasks
		WHERE workflow_id = $1
		ORDER BY id
		`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("read tasks: %v", err)
	}
	defer rows.Close()

	count := 0

	for rows.Next() {
		var (
			id     int64
			status string
			jobID  *int64
		)

		if err := rows.Scan(
			&id,
			&status,
			&jobID,
		); err != nil {
			t.Fatalf("scan task: %v", err)
		}

		if status != "Ready" {
			t.Fatalf(
				"task %d: expected Ready, got %q",
				id,
				status,
			)
		}

		if jobID == nil {
			t.Fatalf(
				"task %d has no job ID",
				id,
			)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tasks: %v", err)
	}

	if count != 2 {
		t.Fatalf(
			"expected 2 dispatched tasks, got %d",
			count,
		)
	}

	// Cleanup jobs created by this test.
	_, err = pool.Exec(
		ctx,
		`
		DELETE FROM jobs
		WHERE id IN (
			$1,
			$2
		)
		`,
		first.task.JobID,
		second.task.JobID,
	)
	if err != nil {
		t.Fatalf("cleanup jobs: %v", err)
	}

	// Cleanup workflow and tasks.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM workflows WHERE id = $1`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("cleanup workflow: %v", err)
	}
}
