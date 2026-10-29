package database_test

import (
	"context"
	"os"
	"testing"
	"time"

	"chronos/database"
	"chronos/job"
	"chronos/worker"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type workflowTestHandler struct {
	executedJobs []int
}

func (h *workflowTestHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	h.executedJobs = append(h.executedJobs, j.ID)
	return nil
}

func connectTestDatabase(
	ctx context.Context,
	t *testing.T,
) *pgxpool.Pool {
	t.Helper()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	databaseURL := os.Getenv("CHRONOS_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("CHRONOS_DATABASE_URL is not set")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}

	return pool
}

func TestWorkflowEndToEnd(t *testing.T) {
	ctx := context.Background()

	pool := connectTestDatabase(ctx, t)
	// defer pool.Close()

	const (
		workflowID int64 = 999950
		taskA      int64 = 999951
		taskB      int64 = 999952
		taskC      int64 = 999953
		taskD      int64 = 999954
	)

	// Clean up any data left by a previous test run.
	cleanup := func() {
		t.Helper()

		_, err := pool.Exec(
			context.Background(),
			`DELETE FROM workflows WHERE id = $1`,
			workflowID,
		)
		if err != nil {
			t.Errorf("cleanup workflow: %v", err)
		}
	}

	cleanup()
	// t.Cleanup(cleanup)
	t.Cleanup(func() {
		cleanup()
		pool.Close()
	})

	// ------------------------------------------------------------
	// Step 30.1 — Create workflow DAG
	// ------------------------------------------------------------

	_, err := pool.Exec(
		ctx,
		`
			INSERT INTO workflows (
				id,
				name,
				status
			)
			VALUES ($1, 'end-to-end-test', 'Pending')
			`,
		workflowID,
	)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	tasks := []struct {
		id   int64
		name string
	}{
		{taskA, "A"},
		{taskB, "B"},
		{taskC, "C"},
		{taskD, "D"},
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
				VALUES ($1, $2, $3, 'Pending')
				`,
			task.id,
			workflowID,
			task.name,
		)
		if err != nil {
			t.Fatalf(
				"insert task %s: %v",
				task.name,
				err,
			)
		}
	}

	// DAG:
	//
	//       A
	//      / \
	//     B   C
	//      \ /
	//       D
	//
	dependencies := [][2]int64{
		{taskB, taskA},
		{taskC, taskA},
		{taskD, taskB},
		{taskD, taskC},
	}

	for _, dependency := range dependencies {
		_, err = pool.Exec(
			ctx,
			`
				INSERT INTO task_dependencies (
					task_id,
					depends_on_task_id
				)
				VALUES ($1, $2)
				`,
			dependency[0],
			dependency[1],
		)
		if err != nil {
			t.Fatalf("insert dependency: %v", err)
		}
	}

	// ------------------------------------------------------------
	// Step 30.2 — Dispatch task A
	// ------------------------------------------------------------

	// A is the only task that should initially be runnable.
	dispatched, err := database.ClaimTaskAndCreateJob(
		ctx,
		pool,
	)
	if err != nil {
		t.Fatalf("dispatch task A: %v", err)
	}

	if dispatched == nil {
		t.Fatal("expected task A to be dispatched")
	}

	if dispatched.TaskID != taskA {
		t.Fatalf(
			"expected task A (%d), got task %d",
			taskA,
			dispatched.TaskID,
		)
	}

	if dispatched.JobID <= 0 {
		t.Fatalf(
			"expected a valid execution job ID, got %d",
			dispatched.JobID,
		)
	}

	var taskStatus string
	var jobStatus string

	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM workflow_tasks
			WHERE id = $1
			`,
		taskA,
	).Scan(&taskStatus)
	if err != nil {
		t.Fatalf("read task A status: %v", err)
	}

	if taskStatus != "Ready" {
		t.Fatalf(
			"expected task A to be Ready, got %s",
			taskStatus,
		)
	}

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
		t.Fatalf("read task A job status: %v", err)
	}

	if jobStatus != "Pending" {
		t.Fatalf(
			"expected task A job to be Pending, got %s",
			jobStatus,
		)
	}

	t.Log("workflow DAG created and task A dispatched successfully")

	// ------------------------------------------------------------
	// Step 30.3 — Execute task A through Worker
	// ------------------------------------------------------------

	// ------------------------------------------------------------
	// Step 30.3 — Execute task A through Worker
	// ------------------------------------------------------------

	// The worker claims jobs globally. For this test we temporarily
	// move unrelated Pending jobs out of the runnable state so that
	// the worker can deterministically claim task A's job.
	// Make this test-owned job the earliest eligible job.
	// Do not modify the status of unrelated jobs.
	_, err = pool.Exec(ctx, `
    UPDATE jobs
    SET run_at = COALESCE(
        (
            SELECT MIN(run_at) - INTERVAL '1 second'
            FROM jobs
            WHERE id <> $1
        ),
        NOW() - INTERVAL '1 minute'
    )
    WHERE id = $1
`, dispatched.JobID)
	if err != nil {
		t.Fatalf("position workflow test job for claiming: %v", err)
	}

	handler := &workflowTestHandler{}

	w := &worker.Worker{
		WorkerID:     "workflow-e2e-worker",
		Pool:         pool,
		Handler:      handler,
		PollInterval: 10 * time.Millisecond,
	}

	executed, err := w.RunOnce(ctx)
	if err != nil {
		t.Fatalf(
			"worker failed to execute task A job: %v",
			err,
		)
	}

	if !executed {
		t.Fatal("expected worker to execute a job")
	}

	var executedJobStatus string

	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM jobs
			WHERE id = $1
			`,
		dispatched.JobID,
	).Scan(&executedJobStatus)

	if err != nil {
		t.Fatalf(
			"read dispatched job status after worker: %v",
			err,
		)
	}

	t.Logf(
		"dispatched job %d status after worker: %s",
		dispatched.JobID,
		executedJobStatus,
	)

	if executedJobStatus != "Completed" {
		t.Fatalf(
			"expected dispatched job %d to be Completed, got %s",
			dispatched.JobID,
			executedJobStatus,
		)
	}

	var (
		debugTaskStatus string
		debugJobID      *int64
		debugJobStatus  string
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT status, job_id
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskA,
	).Scan(&debugTaskStatus, &debugJobID)
	if err != nil {
		t.Fatalf("read task A before sync: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
		SELECT status
		FROM jobs
		WHERE id = $1
		`,
		dispatched.JobID,
	).Scan(&debugJobStatus)
	if err != nil {
		t.Fatalf("read dispatched job before sync: %v", err)
	}

	t.Logf(
		"BEFORE SYNC: taskA status=%s job_id=%v, dispatched job=%d status=%s",
		debugTaskStatus,
		debugJobID,
		dispatched.JobID,
		debugJobStatus,
	)

	if err := database.SyncTaskWithJob(
		ctx,
		pool,
		dispatched.JobID,
	); err != nil {
		t.Fatalf("sync task A with completed job: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
		SELECT status, job_id
		FROM workflow_tasks
		WHERE id = $1
		`,
		taskA,
	).Scan(&debugTaskStatus, &debugJobID)
	if err != nil {
		t.Fatalf("read task A after sync: %v", err)
	}

	t.Logf(
		"AFTER SYNC: taskA status=%s job_id=%v",
		debugTaskStatus,
		debugJobID,
	)

	if debugTaskStatus != "Completed" {
		t.Fatalf(
			"expected task A to be Completed after sync, got %s",
			debugTaskStatus,
		)
	}

	if len(handler.executedJobs) != 1 {
		t.Fatalf(
			"expected worker handler to execute exactly one job, got %d",
			len(handler.executedJobs),
		)
	}

	if int64(handler.executedJobs[0]) != dispatched.JobID {
		t.Fatalf(
			"expected handler to execute job %d, got %d",
			dispatched.JobID,
			handler.executedJobs[0],
		)
	}

	// ------------------------------------------------------------
	// Step 30.4 — Verify A completion unlocks B and C
	// ------------------------------------------------------------

	var (
		statusA string
		statusB string
		statusC string
		statusD string
	)

	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM workflow_tasks
			WHERE id = $1
			`,
		taskA,
	).Scan(&statusA)
	if err != nil {
		t.Fatalf("read task A status before fan-out: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM workflow_tasks
			WHERE id = $1
			`,
		taskB,
	).Scan(&statusB)
	if err != nil {
		t.Fatalf("read task B status before fan-out: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM workflow_tasks
			WHERE id = $1
			`,
		taskC,
	).Scan(&statusC)
	if err != nil {
		t.Fatalf("read task C status before fan-out: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM workflow_tasks
			WHERE id = $1
			`,
		taskD,
	).Scan(&statusD)
	if err != nil {
		t.Fatalf("read task D status before fan-out: %v", err)
	}

	t.Logf(
		"task states before fan-out: A=%s B=%s C=%s D=%s",
		statusA,
		statusB,
		statusC,
		statusD,
	)

	var dependencyStatus string

	err = pool.QueryRow(
		ctx,
		`
			SELECT dependency.status
			FROM task_dependencies d
			JOIN workflow_tasks dependency
				ON dependency.id = d.depends_on_task_id
			WHERE d.task_id = $1
			`,
		taskB,
	).Scan(&dependencyStatus)
	if err != nil {
		t.Fatalf(
			"read task B dependency status: %v",
			err,
		)
	}

	t.Logf(
		"task B dependency status: %s",
		dependencyStatus,
	)

	// B should now be runnable.
	dispatchedB, err := database.ClaimTaskAndCreateJob(
		ctx,
		pool,
	)
	if err != nil {
		t.Fatalf("dispatch task B: %v", err)
	}

	if dispatchedB == nil {
		t.Fatal("expected task B to be dispatched")
	}

	if dispatchedB.TaskID != taskB {
		t.Fatalf(
			"expected task B (%d), got task %d",
			taskB,
			dispatchedB.TaskID,
		)
	}

	// C should also be runnable because it only depends on A.
	dispatchedC, err := database.ClaimTaskAndCreateJob(
		ctx,
		pool,
	)
	if err != nil {
		t.Fatalf("dispatch task C: %v", err)
	}

	if dispatchedC == nil {
		t.Fatal("expected task C to be dispatched")
	}

	if dispatchedC.TaskID != taskC {
		t.Fatalf(
			"expected task C (%d), got task %d",
			taskC,
			dispatchedC.TaskID,
		)
	}

	// D must not be runnable because B and C are not completed.
	dispatchedD, err := database.ClaimTaskAndCreateJob(
		ctx,
		pool,
	)
	if err != nil {
		t.Fatalf("check task D dispatch: %v", err)
	}

	if dispatchedD != nil {
		t.Fatalf(
			"expected task D to remain blocked, but it was dispatched as task %d",
			dispatchedD.TaskID,
		)
	}

	t.Log(
		"fan-out verified: B and C are runnable while D remains blocked",
	)

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
		t.Fatalf("read completed job status: %v", err)
	}

	if jobStatus != "Completed" {
		t.Fatalf(
			"expected execution job to be Completed, got %s",
			jobStatus,
		)
	}

	// ------------------------------------------------------------
	// Step 30.6 — Execute fan-out tasks B and C
	// ------------------------------------------------------------

	// The worker claims jobs globally. Isolate the two fan-out jobs
	// so RunOnce deterministically claims B and then C.
	_, err = pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'Completed'
		WHERE status = 'Pending'
		  AND id <> $1
		  AND id <> $2
	`, dispatchedB.JobID, dispatchedC.JobID)
	if err != nil {
		t.Fatalf(
			"failed to isolate fan-out jobs: %v",
			err,
		)
	}

	// Reset the handler history so we can verify B and C separately.
	handler.executedJobs = nil

	// Execute task B.
	executed, err = w.RunOnce(ctx)
	if err != nil {
		t.Fatalf(
			"failed to execute task B job %d: %v",
			dispatchedB.JobID,
			err,
		)
	}

	if !executed {
		t.Fatalf(
			"expected task B job %d to be executed",
			dispatchedB.JobID,
		)
	}

	// Execute task C.
	executed, err = w.RunOnce(ctx)
	if err != nil {
		t.Fatalf(
			"failed to execute task C job %d: %v",
			dispatchedC.JobID,
			err,
		)
	}

	if !executed {
		t.Fatalf(
			"expected task C job %d to be executed",
			dispatchedC.JobID,
		)
	}

	// Verify the worker executed exactly B and C.
	if len(handler.executedJobs) != 2 {
		t.Fatalf(
			"expected worker to execute exactly two fan-out jobs, got %d",
			len(handler.executedJobs),
		)
	}

	executedJobIDs := map[int]bool{
		handler.executedJobs[0]: true,
		handler.executedJobs[1]: true,
	}

	if !executedJobIDs[int(dispatchedB.JobID)] {
		t.Fatalf(
			"expected worker to execute task B job %d, executed jobs: %v",
			dispatchedB.JobID,
			handler.executedJobs,
		)
	}

	if !executedJobIDs[int(dispatchedC.JobID)] {
		t.Fatalf(
			"expected worker to execute task C job %d, executed jobs: %v",
			dispatchedC.JobID,
			handler.executedJobs,
		)
	}

	t.Logf(
		"fan-out jobs executed: B=%d C=%d",
		dispatchedB.JobID,
		dispatchedC.JobID,
	)

	// ------------------------------------------------------------
	// Step 30.7 — Synchronize B and C with completed jobs
	// ------------------------------------------------------------

	if err := database.SyncTaskWithJob(
		ctx,
		pool,
		dispatchedB.JobID,
	); err != nil {
		t.Fatalf(
			"sync task B with completed job: %v",
			err,
		)
	}

	if err := database.SyncTaskWithJob(
		ctx,
		pool,
		dispatchedC.JobID,
	); err != nil {
		t.Fatalf(
			"sync task C with completed job: %v",
			err,
		)
	}

	// Verify B is Completed.
	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM workflow_tasks
			WHERE id = $1
		`,
		taskB,
	).Scan(&statusB)
	if err != nil {
		t.Fatalf("read task B status after sync: %v", err)
	}

	if statusB != "Completed" {
		t.Fatalf(
			"expected task B to be Completed, got %s",
			statusB,
		)
	}

	// Verify C is Completed.
	err = pool.QueryRow(
		ctx,
		`
			SELECT status
			FROM workflow_tasks
			WHERE id = $1
		`,
		taskC,
	).Scan(&statusC)
	if err != nil {
		t.Fatalf("read task C status after sync: %v", err)
	}

	if statusC != "Completed" {
		t.Fatalf(
			"expected task C to be Completed, got %s",
			statusC,
		)
	}

	t.Log(
		"fan-out tasks B and C executed and synchronized successfully",
	)

	// ------------------------------------------------------------
	// Step 30.8 — Verify fan-in task D becomes runnable
	// ------------------------------------------------------------

	dispatchedD, err = database.ClaimTaskAndCreateJob(
		ctx,
		pool,
	)
	if err != nil {
		t.Fatalf("dispatch task D: %v", err)
	}

	if dispatchedD == nil {
		t.Fatal("expected task D to become runnable")
	}

	if dispatchedD.TaskID != taskD {
		t.Fatalf(
			"expected task D (%d), got task %d",
			taskD,
			dispatchedD.TaskID,
		)
	}

	if dispatchedD.JobID <= 0 {
		t.Fatalf(
			"expected a valid execution job ID for task D, got %d",
			dispatchedD.JobID,
		)
	}

	t.Logf(
		"fan-in verified: task D became runnable with job %d",
		dispatchedD.JobID,
	)
}
