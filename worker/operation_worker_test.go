package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"chronos/database"
	"chronos/execution"
	"chronos/job"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type terminalOperationHandler struct{}

func (h *terminalOperationHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	return nil
}

func (h *terminalOperationHandler) ExecuteWithOperations(
	ctx context.Context,
	j job.Job,
	execContext OperationExecutionContext,
) error {
	return execution.ErrOperationFailed
}

var _ OperationAwareHandler = (*terminalOperationHandler)(nil)

type workerOperationFixture struct {
	ctx        context.Context
	cancel     context.CancelFunc
	pool       *pgxpool.Pool
	workflowID int64
	taskID     int64
	jobID      int
}

func newWorkerOperationFixture(t *testing.T) *workerOperationFixture {
	t.Helper()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)

	pool, err := database.Connect(ctx)
	if err != nil {
		cancel()
		t.Fatalf("connect database: %v", err)
	}

	baseID := time.Now().UnixNano()

	workflowID := baseID
	taskID := baseID + 1
	jobID := int(baseID%700_000_000) + 1_000_000_000

	tx, err := pool.Begin(ctx)
	if err != nil {
		pool.Close()
		cancel()
		t.Fatalf("begin fixture transaction: %v", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflows (id, name, status)
		 VALUES ($1, $2, $3)`,
		workflowID,
		"worker-operation-test",
		"Running",
	)
	if err != nil {
		pool.Close()
		cancel()
		t.Fatalf("insert workflow: %v", err)
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO jobs (
			id, name, status, retry_count,
			max_retries, run_at, fencing_token
		)
		VALUES (
			$1, $2, 'Pending', 0, 3,
			NOW() - INTERVAL '1 minute', 0
		)`,
		jobID,
		"worker-operation-test",
	)
	if err != nil {
		pool.Close()
		cancel()
		t.Fatalf("insert job: %v", err)
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO workflow_tasks (
			id, workflow_id, job_id, name,
			status, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, 'Ready', NOW(), NOW())`,
		taskID,
		workflowID,
		jobID,
		"worker-operation-test",
	)
	if err != nil {
		pool.Close()
		cancel()
		t.Fatalf("insert workflow task: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		pool.Close()
		cancel()
		t.Fatalf("commit fixture: %v", err)
	}

	fixture := &workerOperationFixture{
		ctx:        ctx,
		cancel:     cancel,
		pool:       pool,
		workflowID: workflowID,
		taskID:     taskID,
		jobID:      jobID,
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := pool.Exec(
			cleanupCtx,
			"DELETE FROM workflow_tasks WHERE id = $1",
			taskID,
		)
		if err != nil {
			t.Errorf("cleanup workflow task: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("cleanup job: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			"DELETE FROM workflows WHERE id = $1",
			workflowID,
		)
		if err != nil {
			t.Errorf("cleanup workflow: %v", err)
		}

		pool.Close()
		cancel()
	})

	return fixture
}

type testOperationExecutor struct{}

func (e *testOperationExecutor) Execute(
	ctx context.Context,
	request execution.OperationRequest,
) (execution.OperationResult, error) {
	return execution.OperationResult{}, nil
}

type capturingOperationHandler struct {
	operationCalls int
	legacyCalls    int

	receivedJob     job.Job
	receivedContext OperationExecutionContext
}

// func (h *capturingOperationHandler) Execute(
// 	ctx context.Context,
// 	j job.Job,
// ) error {
// 	h.legacyCalls++
// 	return fmt.Errorf("legacy Execute should not be called")
// }

func (h *capturingOperationHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	h.legacyCalls++
	return nil
}

func (h *capturingOperationHandler) ExecuteWithOperations(
	ctx context.Context,
	j job.Job,
	execContext OperationExecutionContext,
) error {
	h.operationCalls++
	h.receivedJob = j
	h.receivedContext = execContext

	return nil
}

func TestWorkerPassesOperationContextToHandler(t *testing.T) {
	f := newWorkerOperationFixture(t)

	handler := &capturingOperationHandler{}
	executor := &testOperationExecutor{}

	w := &Worker{
		WorkerID:          "operation-test-worker",
		Pool:              f.pool,
		Handler:           handler,
		OperationExecutor: executor,
	}

	claimSpecificJob := func(
		ctx context.Context,
	) (job.Job, error) {
		var claimed job.Job

		err := f.pool.QueryRow(
			ctx,
			`UPDATE jobs
			 SET status = 'Running',
			     worker_id = $2,
			     lease_until = NOW() + INTERVAL '30 seconds',
			     fencing_token = fencing_token + 1
			 WHERE id = $1
			   AND status = 'Pending'
			 RETURNING
			     id,
			     name,
			     status,
			     run_at,
			     retry_count,
			     max_retries,
			     next_retry_at,
			     worker_id,
			     lease_until,
			     fencing_token`,
			f.jobID,
			w.WorkerID,
		).Scan(
			&claimed.ID,
			&claimed.Name,
			&claimed.Status,
			&claimed.RunAt,
			&claimed.RetryCount,
			&claimed.MaxRetries,
			&claimed.NextRetryAt,
			&claimed.WorkerID,
			&claimed.LeaseUntil,
			&claimed.FencingToken,
		)

		if err != nil {
			return job.Job{}, fmt.Errorf("claim test job: %w", err)
		}

		return claimed, nil
	}

	ran, err := w.runOnce(f.ctx, claimSpecificJob)
	if err != nil {
		t.Fatalf("worker runOnce: %v", err)
	}

	if !ran {
		t.Fatal("expected worker to process the job")
	}

	if handler.operationCalls != 1 {
		t.Fatalf(
			"expected ExecuteWithOperations once, got %d",
			handler.operationCalls,
		)
	}

	if handler.legacyCalls != 0 {
		t.Fatalf(
			"expected legacy Execute not to be called, got %d calls",
			handler.legacyCalls,
		)
	}

	if handler.receivedJob.ID != f.jobID {
		t.Fatalf(
			"expected job ID %d, got %d",
			f.jobID,
			handler.receivedJob.ID,
		)
	}

	if handler.receivedContext.WorkflowTaskID != f.taskID {
		t.Fatalf(
			"expected workflow task ID %d, got %d",
			f.taskID,
			handler.receivedContext.WorkflowTaskID,
		)
	}

	if handler.receivedContext.FencingToken != handler.receivedJob.FencingToken {
		t.Fatalf(
			"expected context fencing token %d, got %d",
			handler.receivedJob.FencingToken,
			handler.receivedContext.FencingToken,
		)
	}

	if handler.receivedContext.FencingToken != 1 {
		t.Fatalf(
			"expected first fencing token 1, got %d",
			handler.receivedContext.FencingToken,
		)
	}

	if handler.receivedContext.Operations != executor {
		t.Fatal("worker did not pass the configured operation executor")
	}

	saved, err := database.GetJob(f.ctx, f.pool, f.jobID)
	if err != nil {
		t.Fatalf("get completed job: %v", err)
	}

	if saved.Status != "Completed" {
		t.Fatalf(
			"expected job status Completed, got %s",
			saved.Status,
		)
	}
}

func TestWorkerDoesNotRetryDefinitiveOperationFailure(t *testing.T) {
	f := newWorkerOperationFixture(t)

	w := &Worker{
		WorkerID:          "terminal-failure-worker",
		Pool:              f.pool,
		Handler:           &terminalOperationHandler{},
		OperationExecutor: &testOperationExecutor{},
		RetryPolicy: job.RetryPolicy{
			MaxRetries: 3,
			BaseDelay:  time.Second,
			MaxDelay:   10 * time.Second,
		},
	}

	claimSpecificJob := func(
		ctx context.Context,
	) (job.Job, error) {
		var claimed job.Job

		err := f.pool.QueryRow(
			ctx,
			`UPDATE jobs
			 SET status = 'Running',
			     worker_id = $2,
			     lease_until = NOW() + INTERVAL '30 seconds',
			     fencing_token = fencing_token + 1
			 WHERE id = $1
			   AND status = 'Pending'
			 RETURNING
			     id, name, status, run_at, retry_count,
			     max_retries, next_retry_at, worker_id,
			     lease_until, fencing_token`,
			f.jobID,
			w.WorkerID,
		).Scan(
			&claimed.ID,
			&claimed.Name,
			&claimed.Status,
			&claimed.RunAt,
			&claimed.RetryCount,
			&claimed.MaxRetries,
			&claimed.NextRetryAt,
			&claimed.WorkerID,
			&claimed.LeaseUntil,
			&claimed.FencingToken,
		)

		return claimed, err
	}

	ran, err := w.runOnce(f.ctx, claimSpecificJob)
	if err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	if !ran {
		t.Fatal("expected worker to process the job")
	}

	saved, err := database.GetJob(f.ctx, f.pool, f.jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if saved.Status != "Failed" {
		t.Fatalf(
			"expected Failed status, got %s",
			saved.Status,
		)
	}

	if saved.RetryCount != 0 {
		t.Fatalf(
			"expected zero retries, got %d",
			saved.RetryCount,
		)
	}

	if saved.NextRetryAt != nil {
		t.Fatal("expected no scheduled retry")
	}
}

func TestWorkerUsesLegacyHandlerForStandaloneJob(t *testing.T) {
	f := newWorkerOperationFixture(t)

	// Remove the workflow-task relationship. The job itself remains.
	_, err := f.pool.Exec(
		f.ctx,
		"DELETE FROM workflow_tasks WHERE id = $1",
		f.taskID,
	)
	if err != nil {
		t.Fatalf("remove workflow task: %v", err)
	}

	handler := &capturingOperationHandler{}
	executor := &testOperationExecutor{}

	w := &Worker{
		WorkerID:          "standalone-test-worker",
		Pool:              f.pool,
		Handler:           handler,
		OperationExecutor: executor,
	}

	claimSpecificJob := func(ctx context.Context) (job.Job, error) {
		var claimed job.Job

		err := f.pool.QueryRow(
			ctx,
			`UPDATE jobs
			 SET status = 'Running',
			     worker_id = $2,
			     lease_until = NOW() + INTERVAL '30 seconds',
			     fencing_token = fencing_token + 1
			 WHERE id = $1
			   AND status = 'Pending'
			 RETURNING id, name, status, run_at, retry_count,
			           max_retries, next_retry_at, worker_id,
			           lease_until, fencing_token`,
			f.jobID,
			w.WorkerID,
		).Scan(
			&claimed.ID,
			&claimed.Name,
			&claimed.Status,
			&claimed.RunAt,
			&claimed.RetryCount,
			&claimed.MaxRetries,
			&claimed.NextRetryAt,
			&claimed.WorkerID,
			&claimed.LeaseUntil,
			&claimed.FencingToken,
		)

		return claimed, err
	}

	ran, err := w.runOnce(f.ctx, claimSpecificJob)
	if err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	if !ran {
		t.Fatal("expected worker to process standalone job")
	}

	if handler.legacyCalls != 1 {
		t.Fatalf(
			"expected legacy Execute once, got %d",
			handler.legacyCalls,
		)
	}

	if handler.operationCalls != 0 {
		t.Fatalf(
			"expected ExecuteWithOperations zero times, got %d",
			handler.operationCalls,
		)
	}

	saved, err := database.GetJob(f.ctx, f.pool, f.jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if saved.Status != "Completed" {
		t.Fatalf("expected Completed, got %s", saved.Status)
	}
}
