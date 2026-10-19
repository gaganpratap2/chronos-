package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"chronos/database"
	"chronos/job"

	"github.com/joho/godotenv"
)

type fakeHandler struct {
	err   error
	calls int
}

func (f *fakeHandler) Execute(ctx context.Context, j job.Job) error {
	f.calls++
	return f.err
}

var _ Handler = (*fakeHandler)(nil)

type concurrentHandler struct {
	start    <-chan struct{}
	finished chan<- struct{}
}

func (h *concurrentHandler) Execute(ctx context.Context, j job.Job) error {
	<-h.start

	time.Sleep(200 * time.Millisecond)

	h.finished <- struct{}{}

	return nil
}

var _ Handler = (*concurrentHandler)(nil)

func TestWorkerRejectsMissingDependencies(t *testing.T) {
	ctx := context.Background()

	w := &Worker{}

	ran, err := w.RunOnce(ctx)

	if err == nil {
		t.Fatal("expected error when database pool is nil")
	}

	if ran {
		t.Fatal("expected no job to run")
	}

	if err.Error() != "worker database pool is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFakeHandler(t *testing.T) {
	handler := &fakeHandler{}

	j := job.Job{
		ID:     999,
		Name:   "test-job",
		Status: "Running",
	}

	err := handler.Execute(context.Background(), j)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}
}

var errTestExecution = errors.New("test execution failed")

// Pending → Running → Handler succeeds → Completed
func TestWorkerExecutesJobSuccessfully(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	const testID = 2

	// Remove the test job before and after the test.
	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
	}()

	j, err := job.NewJob(testID, "worker-success-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := database.SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	handler := &fakeHandler{}

	w := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	ran, err := w.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	if !ran {
		t.Fatal("expected worker to run a job")
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}

	saved, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if saved.Status != "Completed" {
		t.Fatalf("expected database status Completed, got %s", saved.Status)
	}
}

// Pending → Running → Handler fails → Failed
func TestWorkerMarksJobFailedWhenHandlerFails(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	const testID = 3
	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
	}()

	j, err := job.NewJob(testID, "worker-failure-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := database.SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	handler := &fakeHandler{
		err: errTestExecution,
	}

	w := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	ran, err := w.RunOnce(ctx)

	// if err == nil {
	// 	t.Fatal("expected RunOnce to return an execution error")
	// }

	if err != nil {
		t.Fatalf(
			"expected retryable job failure to be handled without worker error, got: %v",
			err,
		)
	}

	if !ran {
		t.Fatal("expected worker to run a job")
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}

	saved, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if saved.Status != "Pending" {
		t.Fatalf("expected database status Pending, got %s", saved.Status)
	}

	if saved.RetryCount != 1 {
		t.Fatalf(
			"expected retry count 1, got %d",
			saved.RetryCount,
		)
	}

	if saved.NextRetryAt == nil {
		t.Fatal("expected NextRetryAt to be set")
	}
}

// TEST MULTILE JOB EXECUTION

// func TestWorkerRunProcessesMultipleJobs(t *testing.T) {
// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	// ctx, cancel := context.WithTimeout(
// 	// 	context.Background(),
// 	// 	5*time.Second,
// 	// )
// 	// defer cancel()

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	pool, err := database.Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	testIDs := []int{2, 3}

// 	for _, id := range testIDs {
// 		_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", id)
// 		if err != nil {
// 			t.Fatalf("failed to prepare test job %d: %v", id, err)
// 		}
// 	}

// 	defer func() {
// 		for _, id := range testIDs {
// 			_, _ = pool.Exec(
// 				context.Background(),
// 				"DELETE FROM jobs WHERE id = $1",
// 				id,
// 			)
// 		}
// 	}()

// 	for _, id := range testIDs {
// 		j, err := job.NewJob(id, "worker-loop-test")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		if err := database.SaveJob(ctx, pool, j); err != nil {
// 			t.Fatalf("SaveJob failed for job %d: %v", id, err)
// 		}
// 	}

// 	handler := &fakeHandler{}

// 	w := &Worker{
// 		Pool:    pool,
// 		Handler: handler,
// 	}

// 	err = w.Run(ctx)
// 	if err != nil {
// 		t.Fatalf("Run failed: %v", err)
// 	}

// 	if handler.calls != 2 {
// 		t.Fatalf("expected handler to be called twice, got %d", handler.calls)
// 	}

// 	for _, id := range testIDs {
// 		saved, err := database.GetJob(ctx, pool, id)
// 		if err != nil {
// 			t.Fatalf("GetJob failed for job %d: %v", id, err)
// 		}

// 		if saved.Status != "Completed" {
// 			t.Errorf(
// 				"expected job %d status Completed, got %s",
// 				id,
// 				saved.Status,
// 			)
// 		}
// 	}
// }

func TestWorkerRunProcessesMultipleJobs(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	testIDs := []int{2, 3}

	for _, id := range testIDs {
		_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", id)
		if err != nil {
			t.Fatalf("failed to prepare test job %d: %v", id, err)
		}
	}

	defer func() {
		for _, id := range testIDs {
			_, _ = pool.Exec(
				context.Background(),
				"DELETE FROM jobs WHERE id = $1",
				id,
			)
		}
	}()

	for _, id := range testIDs {
		j, err := job.NewJob(id, "worker-loop-test")
		if err != nil {
			t.Fatal(err)
		}

		if err := database.SaveJob(ctx, pool, j); err != nil {
			t.Fatalf("SaveJob failed for job %d: %v", id, err)
		}
	}

	handler := &fakeHandler{}

	w := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	runDone := make(chan error, 1)

	go func() {
		runDone <- w.Run(ctx)
	}()

	// Wait until both jobs have been executed.
	deadline := time.After(5 * time.Second)

	for handler.calls < 2 {
		select {
		case <-deadline:
			t.Fatalf(
				"worker did not process both jobs; calls=%d",
				handler.calls,
			)
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()

	err = <-runDone
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if handler.calls != 2 {
		t.Fatalf("expected handler to be called twice, got %d", handler.calls)
	}

	for _, id := range testIDs {
		saved, err := database.GetJob(
			context.Background(),
			pool,
			id,
		)
		if err != nil {
			t.Fatalf("GetJob failed for job %d: %v", id, err)
		}

		if saved.Status != "Completed" {
			t.Errorf(
				"expected job %d status Completed, got %s",
				id,
				saved.Status,
			)
		}
	}
}

func TestWorkerRunStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w := &Worker{}

	err := w.Run(ctx)

	if err == nil {
		t.Fatal("expected Run to return context cancellation error")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// Pending → Running → Failed → Pending → Running → Completed
func TestWorkerCompletesJobAfterRetry(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	defer pool.Close()

	const testID = 4
	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1", testID,
		)
	}()

	j, err := job.NewJob(testID, "worker-retry-lifecycle-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := database.SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	handler := &fakeHandler{
		err: errTestExecution,
	}

	w := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	ran, err := w.RunOnce(ctx)

	// if err == nil {
	// 	t.Fatal("expected first execution to fail")
	// }

	if err != nil {
		t.Fatalf(
			"expected retryable job failure to be handled without worker error, got: %v",
			err,
		)
	}

	if !ran {
		t.Fatal("expected worker to run the first attempt")
	}

	if handler.calls != 1 {
		t.Fatalf(
			"expected handler to be called once, got %d",
			handler.calls,
		)
	}

	saved, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if saved.Status != "Pending" {
		t.Fatalf(
			"expected status Pending after failure, got %s",
			saved.Status,
		)
	}

	if saved.RetryCount != 1 {
		t.Fatalf(
			"expected retry count 1, got %d",
			saved.RetryCount,
		)
	}

	if saved.NextRetryAt == nil {
		t.Fatal("expected NextRetryAt to be set")
	}

	// Make the retry immediately eligible.
	_, err = pool.Exec(
		ctx,
		"UPDATE jobs SET next_retry_at = NOW() WHERE id = $1",
		testID,
	)
	if err != nil {
		t.Fatalf("failed to make retry due: %v", err)
	}

	// Make the handler succeed on the retry.
	handler.err = nil

	// Second attempt should succeed.
	ran, err = w.RunOnce(ctx)

	if err != nil {
		t.Fatalf("expected retry execution to succeed: %v", err)
	}

	if !ran {
		t.Fatal("expected worker to run the retry")
	}

	if handler.calls != 2 {
		t.Fatalf(
			"expected handler to be called twice, got %d",
			handler.calls,
		)
	}

	saved, err = database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob after retry failed: %v", err)
	}

	if saved.Status != "Completed" {
		t.Fatalf(
			"expected final status Completed, got %s",
			saved.Status,
		)
	}

	if saved.RetryCount != 1 {
		t.Fatalf(
			"expected retry count to remain 1, got %d",
			saved.RetryCount,
		)
	}

	if saved.NextRetryAt != nil {
		t.Fatal("expected NextRetryAt to be cleared after completion")
	}

}

func TestWorkerRecoversRetryAfterRestart(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	const testID = 987663

	_, err = pool.Exec(ctx,
		"DELETE FROM jobs WHERE id = $1",
		testID,
	)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		"DELETE FROM jobs WHERE id = $1",
		testID,
	)

	j, err := job.NewJob(testID, "restart-recovery-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := database.SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	// Simulate the first worker failing the job.
	if _, err := database.TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Running",
	); err != nil {
		t.Fatalf("failed to mark job Running: %v", err)
	}

	if _, err := database.TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Failed",
	); err != nil {
		t.Fatalf("failed to mark job Failed: %v", err)
	}

	// Schedule the retry before the worker "crashes".
	policy := job.RetryPolicy{
		MaxRetries:    3,
		BaseDelay:     1 * time.Second,
		MaxDelay:      10 * time.Second,
		JitterPercent: 0,
	}

	if _, err := database.RetryJob(
		ctx,
		pool,
		testID,
		policy,
	); err != nil {
		t.Fatalf("RetryJob failed: %v", err)
	}

	// Simulate the retry becoming due while the worker is down.
	_, err = pool.Exec(
		ctx,
		"UPDATE jobs SET next_retry_at = NOW() WHERE id = $1",
		testID,
	)
	if err != nil {
		t.Fatalf("failed to make retry due: %v", err)
	}

	// Start a completely new worker.
	handler := &fakeHandler{}

	worker := &Worker{
		Pool:         pool,
		Handler:      handler,
		PollInterval: 10 * time.Millisecond,
		RetryPolicy:  policy,
	}

	ran, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("recovered worker failed: %v", err)
	}

	if !ran {
		t.Fatal("expected recovered worker to run the job")
	}

	if handler.calls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handler.calls)
	}

	saved, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if saved.Status != "Completed" {
		t.Fatalf(
			"expected recovered job to be Completed, got %s",
			saved.Status,
		)
	}

	if saved.RetryCount != 1 {
		t.Fatalf(
			"expected RetryCount 1, got %d",
			saved.RetryCount,
		)
	}

	if saved.NextRetryAt != nil {
		t.Fatal("expected NextRetryAt to be cleared after completion")
	}
}

func TestTwoWorkersClaimDifferentJobs(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	job1, err := job.NewJob(122, "workerA")
	if err != nil {
		t.Fatalf("create job1: %v", err)
	}

	job2, err := job.NewJob(123, "workerA")
	if err != nil {
		t.Fatalf("create job1: %v", err)
	}

	if err := database.SaveJob(ctx, pool, job1); err != nil {
		t.Fatalf("save job1: %v", err)
	}

	if err := database.SaveJob(ctx, pool, job2); err != nil {
		t.Fatalf("save job2: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(ctx,
			"DELETE FROM jobs WHERE id IN ($1, $2)",
			job1.ID, job2.ID,
		)
	}()

	workerA := &Worker{
		Pool:    pool,
		Handler: &fakeHandler{},
	}

	workerB := &Worker{
		Pool:    pool,
		Handler: &fakeHandler{},
	}

	claimedA, err := database.ClaimNextJob(ctx, workerA.Pool, "test-worker")
	if err != nil {
		t.Fatalf("worker A claim: %v", err)
	}

	claimedB, err := database.ClaimNextJob(ctx, workerB.Pool, "test-worker")
	if err != nil {
		t.Fatalf("worker B claim: %v", err)
	}

	if claimedA.ID == claimedB.ID {
		t.Fatalf(
			"both workers claimed the same job: %d",
			claimedA.ID,
		)
	}
}

func TestTwoWorkersClaimConcurrently(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	job1, err := job.NewJob(987666, "concurrent-worker-a")
	if err != nil {
		t.Fatalf("create job1: %v", err)
	}

	job2, err := job.NewJob(987667, "concurrent-worker-b")
	if err != nil {
		t.Fatalf("create job2: %v", err)
	}

	if err := database.SaveJob(ctx, pool, job1); err != nil {
		t.Fatalf("save job1: %v", err)
	}

	if err := database.SaveJob(ctx, pool, job2); err != nil {
		t.Fatalf("save job2: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(ctx,
			"DELETE FROM jobs WHERE id IN ($1, $2)",
			job1.ID, job2.ID,
		)
	}()

	type result struct {
		job job.Job
		err error
	}

	results := make(chan result, 2)

	go func() {
		j, err := database.ClaimNextJob(ctx, pool, "test-worker")
		results <- result{job: j, err: err}
	}()

	go func() {
		j, err := database.ClaimNextJob(ctx, pool, "test-worker")
		results <- result{job: j, err: err}
	}()

	first := <-results
	second := <-results

	if first.err != nil {
		t.Fatalf("first worker claim: %v", first.err)
	}

	if second.err != nil {
		t.Fatalf("second worker claim: %v", second.err)
	}

	if first.job.ID == second.job.ID {
		t.Fatalf(
			"both workers claimed the same job: %d",
			first.job.ID,
		)
	}
}

func TestTwoWorkersExecuteDifferentJobsConcurrently(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	job1, err := job.NewJob(987668, "concurrent-execution-a")
	if err != nil {
		t.Fatalf("create job1: %v", err)
	}

	job2, err := job.NewJob(987669, "concurrent-execution-b")
	if err != nil {
		t.Fatalf("create job2: %v", err)
	}

	if err := database.SaveJob(ctx, pool, job1); err != nil {
		t.Fatalf("save job1: %v", err)
	}

	if err := database.SaveJob(ctx, pool, job2); err != nil {
		t.Fatalf("save job2: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(ctx,
			"DELETE FROM jobs WHERE id IN ($1, $2)",
			job1.ID, job2.ID,
		)
	}()

	start := make(chan struct{})
	finished := make(chan struct{}, 2)

	handler := &concurrentHandler{
		start:    start,
		finished: finished,
	}

	workerA := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	workerB := &Worker{
		Pool:    pool,
		Handler: handler,
	}

	results := make(chan error, 2)

	go func() {
		_, err := workerA.RunOnce(ctx)
		results <- err
	}()

	go func() {
		_, err := workerB.RunOnce(ctx)
		results <- err
	}()

	close(start)

	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("worker returned error: %v", err)
		}
	}

	close(finished)
}
