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
	const fencingToken int64 = 0
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
		fencingToken,
	); err != nil {
		t.Fatalf("failed to mark job Running: %v", err)
	}

	if _, err := database.TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Failed",
		fencingToken,
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
		fencingToken,
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

func TestWorkerHeartbeatRenewsLease(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const jobID = 987665

	_, err = pool.Exec(ctx, `
		INSERT INTO jobs (
			id,
			name,
			status,
			retry_count,
			max_retries,
			worker_id,
			lease_until,
			fencing_token
		)
		VALUES ($1, 'heartbeat-test', 'Running', 0, 3, 'worker-heartbeat', NOW(), 1)
	`, jobID)
	if err != nil {
		t.Fatalf("insert heartbeat test job: %v", err)
	}
	defer pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, jobID)

	var before time.Time
	err = pool.QueryRow(ctx,
		`SELECT lease_until FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&before)
	if err != nil {
		t.Fatalf("read initial lease: %v", err)
	}

	err = database.RenewJobLease(ctx, pool, jobID, 1)
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}

	var after time.Time
	err = pool.QueryRow(ctx,
		`SELECT lease_until FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&after)
	if err != nil {
		t.Fatalf("read renewed lease: %v", err)
	}

	if !after.After(before) {
		t.Fatalf(
			"expected renewed lease to be later: before=%v after=%v",
			before,
			after,
		)
	}
}

func TestWorkerHeartbeatRenewsLeaseRepeatedly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const jobID = 987666

	_, err = pool.Exec(ctx, `
		DELETE FROM jobs
		WHERE id = $1
	`, jobID)
	if err != nil {
		t.Fatalf("cleanup existing test job: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO jobs (
			id,
			name,
			status,
			retry_count,
			max_retries,
			worker_id,
			lease_until,
			fencing_token
		)
		VALUES (
			$1,
			'heartbeat-loop-test',
			'Running',
			0,
			3,
			'heartbeat-worker',
			NOW(),
			1
		)
	`, jobID)
	if err != nil {
		t.Fatalf("insert heartbeat test job: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM jobs WHERE id = $1`,
		jobID,
	)

	w := &Worker{
		WorkerID:          "heartbeat-worker",
		Pool:              pool,
		HeartbeatInterval: 50 * time.Millisecond,
	}

	var before time.Time

	err = pool.QueryRow(
		ctx,
		`SELECT lease_until FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&before)
	if err != nil {
		t.Fatalf("read initial lease: %v", err)
	}

	heartbeatDone := make(chan struct{})

	go func() {
		defer close(heartbeatDone)
		w.heartbeat(ctx, jobID, 1)
	}()

	var after time.Time
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		err = pool.QueryRow(
			ctx,
			`SELECT lease_until
			 FROM jobs
			 WHERE id = $1`,
			jobID,
		).Scan(&after)

		if err == nil && after.After(before) {
			cancel()

			select {
			case <-heartbeatDone:
			case <-time.After(1 * time.Second):
				t.Fatal("heartbeat did not stop")
			}

			return
		}

		if err != nil {
			t.Fatalf("read renewed lease: %v", err)
		}

		time.Sleep(20 * time.Millisecond)
	}

	cancel()

	select {
	case <-heartbeatDone:
	case <-time.After(1 * time.Second):
		t.Fatal("heartbeat did not stop")
	}

	t.Fatalf(
		"expected heartbeat to renew lease: before=%v after=%v",
		before,
		after,
	)
}

func TestWorkerHeartbeatStopsOnCancellation(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const jobID = 987667

	_, err = pool.Exec(ctx, `
		INSERT INTO jobs (
			id,
			name,
			status,
			retry_count,
			max_retries,
			worker_id,
			lease_until,
			fencing_token
		)
		VALUES (
			$1,
			'heartbeat-cancel-test',
			'Running',
			0,
			3,
			'heartbeat-worker',
			NOW(),
			1
		)
	`, jobID)
	if err != nil {
		t.Fatalf("insert heartbeat test job: %v", err)
	}
	defer pool.Exec(context.Background(),
		`DELETE FROM jobs WHERE id = $1`,
		jobID,
	)

	w := &Worker{
		WorkerID:          "heartbeat-worker",
		Pool:              pool,
		HeartbeatInterval: 50 * time.Millisecond,
	}

	// go w.heartbeat(ctx, jobID, 1)
	// time.Sleep(100 * time.Millisecond)

	heartbeatDone := make(chan struct{})

	go func() {
		defer close(heartbeatDone)
		w.heartbeat(ctx, jobID, 1)
	}()

	time.Sleep(100 * time.Millisecond)

	var beforeCancel time.Time
	err = pool.QueryRow(
		context.Background(),
		`SELECT lease_until FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&beforeCancel)
	if err != nil {
		t.Fatalf("read lease before cancellation: %v", err)
	}

	// cancel()
	// time.Sleep(150 * time.Millisecond)

	cancel()

	select {
	case <-heartbeatDone:
	case <-time.After(1 * time.Second):
		t.Fatal("heartbeat did not stop after cancellation")
	}

	time.Sleep(100 * time.Millisecond)

	var afterCancel time.Time
	err = pool.QueryRow(
		context.Background(),
		`SELECT lease_until FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&afterCancel)
	if err != nil {
		t.Fatalf("read lease after cancellation: %v", err)
	}

	if !afterCancel.Equal(beforeCancel) {
		t.Fatalf(
			"expected lease to stop renewing after cancellation: before=%v after=%v",
			beforeCancel,
			afterCancel,
		)
	}
}

type heartbeatOwnershipHandler struct {
	started chan struct{}
}

func (h *heartbeatOwnershipHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	close(h.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestWorkerStopsWhenHeartbeatLosesOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	const jobID = 987668

	_, err = pool.Exec(ctx, `
		INSERT INTO jobs (
			id, name, status, retry_count, max_retries
		)
		VALUES ($1, 'heartbeat-loss-test', 'Pending', 0, 3)
	`, jobID)
	if err != nil {
		t.Fatalf("insert test job: %v", err)
	}

	defer pool.Exec(
		context.Background(),
		`DELETE FROM jobs WHERE id = $1`,
		jobID,
	)

	handler := &heartbeatOwnershipHandler{
		started: make(chan struct{}),
	}

	w := &Worker{
		WorkerID:          "worker-old-owner",
		Pool:              pool,
		Handler:           handler,
		HeartbeatInterval: 50 * time.Millisecond,
	}

	type outcome struct {
		processed bool
		err       error
	}

	resultCh := make(chan outcome, 1)

	go func() {
		processed, runErr := w.RunOnce(ctx)
		resultCh <- outcome{
			processed: processed,
			err:       runErr,
		}
	}()

	// Wait until the old worker is executing the job.
	select {
	case <-handler.started:
	case <-ctx.Done():
		t.Fatal("handler did not start before timeout")
	}

	// Simulate a new owner taking over the job.
	// Incrementing the token makes the old owner's token stale.
	_, err = pool.Exec(ctx, `
		UPDATE jobs
		SET worker_id = 'worker-new-owner',
		    fencing_token = fencing_token + 1,
		    lease_until = NOW() + INTERVAL '30 seconds'
		WHERE id = $1
	`, jobID)
	if err != nil {
		t.Fatalf("simulate ownership takeover: %v", err)
	}

	// The old worker should detect the stale token via heartbeat.
	select {
	case result := <-resultCh:
		if !result.processed {
			t.Fatal("expected worker to have claimed the job")
		}
		if result.err == nil {
			t.Fatal("expected heartbeat failure after ownership loss")
		}

	case <-ctx.Done():
		t.Fatal("worker did not stop after losing ownership")
	}

	// Verify the old worker did not mark the job Completed.
	var status string
	var workerID string
	var fencingToken int64

	err = pool.QueryRow(ctx, `
		SELECT status, worker_id, fencing_token
		FROM jobs
		WHERE id = $1
	`, jobID).Scan(&status, &workerID, &fencingToken)
	if err != nil {
		t.Fatalf("read job after ownership loss: %v", err)
	}

	if status != "Running" {
		t.Fatalf("expected status Running, got %q", status)
	}

	if workerID != "worker-new-owner" {
		t.Fatalf("expected new owner to remain recorded, got %q", workerID)
	}

	if fencingToken != 2 {
		t.Fatalf("expected fencing token 2, got %d", fencingToken)
	}
}
