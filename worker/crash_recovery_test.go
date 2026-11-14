package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"chronos/database"
	"chronos/job"

	"github.com/joho/godotenv"
)

const (
	crashTestChildEnv = "CHRONOS_CRASH_TEST_CHILD"
	crashTestJobEnv   = "CHRONOS_CRASH_TEST_JOB_ID"
)

type crashBlockingHandler struct{}

func (h *crashBlockingHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	// Deliberately remain active until the operating-system
	// process is forcibly terminated.
	select {}
}

type crashRecoveryHandler struct{}

func (h *crashRecoveryHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	return nil
}

func TestWorkerProcessCrashRecovery(t *testing.T) {
	// Child process: claim the job and block inside its handler.
	if os.Getenv(crashTestChildEnv) == "1" {
		runCrashRecoveryChild(t)
		return
	}

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		25*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	testID := int(time.Now().UnixNano()%900_000_000) + 1_000_000_000

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
	}()

	testJob, err := job.NewJob(testID, "process-crash-recovery-test")
	if err != nil {
		t.Fatalf("create test job: %v", err)
	}

	if err := database.SaveJob(ctx, pool, testJob); err != nil {
		t.Fatalf("save test job: %v", err)
	}

	// Make this test job the earliest eligible pending job.
	_, err = pool.Exec(ctx, `
		UPDATE jobs
		SET run_at = TIMESTAMP '0001-01-01 00:00:00',
		    next_retry_at = NULL
		WHERE id = $1
	`, testID)
	if err != nil {
		t.Fatalf("prioritize test job: %v", err)
	}

	// Start a separate copy of this test binary.
	child := exec.Command(
		os.Args[0],
		"-test.run=^TestWorkerProcessCrashRecovery$",
	)
	child.Env = append(
		os.Environ(),
		crashTestChildEnv+"=1",
		crashTestJobEnv+"="+strconv.Itoa(testID),
	)

	if err := child.Start(); err != nil {
		t.Fatalf("start child worker: %v", err)
	}

	// Always ensure the child is reaped.
	defer func() {
		if child.Process != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()

	// Wait until the child has actually claimed the job.
	var oldToken int64
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		var status string
		var workerID string

		err := pool.QueryRow(ctx, `
			SELECT status, worker_id, fencing_token
			FROM jobs
			WHERE id = $1
		`, testID).Scan(&status, &workerID, &oldToken)

		if err == nil && status == "Running" &&
			workerID == "crash-worker-A" {
			break
		}

		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for child worker to claim job")
		case <-time.After(100 * time.Millisecond):
		}
	}

	var claimedWorker string
	var claimedStatus string

	err = pool.QueryRow(ctx, `
		SELECT status, worker_id
		FROM jobs
		WHERE id = $1
	`, testID).Scan(&claimedStatus, &claimedWorker)

	if err != nil {
		t.Fatalf("verify child claim: %v", err)
	}

	if claimedStatus != "Running" || claimedWorker != "crash-worker-A" {
		t.Fatalf(
			"child did not claim job: status=%s worker=%s",
			claimedStatus,
			claimedWorker,
		)
	}

	if oldToken <= 0 {
		t.Fatalf("expected positive first fencing token, got %d", oldToken)
	}

	t.Logf("Worker A claimed job %d with token %d", testID, oldToken)

	// Abrupt termination: do not send a graceful shutdown signal.
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill child worker: %v", err)
	}

	if err := child.Wait(); err != nil {
		t.Logf("child process terminated: %v", err)
	}

	// Shorten only this test job's lease to avoid waiting 30 seconds.
	// Return the actual database lease expiration time.
	var leaseUntil time.Time

	err = pool.QueryRow(ctx, `
        UPDATE jobs
        SET lease_until = NOW() + INTERVAL '1 second'
        WHERE id = $1
          AND status = 'Running'
          AND fencing_token = $2
        RETURNING lease_until
`, testID, oldToken).Scan(&leaseUntil)

	if err != nil {
		t.Fatalf("shorten test lease: %v", err)
	}

	// Wait until PostgreSQL confirms that the lease has expired.
	expiryDeadline := time.Now().Add(5 * time.Second)
	var recoveryStarted time.Time

	for time.Now().Before(expiryDeadline) {
		var expired bool

		err := pool.QueryRow(ctx, `
                SELECT lease_until <= NOW()
                FROM jobs
                WHERE id = $1
        `, testID).Scan(&expired)

		if err != nil {
			t.Fatalf("check lease expiration: %v", err)
		}

		if expired {
			recoveryStarted = time.Now()
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if recoveryStarted.IsZero() {
		t.Fatalf("job lease did not expire; lease_until=%s", leaseUntil)
	}

	// Replacement worker claims and executes the recovered job.
	replacement := &Worker{
		WorkerID:     "crash-worker-B",
		Pool:         pool,
		Handler:      &crashRecoveryHandler{},
		PollInterval: 50 * time.Millisecond,
	}

	ran, err := replacement.RunOnce(ctx)
	if err != nil {
		t.Fatalf("replacement worker execution: %v", err)
	}
	if !ran {
		t.Fatal("replacement worker did not claim a job")
	}

	recoveryDuration := time.Since(recoveryStarted)

	t.Logf(
		"Crash recovery latency after lease expiration: %s",
		recoveryDuration,
	)

	stored, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("load recovered job: %v", err)
	}

	if stored.Status != "Completed" {
		t.Fatalf("expected Completed, got %s", stored.Status)
	}

	if stored.FencingToken <= oldToken {
		t.Fatalf(
			"expected newer fencing token than %d, got %d",
			oldToken,
			stored.FencingToken,
		)
	}

	t.Logf(
		"Worker B recovered job with token %d; status=%s",
		stored.FencingToken,
		stored.Status,
	)

	// Verify the old worker cannot change the recovered job.
	_, err = database.TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Failed",
		oldToken,
	)
	if err == nil {
		t.Fatal("expected stale worker update to be rejected")
	}

	finalJob, err := database.GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("reload final job: %v", err)
	}

	if finalJob.Status != "Completed" {
		t.Fatalf("stale worker changed final status to %s", finalJob.Status)
	}

	t.Log("stale Worker A update rejected; Worker B completion preserved")
}

func runCrashRecoveryChild(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	jobID, err := strconv.Atoi(os.Getenv(crashTestJobEnv))
	if err != nil {
		t.Fatalf("invalid child job ID: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Minute,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("child database connection: %v", err)
	}
	defer pool.Close()

	w := &Worker{
		WorkerID: "crash-worker-A",
		Pool:     pool,
		Handler:  &crashBlockingHandler{},
	}

	ran, err := w.RunOnce(ctx)
	if err != nil {
		t.Fatalf("child worker execution: %v", err)
	}

	if !ran {
		t.Fatalf("child worker did not claim job %d", jobID)
	}

	t.Fatal(fmt.Sprintf("blocking handler unexpectedly returned for job %d", jobID))
}
