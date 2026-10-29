package database

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"

	"chronos/job"
)

func TestSaveAndGetJob(t *testing.T) {

	// runAt := time.Now().Truncate(time.Microsecond)

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Use a unique ID so we don't overwrite existing data.
	const testID = 987654

	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}
	defer pool.Exec(context.Background(),
		"DELETE FROM jobs WHERE id = $1", testID)

	original, err := job.NewJob(testID, "automated-test")
	if err != nil {
		t.Fatal(err)
	}

	original.RetryCount = 2
	original.MaxRetries = 5

	if err := SaveJob(ctx, pool, original); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	retrieved, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if !retrieved.RunAt.Equal(original.RunAt.Truncate(time.Microsecond)) {
		t.Fatalf(
			"expected RunAt %v, got %v",
			original.RunAt,
			retrieved.RunAt,
		)
	}

	if retrieved.RetryCount != original.RetryCount {
		t.Errorf("expected RetryCount %d, got %d",
			original.RetryCount, retrieved.RetryCount)
	}

	if retrieved.MaxRetries != original.MaxRetries {
		t.Errorf("expected MaxRetries %d, got %d",
			original.MaxRetries, retrieved.MaxRetries)
	}

	// if original.RetryCount != 2 {
	// 	t.Errorf("expected RetryCount 2, got %d", original.RetryCount)
	// }

	// if original.MaxRetries != 5 {
	// 	t.Errorf("expected MaxRetries 5, got %d", original.MaxRetries)
	// }

	if retrieved.ID != original.ID {
		t.Errorf("expected ID %d, got %d", original.ID, retrieved.ID)
	}

	if retrieved.Name != original.Name {
		t.Errorf("expected name %q, got %q", original.Name, retrieved.Name)
	}

	if retrieved.Status != original.Status {
		t.Errorf("expected status %q, got %q",
			original.Status, retrieved.Status)
	}
}

func TestRetryJob(t *testing.T) {

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancle := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	defer cancle()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	j, err := job.NewJob(987657, "retry-test")
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the test row is removed even if an assertion fails.
	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", j.ID)
	}()

	j.Status = "Failed"
	j.RetryCount = 0
	j.MaxRetries = 3

	_, err = pool.Exec(ctx,
		`INSERT INTO jobs
         (id, name, status, retry_count, max_retries)
         VALUES ($1, $2, $3, $4, $5)`,
		j.ID, j.Name, j.Status, j.RetryCount, j.MaxRetries,
	)
	if err != nil {
		t.Fatal(err)
	}

	beforeRetry := time.Now()

	got, err := RetryJob(
		ctx,
		pool,
		j.ID,
		job.RetryPolicy{
			MaxRetries:    3,
			BaseDelay:     1 * time.Second,
			MaxDelay:      10 * time.Second,
			JitterPercent: 0.20,
		},
		0,
	)
	if err != nil {
		t.Fatalf("RetryJob returned error: %v", err)
	}

	if got.Status != "Pending" {
		t.Errorf("expected Pending, got %s", got.Status)
	}

	if got.RetryCount != 1 {
		t.Errorf("expected RetryCount 1, got %d", got.RetryCount)
	}

	if got.NextRetryAt == nil {
		t.Fatal("expected NextRetryAt to be set")
	}

	if got.NextRetryAt.Before(time.Now()) {
		t.Error("expected NextRetryAt to be in the future")
	}

	// Verify the values were persisted in PostgreSQL.
	saved, err := GetJob(ctx, pool, j.ID)
	if err != nil {
		t.Fatal(err)
	}

	if saved.Status != "Pending" || saved.RetryCount != 1 {
		t.Errorf("unexpected saved job: status=%s retry_count=%d",
			saved.Status, saved.RetryCount)
	}

	if saved.NextRetryAt == nil {
		t.Fatal("expected saved NextRetryAt to be set")
	}

	// remaining := time.Until(got.NextRetryAt.UTC())

	delay := got.NextRetryAt.Sub(beforeRetry)

	if delay < 800*time.Millisecond || delay > 1200*time.Millisecond {
		t.Fatalf("expected retry delay around 1s, got %v", delay)
	}
}

func TestRetryIncrementsCount(t *testing.T) {
	j, err := job.NewJob(201, "payment")
	if err != nil {
		t.Fatal(err)
	}

	if err := j.TransitionTo("Running"); err != nil {
		t.Fatal(err)
	}
	if err := j.TransitionTo("Failed"); err != nil {
		t.Fatal(err)
	}

	if err := j.Retry(); err != nil {
		t.Fatalf("expected retry to succeed: %v", err)
	}

	if j.RetryCount != 1 {
		t.Errorf("expected RetryCount 1, got %d", j.RetryCount)
	}
	if j.Status != "Pending" {
		t.Errorf("expected Pending, got %s", j.Status)
	}
}

func TestRetryRejectsNonFailedJob(t *testing.T) {
	j, err := job.NewJob(202, "email")
	if err != nil {
		t.Fatal(err)
	}

	err = j.Retry()

	if err == nil {
		t.Fatal("expected retry to fail for a Pending job")
	}

	if j.RetryCount != 0 {
		t.Errorf("expected RetryCount 0, got %d", j.RetryCount)
	}
}

func TestRetryLimit(t *testing.T) {
	j, err := job.NewJob(203, "report")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < j.MaxRetries; i++ {
		if err := j.TransitionTo("Running"); err != nil {
			t.Fatal(err)
		}
		if err := j.TransitionTo("Failed"); err != nil {
			t.Fatal(err)
		}
		if err := j.Retry(); err != nil {
			t.Fatalf("retry %d should succeed: %v", i+1, err)
		}
	}

	if j.RetryCount != j.MaxRetries {
		t.Fatalf("expected %d retries, got %d",
			j.MaxRetries, j.RetryCount)
	}

	// Fail once more after the retry limit has been reached.
	if err := j.TransitionTo("Running"); err != nil {
		t.Fatal(err)
	}
	if err := j.TransitionTo("Failed"); err != nil {
		t.Fatal(err)
	}

	if err := j.Retry(); err == nil {
		t.Fatal("expected retry to fail after reaching the limit")
	}

	if j.RetryCount != j.MaxRetries {
		t.Errorf("retry count should remain %d, got %d",
			j.MaxRetries, j.RetryCount)
	}
	if j.Status != "Failed" {
		t.Errorf("expected Failed after exhausted retries, got %s",
			j.Status)
	}
}

func TestRetryJobRejectsNonFailedJob(t *testing.T) {

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	j, err := job.NewJob(987658, "non-failed-retry-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", j.ID)
	}()

	if err := SaveJob(ctx, pool, j); err != nil {
		t.Fatal(err)
	}

	_, err = RetryJob(
		ctx,
		pool,
		j.ID,
		job.RetryPolicy{
			MaxRetries:    3,
			BaseDelay:     1 * time.Second,
			MaxDelay:      10 * time.Second,
			JitterPercent: 0.20,
		},
		0,
	)
	if err == nil {
		t.Fatal("expected retry of Pending job to fail")
	}

	saved, err := GetJob(ctx, pool, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != "Pending" || saved.RetryCount != 0 {
		t.Errorf("job changed unexpectedly: status=%s retry_count=%d",
			saved.Status, saved.RetryCount)
	}
}

func TestRetryJobRejectsExhaustedRetries(t *testing.T) {

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	j, err := job.NewJob(987659, "exhausted-retry-test")
	if err != nil {
		t.Fatal(err)
	}

	j.Status = "Failed"
	j.RetryCount = 3
	j.MaxRetries = 3

	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", j.ID)
	}()

	_, err = pool.Exec(ctx,
		`INSERT INTO jobs
         (id, name, status, retry_count, max_retries)
         VALUES ($1, $2, $3, $4, $5)`,
		j.ID, j.Name, j.Status, j.RetryCount, j.MaxRetries,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = RetryJob(
		ctx,
		pool,
		j.ID,
		job.RetryPolicy{
			MaxRetries:    3,
			BaseDelay:     1 * time.Second,
			MaxDelay:      10 * time.Second,
			JitterPercent: 0.20,
		},
		0,
	)
	if err == nil {
		t.Fatal("expected retry with exhausted limit to fail")
	}

	saved, err := GetJob(ctx, pool, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != "Failed" || saved.RetryCount != 3 {
		t.Errorf("job changed unexpectedly: status=%s retry_count=%d",
			saved.Status, saved.RetryCount)
	}
}

func TestClaimNextJob(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
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

	j, err := job.NewJob(testID, "claim-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

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
`, testID)
	if err != nil {
		t.Fatalf("position test job for claiming: %v", err)
	}

	claimed, err := ClaimNextJob(ctx, pool, "test-worker")
	if err != nil {
		t.Fatalf("ClaimNextJob failed: %v", err)
	}

	// _, err = pool.Exec(ctx, `
	// 	UPDATE jobs
	// 	SET status = 'Running'
	// 	WHERE status = 'Pending' AND id <> $1
	// 	`, testID)
	// if err != nil {
	// 	t.Fatalf("failed to isolate test: %v", err)
	// }

	// claimed, err := ClaimNextJob(ctx, pool)

	if claimed.ID != testID {
		t.Fatalf("expected claimed job ID %d, got %d", testID, claimed.ID)
	}

	if claimed.Status != "Running" {
		t.Errorf("expected status Running, got %s", claimed.Status)
	}

	stored, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if stored.Status != "Running" {
		t.Errorf("expected database status Running, got %s", stored.Status)
	}
}

func TestTransitionJobStatusClearsNextRetryAt(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatal(err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	testID := 786

	_, _ = pool.Exec(
		ctx,
		"DELETE FROM jobs WHERE id = $1",
		testID,
	)
	defer pool.Exec(
		ctx,
		"DELETE FROM jobs WHERE id = $1",
		testID,
	)

	nextRetry := time.Now().Add(10 * time.Minute)
	fencingToken := int64(0)

	_, err = pool.Exec(ctx,
		`INSERT INTO jobs
        (id, name, status, retry_count, max_retries,
         next_retry_at, fencing_token)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		testID,
		"transition-test",
		"Pending",
		1,
		3,
		nextRetry,
		fencingToken,
	)

	if err != nil {
		t.Fatal(err)
	}

	_, err = TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Running",
		fencingToken,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Completed",
		fencingToken,
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatal(err)
	}

	if got.Status != "Completed" {
		t.Fatalf(
			"expected Completed, got %s",
			got.Status,
		)
	}

	if got.NextRetryAt != nil {
		t.Fatal(
			"expected NextRetryAt to be cleared after completion",
		)
	}
}

func TestClaimNextJobRecoversExpiredLease(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	testID := int(time.Now().UnixNano()%900_000_000) + 1_000_000_000

	_, err = pool.Exec(
		ctx,
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

	_, err = pool.Exec(ctx, `
    INSERT INTO jobs (
        id,
        name,
        status,
        run_at,
        retry_count,
        max_retries,
        next_retry_at,
        worker_id,
        lease_until,
        fencing_token
    )
    VALUES (
        $1,
        $2,
        'Running',
        COALESCE(
            (
                SELECT MIN(run_at) - INTERVAL '1 second'
                FROM jobs
            ),
            NOW() - INTERVAL '1 minute'
        ),
        0,
        3,
        NULL,
        $3,
        NOW() - INTERVAL '1 second',
        $4
    )
`,
		testID,
		"expired-lease-test",
		"worker-A",
		int64(5),
	)
	if err != nil {
		t.Fatalf("failed to insert test job: %v", err)
	}

	claimed, err := ClaimNextJob(
		ctx,
		pool,
		"worker-B",
	)
	if err != nil {
		t.Fatalf("ClaimNextJob failed: %v", err)
	}

	if claimed.ID != testID {
		t.Fatalf(
			"expected claimed job ID %d, got %d",
			testID,
			claimed.ID,
		)
	}

	if claimed.Status != "Running" {
		t.Fatalf(
			"expected status Running, got %s",
			claimed.Status,
		)
	}

	if claimed.FencingToken != 6 {
		t.Fatalf(
			"expected fencing token 6, got %d",
			claimed.FencingToken,
		)
	}

	stored, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}

	if stored.Status != "Running" {
		t.Fatalf(
			"expected stored status Running, got %s",
			stored.Status,
		)
	}

	if stored.FencingToken != 6 {
		t.Fatalf(
			"expected stored fencing token 6, got %d",
			stored.FencingToken,
		)
	}

	if stored.WorkerID != "worker-B" {
		t.Fatalf(
			"expected worker-B to own job, got %s",
			stored.WorkerID,
		)
	}

	if stored.LeaseUntil == nil {
		t.Fatal("expected new lease to be assigned")
	}

	if !stored.LeaseUntil.After(time.Now()) {
		t.Fatal("expected new lease to be in the future")
	}
}

func TestRetryJobSecondRetryUsesExponentialBackoff(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	j, err := job.NewJob(32345, "retry-backoff-test")
	if err != nil {
		t.Fatal(err)
	}

	j.Status = "Failed"
	j.RetryCount = 1
	j.MaxRetries = 3

	defer func() {
		_, _ = pool.Exec(ctx,
			"DELETE FROM jobs WHERE id = $1",
			j.ID,
		)
	}()

	_, err = pool.Exec(ctx,
		`INSERT INTO jobs
		 (id, name, status, retry_count, max_retries)
		 VALUES ($1, $2, $3, $4, $5)`,
		j.ID,
		j.Name,
		j.Status,
		j.RetryCount,
		j.MaxRetries,
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := RetryJob(
		ctx,
		pool,
		j.ID,
		job.RetryPolicy{
			MaxRetries:    3,
			BaseDelay:     1 * time.Second,
			MaxDelay:      10 * time.Second,
			JitterPercent: 0.20,
		},
		0,
	)
	if err != nil {
		t.Fatalf("RetryJob returned error: %v", err)
	}

	if got.RetryCount != 2 {
		t.Fatalf(
			"expected RetryCount 2, got %d",
			got.RetryCount,
		)
	}

	if got.NextRetryAt == nil {
		t.Fatal("expected NextRetryAt to be set")
	}

	remaining := time.Until(got.NextRetryAt.UTC())

	if remaining < 1600*time.Millisecond || remaining > 2400*time.Millisecond {
		t.Fatalf(
			"expected retry delay between 1.6s and 2.4s, got %v",
			remaining,
		)
	}
}

func TestRetryJobCapsExponentialBackoff(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	j, err := job.NewJob(987662, "retry-cap-test")
	if err != nil {
		t.Fatal(err)
	}

	j.Status = "Failed"
	j.RetryCount = 4
	j.MaxRetries = 6

	defer func() {
		_, _ = pool.Exec(
			ctx,
			"DELETE FROM jobs WHERE id = $1",
			j.ID,
		)
	}()

	_, err = pool.Exec(ctx,
		`INSERT INTO jobs
		 (id, name, status, retry_count, max_retries)
		 VALUES ($1, $2, $3, $4, $5)`,
		j.ID,
		j.Name,
		j.Status,
		j.RetryCount,
		j.MaxRetries,
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := RetryJob(
		ctx,
		pool,
		j.ID,
		job.RetryPolicy{
			MaxRetries:    3,
			BaseDelay:     1 * time.Second,
			MaxDelay:      10 * time.Second,
			JitterPercent: 0.20,
		},
		0,
	)
	if err != nil {
		t.Fatalf("RetryJob returned error: %v", err)
	}

	if got.RetryCount != 5 {
		t.Fatalf(
			"expected RetryCount 5, got %d",
			got.RetryCount,
		)
	}

	if got.NextRetryAt == nil {
		t.Fatal("expected NextRetryAt to be set")
	}

	remaining := time.Until(got.NextRetryAt.UTC())

	if remaining < 8*time.Second || remaining > 12*time.Second {
		t.Fatalf(
			"expected retry delay between 8s and 12s, got %v",
			remaining,
		)
	}
}
func TestClaimNextJobIncrementsFencingToken(t *testing.T) {
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

	testID := int(time.Now().UnixNano()%800_000_000) + 1_000_000_000

	j, err := job.NewJob(testID, "fencing-token-test")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	if err := SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("save job: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			testID,
		)
	}()

	// Position only this test's job ahead of existing jobs.
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
	`, testID)
	if err != nil {
		t.Fatalf("position test job for claiming: %v", err)
	}

	claimed, err := ClaimNextJob(ctx, pool, "worker-fencing-test")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	if claimed.ID != testID {
		t.Fatalf(
			"expected to claim test job %d, got job %d",
			testID,
			claimed.ID,
		)
	}

	if claimed.FencingToken != 1 {
		t.Fatalf(
			"expected first fencing token 1, got %d",
			claimed.FencingToken,
		)
	}
}

func TestTransitionJobStatusRejectsStaleFencingToken(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	testID := 987661

	_, err = pool.Exec(ctx, `
		DELETE FROM jobs
		WHERE id = $1
	`, testID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO jobs (
			id,
			name,
			status,
			retry_count,
			max_retries,
			fencing_token
		)
		VALUES ($1, 'stale-token-test', 'Running', 0, 3, 2)
	`, testID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Completed",
		1,
	)

	if err == nil {
		t.Fatal("expected stale fencing token error")
	}

	if !strings.Contains(err.Error(), "stale fencing token") {
		t.Fatalf("expected stale fencing token error, got: %v", err)
	}
}

func TestRenewJobLease(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	const testID = 987666
	const fencingToken int64 = 5

	_, err = pool.Exec(
		ctx,
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

	_, err = pool.Exec(ctx,
		`INSERT INTO jobs
		(id, name, status, retry_count, max_retries,
		 worker_id, lease_until, fencing_token)
		VALUES ($1, $2, 'Running', 0, 3, $3,
		        NOW() + INTERVAL '1 second', $4)`,
		testID,
		"lease-renew-test",
		"worker-A",
		fencingToken,
	)
	if err != nil {
		t.Fatalf("failed to insert test job: %v", err)
	}

	before, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob before renewal failed: %v", err)
	}

	if before.LeaseUntil == nil {
		t.Fatal("expected existing lease")
	}

	err = RenewJobLease(
		ctx,
		pool,
		testID,
		fencingToken,
	)
	if err != nil {
		t.Fatalf("RenewJobLease failed: %v", err)
	}

	after, err := GetJob(ctx, pool, testID)
	if err != nil {
		t.Fatalf("GetJob after renewal failed: %v", err)
	}

	if after.LeaseUntil == nil {
		t.Fatal("expected renewed lease")
	}

	if !after.LeaseUntil.After(*before.LeaseUntil) {
		t.Fatalf(
			"expected renewed lease %v to be after previous lease %v",
			*after.LeaseUntil,
			*before.LeaseUntil,
		)
	}
}

func TestRenewJobLeaseRejectsStaleFencingToken(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	const testID = 987667

	_, err = pool.Exec(
		ctx,
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

	_, err = pool.Exec(ctx,
		`INSERT INTO jobs
		(id, name, status, retry_count, max_retries,
		 worker_id, lease_until, fencing_token)
		VALUES ($1, $2, 'Running', 0, 3, $3,
		        NOW() + INTERVAL '30 seconds', $4)`,
		testID,
		"stale-heartbeat-test",
		"worker-B",
		int64(6),
	)
	if err != nil {
		t.Fatalf("failed to insert test job: %v", err)
	}

	err = RenewJobLease(
		ctx,
		pool,
		testID,
		5,
	)

	if err == nil {
		t.Fatal("expected stale fencing token to be rejected")
	}
}

func TestCreateExecutionJob(t *testing.T) {
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

	const jobID = 998101

	// Cleanup before the test.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM jobs WHERE id = $1`,
		jobID,
	)
	if err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	// Create the execution job.
	createdJob, err := CreateExecutionJob(
		ctx,
		pool,
		jobID,
		"workflow-execution-test",
	)
	if err != nil {
		t.Fatalf("create execution job: %v", err)
	}

	// Verify the returned job.
	if createdJob.ID != jobID {
		t.Fatalf(
			"expected job ID %d, got %d",
			jobID,
			createdJob.ID,
		)
	}

	if createdJob.Name != "workflow-execution-test" {
		t.Fatalf(
			"expected job name %q, got %q",
			"workflow-execution-test",
			createdJob.Name,
		)
	}

	if createdJob.Status != "Pending" {
		t.Fatalf(
			"expected job status %q, got %q",
			"Pending",
			createdJob.Status,
		)
	}

	if createdJob.RetryCount != 0 {
		t.Fatalf(
			"expected retry count 0, got %d",
			createdJob.RetryCount,
		)
	}

	if createdJob.MaxRetries != 3 {
		t.Fatalf(
			"expected max retries 3, got %d",
			createdJob.MaxRetries,
		)
	}

	if createdJob.NextRetryAt != nil {
		t.Fatal("expected NextRetryAt to be nil")
	}

	// Verify that the job was actually persisted.
	savedJob, err := GetJob(ctx, pool, jobID)
	if err != nil {
		t.Fatalf("get created job: %v", err)
	}

	if savedJob.ID != jobID {
		t.Fatalf(
			"expected saved job ID %d, got %d",
			jobID,
			savedJob.ID,
		)
	}

	if savedJob.Name != "workflow-execution-test" {
		t.Fatalf(
			"expected saved job name %q, got %q",
			"workflow-execution-test",
			savedJob.Name,
		)
	}

	if savedJob.Status != "Pending" {
		t.Fatalf(
			"expected saved job status %q, got %q",
			"Pending",
			savedJob.Status,
		)
	}

	if savedJob.RetryCount != 0 {
		t.Fatalf(
			"expected saved retry count 0, got %d",
			savedJob.RetryCount,
		)
	}

	if savedJob.MaxRetries != 3 {
		t.Fatalf(
			"expected max retries 3, got %d",
			savedJob.MaxRetries,
		)
	}

	if savedJob.NextRetryAt != nil {
		t.Fatal("expected saved NextRetryAt to be nil")
	}

	if savedJob.WorkerID != "" {
		t.Fatalf(
			"expected empty worker ID, got %q",
			savedJob.WorkerID,
		)
	}

	if savedJob.LeaseUntil != nil {
		t.Fatal("expected LeaseUntil to be nil")
	}

	if savedJob.FencingToken != 0 {
		t.Fatalf(
			"expected fencing token 0, got %d",
			savedJob.FencingToken,
		)
	}

	// Cleanup after the test.
	_, err = pool.Exec(
		ctx,
		`DELETE FROM jobs WHERE id = $1`,
		jobID,
	)
	if err != nil {
		t.Fatalf("cleanup after test: %v", err)
	}
}
