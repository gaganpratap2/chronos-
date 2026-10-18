package database

import (
	"context"
	"testing"
	"time"

	"github.com/joho/godotenv"

	"chronos/job"
)

func TestSaveAndGetJob(t *testing.T) {

	// if err := godotenv.Load(); err != nil {
	// 	t.Log(".env not found; using system environment variables")
	// }

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

	if original.RetryCount != 2 {
		t.Errorf("expected RetryCount 2, got %d", original.RetryCount)
	}

	if original.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", original.MaxRetries)
	}

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

	remaining := time.Until(got.NextRetryAt.UTC())

	if remaining < 900*time.Millisecond || remaining > 1100*time.Millisecond {
		t.Fatalf("expected retry delay around 1s, got %v", remaining)
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

	const testID = 1

	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
	if err != nil {
		t.Fatalf("failed to prepare test: %v", err)
	}
	defer pool.Exec(context.Background(),
		"DELETE FROM jobs WHERE id = $1", testID)

	j, err := job.NewJob(testID, "claim-test")
	if err != nil {
		t.Fatal(err)
	}

	if err := SaveJob(ctx, pool, j); err != nil {
		t.Fatalf("SaveJob failed: %v", err)
	}

	claimed, err := ClaimNextJob(ctx, pool)
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

	testID := 987660

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

	_, err = pool.Exec(
		ctx,
		`INSERT INTO jobs
			(id, name, status, retry_count, max_retries, next_retry_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
		testID,
		"transition-test",
		"Pending",
		1,
		3,
		nextRetry,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Running",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = TransitionJobStatus(
		ctx,
		pool,
		testID,
		"Completed",
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

	j, err := job.NewJob(987661, "retry-backoff-test")
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

// package database

// import (
// 	"context"
// 	"testing"
// 	"time"

// 	"github.com/joho/godotenv"

// 	// "chronos/database"

// 	// "context"
// 	// "testing"
// 	// "time"

// 	// // "chronos/database"
// 	"chronos/job"
// 	// "github.com/joho/godotenv"
// )

// func TestSaveAndGetJob(t *testing.T) {

// 	// if err := godotenv.Load(); err != nil {
// 	// 	t.Log(".env not found; using system environment variables")
// 	// }

// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		5*time.Second,
// 	)
// 	defer cancel()

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	// Use a unique ID so we don't overwrite existing data.
// 	const testID = 987654

// 	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
// 	if err != nil {
// 		t.Fatalf("failed to prepare test: %v", err)
// 	}
// 	defer pool.Exec(context.Background(),
// 		"DELETE FROM jobs WHERE id = $1", testID)

// 	original, err := job.NewJob(testID, "automated-test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	original.RetryCount = 2
// 	original.MaxRetries = 5

// 	if err := SaveJob(ctx, pool, original); err != nil {
// 		t.Fatalf("SaveJob failed: %v", err)
// 	}

// 	retrieved, err := GetJob(ctx, pool, testID)
// 	if err != nil {
// 		t.Fatalf("GetJob failed: %v", err)
// 	}

// 	if original.RetryCount != 2 {
// 		t.Errorf("expected RetryCount 2, got %d", original.RetryCount)
// 	}

// 	if original.MaxRetries != 5 {
// 		t.Errorf("expected MaxRetries 5, got %d", original.MaxRetries)
// 	}

// 	if retrieved.ID != original.ID {
// 		t.Errorf("expected ID %d, got %d", original.ID, retrieved.ID)
// 	}

// 	if retrieved.Name != original.Name {
// 		t.Errorf("expected name %q, got %q", original.Name, retrieved.Name)
// 	}

// 	if retrieved.Status != original.Status {
// 		t.Errorf("expected status %q, got %q",
// 			original.Status, retrieved.Status)
// 	}
// }

// func TestRetryJob(t *testing.T) {

// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancle := context.WithTimeout(
// 		context.Background(),
// 		5*time.Second,
// 	)

// 	defer cancle()

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	j, err := job.NewJob(987657, "retry-test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// Ensure the test row is removed even if an assertion fails.
// 	defer func() {
// 		_, _ = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", j.ID)
// 	}()

// 	j.Status = "Failed"
// 	j.RetryCount = 0
// 	j.MaxRetries = 3

// 	_, err = pool.Exec(ctx,
// 		`INSERT INTO jobs
//          (id, name, status, retry_count, max_retries)
//          VALUES ($1, $2, $3, $4, $5)`,
// 		j.ID, j.Name, j.Status, j.RetryCount, j.MaxRetries,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	got, err := RetryJob(ctx, pool, j.ID)
// 	if err != nil {
// 		t.Fatalf("RetryJob returned error: %v", err)
// 	}

// 	if got.Status != "Pending" {
//     	t.Errorf("expected Pending, got %s", got.Status)
// 	}

// 	if got.RetryCount != 1 {
//     	t.Errorf("expected RetryCount 1, got %d", got.RetryCount)
// 	}

// 	if got.NextRetryAt == nil {
//     	t.Fatal("expected NextRetryAt to be set")
// 	}

// 	if got.NextRetryAt.Before(time.Now()) {
//     	t.Error("expected NextRetryAt to be in the future")
// 	}

// // Verify the values were persisted in PostgreSQL.
// 	saved, err := GetJob(ctx, pool, j.ID)
// 	if err != nil {
//     	t.Fatal(err)
// 	}

// 	if saved.Status != "Pending" || saved.RetryCount != 1 {
//     	t.Errorf("unexpected saved job: status=%s retry_count=%d",
//         saved.Status, saved.RetryCount)
// 	}

// 	if saved.NextRetryAt == nil {
//     	t.Fatal("expected saved NextRetryAt to be set")
// 	}

// 	remaining := time.Until(got.NextRetryAt.UTC())

// 	if remaining < 900*time.Millisecond || remaining > 1100*time.Millisecond {
//     	t.Fatalf("expected retry delay around 1s, got %v", remaining)
// 	}
// }

// func TestRetryIncrementsCount(t *testing.T) {
// 	j, err := job.NewJob(201, "payment")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	if err := j.TransitionTo("Running"); err != nil {
// 		t.Fatal(err)
// 	}
// 	if err := j.TransitionTo("Failed"); err != nil {
// 		t.Fatal(err)
// 	}

// 	if err := j.Retry(); err != nil {
// 		t.Fatalf("expected retry to succeed: %v", err)
// 	}

// 	if j.RetryCount != 1 {
// 		t.Errorf("expected RetryCount 1, got %d", j.RetryCount)
// 	}
// 	if j.Status != "Pending" {
// 		t.Errorf("expected Pending, got %s", j.Status)
// 	}
// }

// func TestRetryRejectsNonFailedJob(t *testing.T) {
// 	j, err := job.NewJob(202, "email")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	err = j.Retry()

// 	if err == nil {
// 		t.Fatal("expected retry to fail for a Pending job")
// 	}

// 	if j.RetryCount != 0 {
// 		t.Errorf("expected RetryCount 0, got %d", j.RetryCount)
// 	}
// }

// func TestRetryLimit(t *testing.T) {
// 	j, err := job.NewJob(203, "report")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	for i := 0; i < j.MaxRetries; i++ {
// 		if err := j.TransitionTo("Running"); err != nil {
// 			t.Fatal(err)
// 		}
// 		if err := j.TransitionTo("Failed"); err != nil {
// 			t.Fatal(err)
// 		}
// 		if err := j.Retry(); err != nil {
// 			t.Fatalf("retry %d should succeed: %v", i+1, err)
// 		}
// 	}

// 	if j.RetryCount != j.MaxRetries {
// 		t.Fatalf("expected %d retries, got %d",
// 			j.MaxRetries, j.RetryCount)
// 	}

// 	// Fail once more after the retry limit has been reached.
// 	if err := j.TransitionTo("Running"); err != nil {
// 		t.Fatal(err)
// 	}
// 	if err := j.TransitionTo("Failed"); err != nil {
// 		t.Fatal(err)
// 	}

// 	if err := j.Retry(); err == nil {
// 		t.Fatal("expected retry to fail after reaching the limit")
// 	}

// 	if j.RetryCount != j.MaxRetries {
// 		t.Errorf("retry count should remain %d, got %d",
// 			j.MaxRetries, j.RetryCount)
// 	}
// 	if j.Status != "Failed" {
// 		t.Errorf("expected Failed after exhausted retries, got %s",
// 			j.Status)
// 	}
// }

// func TestRetryJobRejectsNonFailedJob(t *testing.T) {

// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		5*time.Second,
// 	)
// 	defer cancel()

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	j, err := job.NewJob(987658, "non-failed-retry-test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	defer func() {
// 		_, _ = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", j.ID)
// 	}()

// 	if err := SaveJob(ctx, pool, j); err != nil {
// 		t.Fatal(err)
// 	}

// 	_, err = RetryJob(ctx, pool, j.ID)
// 	if err == nil {
// 		t.Fatal("expected retry of Pending job to fail")
// 	}

// 	saved, err := GetJob(ctx, pool, j.ID)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if saved.Status != "Pending" || saved.RetryCount != 0 {
// 		t.Errorf("job changed unexpectedly: status=%s retry_count=%d",
// 			saved.Status, saved.RetryCount)
// 	}
// }

// func TestRetryJobRejectsExhaustedRetries(t *testing.T) {

// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		5*time.Second,
// 	)
// 	defer cancel()

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	j, err := job.NewJob(987659, "exhausted-retry-test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	j.Status = "Failed"
// 	j.RetryCount = 3
// 	j.MaxRetries = 3

// 	defer func() {
// 		_, _ = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", j.ID)
// 	}()

// 	_, err = pool.Exec(ctx,
// 		`INSERT INTO jobs
//          (id, name, status, retry_count, max_retries)
//          VALUES ($1, $2, $3, $4, $5)`,
// 		j.ID, j.Name, j.Status, j.RetryCount, j.MaxRetries,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	_, err = RetryJob(ctx, pool, j.ID)
// 	if err == nil {
// 		t.Fatal("expected retry with exhausted limit to fail")
// 	}

// 	saved, err := GetJob(ctx, pool, j.ID)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if saved.Status != "Failed" || saved.RetryCount != 3 {
// 		t.Errorf("job changed unexpectedly: status=%s retry_count=%d",
// 			saved.Status, saved.RetryCount)
// 	}
// }

// func TestClaimNextJob(t *testing.T) {
// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		5*time.Second,
// 	)
// 	defer cancel()

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	const testID = 1

// 	_, err = pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", testID)
// 	if err != nil {
// 		t.Fatalf("failed to prepare test: %v", err)
// 	}
// 	defer pool.Exec(context.Background(),
// 		"DELETE FROM jobs WHERE id = $1", testID)

// 	j, err := job.NewJob(testID, "claim-test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	if err := SaveJob(ctx, pool, j); err != nil {
// 		t.Fatalf("SaveJob failed: %v", err)
// 	}

// 	claimed, err := ClaimNextJob(ctx, pool)
// 	if err != nil {
// 		t.Fatalf("ClaimNextJob failed: %v", err)
// 	}

// 	// _, err = pool.Exec(ctx, `
// 	// 	UPDATE jobs
// 	// 	SET status = 'Running'
// 	// 	WHERE status = 'Pending' AND id <> $1
// 	// 	`, testID)
// 	// if err != nil {
// 	// 	t.Fatalf("failed to isolate test: %v", err)
// 	// }

// 	// claimed, err := ClaimNextJob(ctx, pool)

// 	if claimed.ID != testID {
// 		t.Fatalf("expected claimed job ID %d, got %d", testID, claimed.ID)
// 	}

// 	if claimed.Status != "Running" {
// 		t.Errorf("expected status Running, got %s", claimed.Status)
// 	}

// 	stored, err := GetJob(ctx, pool, testID)
// 	if err != nil {
// 		t.Fatalf("GetJob failed: %v", err)
// 	}

// 	if stored.Status != "Running" {
// 		t.Errorf("expected database status Running, got %s", stored.Status)
// 	}
// }

// func TestTransitionJobStatusClearsNextRetryAt(t *testing.T) {
// 	ctx := context.Background()

// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Fatal(err)
// 	}

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer pool.Close()

// 	testID := 987660

// 	_, _ = pool.Exec(
// 		ctx,
// 		"DELETE FROM jobs WHERE id = $1",
// 		testID,
// 	)
// 	defer pool.Exec(
// 		ctx,
// 		"DELETE FROM jobs WHERE id = $1",
// 		testID,
// 	)

// 	nextRetry := time.Now().Add(10 * time.Minute)

// 	_, err = pool.Exec(
// 		ctx,
// 		`INSERT INTO jobs
// 			(id, name, status, retry_count, max_retries, next_retry_at)
// 			VALUES ($1, $2, $3, $4, $5, $6)`,
// 		testID,
// 		"transition-test",
// 		"Pending",
// 		1,
// 		3,
// 		nextRetry,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	_, err = TransitionJobStatus(
// 		ctx,
// 		pool,
// 		testID,
// 		"Running",
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	_, err = TransitionJobStatus(
// 		ctx,
// 		pool,
// 		testID,
// 		"Completed",
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	got, err := GetJob(ctx, pool, testID)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	if got.Status != "Completed" {
// 		t.Fatalf(
// 			"expected Completed, got %s",
// 			got.Status,
// 		)
// 	}

// 	if got.NextRetryAt != nil {
// 		t.Fatal(
// 			"expected NextRetryAt to be cleared after completion",
// 		)
// 	}
// }

// func TestRetryJobSecondRetryUsesExponentialBackoff(t *testing.T) {
// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		5*time.Second,
// 	)
// 	defer cancel()

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	j, err := job.NewJob(987661, "retry-backoff-test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	j.Status = "Failed"
// 	j.RetryCount = 1
// 	j.MaxRetries = 3

// 	defer func() {
// 		_, _ = pool.Exec(ctx,
// 			"DELETE FROM jobs WHERE id = $1",
// 			j.ID,
// 		)
// 	}()

// 	_, err = pool.Exec(ctx,
// 		`INSERT INTO jobs
// 		 (id, name, status, retry_count, max_retries)
// 		 VALUES ($1, $2, $3, $4, $5)`,
// 		j.ID,
// 		j.Name,
// 		j.Status,
// 		j.RetryCount,
// 		j.MaxRetries,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	got, err := RetryJob(ctx, pool, j.ID)
// 	if err != nil {
// 		t.Fatalf("RetryJob returned error: %v", err)
// 	}

// 	if got.RetryCount != 2 {
// 		t.Fatalf(
// 			"expected RetryCount 2, got %d",
// 			got.RetryCount,
// 		)
// 	}

// 	if got.NextRetryAt == nil {
// 		t.Fatal("expected NextRetryAt to be set")
// 	}

// 	remaining := time.Until(got.NextRetryAt.UTC())

// 	if remaining < 1600*time.Millisecond || remaining > 2400*time.Millisecond {
//     t.Fatalf(
//         "expected retry delay between 1.6s and 2.4s, got %v",
//         remaining,
//     )
// }
// }

// func TestRetryJobCapsExponentialBackoff(t *testing.T) {
// 	if err := godotenv.Load("../.env"); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		5*time.Second,
// 	)
// 	defer cancel()

// 	pool, err := Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("failed to connect to database: %v", err)
// 	}
// 	defer pool.Close()

// 	j, err := job.NewJob(987662, "retry-cap-test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	j.Status = "Failed"
// 	j.RetryCount = 4
// 	j.MaxRetries = 6

// 	defer func() {
// 		_, _ = pool.Exec(
// 			ctx,
// 			"DELETE FROM jobs WHERE id = $1",
// 			j.ID,
// 		)
// 	}()

// 	_, err = pool.Exec(ctx,
// 		`INSERT INTO jobs
// 		 (id, name, status, retry_count, max_retries)
// 		 VALUES ($1, $2, $3, $4, $5)`,
// 		j.ID,
// 		j.Name,
// 		j.Status,
// 		j.RetryCount,
// 		j.MaxRetries,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	got, err := RetryJob(ctx, pool, j.ID)
// 	if err != nil {
// 		t.Fatalf("RetryJob returned error: %v", err)
// 	}

// 	if got.RetryCount != 5 {
// 		t.Fatalf(
// 			"expected RetryCount 5, got %d",
// 			got.RetryCount,
// 		)
// 	}

// 	if got.NextRetryAt == nil {
// 		t.Fatal("expected NextRetryAt to be set")
// 	}

// 	remaining := time.Until(got.NextRetryAt.UTC())

// 	if remaining < 8*time.Second || remaining > 12*time.Second {
//     t.Fatalf(
//         "expected retry delay between 8s and 12s, got %v",
//         remaining,
//     )
// }
// }
