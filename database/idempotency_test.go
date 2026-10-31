package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"chronos/job"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func setupIdempotencyTest(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)

	pool, err := Connect(ctx)
	if err != nil {
		cancel()
		t.Fatalf("connect database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		cancel()
	})

	return pool, ctx
}

func newTestIdempotencyData(t *testing.T) (
	string,
	string,
	[]byte,
	int,
	job.Job,
	[]byte,
) {
	t.Helper()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	scope := "POST /jobs"
	key := "test-key-" + suffix
	hash := bytes.Repeat([]byte{0xAB}, 32)
	jobID := int(time.Now().UnixNano()%800_000_000) + 1_000_000_000

	newJob, err := job.NewJob(jobID, "idempotency-test")
	if err != nil {
		t.Fatal(err)
	}

	response := []byte(`{"id":` +
		fmt.Sprintf("%d", jobID) +
		`,"name":"idempotency-test","status":"Pending"}`)

	return scope, key, hash, jobID, newJob, response
}

func cleanupIdempotencyTest(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
	scope string,
	key string,
	jobID int,
) {
	t.Helper()

	_, err := pool.Exec(
		ctx,
		`DELETE FROM idempotency_records
		 WHERE operation_scope = $1
		   AND idempotency_key = $2`,
		scope,
		key,
	)
	if err != nil {
		t.Errorf("cleanup idempotency record: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		"DELETE FROM jobs WHERE id = $1",
		jobID,
	)
	if err != nil {
		t.Errorf("cleanup job: %v", err)
	}
}

// Test that a new request creates a job and an idempotency record.
func TestCreateJobIdempotently(t *testing.T) {
	pool, ctx := setupIdempotencyTest(t)

	scope, key, hash, jobID, newJob, response :=
		newTestIdempotencyData(t)

	defer cleanupIdempotencyTest(
		t, pool, ctx, scope, key, jobID,
	)

	result, err := CreateJobIdempotently(
		ctx,
		pool,
		scope,
		key,
		hash,
		newJob,
		201,
		"application/json",
		response,
	)

	if err != nil {
		t.Fatalf("CreateJobIdempotently failed: %v", err)
	}

	if result.Replayed {
		t.Fatal("expected a new request, got replay")
	}

	if result.HTTPStatus != 201 {
		t.Errorf("expected HTTP 201, got %d", result.HTTPStatus)
	}

	if !bytes.Equal(result.Body, response) {
		t.Errorf("unexpected response body: %s", result.Body)
	}

	savedJob, err := GetJob(ctx, pool, jobID)
	if err != nil {
		t.Fatalf("job was not saved: %v", err)
	}

	if savedJob.Name != newJob.Name {
		t.Errorf("expected job name %q, got %q",
			newJob.Name, savedJob.Name)
	}
}

// Test that the same key and hash replay the original response.
func TestCreateJobIdempotentlyReplay(t *testing.T) {
	pool, ctx := setupIdempotencyTest(t)

	scope, key, hash, jobID, newJob, response :=
		newTestIdempotencyData(t)

	defer cleanupIdempotencyTest(
		t, pool, ctx, scope, key, jobID,
	)

	_, err := CreateJobIdempotently(
		ctx,
		pool,
		scope,
		key,
		hash,
		newJob,
		201,
		"application/json",
		response,
	)

	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	// A different job ID proves that replay does not create
	// another job.
	duplicateJob, err := job.NewJob(jobID+1, "different-job")
	if err != nil {
		t.Fatal(err)
	}

	result, err := CreateJobIdempotently(
		ctx,
		pool,
		scope,
		key,
		hash,
		duplicateJob,
		201,
		"application/json",
		response,
	)

	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if !result.Replayed {
		t.Fatal("expected Replayed to be true")
	}

	if !bytes.Equal(result.Body, response) {
		t.Errorf("expected original response, got %s", result.Body)
	}

	_, err = GetJob(ctx, pool, jobID+1)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("unexpected duplicate job: %v", err)
	}
}

// Test that reusing a key with a different hash is rejected.
func TestCreateJobIdempotentlyRejectsDifferentHash(t *testing.T) {
	pool, ctx := setupIdempotencyTest(t)

	scope, key, hash, jobID, newJob, response :=
		newTestIdempotencyData(t)

	defer cleanupIdempotencyTest(
		t, pool, ctx, scope, key, jobID,
	)

	_, err := CreateJobIdempotently(
		ctx,
		pool,
		scope,
		key,
		hash,
		newJob,
		201,
		"application/json",
		response,
	)

	if err != nil {
		t.Fatal(err)
	}

	differentHash := bytes.Repeat([]byte{0xCD}, 32)

	differentJob, err := job.NewJob(
		jobID+2,
		"different-payload",
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = CreateJobIdempotently(
		ctx,
		pool,
		scope,
		key,
		differentHash,
		differentJob,
		201,
		"application/json",
		[]byte(`{"name":"different-payload"}`),
	)

	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

// Test that a failed job insertion rolls back the reservation.
func TestCreateJobIdempotentlyRollsBackReservation(t *testing.T) {
	pool, ctx := setupIdempotencyTest(t)

	scope, key, hash, jobID, newJob, response :=
		newTestIdempotencyData(t)

	// Create a conflicting job ID before calling the function.
	if err := SaveJob(ctx, pool, newJob); err != nil {
		t.Fatal(err)
	}

	defer func() {
		cleanupIdempotencyTest(
			t, pool, ctx, scope, key, jobID,
		)
	}()

	// This must fail because the job ID already exists.
	_, err := CreateJobIdempotently(
		ctx,
		pool,
		scope,
		key,
		hash,
		newJob,
		201,
		"application/json",
		response,
	)

	if err == nil {
		t.Fatal("expected job insertion to fail")
	}

	// The idempotency reservation must also have rolled back.
	var count int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM idempotency_records
		 WHERE operation_scope = $1
		   AND idempotency_key = $2`,
		scope,
		key,
	).Scan(&count)

	if err != nil {
		t.Fatal(err)
	}

	if count != 0 {
		t.Fatalf("expected no idempotency record, got %d", count)
	}
}

func TestCreateJobIdempotentlyConcurrentSameKey(t *testing.T) {
	pool, ctx := setupIdempotencyTest(t)

	scope, key, hash, jobID, newJob, response :=
		newTestIdempotencyData(t)

	defer cleanupIdempotencyTest(
		t, pool, ctx, scope, key, jobID,
	)

	type callResult struct {
		result IdempotencyResult
		err    error
	}

	const callers = 2

	start := make(chan struct{})
	results := make(chan callResult, callers)

	var wg sync.WaitGroup

	for i := 0; i < callers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Both requests start together.
			<-start

			result, err := CreateJobIdempotently(
				ctx,
				pool,
				scope,
				key,
				hash,
				newJob,
				201,
				"application/json",
				response,
			)

			results <- callResult{
				result: result,
				err:    err,
			}
		}()
	}

	// Release both goroutines simultaneously.
	close(start)

	wg.Wait()
	close(results)

	var createdCount int
	var replayCount int

	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent request failed: %v", result.err)
		}

		if result.result.HTTPStatus != 201 {
			t.Errorf(
				"expected HTTP 201, got %d",
				result.result.HTTPStatus,
			)
		}

		if !bytes.Equal(result.result.Body, response) {
			t.Errorf(
				"unexpected response body: %s",
				result.result.Body,
			)
		}

		if result.result.Replayed {
			replayCount++
		} else {
			createdCount++
		}
	}

	if createdCount != 1 {
		t.Fatalf(
			"expected exactly 1 creation, got %d",
			createdCount,
		)
	}

	if replayCount != 1 {
		t.Fatalf(
			"expected exactly 1 replay, got %d",
			replayCount,
		)
	}

	// Verify exactly one job exists.
	var jobCount int

	err := pool.QueryRow(
		ctx,
		"SELECT COUNT(*) FROM jobs WHERE id = $1",
		jobID,
	).Scan(&jobCount)

	if err != nil {
		t.Fatalf("count jobs: %v", err)
	}

	if jobCount != 1 {
		t.Fatalf("expected exactly 1 job, got %d", jobCount)
	}

	// Verify exactly one idempotency record exists.
	var recordCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM idempotency_records
		 WHERE operation_scope = $1
		   AND idempotency_key = $2`,
		scope,
		key,
	).Scan(&recordCount)

	if err != nil {
		t.Fatalf("count idempotency records: %v", err)
	}

	if recordCount != 1 {
		t.Fatalf(
			"expected exactly 1 idempotency record, got %d",
			recordCount,
		)
	}
}

func TestCreateJobIdempotentlyConcurrentDifferentHashes(t *testing.T) {
	pool, ctx := setupIdempotencyTest(t)

	scope, key, hashA, jobIDA, jobA, responseA :=
		newTestIdempotencyData(t)

	hashB := bytes.Repeat([]byte{0xCD}, 32)

	jobIDB := jobIDA + 1
	jobB, err := job.NewJob(jobIDB, "generate-report")
	if err != nil {
		t.Fatal(err)
	}

	responseB := []byte(
		fmt.Sprintf(
			`{"id":%d,"name":"generate-report","status":"Pending"}`,
			jobIDB,
		),
	)

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		// First delete the idempotency record.
		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM idempotency_records
         WHERE operation_scope = $1
           AND idempotency_key = $2`,
			scope,
			key,
		)
		if err != nil {
			t.Errorf("cleanup idempotency record: %v", err)
		}

		// Then delete both possible candidate jobs.
		_, err = pool.Exec(
			cleanupCtx,
			`DELETE FROM jobs
         WHERE id IN ($1, $2)`,
			jobIDA,
			jobIDB,
		)
		if err != nil {
			t.Errorf("cleanup candidate jobs: %v", err)
		}
	}()

	type callResult struct {
		result IdempotencyResult
		err    error
	}

	start := make(chan struct{})
	results := make(chan callResult, 2)

	var wg sync.WaitGroup

	requests := []struct {
		hash     []byte
		newJob   job.Job
		response []byte
	}{
		{hashA, jobA, responseA},
		{hashB, jobB, responseB},
	}

	for _, request := range requests {
		request := request

		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			result, err := CreateJobIdempotently(
				ctx,
				pool,
				scope,
				key,
				request.hash,
				request.newJob,
				201,
				"application/json",
				request.response,
			)

			results <- callResult{
				result: result,
				err:    err,
			}
		}()
	}

	close(start)

	wg.Wait()
	close(results)

	var successCount int
	var conflictCount int

	for result := range results {
		if errors.Is(result.err, ErrIdempotencyConflict) {
			conflictCount++
			continue
		}

		if result.err != nil {
			t.Fatalf("unexpected concurrent request error: %v", result.err)
		}

		successCount++
	}

	if successCount != 1 {
		t.Fatalf("expected 1 successful request, got %d", successCount)
	}

	if conflictCount != 1 {
		t.Fatalf("expected 1 conflict, got %d", conflictCount)
	}

	var jobCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM jobs
		 WHERE id IN ($1, $2)`,
		jobIDA,
		jobIDB,
	).Scan(&jobCount)

	if err != nil {
		t.Fatalf("count created jobs: %v", err)
	}

	if jobCount != 1 {
		t.Fatalf("expected exactly 1 created job, got %d", jobCount)
	}

	var recordCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM idempotency_records
		 WHERE operation_scope = $1
		   AND idempotency_key = $2`,
		scope,
		key,
	).Scan(&recordCount)

	if err != nil {
		t.Fatalf("count idempotency records: %v", err)
	}

	if recordCount != 1 {
		t.Fatalf("expected 1 idempotency record, got %d", recordCount)
	}
}
