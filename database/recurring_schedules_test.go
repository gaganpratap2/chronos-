package database

import (
	"context"
	"testing"
	"time"

	"chronos/job"
	"chronos/schedule"

	"github.com/joho/godotenv"
)

func TestSaveAndGetRecurringSchedule(t *testing.T) {
	ctx := context.Background()

	if err := godotenv.Load("../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	interval := 5 * time.Minute
	nextRunAt := time.Now().Add(10 * time.Minute).Truncate(time.Microsecond)

	original, err := schedule.NewRecurringSchedule(
		"report",
		interval,
		nextRunAt,
	)
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save schedule: %v", err)
	}

	var id int64

	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1
		 ORDER BY id DESC
		 LIMIT 1`,
		original.Name,
	).Scan(&id)
	if err != nil {
		t.Fatalf("get inserted schedule ID: %v", err)
	}

	retrieved, err := GetRecurringSchedule(ctx, pool, id)
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}

	if retrieved.ID != id {
		t.Fatalf("expected ID %d, got %d", id, retrieved.ID)
	}

	if retrieved.Name != original.Name {
		t.Fatalf("expected name %q, got %q",
			original.Name,
			retrieved.Name,
		)
	}

	if retrieved.Interval != original.Interval {
		t.Fatalf("expected interval %v, got %v",
			original.Interval,
			retrieved.Interval,
		)
	}

	if !retrieved.NextRunAt.Equal(original.NextRunAt) {
		t.Fatalf("expected NextRunAt %v, got %v",
			original.NextRunAt,
			retrieved.NextRunAt,
		)
	}

	if retrieved.Enabled != original.Enabled {
		t.Fatalf("expected Enabled %v, got %v",
			original.Enabled,
			retrieved.Enabled,
		)
	}
}

func TestClaimRecurringScheduleConcurrent(t *testing.T) {
	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	s, err := schedule.NewRecurringSchedule(
		"concurrent-test",
		5*time.Minute,
		time.Now().Add(-1*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := SaveRecurringSchedule(ctx, pool, s); err != nil {
		t.Fatal(err)
	}

	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules
		 WHERE name = $1
		 ORDER BY id DESC
		 LIMIT 1`,
		s.Name,
	).Scan(&s.ID)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_, _ = pool.Exec(
			ctx,
			`DELETE FROM jobs
			 WHERE name LIKE 'concurrent-test-%'`,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM recurring_schedules
			 WHERE id = $1`,
			s.ID,
		)
	}()

	type result struct {
		job job.Job
		err error
	}

	results := make(chan result, 2)

	for i := 0; i < 2; i++ {
		go func() {
			_, createdJob, err := ClaimRecurringSchedule(
				ctx,
				pool,
				s.ID,
			)

			results <- result{
				job: createdJob,
				err: err,
			}
		}()
	}

	var successes int
	var createdJobs []job.Job

	for i := 0; i < 2; i++ {
		r := <-results

		if r.err == nil {
			successes++
			createdJobs = append(createdJobs, r.job)
		}
	}

	if successes != 1 {
		t.Fatalf(
			"expected exactly 1 successful claim, got %d",
			successes,
		)
	}

	var count int
	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM jobs
		 WHERE name LIKE 'concurrent-test-%'`,
	).Scan(&count)

	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Fatalf(
			"expected exactly 1 recurring job, got %d",
			count,
		)
	}
}
