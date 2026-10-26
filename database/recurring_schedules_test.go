package database

import (
	"context"
	"fmt"
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
	// t.Logf("CatchUpPolicy = %q", original.CatchUpPolicy)
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

	if retrieved.CatchUpPolicy != original.CatchUpPolicy {
		t.Fatalf(
			"expected CatchUpPolicy %q, got %q",
			original.CatchUpPolicy,
			retrieved.CatchUpPolicy,
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
	s.CatchUpPolicy = schedule.CatchUpFireOnce

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
			_, createdJobs, err := ClaimRecurringSchedule(
				ctx,
				pool,
				s.ID,
			)

			if err != nil {
				results <- result{
					err: err,
				}
				return
			}

			if len(createdJobs) != 1 {
				results <- result{
					err: fmt.Errorf(
						"expected 1 created job, got %d",
						len(createdJobs),
					),
				}
				return
			}

			results <- result{
				job: createdJobs[0],
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
		} else {
			t.Logf("claim attempt failed: %v", r.err)
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

func TestClaimRecurringScheduleFireOnce(t *testing.T) {
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

	start := time.Now().Add(-16 * time.Minute)

	s, err := schedule.NewRecurringSchedule(
		"fire-once-test",
		5*time.Minute,
		start,
	)
	if err != nil {
		t.Fatal(err)
	}

	s.CatchUpPolicy = schedule.CatchUpFireOnce

	if err := SaveRecurringSchedule(ctx, pool, s); err != nil {
		t.Fatal(err)
	}

	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
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
			 WHERE name LIKE 'fire-once-test-%'`,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM recurring_schedules
			 WHERE id = $1`,
			s.ID,
		)
	}()

	_, createdJobs, err := ClaimRecurringSchedule(
		ctx,
		pool,
		s.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(createdJobs) != 1 {
		t.Fatalf(
			"expected 1 created job, got %d",
			len(createdJobs),
		)
	}

	if !createdJobs[0].RunAt.Equal(start.Truncate(time.Microsecond)) {
		t.Fatalf(
			"expected RunAt %v, got %v",
			start,
			createdJobs[0].RunAt,
		)
	}

	var nextRunAt time.Time

	err = pool.QueryRow(
		ctx,
		`SELECT next_run_at
		 FROM recurring_schedules
		 WHERE id = $1`,
		s.ID,
	).Scan(&nextRunAt)
	if err != nil {
		t.Fatal(err)
	}

	expectedNextRun := start.Add(20 * time.Minute)

	if !nextRunAt.Equal(expectedNextRun.Truncate(time.Microsecond)) {
		t.Fatalf(
			"expected next_run_at %v, got %v",
			expectedNextRun,
			nextRunAt,
		)
	}
}

func TestClaimRecurringScheduleSkip(t *testing.T) {
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

	start := time.Now().Add(-16 * time.Minute)

	s, err := schedule.NewRecurringSchedule(
		"skip-test",
		5*time.Minute,
		start,
	)
	if err != nil {
		t.Fatal(err)
	}

	s.CatchUpPolicy = schedule.CatchUpSkip

	if err := SaveRecurringSchedule(ctx, pool, s); err != nil {
		t.Fatal(err)
	}

	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
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
			 WHERE name LIKE 'skip-test-%'`,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM recurring_schedules
			 WHERE id = $1`,
			s.ID,
		)
	}()

	_, createdJobs, err := ClaimRecurringSchedule(
		ctx,
		pool,
		s.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(createdJobs) != 0 {
		t.Fatalf(
			"expected 0 created jobs, got %d",
			len(createdJobs),
		)
	}

	var nextRunAt time.Time

	err = pool.QueryRow(
		ctx,
		`SELECT next_run_at
		 FROM recurring_schedules
		 WHERE id = $1`,
		s.ID,
	).Scan(&nextRunAt)
	if err != nil {
		t.Fatal(err)
	}

	expectedNextRun := start.Add(20 * time.Minute)

	if !nextRunAt.Equal(expectedNextRun.Truncate(time.Microsecond)) {
		t.Fatalf(
			"expected next_run_at %v, got %v",
			expectedNextRun,
			nextRunAt,
		)
	}
}

func TestClaimRecurringScheduleFireAll(t *testing.T) {
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

	start := time.Now().Add(-26 * time.Minute)

	s, err := schedule.NewRecurringSchedule(
		"fire-all-test",
		5*time.Minute,
		start,
	)
	if err != nil {
		t.Fatal(err)
	}

	s.CatchUpPolicy = schedule.CatchUpFireAll
	s.MaxCatchUp = 3

	if err := SaveRecurringSchedule(ctx, pool, s); err != nil {
		t.Fatal(err)
	}

	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1
		 ORDER BY id DESC
		 LIMIT 1`,
		s.Name,
	).Scan(&s.ID)
	if err != nil {
		t.Fatal(err)
	}

	var storedNextRunAt time.Time
	var storedPolicy string
	var storedMaxCatchUp int

	err = pool.QueryRow(
		ctx,
		`SELECT next_run_at, catch_up_policy, max_catch_up
	 FROM recurring_schedules
	 WHERE id = $1`,
		s.ID,
	).Scan(
		&storedNextRunAt,
		&storedPolicy,
		&storedMaxCatchUp,
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf(
		"stored: next_run_at=%v policy=%q max_catch_up=%d now=%v",
		storedNextRunAt,
		storedPolicy,
		storedMaxCatchUp,
		time.Now(),
	)

	defer func() {
		_, _ = pool.Exec(
			ctx,
			`DELETE FROM jobs
			 WHERE name LIKE 'fire-all-test-%'`,
		)

		_, _ = pool.Exec(
			ctx,
			`DELETE FROM recurring_schedules
			 WHERE id = $1`,
			s.ID,
		)
	}()

	_, createdJobs, err := ClaimRecurringSchedule(
		ctx,
		pool,
		s.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(createdJobs) != 3 {
		t.Fatalf(
			"expected 3 created jobs, got %d",
			len(createdJobs),
		)
	}

	expectedRunTimes := []time.Time{
		start,
		start.Add(5 * time.Minute),
		start.Add(10 * time.Minute),
	}

	for i, createdJob := range createdJobs {
		expected := expectedRunTimes[i].Truncate(time.Microsecond)

		if !createdJob.RunAt.Equal(expected) {
			t.Fatalf(
				"job %d: expected RunAt %v, got %v",
				i,
				expected,
				createdJob.RunAt,
			)
		}
	}

	var nextRunAt time.Time

	err = pool.QueryRow(
		ctx,
		`SELECT next_run_at
		 FROM recurring_schedules
		 WHERE id = $1`,
		s.ID,
	).Scan(&nextRunAt)
	if err != nil {
		t.Fatal(err)
	}

	expectedNextRun := start.Add(15 * time.Minute)

	if !nextRunAt.Equal(expectedNextRun.Truncate(time.Microsecond)) {
		t.Fatalf(
			"expected next_run_at %v, got %v",
			expectedNextRun,
			nextRunAt,
		)
	}
}
func TestSaveAndGetCronSchedule(t *testing.T) {
	ctx := context.Background()

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	s, err := schedule.NewCronSchedule(
		"daily-report",
		"0 9 * * *",
		"Asia/Kolkata",
		time.Date(
			2026,
			10,
			26,
			9,
			0,
			0,
			0,
			time.FixedZone("IST", 5*60*60+30*60),
		),
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, s); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	got, err := GetRecurringSchedule(ctx, pool, s.ID)
	if err != nil {
		t.Fatalf("get cron schedule: %v", err)
	}

	if got.Type != s.Type {
		t.Fatalf("expected type %q, got %q", s.Type, got.Type)
	}

	if got.CronExpression != "0 9 * * *" {
		t.Fatalf("expected cron expression %q, got %q",
			"0 9 * * *", got.CronExpression)
	}

	if got.Timezone != "Asia/Kolkata" {
		t.Fatalf("expected timezone %q, got %q",
			"Asia/Kolkata", got.Timezone)
	}

	if !got.NextRunAt.Equal(s.NextRunAt) {
		t.Fatalf("expected next_run_at %v, got %v",
			s.NextRunAt, got.NextRunAt)
	}
}

func TestClaimRecurringScheduleCronFireAll(t *testing.T) {
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

	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}

	nextRunAt := time.Date(
		2026, 10, 23, 9, 0, 0, 0, loc,
	)

	// now := time.Date(
	// 	2026, 10, 26, 11, 0, 0, 0, loc,
	// )

	s, err := schedule.NewCronSchedule(
		"cron-report",
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt,
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	s.CatchUpPolicy = schedule.CatchUpFireAll
	s.MaxCatchUp = 10

	if err := SaveRecurringSchedule(ctx, pool, s); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	var scheduleID int64

	err = pool.QueryRow(
		ctx,
		`SELECT id
	 FROM recurring_schedules
	 WHERE name = $1
	 ORDER BY id DESC
	 LIMIT 1`,
		s.Name,
	).Scan(&scheduleID)

	if err != nil {
		t.Fatalf("get saved cron schedule ID: %v", err)
	}

	claimed, jobs, err := ClaimRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err != nil {
		t.Fatalf("claim cron schedule: %v", err)
	}

	if len(jobs) != 4 {
		t.Fatalf("expected 4 jobs, got %d", len(jobs))
	}

	expectedRuns := []time.Time{
		time.Date(2026, 10, 23, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 24, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 25, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 26, 9, 0, 0, 0, loc),
	}

	for i, expected := range expectedRuns {
		if !jobs[i].RunAt.Equal(expected) {
			t.Fatalf(
				"job %d: expected RunAt %v, got %v",
				i,
				expected,
				jobs[i].RunAt,
			)
		}
	}

	expectedNext := time.Date(
		2026, 10, 27, 9, 0, 0, 0, loc,
	)

	if !claimed.NextRunAt.Equal(expectedNext) {
		t.Fatalf(
			"expected next_run_at %v, got %v",
			expectedNext,
			claimed.NextRunAt,
		)
	}
}
