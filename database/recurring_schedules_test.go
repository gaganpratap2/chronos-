package database

import (
	"context"
	"fmt"
	"strings"
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

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}

	// Use a unique name and a future run time to isolate this test.
	now := time.Now().In(loc)
	nextRunAt := time.Date(
		now.Year(),
		now.Month(),
		now.Day()+1,
		9, 0, 0, 0,
		loc,
	)
	scheduleName := fmt.Sprintf(
		"daily-report-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewCronSchedule(
		scheduleName,
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt,
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	// Clean up only the schedule created by this test.
	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE name = $1`,
			scheduleName,
		)
	}()

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	// SaveRecurringSchedule doesn't return the generated database ID.
	var id int64
	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1
		 ORDER BY id DESC
		 LIMIT 1`,
		scheduleName,
	).Scan(&id)
	if err != nil {
		t.Fatalf("get saved cron schedule ID: %v", err)
	}

	got, err := GetRecurringSchedule(ctx, pool, id)
	if err != nil {
		t.Fatalf("get cron schedule: %v", err)
	}

	if got.ID != id {
		t.Fatalf("expected ID %d, got %d", id, got.ID)
	}

	if got.Name != original.Name {
		t.Fatalf("expected name %q, got %q", original.Name, got.Name)
	}

	if got.Type != original.Type {
		t.Fatalf("expected type %q, got %q", original.Type, got.Type)
	}

	if got.CronExpression != original.CronExpression {
		t.Fatalf(
			"expected cron expression %q, got %q",
			original.CronExpression,
			got.CronExpression,
		)
	}

	if got.Timezone != original.Timezone {
		t.Fatalf(
			"expected timezone %q, got %q",
			original.Timezone,
			got.Timezone,
		)
	}

	if !got.NextRunAt.Equal(original.NextRunAt) {
		t.Fatalf(
			"expected next_run_at %v, got %v",
			original.NextRunAt,
			got.NextRunAt,
		)
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

	scheduleName := fmt.Sprintf(
		"cron-report-%d",
		time.Now().UnixNano(),
	)

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM jobs WHERE name LIKE $1`,
			scheduleName+"-%",
		)

		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE name = $1`,
			scheduleName,
		)
	}()

	s, err := schedule.NewCronSchedule(
		scheduleName,
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt,
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	s.CatchUpPolicy = schedule.CatchUpFireAll
	s.MaxCatchUp = 4

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

func TestDisabledRecurringScheduleDoesNotCreateJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	testID := int(time.Now().UnixNano()%800_000_000) + 1_000_000_000
	scheduleName := fmt.Sprintf("disabled-schedule-%d", testID)

	s, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, s); err != nil {
		t.Fatalf("save schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	// Clean up only records created by this test.
	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM jobs WHERE name LIKE $1`,
			scheduleName+"-%",
		)
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	// Simulate a job that was already created before cancellation.
	existingJobName := scheduleName + "-already-created"
	_, err = pool.Exec(ctx, `
		INSERT INTO jobs (
			id, name, status, run_at, retry_count, max_retries
		)
		VALUES ($1, $2, 'Pending', NOW() + INTERVAL '1 hour', 0, 3)
	`, testID, existingJobName)
	if err != nil {
		t.Fatalf("insert existing job: %v", err)
	}

	// Disable future generation for this schedule.
	if err := SetRecurringScheduleEnabled(
		ctx,
		pool,
		scheduleID,
		false,
	); err != nil {
		t.Fatalf("disable schedule: %v", err)
	}

	// A disabled schedule must not generate new jobs.
	_, _, err = ClaimRecurringSchedule(ctx, pool, scheduleID)
	if err == nil {
		t.Fatal("expected disabled schedule claim to fail")
	}

	// The pre-existing job must remain intact.
	var status string
	err = pool.QueryRow(
		ctx,
		`SELECT status FROM jobs WHERE id = $1`,
		testID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read existing job: %v", err)
	}

	if status != "Pending" {
		t.Fatalf("expected existing job to remain Pending, got %q", status)
	}

	// No additional jobs should have appeared for this schedule.
	var count int
	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM jobs WHERE name LIKE $1`,
		scheduleName+"-%",
	).Scan(&count)
	if err != nil {
		t.Fatalf("count schedule jobs: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 existing job and no new jobs, got %d", count)
	}
}

func TestRescheduleIntervalSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"reschedule-interval-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create recurring schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save recurring schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	// Reschedule from 5 minutes to 15 minutes.
	before := time.Now()

	err = RescheduleIntervalSchedule(
		ctx,
		pool,
		scheduleID,
		15*time.Minute,
	)
	if err != nil {
		t.Fatalf("reschedule interval: %v", err)
	}

	updated, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get rescheduled schedule: %v", err)
	}

	if updated.Interval != 15*time.Minute {
		t.Fatalf(
			"expected interval 15m, got %v",
			updated.Interval,
		)
	}

	if !updated.Enabled {
		t.Fatal("expected schedule to remain enabled")
	}

	// next_run_at should be approximately 15 minutes from now.
	remaining := time.Until(updated.NextRunAt)

	if remaining < 14*time.Minute+50*time.Second ||
		remaining > 15*time.Minute+5*time.Second {
		t.Fatalf(
			"expected next_run_at approximately 15 minutes from now, got %v (before rescheduling at %v)",
			remaining,
			before,
		)
	}
}

func TestRescheduleIntervalScheduleRejectsInvalidIntervals(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"invalid-interval-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	invalidIntervals := []time.Duration{
		0,
		500 * time.Millisecond,
		1500 * time.Millisecond,
	}

	for _, interval := range invalidIntervals {
		t.Run(interval.String(), func(t *testing.T) {
			err := RescheduleIntervalSchedule(
				ctx,
				pool,
				scheduleID,
				interval,
			)
			if err == nil {
				t.Fatalf(
					"expected interval %v to be rejected",
					interval,
				)
			}

			// An invalid request must not modify the saved schedule.
			saved, err := GetRecurringSchedule(ctx, pool, scheduleID)
			if err != nil {
				t.Fatalf("get schedule after rejected request: %v", err)
			}

			if saved.Interval != 5*time.Minute {
				t.Errorf(
					"expected interval to remain 5m, got %v",
					saved.Interval,
				)
			}
		})
	}
}

func TestRescheduleIntervalScheduleRejectsCronSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}

	scheduleName := fmt.Sprintf(
		"cron-type-protection-%d",
		time.Now().UnixNano(),
	)
	nextRunAt := time.Now().Add(24 * time.Hour).Truncate(time.Microsecond)

	original, err := schedule.NewCronSchedule(
		scheduleName,
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt.In(loc),
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	err = RescheduleIntervalSchedule(
		ctx,
		pool,
		scheduleID,
		15*time.Minute,
	)
	if err == nil {
		t.Fatal("expected interval rescheduling to reject a cron schedule")
	}

	// Verify that the rejected operation did not alter the schedule.
	saved, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get cron schedule after rejection: %v", err)
	}

	if saved.Type != schedule.ScheduleCron {
		t.Fatalf("expected cron type to remain unchanged, got %q", saved.Type)
	}

	if saved.CronExpression != original.CronExpression {
		t.Fatalf(
			"cron expression changed: expected %q, got %q",
			original.CronExpression,
			saved.CronExpression,
		)
	}

	if saved.Timezone != original.Timezone {
		t.Fatalf(
			"timezone changed: expected %q, got %q",
			original.Timezone,
			saved.Timezone,
		)
	}
}

func TestRescheduleCronSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"reschedule-cron-%d",
		time.Now().UnixNano(),
	)

	initialRun := time.Now().Add(24 * time.Hour)

	original, err := schedule.NewCronSchedule(
		scheduleName,
		"0 9 * * *",
		"Asia/Kolkata",
		initialRun,
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	before := time.Now()
	newExpression := "* * * * *"

	err = RescheduleCronSchedule(
		ctx,
		pool,
		scheduleID,
		newExpression,
		"Asia/Kolkata",
	)
	if err != nil {
		t.Fatalf("reschedule cron schedule: %v", err)
	}

	updated, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get rescheduled cron schedule: %v", err)
	}

	if updated.Type != schedule.ScheduleCron {
		t.Fatalf("expected cron schedule type, got %q", updated.Type)
	}

	if updated.CronExpression != newExpression {
		t.Fatalf(
			"expected expression %q, got %q",
			newExpression,
			updated.CronExpression,
		)
	}

	if updated.Timezone != "Asia/Kolkata" {
		t.Fatalf(
			"expected timezone Asia/Kolkata, got %q",
			updated.Timezone,
		)
	}

	if !updated.NextRunAt.After(before) {
		t.Fatalf(
			"expected next_run_at to be in the future, got %v",
			updated.NextRunAt,
		)
	}

	// The new expression runs every minute, so the next
	// occurrence should be aligned to a minute boundary.
	localNextRun := updated.NextRunAt.In(
		time.FixedZone("IST", 5*60*60+30*60),
	)

	if localNextRun.Second() != 0 ||
		localNextRun.Nanosecond() != 0 {
		t.Fatalf(
			"expected next run at the start of a minute, got %v",
			localNextRun,
		)
	}

	if !updated.Enabled {
		t.Fatal("expected cron schedule to remain enabled")
	}
}

func TestRescheduleCronScheduleRejectsInvalidConfiguration(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"invalid-cron-reschedule-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewCronSchedule(
		scheduleName,
		"0 9 * * *",
		"Asia/Kolkata",
		time.Now().Add(24*time.Hour),
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	before, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get original schedule: %v", err)
	}

	tests := []struct {
		name           string
		cronExpression string
		timezone       string
	}{
		{
			name:           "invalid cron expression",
			cronExpression: "not a valid cron expression",
			timezone:       "Asia/Kolkata",
		},
		{
			name:           "invalid timezone",
			cronExpression: "*/5 * * * *",
			timezone:       "Invalid/Timezone",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := RescheduleCronSchedule(
				ctx,
				pool,
				scheduleID,
				tc.cronExpression,
				tc.timezone,
			)
			if err == nil {
				t.Fatal("expected rescheduling to fail")
			}

			after, err := GetRecurringSchedule(
				ctx,
				pool,
				scheduleID,
			)
			if err != nil {
				t.Fatalf("get schedule after rejected rescheduling: %v", err)
			}

			if after.CronExpression != before.CronExpression {
				t.Errorf(
					"cron expression changed: got %q, want %q",
					after.CronExpression,
					before.CronExpression,
				)
			}

			if after.Timezone != before.Timezone {
				t.Errorf(
					"timezone changed: got %q, want %q",
					after.Timezone,
					before.Timezone,
				)
			}

			if !after.NextRunAt.Equal(before.NextRunAt) {
				t.Errorf(
					"next_run_at changed: got %v, want %v",
					after.NextRunAt,
					before.NextRunAt,
				)
			}
		})
	}
}

func TestRescheduleCronScheduleRejectsMissingSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	// This large, positive ID is used only for this missing-record test.
	missingID := time.Now().UnixNano()

	err = RescheduleCronSchedule(
		ctx,
		pool,
		missingID,
		"*/5 * * * *",
		"Asia/Kolkata",
	)
	if err == nil {
		t.Fatal("expected an error when rescheduling a nonexistent schedule")
	}
}
func TestRescheduleCronScheduleRejectsIntervalSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"reschedule-interval-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create recurring schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save recurring schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id
		 FROM recurring_schedules
		 WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	// Save the original configuration before attempting the invalid operation.
	before, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get interval schedule before rescheduling: %v", err)
	}

	if before.Type != schedule.ScheduleInterval {
		t.Fatalf("expected interval schedule, got %q", before.Type)
	}

	if before.Interval != 5*time.Minute {
		t.Fatalf("expected initial interval 5m, got %v", before.Interval)
	}

	// A cron rescheduling function must reject an interval schedule.
	err = RescheduleCronSchedule(
		ctx,
		pool,
		scheduleID,
		"*/5 * * * *",
		"Asia/Kolkata",
	)
	if err == nil {
		t.Fatal("expected cron rescheduling to reject an interval schedule")
	}

	// Confirm the rejected operation did not change the stored configuration.
	after, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get interval schedule after rescheduling: %v", err)
	}

	if after.Type != before.Type {
		t.Errorf(
			"schedule type changed: got %q, want %q",
			after.Type,
			before.Type,
		)
	}

	if after.Interval != before.Interval {
		t.Errorf(
			"interval changed: got %v, want %v",
			after.Interval,
			before.Interval,
		)
	}

	if after.Enabled != before.Enabled {
		t.Errorf(
			"enabled state changed: got %v, want %v",
			after.Enabled,
			before.Enabled,
		)
	}

	if !after.NextRunAt.Equal(before.NextRunAt) {
		t.Errorf(
			"next_run_at changed: got %v, want %v",
			after.NextRunAt,
			before.NextRunAt,
		)
	}
}

func TestCancelRecurringSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"cancel-schedule-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create recurring schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save recurring schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	// First cancellation.
	if err := CancelRecurringSchedule(ctx, pool, scheduleID); err != nil {
		t.Fatalf("cancel recurring schedule: %v", err)
	}

	cancelled, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get cancelled schedule: %v", err)
	}

	if cancelled.Enabled {
		t.Fatal("expected cancelled schedule to be disabled")
	}

	if cancelled.CancelledAt == nil {
		t.Fatal("expected cancellation timestamp to be recorded")
	}

	firstCancellation := *cancelled.CancelledAt

	// Wait briefly so an accidental timestamp overwrite is detectable.
	time.Sleep(20 * time.Millisecond)

	// Second cancellation should succeed without changing the timestamp.
	if err := CancelRecurringSchedule(ctx, pool, scheduleID); err != nil {
		t.Fatalf("repeated cancellation should succeed: %v", err)
	}

	cancelledAgain, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get schedule after repeated cancellation: %v", err)
	}

	if cancelledAgain.CancelledAt == nil {
		t.Fatal("expected cancellation timestamp to remain present")
	}

	if !cancelledAgain.CancelledAt.Equal(firstCancellation) {
		t.Fatalf(
			"cancellation timestamp changed: first=%v, second=%v",
			firstCancellation,
			*cancelledAgain.CancelledAt,
		)
	}

	if cancelledAgain.Enabled {
		t.Fatal("expected schedule to remain disabled after repeated cancellation")
	}
}

func TestCancelledRecurringScheduleCannotBeResumedOrClaimed(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"cancelled-lifecycle-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create recurring schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save recurring schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM jobs WHERE name LIKE $1`,
			scheduleName+"-%",
		)

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	// Make the schedule due so a claim would generate jobs
	// if cancellation protection were missing.
	_, err = pool.Exec(ctx, `
		UPDATE recurring_schedules
		SET next_run_at = NOW() - INTERVAL '10 minutes'
		WHERE id = $1
	`, scheduleID)
	if err != nil {
		t.Fatalf("make schedule due: %v", err)
	}

	// Permanently cancel the schedule.
	if err := CancelRecurringSchedule(ctx, pool, scheduleID); err != nil {
		t.Fatalf("cancel recurring schedule: %v", err)
	}

	// The application-level function must reject resuming it.
	err = SetRecurringScheduleEnabled(
		ctx,
		pool,
		scheduleID,
		true,
	)
	if err == nil {
		t.Fatal("expected cancelled schedule to reject re-enabling")
	}

	// The database constraint must also reject direct re-enabling.
	_, err = pool.Exec(ctx, `
		UPDATE recurring_schedules
		SET enabled = TRUE
		WHERE id = $1
	`, scheduleID)
	if err == nil {
		t.Fatal("expected database constraint to reject re-enabling")
	}

	// A direct claim must not generate jobs.
	_, createdJobs, err := ClaimRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err == nil {
		t.Fatal("expected cancelled schedule claim to fail")
	}

	if len(createdJobs) != 0 {
		t.Fatalf(
			"expected zero jobs, got %d",
			len(createdJobs),
		)
	}

	// Verify the permanent cancellation state remains intact.
	saved, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get cancelled schedule: %v", err)
	}

	if saved.Enabled {
		t.Fatal("cancelled schedule became enabled")
	}

	if saved.CancelledAt == nil {
		t.Fatal("expected cancellation timestamp to remain present")
	}

	// Verify that no recurring jobs were generated.
	var jobCount int
	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM jobs WHERE name LIKE $1`,
		scheduleName+"-%",
	).Scan(&jobCount)
	if err != nil {
		t.Fatalf("count generated jobs: %v", err)
	}

	if jobCount != 0 {
		t.Fatalf("expected zero generated jobs, got %d", jobCount)
	}
}

func TestConcurrentCancelAndEnableRecurringSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	name := fmt.Sprintf(
		"concurrent-cancel-enable-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		name,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create recurring schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save recurring schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		name,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	start := make(chan struct{})

	cancelResult := make(chan error, 1)
	enableResult := make(chan error, 1)

	// Request A: permanently cancel the schedule.
	go func() {
		<-start
		cancelResult <- CancelRecurringSchedule(
			ctx,
			pool,
			scheduleID,
		)
	}()

	// Request B: attempt to enable the schedule.
	go func() {
		<-start
		enableResult <- SetRecurringScheduleEnabled(
			ctx,
			pool,
			scheduleID,
			true,
		)
	}()

	// Release both requests at approximately the same time.
	close(start)

	cancelErr := <-cancelResult
	enableErr := <-enableResult

	if cancelErr != nil {
		t.Fatalf("cancellation failed: %v", cancelErr)
	}

	// Enabling may succeed if it commits before cancellation.
	// Otherwise, it must be rejected because cancellation won.
	if enableErr != nil &&
		!strings.Contains(
			enableErr.Error(),
			"permanently cancelled",
		) {
		t.Fatalf("unexpected enable error: %v", enableErr)
	}

	saved, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get final schedule state: %v", err)
	}

	if saved.CancelledAt == nil {
		t.Fatal("expected schedule to be permanently cancelled")
	}

	if saved.Enabled {
		t.Fatal("schedule must remain disabled after concurrent cancellation")
	}
}

func TestCancelledRecurringSchedulesCannotBeRescheduled(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	// Create an interval schedule.
	intervalName := fmt.Sprintf(
		"cancelled-interval-%d",
		time.Now().UnixNano(),
	)

	intervalSchedule, err := schedule.NewRecurringSchedule(
		intervalName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create interval schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, intervalSchedule); err != nil {
		t.Fatalf("save interval schedule: %v", err)
	}

	var intervalID int64
	err = pool.QueryRow(ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		intervalName,
	).Scan(&intervalID)
	if err != nil {
		t.Fatalf("get interval schedule ID: %v", err)
	}

	// Create a cron schedule.
	cronName := fmt.Sprintf(
		"cancelled-cron-%d",
		time.Now().UnixNano(),
	)

	cronSchedule, err := schedule.NewCronSchedule(
		cronName,
		"0 9 * * *",
		"Asia/Kolkata",
		time.Now().Add(24*time.Hour),
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, cronSchedule); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	var cronID int64
	err = pool.QueryRow(ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		cronName,
	).Scan(&cronID)
	if err != nil {
		t.Fatalf("get cron schedule ID: %v", err)
	}

	// Clean up only the schedules created by this test.
	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM recurring_schedules WHERE id IN ($1, $2)`,
			intervalID,
			cronID,
		)
	}()

	// Permanently cancel both schedules.
	if err := CancelRecurringSchedule(ctx, pool, intervalID); err != nil {
		t.Fatalf("cancel interval schedule: %v", err)
	}

	if err := CancelRecurringSchedule(ctx, pool, cronID); err != nil {
		t.Fatalf("cancel cron schedule: %v", err)
	}

	// Save their original configuration.
	beforeInterval, err := GetRecurringSchedule(ctx, pool, intervalID)
	if err != nil {
		t.Fatalf("get cancelled interval schedule: %v", err)
	}

	beforeCron, err := GetRecurringSchedule(ctx, pool, cronID)
	if err != nil {
		t.Fatalf("get cancelled cron schedule: %v", err)
	}

	// Attempt to reschedule the cancelled interval schedule.
	err = RescheduleIntervalSchedule(
		ctx,
		pool,
		intervalID,
		15*time.Minute,
	)
	if err == nil {
		t.Error("expected cancelled interval schedule to reject rescheduling")
	}

	afterInterval, err := GetRecurringSchedule(ctx, pool, intervalID)
	if err != nil {
		t.Fatalf("get interval schedule after rescheduling attempt: %v", err)
	}

	if afterInterval.Interval != beforeInterval.Interval {
		t.Errorf(
			"cancelled interval changed: got %v, want %v",
			afterInterval.Interval,
			beforeInterval.Interval,
		)
	}

	if !afterInterval.NextRunAt.Equal(beforeInterval.NextRunAt) {
		t.Error("cancelled interval next_run_at was modified")
	}

	// Attempt to reschedule the cancelled cron schedule.
	err = RescheduleCronSchedule(
		ctx,
		pool,
		cronID,
		"*/5 * * * *",
		"UTC",
	)
	if err == nil {
		t.Error("expected cancelled cron schedule to reject rescheduling")
	}

	afterCron, err := GetRecurringSchedule(ctx, pool, cronID)
	if err != nil {
		t.Fatalf("get cron schedule after rescheduling attempt: %v", err)
	}

	if afterCron.CronExpression != beforeCron.CronExpression {
		t.Errorf(
			"cancelled cron expression changed: got %q, want %q",
			afterCron.CronExpression,
			beforeCron.CronExpression,
		)
	}

	if afterCron.Timezone != beforeCron.Timezone {
		t.Errorf(
			"cancelled cron timezone changed: got %q, want %q",
			afterCron.Timezone,
			beforeCron.Timezone,
		)
	}

	if !afterCron.NextRunAt.Equal(beforeCron.NextRunAt) {
		t.Error("cancelled cron next_run_at was modified")
	}
}

func TestCancelRecurringSchedulePreservesExistingJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"cancel-preserve-jobs-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create recurring schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save recurring schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM jobs WHERE name LIKE $1`,
			scheduleName+"-%",
		)

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	// Make the schedule due and use fire_once to generate
	// one job for the missed occurrence.
	_, err = pool.Exec(ctx, `
		UPDATE recurring_schedules
		SET next_run_at = NOW() - INTERVAL '5 minutes',
		    catch_up_policy = 'fire_once'
		WHERE id = $1
	`, scheduleID)
	if err != nil {
		t.Fatalf("make schedule due: %v", err)
	}

	// Generate a recurring job before cancellation.
	_, createdJobs, err := ClaimRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err != nil {
		t.Fatalf("claim recurring schedule: %v", err)
	}

	if len(createdJobs) == 0 {
		t.Fatal("expected at least one job to be generated")
	}

	// Confirm generated jobs are pending.
	for _, createdJob := range createdJobs {
		var status string

		err := pool.QueryRow(
			ctx,
			`SELECT status FROM jobs WHERE id = $1`,
			createdJob.ID,
		).Scan(&status)
		if err != nil {
			t.Fatalf(
				"get generated job %d: %v",
				createdJob.ID,
				err,
			)
		}

		if status != "Pending" {
			t.Fatalf(
				"expected job %d to be Pending, got %q",
				createdJob.ID,
				status,
			)
		}
	}

	// Permanently cancel the recurring schedule.
	if err := CancelRecurringSchedule(ctx, pool, scheduleID); err != nil {
		t.Fatalf("cancel recurring schedule: %v", err)
	}

	// Existing jobs must remain pending.
	for _, createdJob := range createdJobs {
		var status string

		err := pool.QueryRow(
			ctx,
			`SELECT status FROM jobs WHERE id = $1`,
			createdJob.ID,
		).Scan(&status)
		if err != nil {
			t.Fatalf(
				"get existing job %d after cancellation: %v",
				createdJob.ID,
				err,
			)
		}

		if status != "Pending" {
			t.Errorf(
				"existing job %d changed status after cancellation: got %q, want Pending",
				createdJob.ID,
				status,
			)
		}
	}
}

func TestConcurrentCancelAndClaimRecurringSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := godotenv.Load("../.env"); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	pool, err := Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	scheduleName := fmt.Sprintf(
		"concurrent-cancel-claim-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		scheduleName,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create recurring schedule: %v", err)
	}

	if err := SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save recurring schedule: %v", err)
	}

	var scheduleID int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		scheduleName,
	).Scan(&scheduleID)
	if err != nil {
		t.Fatalf("get schedule ID: %v", err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM jobs WHERE name LIKE $1`,
			scheduleName+"-%",
		)

		_, _ = pool.Exec(
			cleanupCtx,
			`DELETE FROM recurring_schedules WHERE id = $1`,
			scheduleID,
		)
	}()

	// Make the schedule due. fire_once ensures that this
	// claim creates at most one job for the missed occurrence.
	_, err = pool.Exec(ctx, `
		UPDATE recurring_schedules
		SET next_run_at = NOW() - INTERVAL '1 hour',
		    catch_up_policy = 'fire_once'
		WHERE id = $1
	`, scheduleID)
	if err != nil {
		t.Fatalf("make schedule due: %v", err)
	}

	start := make(chan struct{})

	cancelResult := make(chan error, 1)

	type claimOutcome struct {
		jobCount int
		err      error
	}

	claimResult := make(chan claimOutcome, 1)

	// Goroutine A: permanently cancel the schedule.
	// go func() {
	// 	<-start

	// 	claimResult <- claimOutcome{
	// 		jobCount: 0,
	// 		err:      nil,
	// 	}
	// }()

	go func() {
		<-start

		cancelResult <- CancelRecurringSchedule(
			ctx,
			pool,
			scheduleID,
		)
	}()

	// Goroutine B: attempt to generate recurring jobs.
	go func() {
		<-start

		_, jobs, err := ClaimRecurringSchedule(
			ctx,
			pool,
			scheduleID,
		)

		claimResult <- claimOutcome{
			jobCount: len(jobs),
			err:      err,
		}
	}()

	close(start)

	cancelErr := <-cancelResult
	if cancelErr != nil {
		t.Fatalf("cancel recurring schedule: %v", cancelErr)
	}

	claim := <-claimResult

	// Claim may succeed before cancellation, or be rejected
	// after cancellation. No other result is acceptable.
	if claim.err != nil &&
		!strings.Contains(
			claim.err.Error(),
			"permanently cancelled",
		) {
		t.Fatalf("unexpected claim error: %v", claim.err)
	}

	if claim.err == nil && claim.jobCount == 0 {
		t.Fatal("expected claim to generate a job or return a cancellation error")
	}

	if claim.err != nil && claim.jobCount != 0 {
		t.Fatalf(
			"claim returned an error but reported %d jobs",
			claim.jobCount,
		)
	}

	// Verify the actual number of persisted jobs.
	var persistedJobs int
	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM jobs WHERE name LIKE $1`,
		scheduleName+"-%",
	).Scan(&persistedJobs)
	if err != nil {
		t.Fatalf("count persisted recurring jobs: %v", err)
	}

	if persistedJobs != claim.jobCount {
		t.Fatalf(
			"persisted jobs=%d, claim reported=%d",
			persistedJobs,
			claim.jobCount,
		)
	}

	// Cancellation must win in the final state.
	saved, err := GetRecurringSchedule(ctx, pool, scheduleID)
	if err != nil {
		t.Fatalf("get final recurring schedule: %v", err)
	}

	if saved.CancelledAt == nil {
		t.Fatal("expected permanent cancellation timestamp")
	}

	if saved.Enabled {
		t.Fatal("expected schedule to remain disabled")
	}
}
