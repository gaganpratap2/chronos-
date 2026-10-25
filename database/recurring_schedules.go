package database

import (
	"context"
	"fmt"
	"time"

	"chronos/job"
	"chronos/schedule"

	"github.com/jackc/pgx/v5/pgxpool"
)

func SaveRecurringSchedule(
	ctx context.Context,
	pool *pgxpool.Pool,
	s schedule.RecurringSchedule,
) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO recurring_schedules (
			name,
			interval_seconds,
			next_run_at,
			enabled
		)
		VALUES ($1, $2, $3, $4)
	`,
		s.Name,
		int64(s.Interval.Seconds()),
		s.NextRunAt,
		s.Enabled,
	)

	if err != nil {
		return fmt.Errorf("save recurring schedule: %w", err)
	}

	return nil
}

func GetRecurringSchedule(ctx context.Context, pool *pgxpool.Pool, id int64) (schedule.RecurringSchedule, error) {

	var s schedule.RecurringSchedule
	var intervalSeconds int64

	err := pool.QueryRow(ctx, `
		SELECT
			id,
			name,
			interval_seconds,
			next_run_at,
			enabled
		FROM recurring_schedules
		WHERE id = $1
	`, id).Scan(
		&s.ID,
		&s.Name,
		&intervalSeconds,
		&s.NextRunAt,
		&s.Enabled,
	)

	if err != nil {
		return schedule.RecurringSchedule{}, fmt.Errorf(
			"get recurring schedule %d: %w",
			id,
			err,
		)
	}

	s.Interval = time.Duration(intervalSeconds) * time.Second

	return s, nil
}

func ClaimRecurringSchedule(ctx context.Context, pool *pgxpool.Pool, id int64) (schedule.RecurringSchedule, job.Job, error) {

	tx, err := pool.Begin(ctx)
	if err != nil {
		return schedule.RecurringSchedule{}, job.Job{}, fmt.Errorf(
			"begin recurring schedule claim: %w",
			err,
		)
	}
	defer tx.Rollback(ctx)

	var s schedule.RecurringSchedule
	var intervalSeconds int64

	err = tx.QueryRow(ctx, `
		SELECT
			id,
			name,
			interval_seconds,
			next_run_at,
			enabled
		FROM recurring_schedules
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(
		&s.ID,
		&s.Name,
		&intervalSeconds,
		&s.NextRunAt,
		&s.Enabled,
	)

	if err != nil {
		return schedule.RecurringSchedule{}, job.Job{}, fmt.Errorf(
			"lock recurring schedule %d: %w",
			id,
			err,
		)
	}

	s.Interval = time.Duration(intervalSeconds) * time.Second

	if !s.Enabled {
		return schedule.RecurringSchedule{}, job.Job{}, fmt.Errorf(
			"recurring schedule %d is disabled",
			id,
		)
	}

	if s.NextRunAt.After(time.Now()) {
		return schedule.RecurringSchedule{}, job.Job{}, fmt.Errorf(
			"recurring schedule %d is not due",
			id,
		)
	}

	// Generate a unique execution ID.
	var jobID int

	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (
    name,
    status,
    run_at,
    retry_count,
    max_retries
)
VALUES (
    $1,
    'Pending',
    $2,
    0,
    3
)
RETURNING id
	`,
		fmt.Sprintf(
			"%s-%s",
			s.Name,
			s.NextRunAt.UTC().Format("20060102T150405"),
		),
		s.NextRunAt,
	).Scan(&jobID)

	if err != nil {
		return schedule.RecurringSchedule{}, job.Job{}, fmt.Errorf(
			"create recurring job: %w",
			err,
		)
	}

	_, err = tx.Exec(ctx, `
		UPDATE recurring_schedules
		SET next_run_at = next_run_at + (interval_seconds * INTERVAL '1 second')
		WHERE id = $1
	`, id)

	if err != nil {
		return schedule.RecurringSchedule{}, job.Job{}, fmt.Errorf(
			"advance recurring schedule %d: %w",
			id,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return schedule.RecurringSchedule{}, job.Job{}, fmt.Errorf(
			"commit recurring schedule claim: %w",
			err,
		)
	}

	createdJob := job.Job{
		ID:     jobID,
		Name:   fmt.Sprintf("%s-%s", s.Name, s.NextRunAt.UTC().Format("20060102T150405")),
		Status: "Pending",
		RunAt:  s.NextRunAt,
	}

	return s, createdJob, nil
}
