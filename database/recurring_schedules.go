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
			enabled,
			catch_up_policy,
			max_catch_up
		)
		VALUES ($1, $2, $3, $4 , $5 , $6)
	`,
		s.Name,
		int64(s.Interval.Seconds()),
		s.NextRunAt,
		s.Enabled,
		s.CatchUpPolicy,
		s.MaxCatchUp,
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
			enabled,
			catch_up_policy,
			max_catch_up
		FROM recurring_schedules
		WHERE id = $1
	`, id).Scan(
		&s.ID,
		&s.Name,
		&intervalSeconds,
		&s.NextRunAt,
		&s.Enabled,
		&s.CatchUpPolicy,
		&s.MaxCatchUp,
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

func ClaimRecurringSchedule(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int64,
) (schedule.RecurringSchedule, []job.Job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
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
			enabled,
			catch_up_policy,
			max_catch_up
		FROM recurring_schedules
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(
		&s.ID,
		&s.Name,
		&intervalSeconds,
		&s.NextRunAt,
		&s.Enabled,
		&s.CatchUpPolicy,
		&s.MaxCatchUp,
	)
	if err != nil {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"lock recurring schedule %d: %w",
			id,
			err,
		)
	}

	s.Interval = time.Duration(intervalSeconds) * time.Second

	fmt.Printf(
		"DEBUG ClaimRecurringSchedule: id=%d next_run_at=%v interval=%v policy=%q max_catch_up=%d now=%v\n",
		s.ID,
		s.NextRunAt,
		s.Interval,
		s.CatchUpPolicy,
		s.MaxCatchUp,
		time.Now(),
	)

	if !s.Enabled {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"recurring schedule %d is disabled",
			id,
		)
	}

	now := time.Now()

	result := schedule.CalculateCatchUp(
		s.NextRunAt,
		s.Interval,
		now,
		s.CatchUpPolicy,
		s.MaxCatchUp,
	)

	if len(result.Occurrences) == 0 {
		if result.NextRunAt.Equal(s.NextRunAt) {
			return schedule.RecurringSchedule{}, nil, fmt.Errorf(
				"recurring schedule %d is not due",
				id,
			)
		}

		_, err = tx.Exec(ctx, `
			UPDATE recurring_schedules
			SET next_run_at = $2
			WHERE id = $1
		`,
			id,
			result.NextRunAt,
		)
		if err != nil {
			return schedule.RecurringSchedule{}, nil, fmt.Errorf(
				"advance recurring schedule %d: %w",
				id,
				err,
			)
		}

		s.NextRunAt = result.NextRunAt

		if err := tx.Commit(ctx); err != nil {
			return schedule.RecurringSchedule{}, nil, fmt.Errorf(
				"commit recurring schedule claim: %w",
				err,
			)
		}

		return s, nil, nil
	}

	createdJobs := make([]job.Job, 0, len(result.Occurrences))

	for _, occurrence := range result.Occurrences {
		jobName := fmt.Sprintf(
			"%s-%s",
			s.Name,
			occurrence.UTC().Format("20060102T150405"),
		)

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
			jobName,
			occurrence,
		).Scan(&jobID)

		if err != nil {
			return schedule.RecurringSchedule{}, nil, fmt.Errorf(
				"create recurring job: %w",
				err,
			)
		}

		createdJobs = append(createdJobs, job.Job{
			ID:     jobID,
			Name:   jobName,
			Status: "Pending",
			RunAt:  occurrence,
		})
	}

	_, err = tx.Exec(ctx, `
		UPDATE recurring_schedules
		SET next_run_at = $2
		WHERE id = $1
	`,
		id,
		result.NextRunAt,
	)
	if err != nil {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"advance recurring schedule %d: %w",
			id,
			err,
		)
	}

	s.NextRunAt = result.NextRunAt

	if err := tx.Commit(ctx); err != nil {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"commit recurring schedule claim: %w",
			err,
		)
	}

	return s, createdJobs, nil
}
