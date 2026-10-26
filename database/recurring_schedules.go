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
	scheduleType := s.Type

	if scheduleType == "" {
		scheduleType = schedule.ScheduleInterval
	}

	var cronExpression *string
	var timezone *string

	if s.CronExpression != "" {
		cronExpression = &s.CronExpression
	}

	if s.Timezone != "" {
		timezone = &s.Timezone
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO recurring_schedules (
			name,
			schedule_type,
			interval_seconds,
			cron_expression,
			timezone,
			next_run_at,
			enabled,
			catch_up_policy,
			max_catch_up
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`,
		s.Name,
		scheduleType,
		int64(s.Interval.Seconds()),
		cronExpression,
		timezone,
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

func GetRecurringSchedule(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int64,
) (schedule.RecurringSchedule, error) {
	var s schedule.RecurringSchedule
	var intervalSeconds int64
	var cronExpression *string
	var timezone *string

	err := pool.QueryRow(ctx, `
		SELECT
			id,
			name,
			schedule_type,
			interval_seconds,
			cron_expression,
			timezone,
			next_run_at,
			enabled,
			catch_up_policy,
			max_catch_up
		FROM recurring_schedules
		WHERE id = $1
	`, id).Scan(
		&s.ID,
		&s.Name,
		&s.Type,
		&intervalSeconds,
		&cronExpression,
		&timezone,
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

	if cronExpression != nil {
		s.CronExpression = *cronExpression
	}

	if timezone != nil {
		s.Timezone = *timezone
	}

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

	var cronExpression *string
	var timezone *string

	err = tx.QueryRow(ctx, `
	SELECT
		id,
		name,
		schedule_type,
		interval_seconds,
		cron_expression,
		timezone,
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
		&s.Type,
		&intervalSeconds,
		&cronExpression,
		&timezone,
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

	if cronExpression != nil {
		s.CronExpression = *cronExpression
	}

	if timezone != nil {
		s.Timezone = *timezone
	}

	s.Interval = time.Duration(intervalSeconds) * time.Second

	if !s.Enabled {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"recurring schedule %d is disabled",
			id,
		)
	}

	now := time.Now()

	var occurrences []time.Time
	var nextRunAt time.Time

	switch s.Type {
	case schedule.ScheduleInterval:
		result := schedule.CalculateCatchUp(
			s.NextRunAt,
			s.Interval,
			now,
			s.CatchUpPolicy,
			s.MaxCatchUp,
		)

		occurrences = result.Occurrences
		nextRunAt = result.NextRunAt

	case schedule.ScheduleCron:
		result, err := schedule.CalculateCronCatchUp(
			s.CronExpression,
			s.Timezone,
			s.NextRunAt,
			now,
			s.CatchUpPolicy,
			s.MaxCatchUp,
		)
		if err != nil {
			return schedule.RecurringSchedule{}, nil, fmt.Errorf(
				"calculate cron catch-up: %w",
				err,
			)
		}

		occurrences = result.Occurrences
		nextRunAt = result.NextRunAt

	default:
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"unsupported schedule type: %q",
			s.Type,
		)
	}

	if len(occurrences) == 0 {
		if nextRunAt.Equal(s.NextRunAt) {
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
			nextRunAt,
		)
		if err != nil {
			return schedule.RecurringSchedule{}, nil, fmt.Errorf(
				"advance recurring schedule %d: %w",
				id,
				err,
			)
		}

		s.NextRunAt = nextRunAt

		if err := tx.Commit(ctx); err != nil {
			return schedule.RecurringSchedule{}, nil, fmt.Errorf(
				"commit recurring schedule claim: %w",
				err,
			)
		}

		return s, nil, nil
	}

	createdJobs := make([]job.Job, 0, len(occurrences))

	for _, occurrence := range occurrences {
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
		nextRunAt,
	)
	if err != nil {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"advance recurring schedule %d: %w",
			id,
			err,
		)
	}

	s.NextRunAt = nextRunAt

	if err := tx.Commit(ctx); err != nil {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"commit recurring schedule claim: %w",
			err,
		)
	}

	return s, createdJobs, nil
}
