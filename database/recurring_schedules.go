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
	var cancelledAt *time.Time

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
			max_catch_up,
			cancelled_at
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
		&cancelledAt,
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
	s.CancelledAt = cancelledAt
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
	var cancelledAt *time.Time
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
		max_catch_up,
		cancelled_at
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
		&cancelledAt,
	)

	s.CancelledAt = cancelledAt

	if s.CancelledAt != nil {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"recurring schedule %d is permanently cancelled",
			id,
		)
	}

	if !s.Enabled {
		return schedule.RecurringSchedule{}, nil, fmt.Errorf(
			"recurring schedule %d is disabled",
			id,
		)
	}

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

func SetRecurringScheduleEnabled(ctx context.Context, pool *pgxpool.Pool, id int64, enabled bool) error {
	if id <= 0 {
		return fmt.Errorf("recurring schedule ID must be positive")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin recurring schedule update: %w",
			err,
		)
	}

	defer tx.Rollback(ctx)

	var cancelledAt *time.Time

	err = tx.QueryRow(ctx, `
		SELECT cancelled_at
		FROM recurring_schedules
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&cancelledAt)

	if err != nil {
		return fmt.Errorf(
			"load recurring schedule %d: %w",
			id,
			err,
		)
	}

	if cancelledAt != nil {
		return fmt.Errorf(
			"recurring schedule %d is permanently cancelled",
			id,
		)
	}

	_, err = tx.Exec(ctx, `
		UPDATE recurring_schedules
		SET enabled = $2
		WHERE id = $1
	`, id, enabled)

	if err != nil {
		return fmt.Errorf(
			"update recurring schedule %d enabled state: %w",
			id,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit recurring schedule update: %w",
			err,
		)
	}

	return nil
}

func RescheduleIntervalSchedule(
	ctx context.Context,
	pool *pgxpool.Pool,
	id int64,
	newInterval time.Duration,
) error {
	if id <= 0 {
		return fmt.Errorf("recurring schedule ID must be positive")
	}

	if newInterval < time.Second ||
		newInterval%time.Second != 0 {
		return fmt.Errorf(
			"interval must be at least one second and use whole seconds",
		)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin schedule rescheduling: %w", err)
	}
	defer tx.Rollback(ctx)

	var scheduleType string
	var cancelledAt *time.Time

	err = tx.QueryRow(ctx, `
    SELECT schedule_type, cancelled_at
    FROM recurring_schedules
    WHERE id = $1
    FOR UPDATE
`, id).Scan(&scheduleType, &cancelledAt)

	if cancelledAt != nil {
		return fmt.Errorf(
			"recurring schedule %d is permanently cancelled",
			id,
		)
	}

	if err != nil {
		return fmt.Errorf(
			"load recurring schedule %d: %w",
			id,
			err,
		)
	}

	if scheduleType != string(schedule.ScheduleInterval) {
		return fmt.Errorf(
			"recurring schedule %d is not an interval schedule",
			id,
		)
	}

	seconds := int64(newInterval / time.Second)

	_, err = tx.Exec(ctx, `
		UPDATE recurring_schedules
SET interval_seconds = $2::bigint,
    next_run_at = NOW() + (
        $2::double precision * INTERVAL '1 second'
    )
WHERE id = $1
	`, id, seconds)
	if err != nil {
		return fmt.Errorf(
			"reschedule recurring schedule %d: %w",
			id,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit recurring schedule rescheduling: %w",
			err,
		)
	}

	return nil
}

func RescheduleCronSchedule(ctx context.Context, pool *pgxpool.Pool, id int64, cronExpression string, timezone string) error {
	if id <= 0 {
		return fmt.Errorf("recurring schedule ID must be positive")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin cron rescheduling: %w", err)
	}

	defer tx.Rollback(ctx)

	var scheduleType string
	var cancelledAt *time.Time

	err = tx.QueryRow(ctx, `
    SELECT schedule_type, cancelled_at
    FROM recurring_schedules
    WHERE id = $1
    FOR UPDATE
`, id).Scan(&scheduleType, &cancelledAt)

	if cancelledAt != nil {
		return fmt.Errorf(
			"recurring schedule %d is permanently cancelled",
			id,
		)
	}

	if err != nil {
		return fmt.Errorf(
			"load recurring schedule %d: %w",
			id,
			err,
		)
	}

	if scheduleType != string(schedule.ScheduleCron) {
		return fmt.Errorf(
			"recurring schedule %d is not a cron schedule",
			id,
		)
	}

	nextRunAt, err := schedule.NextCronOccurrence(
		cronExpression,
		timezone,
		time.Now(),
	)
	if err != nil {
		return fmt.Errorf("calculate next cron occurrence: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE recurring_schedules
		SET cron_expression = $2,
		    timezone = $3,
		    next_run_at = $4
		WHERE id = $1
	`,
		id,
		cronExpression,
		timezone,
		nextRunAt,
	)
	if err != nil {
		return fmt.Errorf(
			"reschedule cron schedule %d: %w",
			id,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cron rescheduling: %w", err)
	}

	return nil
}

func CancelRecurringSchedule(ctx context.Context, pool *pgxpool.Pool, id int64) error {

	if id <= 0 {
		return fmt.Errorf("recurring schedule ID must be positive")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin recurring schedule cancellation: %w",
			err,
		)
	}
	defer tx.Rollback(ctx)

	// Lock the schedule so cancellation cannot race with
	// claiming, enabling, or another cancellation.
	var scheduleID int64

	err = tx.QueryRow(ctx, `
		SELECT id
		FROM recurring_schedules
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&scheduleID)

	if err != nil {
		return fmt.Errorf(
			"load recurring schedule %d: %w",
			id,
			err,
		)
	}

	// COALESCE preserves the original cancellation timestamp
	// when the schedule has already been cancelled.
	_, err = tx.Exec(ctx, `
		UPDATE recurring_schedules
		SET enabled = FALSE,
		    cancelled_at = COALESCE(cancelled_at, NOW())
		WHERE id = $1
	`, id)

	if err != nil {
		return fmt.Errorf(
			"cancel recurring schedule %d: %w",
			id,
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit recurring schedule cancellation: %w",
			err,
		)
	}

	return nil

}
