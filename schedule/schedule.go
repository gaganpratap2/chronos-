package schedule

import (
	// "errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type ScheduleType string

const (
	ScheduleInterval ScheduleType = "interval"
	ScheduleCron     ScheduleType = "cron"
)

type RecurringSchedule struct {
	ID             int64
	Name           string
	Type           ScheduleType
	Interval       time.Duration
	CronExpression string
	Timezone       string
	NextRunAt      time.Time
	Enabled        bool
	CatchUpPolicy  CatchUpPolicy
	MaxCatchUp     int
	CancelledAt    *time.Time
}

type CatchUpPolicy string

const (
	CatchUpFireAll  CatchUpPolicy = "fire_all"
	CatchUpFireOnce CatchUpPolicy = "fire_once"
	CatchUpSkip     CatchUpPolicy = "skip"
)

func NewRecurringSchedule(
	name string,
	interval time.Duration,
	nextRunAt time.Time,
) (RecurringSchedule, error) {
	if name == "" {
		return RecurringSchedule{}, fmt.Errorf("name cannot be empty")
	}

	if interval <= 0 {
		return RecurringSchedule{}, fmt.Errorf("interval must be positive")
	}

	if nextRunAt.IsZero() {
		return RecurringSchedule{}, fmt.Errorf("next run time cannot be zero")
	}

	return RecurringSchedule{
		Name:          name,
		Type:          ScheduleInterval,
		Interval:      interval,
		NextRunAt:     nextRunAt,
		Enabled:       true,
		CatchUpPolicy: CatchUpSkip,
		MaxCatchUp:    100,
	}, nil
}

func NewCronSchedule(
	name string,
	expression string,
	timezone string,
	nextRunAt time.Time,
) (RecurringSchedule, error) {
	if name == "" {
		return RecurringSchedule{}, fmt.Errorf("name cannot be empty")
	}

	if expression == "" {
		return RecurringSchedule{}, fmt.Errorf("cron expression cannot be empty")
	}

	if timezone == "" {
		return RecurringSchedule{}, fmt.Errorf("timezone cannot be empty")
	}

	if nextRunAt.IsZero() {
		return RecurringSchedule{}, fmt.Errorf("next run time cannot be zero")
	}

	if err := ValidateCronSchedule(expression, timezone); err != nil {
		return RecurringSchedule{}, err
	}

	return RecurringSchedule{
		Name:           name,
		Type:           ScheduleCron,
		CronExpression: expression,
		Timezone:       timezone,
		NextRunAt:      nextRunAt,
		Enabled:        true,
		CatchUpPolicy:  CatchUpSkip,
		MaxCatchUp:     100,
	}, nil
}

func ValidateCronSchedule(
	expression string,
	timezone string,
) error {
	if expression == "" {
		return fmt.Errorf("cron expression cannot be empty")
	}

	if timezone == "" {
		return fmt.Errorf("timezone cannot be empty")
	}

	if _, err := cron.ParseStandard(expression); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	if _, err := time.LoadLocation(timezone); err != nil {
		return fmt.Errorf("invalid timezone: %w", err)
	}

	return nil
}
