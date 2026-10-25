package schedule

import (
	"errors"
	"time"
)

type RecurringSchedule struct {
	ID        int64
	Name      string
	Interval  time.Duration
	NextRunAt time.Time
	Enabled   bool
}

func NewRecurringSchedule(
	name string,
	interval time.Duration,
	nextRunAt time.Time,
) (RecurringSchedule, error) {
	if name == "" {
		return RecurringSchedule{}, errors.New("schedule name cannot be empty")
	}

	if interval <= 0 {
		return RecurringSchedule{}, errors.New("schedule interval must be greater than zero")
	}

	if nextRunAt.IsZero() {
		return RecurringSchedule{}, errors.New("next run time cannot be zero")
	}

	return RecurringSchedule{
		Name:      name,
		Interval:  interval,
		NextRunAt: nextRunAt,
		Enabled:   true,
	}, nil
}
