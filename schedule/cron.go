package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

func NextCronOccurrence(
	expression string,
	timezone string,
	from time.Time,
) (time.Time, error) {
	if err := ValidateCronSchedule(expression, timezone); err != nil {
		return time.Time{}, err
	}

	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("load timezone: %w", err)
	}

	schedule, err := cron.ParseStandard(expression)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron expression: %w", err)
	}

	localTime := from.In(loc)

	next := schedule.Next(localTime)

	return next, nil
}
