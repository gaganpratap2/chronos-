package schedule

import (
	"testing"
	"time"
)

func TestNewCronSchedule(t *testing.T) {
	nextRun := time.Date(
		2026,
		10,
		27,
		9,
		0,
		0,
		0,
		time.FixedZone("IST", 5*60*60+30*60),
	)

	s, err := NewCronSchedule(
		"daily-report",
		"0 9 * * *",
		"Asia/Kolkata",
		nextRun,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if s.Type != ScheduleCron {
		t.Fatalf("expected cron schedule, got %q", s.Type)
	}

	if s.CronExpression != "0 9 * * *" {
		t.Fatalf("unexpected cron expression: %q", s.CronExpression)
	}

	if s.Timezone != "Asia/Kolkata" {
		t.Fatalf("unexpected timezone: %q", s.Timezone)
	}

	if !s.NextRunAt.Equal(nextRun) {
		t.Fatalf("unexpected next run time: %v", s.NextRunAt)
	}

	if !s.Enabled {
		t.Fatal("expected schedule to be enabled")
	}
}

func TestValidateCronSchedule(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		timezone   string
		wantErr    bool
	}{
		{
			name:       "valid daily schedule",
			expression: "0 9 * * *",
			timezone:   "Asia/Kolkata",
			wantErr:    false,
		},
		{
			name:       "valid weekday schedule",
			expression: "30 18 * * 1-5",
			timezone:   "America/New_York",
			wantErr:    false,
		},
		{
			name:       "invalid cron expression",
			expression: "not-a-cron",
			timezone:   "Asia/Kolkata",
			wantErr:    true,
		},
		{
			name:       "invalid timezone",
			expression: "0 9 * * *",
			timezone:   "Not/A/Timezone",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCronSchedule(
				tt.expression,
				tt.timezone,
			)

			if (err != nil) != tt.wantErr {
				t.Fatalf(
					"ValidateCronSchedule() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
			}
		})
	}
}
