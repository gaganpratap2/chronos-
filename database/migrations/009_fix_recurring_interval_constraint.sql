ALTER TABLE recurring_schedules
DROP CONSTRAINT recurring_schedules_interval_seconds_check;

ALTER TABLE recurring_schedules
ADD CONSTRAINT recurring_schedules_interval_seconds_check
CHECK (
    (schedule_type = 'interval' AND interval_seconds > 0)
    OR
    (schedule_type = 'cron' AND interval_seconds = 0)
);