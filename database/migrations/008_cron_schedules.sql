ALTER TABLE recurring_schedules
ADD COLUMN schedule_type TEXT NOT NULL DEFAULT 'interval';

ALTER TABLE recurring_schedules
ADD COLUMN cron_expression TEXT;

ALTER TABLE recurring_schedules
ADD COLUMN timezone TEXT;

ALTER TABLE recurring_schedules
ADD CONSTRAINT recurring_schedules_type_check
CHECK (schedule_type IN ('interval', 'cron'));

ALTER TABLE recurring_schedules
ADD CONSTRAINT recurring_schedules_cron_fields_check
CHECK (
    (schedule_type = 'interval'
        AND cron_expression IS NULL
        AND timezone IS NULL)
    OR
    (schedule_type = 'cron'
        AND cron_expression IS NOT NULL
        AND timezone IS NOT NULL)
);