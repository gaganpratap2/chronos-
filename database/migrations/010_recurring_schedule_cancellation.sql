ALTER TABLE recurring_schedules
ADD COLUMN cancelled_at TIMESTAMPTZ;

ALTER TABLE recurring_schedules
ADD CONSTRAINT recurring_schedules_cancelled_disabled_check
CHECK (
    cancelled_at IS NULL
    OR enabled = FALSE
);