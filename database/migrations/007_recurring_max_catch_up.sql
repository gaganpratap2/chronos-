ALTER TABLE recurring_schedules
ADD COLUMN max_catch_up INTEGER NOT NULL DEFAULT 100;

ALTER TABLE recurring_schedules
ADD CONSTRAINT recurring_schedules_max_catch_up_check
CHECK (max_catch_up > 0);