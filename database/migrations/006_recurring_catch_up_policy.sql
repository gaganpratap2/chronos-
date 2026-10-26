ALTER TABLE recurring_schedules
ADD COLUMN catch_up_policy TEXT NOT NULL DEFAULT 'skip';

ALTER TABLE recurring_schedules
ADD CONSTRAINT recurring_schedules_catch_up_policy_check
CHECK (catch_up_policy IN ('fire_all', 'fire_once', 'skip'));