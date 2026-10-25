CREATE TABLE recurring_schedules (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    interval_seconds BIGINT NOT NULL CHECK (interval_seconds > 0),
    next_run_at TIMESTAMPTZ NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_recurring_schedules_due
ON recurring_schedules(next_run_at)
WHERE enabled = TRUE;   