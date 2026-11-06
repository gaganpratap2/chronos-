BEGIN;

CREATE TABLE scheduler_instances (
    id TEXT PRIMARY KEY,

    hostname TEXT NOT NULL,

    status TEXT NOT NULL DEFAULT 'running',

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    lease_until TIMESTAMPTZ NOT NULL,

    stopped_at TIMESTAMPTZ,

    CONSTRAINT scheduler_instance_status_valid
        CHECK (status IN ('running', 'stopped')),

    CONSTRAINT scheduler_instance_stopped_valid
        CHECK (
            (status = 'running' AND stopped_at IS NULL)
            OR
            (status = 'stopped' AND stopped_at IS NOT NULL)
        )
);

CREATE INDEX idx_scheduler_instances_active_lease
ON scheduler_instances (lease_until)
WHERE status = 'running';

COMMIT;