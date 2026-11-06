
BEGIN;

CREATE TABLE scheduler_leadership (
    singleton_id SMALLINT PRIMARY KEY DEFAULT 1,

    instance_id TEXT REFERENCES scheduler_instances(id),

    fencing_token BIGINT NOT NULL DEFAULT 0,

    lease_until TIMESTAMPTZ,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT scheduler_leadership_singleton_valid
        CHECK (singleton_id = 1),

    CONSTRAINT scheduler_leadership_fencing_token_valid
        CHECK (fencing_token >= 0),

    CONSTRAINT scheduler_leadership_owner_lease_valid
        CHECK (
            (instance_id IS NULL AND lease_until IS NULL)
            OR
            (instance_id IS NOT NULL AND lease_until IS NOT NULL)
        )
);

-- Create the single leadership record.
-- The row remains even when leadership is released.
INSERT INTO scheduler_leadership (
    singleton_id,
    fencing_token
)
VALUES (1, 0);

COMMIT;
