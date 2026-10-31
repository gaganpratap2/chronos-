
BEGIN;

CREATE TABLE idempotency_records (
    operation_scope TEXT NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    request_hash BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'processing',
    job_id INTEGER,
    response_status SMALLINT,
    response_content_type TEXT,
    response_body BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
        DEFAULT (NOW() + INTERVAL '24 hours'),

    CONSTRAINT idempotency_records_pkey
        PRIMARY KEY (operation_scope, idempotency_key),

    CONSTRAINT idempotency_scope_not_empty
        CHECK (BTRIM(operation_scope) <> ''),

    CONSTRAINT idempotency_key_not_empty
        CHECK (BTRIM(idempotency_key) <> ''),

    CONSTRAINT idempotency_request_hash_length
        CHECK (OCTET_LENGTH(request_hash) = 32),

    CONSTRAINT idempotency_status_valid
        CHECK (status IN ('processing', 'completed')),

    CONSTRAINT idempotency_expiration_valid
        CHECK (expires_at > created_at),

    CONSTRAINT idempotency_state_consistent
        CHECK (
            (
                status = 'processing'
                AND job_id IS NULL
                AND response_status IS NULL
                AND response_content_type IS NULL
                AND response_body IS NULL
            )
            OR
            (
                status = 'completed'
                AND job_id IS NOT NULL
                AND response_status IS NOT NULL
                AND response_status BETWEEN 100 AND 599
                AND response_content_type IS NOT NULL
                AND BTRIM(response_content_type) <> ''
                AND response_body IS NOT NULL
            )
        ),

    CONSTRAINT idempotency_job_fkey
        FOREIGN KEY (job_id)
        REFERENCES jobs(id)
);

CREATE INDEX idx_idempotency_records_completed_expiry
ON idempotency_records (expires_at)
WHERE status = 'completed';

COMMIT;
