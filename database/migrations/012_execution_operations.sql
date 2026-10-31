
BEGIN;

CREATE TABLE execution_operations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    workflow_task_id BIGINT NOT NULL,

    operation_name TEXT NOT NULL,

    provider TEXT NOT NULL,

    idempotency_key VARCHAR(255) NOT NULL,

    request_hash BYTEA NOT NULL,

    status TEXT NOT NULL DEFAULT 'processing',

    fencing_token BIGINT NOT NULL DEFAULT 0,

    provider_reference TEXT,

    result BYTEA,

    last_error TEXT,

    attempt_count INTEGER NOT NULL DEFAULT 1,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    completed_at TIMESTAMPTZ,

    CONSTRAINT execution_operations_task_fkey
        FOREIGN KEY (workflow_task_id)
        REFERENCES workflow_tasks(id)
        ON DELETE RESTRICT,

    CONSTRAINT execution_operation_name_not_empty
        CHECK (BTRIM(operation_name) <> ''),

    CONSTRAINT execution_provider_not_empty
        CHECK (BTRIM(provider) <> ''),

    CONSTRAINT execution_idempotency_key_not_empty
        CHECK (
            BTRIM(idempotency_key) <> ''
            AND OCTET_LENGTH(idempotency_key) <= 255
        ),

    CONSTRAINT execution_request_hash_length
        CHECK (OCTET_LENGTH(request_hash) = 32),

    CONSTRAINT execution_status_valid
        CHECK (
            status IN (
                'processing',
                'succeeded',
                'failed',
                'outcome_unknown'
            )
        ),

    CONSTRAINT execution_fencing_token_valid
        CHECK (fencing_token >= 0),

    CONSTRAINT execution_attempt_count_valid
        CHECK (attempt_count > 0),

    CONSTRAINT execution_completion_consistent
        CHECK (
            (
                status IN ('succeeded', 'failed')
                AND completed_at IS NOT NULL
            )
            OR
            (
                status IN ('processing', 'outcome_unknown')
                AND completed_at IS NULL
            )
        ),

    CONSTRAINT execution_task_operation_unique
        UNIQUE (workflow_task_id, operation_name),

    CONSTRAINT execution_provider_key_unique
        UNIQUE (provider, idempotency_key)
);

CREATE INDEX idx_execution_operations_task
ON execution_operations (workflow_task_id);

CREATE INDEX idx_execution_operations_recovery
ON execution_operations (updated_at)
WHERE status IN ('processing', 'outcome_unknown');

COMMIT;
