CREATE TABLE IF NOT EXISTS development_provider_receipts (
    idempotency_key TEXT PRIMARY KEY
        CHECK (length(btrim(idempotency_key)) > 0),

    request_hash BYTEA NOT NULL
        CHECK (octet_length(request_hash) = 32),

    provider_reference TEXT NOT NULL UNIQUE,

    result JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);