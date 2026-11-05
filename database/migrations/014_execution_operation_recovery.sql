BEGIN;

ALTER TABLE execution_operations
ADD COLUMN recovery_attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE execution_operations
ADD COLUMN next_recovery_at TIMESTAMPTZ;

ALTER TABLE execution_operations
ADD COLUMN recovery_worker_id TEXT;

ALTER TABLE execution_operations
ADD COLUMN recovery_lease_until TIMESTAMPTZ;

ALTER TABLE execution_operations
ADD CONSTRAINT execution_recovery_attempt_count_valid
CHECK (recovery_attempt_count >= 0);

ALTER TABLE execution_operations
ADD CONSTRAINT execution_recovery_lease_valid
CHECK (
    (recovery_worker_id IS NULL AND recovery_lease_until IS NULL)
    OR
    (recovery_worker_id IS NOT NULL AND recovery_lease_until IS NOT NULL)
);

CREATE INDEX idx_execution_operations_due_recovery
ON execution_operations (next_recovery_at, updated_at)
WHERE status IN ('processing', 'outcome_unknown')
  AND next_recovery_at IS NOT NULL;

COMMIT;