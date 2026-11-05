BEGIN;

ALTER TABLE execution_operations
ADD COLUMN recovery_fencing_token BIGINT NOT NULL DEFAULT 0;

ALTER TABLE execution_operations
ADD CONSTRAINT execution_recovery_fencing_token_valid
CHECK (recovery_fencing_token >= 0);

COMMIT;