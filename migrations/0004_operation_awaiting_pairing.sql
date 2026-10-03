-- A migration whose infrastructure steps are done but whose new owner still needs a QR scan
-- is AWAITING_PAIRING (an active status), not FAILED.
ALTER TABLE operations DROP CONSTRAINT operations_status_check;
ALTER TABLE operations ADD CONSTRAINT operations_status_check
    CHECK (status IN ('PENDING','RUNNING','BLOCKED','AWAITING_PAIRING','SUCCEEDED','FAILED'));

DROP INDEX one_active_migration_per_instance;
CREATE UNIQUE INDEX one_active_migration_per_instance
    ON operations (instance_id) WHERE type = 'MIGRATE' AND status IN ('PENDING','RUNNING','BLOCKED','AWAITING_PAIRING');

DROP INDEX operations_active_idx;
CREATE INDEX operations_active_idx ON operations (type, status) WHERE status IN ('PENDING','RUNNING','BLOCKED','AWAITING_PAIRING');
