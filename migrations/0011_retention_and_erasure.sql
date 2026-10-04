-- Retention and erasure of personal data.
--
-- erased_at marks a message whose recipient and content were removed (retention or an erasure request); the ledger row
-- itself (ids, status, sequence) stays so ordering and idempotency keep working.
ALTER TABLE outbound_messages ADD COLUMN erased_at timestamptz;
CREATE INDEX outbound_messages_retention_idx ON outbound_messages (created_at)
    WHERE erased_at IS NULL AND status IN ('ACCEPTED','DELIVERED','READ','FAILED');
CREATE INDEX outbound_messages_recipient_idx ON outbound_messages (tenant_id, recipient) WHERE erased_at IS NULL;

-- who an inbound attachment came from, so an erasure request finds the stored bytes even after the events are gone
ALTER TABLE blob_metadata ADD COLUMN subject text NOT NULL DEFAULT '';
CREATE INDEX blob_metadata_subject_idx ON blob_metadata (tenant_id, subject) WHERE subject <> '' AND status <> 'DELETED';

-- inbound events carry the sender in payload.from: find them by contact
CREATE INDEX webhook_deliveries_contact_idx ON webhook_deliveries (tenant_id, (event #>> '{payload,from}'));
CREATE INDEX webhook_deliveries_dead_idx ON webhook_deliveries (created_at) WHERE status = 'DEAD';
CREATE INDEX inbound_media_contact_idx ON inbound_media (tenant_id, (event #>> '{payload,from}'));
