-- Tombstones of erased contacts. Erasing deletes what exists at that moment; the tombstone stops the events and attachments that
-- were already in flight from recreating the contact's data afterwards. The subject is a hash of the number, never the number.
CREATE TABLE contact_erasures (
    tenant_id text        NOT NULL,
    subject   text        NOT NULL,
    erased_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, subject)
);
CREATE INDEX contact_erasures_age ON contact_erasures (erased_at);
