-- Inbound attachments: the message.received event of a message with media is held here until the bytes are downloaded
-- from the provider and stored (or the attachment is rejected/failed), so the tenant never sees a half-resolved media.
CREATE TABLE inbound_media (
    id              text PRIMARY KEY,                       -- the media id shown to the tenant
    tenant_id       text        NOT NULL REFERENCES tenants(id),
    instance_id     text        NOT NULL,
    event_id        text        NOT NULL UNIQUE,            -- idempotency: one job per inbound event
    event           jsonb       NOT NULL,
    ref             jsonb       NOT NULL,                   -- provider download reference (may hold decryption keys)
    stage           text        NOT NULL DEFAULT 'DOWNLOAD' CHECK (stage IN ('DOWNLOAD','PUBLISH','DONE')),
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_until     timestamptz,
    last_error      text        NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    done_at         timestamptz
);
CREATE INDEX inbound_media_due ON inbound_media (next_attempt_at) WHERE stage <> 'DONE';
