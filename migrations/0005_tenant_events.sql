-- Tenant-facing events: durable outbox of outbound-message status changes, subscriptions and webhook deliveries.

ALTER TABLE outbound_messages ADD COLUMN accepted_at timestamptz;

-- Written in the SAME transaction as the message status change, then published to the event bus by the reconciler
-- (transactional outbox: a status change can never be lost between the database and the bus).
CREATE TABLE event_outbox (
    seq          bigserial PRIMARY KEY,
    event_id     text        NOT NULL UNIQUE,
    tenant_id    text        NOT NULL,
    instance_id  text        NOT NULL,
    event        jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX event_outbox_unpublished ON event_outbox (seq) WHERE published_at IS NULL;

CREATE TABLE subscriptions (
    id             text PRIMARY KEY,
    tenant_id      text        NOT NULL REFERENCES tenants(id),
    url            text        NOT NULL,
    event_types    text[]      NOT NULL DEFAULT '{}',   -- empty = every tenant-facing event
    instance_ids   text[]      NOT NULL DEFAULT '{}',   -- empty = every instance of the tenant
    secret_version integer     NOT NULL DEFAULT 1,
    rotated_at     timestamptz,
    active         boolean     NOT NULL DEFAULT true,
    created_at     timestamptz NOT NULL DEFAULT now(),
    CHECK (url <> '')
);
CREATE INDEX subscriptions_tenant_idx ON subscriptions (tenant_id) WHERE active;

CREATE TABLE webhook_deliveries (
    id              text PRIMARY KEY,
    subscription_id text        NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    tenant_id       text        NOT NULL,
    instance_id     text        NOT NULL,
    event_id        text        NOT NULL,
    event_type      text        NOT NULL,
    event           jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','DELIVERED','DEAD')),
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_until     timestamptz,
    last_error      text        NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    delivered_at    timestamptz,
    UNIQUE (subscription_id, event_id)
);
CREATE INDEX webhook_deliveries_due ON webhook_deliveries (next_attempt_at) WHERE status = 'PENDING';
CREATE INDEX webhook_deliveries_sub_status ON webhook_deliveries (subscription_id, status, created_at);
