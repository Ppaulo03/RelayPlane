-- RelayPlane initial schema.
-- Invariants enforced by the database wherever possible:
--   INV-01  one open assignment per instance (partial unique index)
--           assignment epochs strictly +1 and never decrease (triggers)
--   capacity active_instances never exceeds capacity (check constraint)
--   one active migration per instance (partial unique index)

CREATE TABLE tenants (
    id            text PRIMARY KEY,
    name          text        NOT NULL,
    api_key_hash  text        NOT NULL UNIQUE,
    rate_policy   jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE provider_nodes (
    id               text PRIMARY KEY,
    provider         text        NOT NULL,
    provider_version text        NOT NULL DEFAULT '',
    endpoint         text        NOT NULL,
    capacity         integer     NOT NULL,
    active_instances integer     NOT NULL DEFAULT 0,
    status           text        NOT NULL DEFAULT 'STARTING'
                     CHECK (status IN ('STARTING','READY','DEGRADED','DRAINING','OFFLINE')),
    heartbeat_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT capacity_non_negative   CHECK (capacity >= 0),
    CONSTRAINT active_within_capacity  CHECK (active_instances >= 0 AND active_instances <= capacity)
);
CREATE INDEX provider_nodes_placement_idx ON provider_nodes (provider, status);

CREATE TABLE instances (
    id                      text PRIMARY KEY,
    tenant_id               text        NOT NULL REFERENCES tenants(id),
    name                    text        NOT NULL,
    provider                text        NOT NULL,
    provider_instance_id    text        NOT NULL DEFAULT '',
    node_id                 text        REFERENCES provider_nodes(id),
    assignment_epoch        bigint      NOT NULL DEFAULT 0 CHECK (assignment_epoch >= 0),
    desired_state           text        NOT NULL
                            CHECK (desired_state IN ('CONNECTED','DISCONNECTED','DELETED')),
    observed_state          text        NOT NULL
                            CHECK (observed_state IN ('ALLOCATING','CREATING','AWAITING_PAIRING','CONNECTING',
                                   'CONNECTED','DISCONNECTED','RECONNECTING','LOGGED_OUT','MIGRATING',
                                   'DELETING','DELETED','FAILED')),
    last_provider_heartbeat timestamptz,
    last_status_change      timestamptz NOT NULL DEFAULT now(),
    rate_policy             jsonb,
    reconciled_at           timestamptz NOT NULL DEFAULT 'epoch',
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    deleted_at              timestamptz,
    -- an instance without an owner keeps its last epoch; an owned one has epoch >= 1
    CONSTRAINT owned_has_epoch CHECK (node_id IS NULL OR assignment_epoch >= 1)
);
CREATE INDEX instances_tenant_idx     ON instances (tenant_id) WHERE deleted_at IS NULL;
CREATE INDEX instances_node_idx       ON instances (node_id)   WHERE deleted_at IS NULL;
CREATE INDEX instances_reconcile_idx  ON instances (reconciled_at) WHERE deleted_at IS NULL;

CREATE TABLE instance_assignments (
    id             bigserial   PRIMARY KEY,
    instance_id    text        NOT NULL REFERENCES instances(id),
    node_id        text        NOT NULL REFERENCES provider_nodes(id),
    epoch          bigint      NOT NULL CHECK (epoch >= 1),
    assigned_at    timestamptz NOT NULL DEFAULT now(),
    released_at    timestamptz,
    release_reason text,
    CONSTRAINT assignment_epoch_unique UNIQUE (instance_id, epoch),
    CONSTRAINT release_consistency CHECK ((released_at IS NULL) = (release_reason IS NULL))
);
-- INV-01: at most one open (unreleased) assignment per instance.
CREATE UNIQUE INDEX one_open_assignment_per_instance
    ON instance_assignments (instance_id) WHERE released_at IS NULL;
CREATE INDEX instance_assignments_node_idx ON instance_assignments (node_id);

-- Epochs are monotonic: each new assignment is exactly previous + 1.
CREATE FUNCTION enforce_assignment_epoch() RETURNS trigger AS $$
DECLARE
    prev bigint;
BEGIN
    SELECT COALESCE(MAX(epoch), 0) INTO prev FROM instance_assignments WHERE instance_id = NEW.instance_id;
    IF NEW.epoch <> prev + 1 THEN
        RAISE EXCEPTION 'assignment epoch must be % (got %) for instance %', prev + 1, NEW.epoch, NEW.instance_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER instance_assignments_epoch BEFORE INSERT ON instance_assignments
    FOR EACH ROW EXECUTE FUNCTION enforce_assignment_epoch();

-- Released assignments are history: they can only be closed, never reopened or rewritten.
CREATE FUNCTION protect_assignment_history() RETURNS trigger AS $$
BEGIN
    IF OLD.released_at IS NOT NULL THEN
        RAISE EXCEPTION 'released assignment history is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.instance_id <> OLD.instance_id OR NEW.node_id <> OLD.node_id OR NEW.epoch <> OLD.epoch THEN
        RAISE EXCEPTION 'assignment identity is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER instance_assignments_immutable BEFORE UPDATE ON instance_assignments
    FOR EACH ROW EXECUTE FUNCTION protect_assignment_history();

CREATE FUNCTION enforce_instance_epoch() RETURNS trigger AS $$
BEGIN
    IF NEW.assignment_epoch < OLD.assignment_epoch THEN
        RAISE EXCEPTION 'assignment_epoch can never decrease' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER instances_epoch_monotonic BEFORE UPDATE ON instances
    FOR EACH ROW EXECUTE FUNCTION enforce_instance_epoch();

CREATE TABLE operations (
    id             text PRIMARY KEY,
    tenant_id      text        NOT NULL REFERENCES tenants(id),
    instance_id    text        REFERENCES instances(id),
    type           text        NOT NULL,
    status         text        NOT NULL CHECK (status IN ('PENDING','RUNNING','BLOCKED','SUCCEEDED','FAILED')),
    step           text        NOT NULL DEFAULT '',
    target_node_id text        NOT NULL DEFAULT '',
    source_node_id text        NOT NULL DEFAULT '',
    source_epoch   bigint      NOT NULL DEFAULT 0,
    error_code     text        NOT NULL DEFAULT '',
    error_message  text        NOT NULL DEFAULT '',
    attempts       integer     NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    completed_at   timestamptz
);
CREATE INDEX operations_instance_idx ON operations (instance_id, type);
CREATE INDEX operations_active_idx   ON operations (type, status) WHERE status IN ('PENDING','RUNNING','BLOCKED');
-- At most one active migration per instance.
CREATE UNIQUE INDEX one_active_migration_per_instance
    ON operations (instance_id) WHERE type = 'MIGRATE' AND status IN ('PENDING','RUNNING','BLOCKED');

CREATE TABLE outbound_messages (
    id                  text PRIMARY KEY,
    tenant_id           text        NOT NULL REFERENCES tenants(id),
    instance_id         text        NOT NULL REFERENCES instances(id),
    idempotency_key     text        NOT NULL DEFAULT '',
    node_id             text        NOT NULL,
    assignment_epoch    bigint      NOT NULL,
    partition_key       text        NOT NULL,
    recipient           text        NOT NULL,
    type                text        NOT NULL,
    payload             jsonb       NOT NULL,
    status              text        NOT NULL
                        CHECK (status IN ('QUEUED','DISPATCHING','ACCEPTED','DELIVERED','READ','FAILED','UNKNOWN')),
    provider_message_id text        NOT NULL DEFAULT '',
    attempt_count       integer     NOT NULL DEFAULT 0,
    error_code          text        NOT NULL DEFAULT '',
    error_message       text        NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT partition_is_instance CHECK (partition_key = instance_id)
);
CREATE INDEX outbound_messages_instance_idx ON outbound_messages (instance_id, created_at);
CREATE INDEX outbound_messages_provider_idx ON outbound_messages (instance_id, provider_message_id)
    WHERE provider_message_id <> '';
CREATE INDEX outbound_messages_queued_idx   ON outbound_messages (updated_at) WHERE status = 'QUEUED';
CREATE UNIQUE INDEX outbound_messages_idem_idx ON outbound_messages (tenant_id, idempotency_key)
    WHERE idempotency_key <> '';

CREATE TABLE idempotency_keys (
    tenant_id    text        NOT NULL,
    key          text        NOT NULL,
    request_hash text        NOT NULL,
    operation    text        NOT NULL,
    resource_id  text        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED')),
    result       jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, key)
);
CREATE INDEX idempotency_keys_expiry_idx ON idempotency_keys (expires_at);

CREATE TABLE event_deduplication (
    key         text PRIMARY KEY,
    instance_id text        NOT NULL,
    status      text        NOT NULL CHECK (status IN ('IN_FLIGHT','PUBLISHED')),
    claimed_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL
);
CREATE INDEX event_deduplication_expiry_idx ON event_deduplication (expires_at);

CREATE TABLE blob_metadata (
    id           text PRIMARY KEY,
    tenant_id    text        NOT NULL REFERENCES tenants(id),
    object_key   text        NOT NULL UNIQUE,
    content_type text        NOT NULL,
    size         bigint      NOT NULL CHECK (size >= 0),
    sha256       text        NOT NULL,
    filename     text        NOT NULL DEFAULT '',
    status       text        NOT NULL CHECK (status IN ('PENDING','READY','DELETED')),
    expires_at   timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    deleted_at   timestamptz,
    -- a tenant can only own objects inside its own key namespace
    CONSTRAINT object_in_tenant_namespace CHECK (left(object_key, length(tenant_id) + 1) = tenant_id || '/')
);
CREATE INDEX blob_metadata_expiry_idx ON blob_metadata (expires_at) WHERE status <> 'DELETED';
CREATE INDEX blob_metadata_tenant_idx ON blob_metadata (tenant_id);
