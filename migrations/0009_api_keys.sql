-- A tenant holds several API keys at once so credentials can be rotated without downtime; keys can be revoked and show
-- when they were last used. Only hashes are stored.
CREATE TABLE api_keys (
    id           text PRIMARY KEY,
    tenant_id    text        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name         text        NOT NULL DEFAULT '',
    key_prefix   text        NOT NULL DEFAULT '',
    key_hash     text        NOT NULL UNIQUE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz,
    last_used_at timestamptz,
    revoked_at   timestamptz
);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);

-- the key every existing tenant already has becomes its first api_keys row
INSERT INTO api_keys (id, tenant_id, name, key_hash, created_at)
SELECT left('key_' || api_key_hash, 28), id, 'initial', api_key_hash, created_at FROM tenants;

-- authentication goes through api_keys from now on; the column stays only as history
ALTER TABLE tenants ALTER COLUMN api_key_hash DROP NOT NULL;
