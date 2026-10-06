-- Internal projection no longer depends on Redis retaining an event. Publication, projection and tenant fan-out are independent
-- consumers of the same durable row, each with its own crash-recoverable lease.
ALTER TABLE event_outbox ADD COLUMN publish_lease_until timestamptz;
ALTER TABLE event_outbox ADD COLUMN projected_at timestamptz;
ALTER TABLE event_outbox ADD COLUMN projection_lease_until timestamptz;

-- Existing retained rows are deliberately left pending. Projection is idempotent and epoch/timestamp guarded, so replaying them closes
-- the exact crash window this migration fixes instead of assuming every event previously published to Redis was consumed.

CREATE INDEX event_outbox_unprojected ON event_outbox (seq) WHERE projected_at IS NULL;
