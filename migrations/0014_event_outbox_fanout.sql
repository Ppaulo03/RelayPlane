-- The event outbox now also carries the tenant fan-out: an accepted event is "done" only when the deliveries for the tenant exist in this
-- database, not when the broker said it took it. published_at keeps meaning "the broker accepted it" (internal consumers); fanout_at means
-- "the tenant's deliveries were created". The broker is a transport, never the only durable copy in between.
ALTER TABLE event_outbox ADD COLUMN fanout_at timestamptz;
ALTER TABLE event_outbox ADD COLUMN fanout_lease_until timestamptz;
-- what was already published went through the old path, whose fan-out ran from the broker: it is done
UPDATE event_outbox SET fanout_at = published_at WHERE published_at IS NOT NULL;
CREATE INDEX event_outbox_unfanned ON event_outbox (seq) WHERE fanout_at IS NULL;
