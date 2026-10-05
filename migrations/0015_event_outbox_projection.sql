-- The projector (delivery receipts -> message status, session state -> catalog) now works from this table too, like the tenant fan-out:
-- projected_at means "the catalog applied it". Events the projector does not care about are born projected, so the column is a plain
-- "is there work left" flag. What was already published went through the old path (the broker): it is done.
ALTER TABLE event_outbox ADD COLUMN projected_at timestamptz;
ALTER TABLE event_outbox ADD COLUMN projection_lease_until timestamptz;
UPDATE event_outbox SET projected_at = COALESCE(published_at, created_at)
 WHERE published_at IS NOT NULL OR event ->> 'event_type' NOT IN ('message.status', 'instance.status_changed');
CREATE INDEX event_outbox_unprojected ON event_outbox (seq) WHERE projected_at IS NULL;
