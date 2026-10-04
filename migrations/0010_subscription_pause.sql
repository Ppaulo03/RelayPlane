-- A consumer can pause its subscription (its own backpressure): deliveries keep being created and wait, none is sent
-- until it resumes. Nothing is lost, unlike deactivating.
ALTER TABLE subscriptions ADD COLUMN paused boolean NOT NULL DEFAULT false;
