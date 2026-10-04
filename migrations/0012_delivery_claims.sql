-- How many times a delivery was leased by a dispatcher. More than one means a worker died (or hung) holding it and the lease had to
-- expire before another could send it: the delivery latency of those is the lease, not the healthy path.
ALTER TABLE webhook_deliveries ADD COLUMN claims integer NOT NULL DEFAULT 0;
