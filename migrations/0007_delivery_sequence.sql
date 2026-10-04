-- Per (subscription, instance) delivery sequence: 1, 2, 3... without gaps, assigned when the delivery is created, so a
-- consumer can reorder what retries reordered and notice what it never received.
ALTER TABLE webhook_deliveries ADD COLUMN sequence bigint NOT NULL DEFAULT 0;

CREATE TABLE delivery_sequences (
    subscription_id text   NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    instance_id     text   NOT NULL,
    last            bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (subscription_id, instance_id)
);
