-- Transactional outbox + per-instance dispatch sequence (INV-07 end to end).
--
-- sequence_no is allocated from instances.next_sequence in the same transaction
-- that inserts the message; the UPDATE holds the instance row lock until commit,
-- so sequence order == commit order and numbers are gapless.

ALTER TABLE instances ADD COLUMN next_sequence bigint NOT NULL DEFAULT 1 CHECK (next_sequence >= 1);

ALTER TABLE outbound_messages ADD COLUMN sequence_no bigint NOT NULL DEFAULT 0;
-- rows created before this migration keep 0 (no ordering information)
CREATE UNIQUE INDEX outbound_messages_sequence_idx ON outbound_messages (instance_id, sequence_no) WHERE sequence_no > 0;
CREATE INDEX outbound_messages_unresolved_idx ON outbound_messages (instance_id, sequence_no)
    WHERE status IN ('QUEUED','DISPATCHING','UNKNOWN');

CREATE TABLE outbox (
    instance_id   text        NOT NULL REFERENCES instances(id),
    sequence_no   bigint      NOT NULL,
    message_id    text        NOT NULL UNIQUE REFERENCES outbound_messages(id),
    command       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    dispatched_at timestamptz,
    PRIMARY KEY (instance_id, sequence_no)
);
CREATE INDEX outbox_pending_idx ON outbox (instance_id, sequence_no) WHERE dispatched_at IS NULL;
CREATE INDEX outbox_dispatched_idx ON outbox (dispatched_at) WHERE dispatched_at IS NOT NULL;
