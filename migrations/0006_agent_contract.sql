-- Trace context of the request that created a message: carried by its status events so a consumer's trace links to ours.
ALTER TABLE outbound_messages ADD COLUMN traceparent text NOT NULL DEFAULT '';

-- A subscription can opt out of group chats (events of groups are then never delivered to it).
ALTER TABLE subscriptions ADD COLUMN exclude_groups boolean NOT NULL DEFAULT false;
