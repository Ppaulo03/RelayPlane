package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// OutboxService publishes the transactional outbox to the CommandQueue.
//
// A message is accepted by writing the message row, its per-instance
// sequence number and its outbox entry in ONE transaction, so "accepted"
// always implies "will be published". Entries of an instance are published
// strictly in sequence order under a per-instance dispatch lock; a crash
// between publish and mark only produces a duplicate command (harmless: the
// worker claims messages with a compare-and-set). Workers additionally refuse
// to dispatch sequence N while an earlier sequence is unresolved, so even a
// late re-publication cannot be overtaken (INV-07 end to end).
type OutboxService struct{ d Deps }

const (
	outboxLockTTL = 30 * time.Second
	outboxBatch   = 100
)

func outboxKey(instanceID string) string { return "outbox:" + instanceID }

// DispatchInstance publishes the pending entries of one instance in order. It
// returns how many were published. A busy instance (another dispatcher is
// publishing it) is skipped; a publish failure stops at that entry so order is
// never violated, and is retried by the next pass.
func (o *OutboxService) DispatchInstance(ctx context.Context, instanceID string) (int, error) {
	ctx, span := observability.Start(ctx, "outbox.dispatch")
	defer span.End()
	lease, ok, err := o.d.Locker.TryLock(ctx, outboxKey(instanceID), outboxLockTTL)
	if err != nil || !ok {
		return 0, err
	}
	defer lease.Release(context.WithoutCancel(ctx)) //nolint:errcheck

	published := 0
	for {
		entries, err := o.d.Repos.Messages.ListOutbox(ctx, instanceID, outboxBatch)
		if err != nil {
			return published, err
		}
		for _, e := range entries {
			if err := o.publish(ctx, e); err != nil {
				observability.Fail(span, err)
				return published, err
			}
			published++
			if err := lease.Extend(ctx, outboxLockTTL); err != nil {
				return published, fmt.Errorf("outbox lease lost: %w", err)
			}
		}
		if len(entries) < outboxBatch {
			return published, nil
		}
	}
}

func (o *OutboxService) publish(ctx context.Context, e messaging.OutboxEntry) error {
	if err := o.d.Queue.Publish(ctx, ports.Command{ID: e.MessageID, PartitionKey: e.InstanceID, Payload: json.RawMessage(e.Command)}); err != nil {
		return fmt.Errorf("publish message %s (sequence %d): %w", e.MessageID, e.Sequence, err)
	}
	o.d.Metrics.OutboxPublished.Inc()
	return o.d.Repos.Messages.MarkOutboxDispatched(ctx, e.InstanceID, e.Sequence, o.d.now())
}

// DispatchPending publishes every instance's backlog (reconciler loop).
func (o *OutboxService) DispatchPending(ctx context.Context, limit int) (int, error) {
	ids, err := o.d.Repos.Messages.ListInstancesWithPendingOutbox(ctx, limit)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, id := range ids {
		n, err := o.DispatchInstance(ctx, id)
		total += n
		if err != nil {
			o.d.Log.WarnContext(ctx, "outbox dispatch failed", "instance_id", id, "error", err)
		}
	}
	return total, nil
}

// Redispatch publishes again the commands that were dispatched more than
// `after` ago although their message is still QUEUED (the broker lost them,
// e.g. a Redis flush). The sequence barrier in the worker keeps later messages
// of the instance behind the re-published one.
func (o *OutboxService) Redispatch(ctx context.Context, after time.Duration, limit int) (int, error) {
	stuck, err := o.d.Repos.Messages.ListStuckOutbox(ctx, o.d.now().Add(-after), limit)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range stuck {
		if err := o.publish(ctx, e); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Purge removes old dispatched entries.
func (o *OutboxService) Purge(ctx context.Context, olderThan time.Duration) (int64, error) {
	return o.d.Repos.Messages.PurgeOutbox(ctx, o.d.now().Add(-olderThan))
}
