package redisstreams

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// BusConfig configures the event bus.
type BusConfig struct {
	Prefix        string        // default "relayplane"
	MaxLen        int64         // retention: approximate stream cap in events, default 100000. A consumer group that falls further behind loses events (see Stats / the trim-risk alert)
	Block         time.Duration // default 500ms
	ReclaimIdle   time.Duration // pending entries idle this long are redelivered, default 5s
	MaxDeliveries int64         // after this many deliveries an event is parked in the dead-event stream, default 10
}

// Bus is the Redis Streams ports.EventBus. Semantically separate from the
// command queue: events are facts, every consumer group sees all of them.
type Bus struct {
	rdb redis.UniversalClient
	cfg BusConfig
}

// NewBus returns an event bus.
func NewBus(rdb redis.UniversalClient, cfg BusConfig) *Bus {
	if cfg.Prefix == "" {
		cfg.Prefix = "relayplane"
	}
	if cfg.MaxLen <= 0 {
		cfg.MaxLen = 100000
	}
	if cfg.Block <= 0 {
		cfg.Block = 500 * time.Millisecond
	}
	if cfg.ReclaimIdle <= 0 {
		cfg.ReclaimIdle = 5 * time.Second
	}
	if cfg.MaxDeliveries <= 0 {
		cfg.MaxDeliveries = 10
	}
	return &Bus{rdb: rdb, cfg: cfg}
}

func (b *Bus) stream() string { return b.cfg.Prefix + ":events" }
func (b *Bus) dead() string   { return b.cfg.Prefix + ":events:dead" }

// Publish implements ports.EventBus.
func (b *Bus) Publish(ctx context.Context, ev events.Event) error {
	ctx, span := observability.Start(ctx, "eventbus.publish", attribute.String("messaging.system", "redis"))
	defer span.End()
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	err = b.rdb.XAdd(ctx, &redis.XAddArgs{Stream: b.stream(), MaxLen: b.cfg.MaxLen, Approx: true,
		Values: map[string]any{"type": string(ev.EventType), "data": string(raw)}}).Err()
	observability.Fail(span, err)
	return err
}

// Subscribe implements ports.EventBus.
func (b *Bus) Subscribe(ctx context.Context, group string, h ports.EventHandler) error {
	if err := b.rdb.XGroupCreateMkStream(ctx, b.stream(), group, "0").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	host, _ := os.Hostname()
	rb := make([]byte, 4)
	_, _ = rand.Read(rb)
	consumer := fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(rb))

	process := func(m redis.XMessage) {
		raw, _ := m.Values["data"].(string)
		var ev events.Event
		if err := json.Unmarshal([]byte(raw), &ev); err != nil { // poison: park it
			b.park(ctx, group, m, "undecodable: "+err.Error())
			return
		}
		hctx, span := observability.Start(ctx, "eventbus.consume", attribute.String("messaging.system", "redis"))
		err := h(hctx, ev)
		observability.Fail(span, err)
		span.End()
		if err != nil {
			return // stays pending; redelivered by the reclaim pass
		}
		_ = b.rdb.XAck(context.WithoutCancel(ctx), b.stream(), group, m.ID).Err()
	}

	lastReclaim := time.Now()
	for ctx.Err() == nil {
		res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: consumer,
			Streams: []string{b.stream(), ">"}, Count: 20, Block: b.cfg.Block}).Result()
		if err != nil && !errors.Is(err, redis.Nil) && ctx.Err() == nil {
			if isNoGroup(err) { // Redis lost its data: recreate the group (see Queue.servePartition)
				_ = b.rdb.XGroupCreateMkStream(ctx, b.stream(), group, "0").Err()
			}
			sleepCtx(ctx, 200*time.Millisecond)
		}
		for _, s := range res {
			for _, m := range s.Messages {
				process(m)
			}
		}
		if time.Since(lastReclaim) >= b.cfg.ReclaimIdle/2 {
			lastReclaim = time.Now()
			start := "0-0"
			for ctx.Err() == nil {
				msgs, next, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: b.stream(), Group: group, Consumer: consumer,
					MinIdle: b.cfg.ReclaimIdle, Start: start, Count: 20}).Result()
				if err != nil {
					break
				}
				for _, m := range msgs {
					if n := b.deliveries(ctx, group, m.ID); n > b.cfg.MaxDeliveries {
						b.park(ctx, group, m, fmt.Sprintf("exceeded %d deliveries", b.cfg.MaxDeliveries))
						continue
					}
					process(m)
				}
				if next == "0-0" || next == "" {
					break
				}
				start = next
			}
		}
	}
	return ctx.Err()
}

// Stats implements ports.EventBusInspector: stream length, per-group lag/pending and
// how many events were trimmed away before a group could read them.
func (b *Bus) Stats(ctx context.Context) (ports.EventBusStats, error) {
	out := ports.EventBusStats{Retention: b.cfg.MaxLen}
	n, err := b.rdb.XLen(ctx, b.stream()).Result()
	if err != nil {
		return out, err
	}
	out.Length = n
	info, ierr := b.rdb.XInfoStream(ctx, b.stream()).Result()
	if ierr != nil {
		if strings.Contains(ierr.Error(), "no such key") {
			return out, nil
		}
		return out, ierr
	}
	groups, err := b.rdb.XInfoGroups(ctx, b.stream()).Result()
	if err != nil {
		return out, err
	}
	// Redis reports a group's lag only over the entries it still retains, so a group that
	// was overtaken by trimming looks healthy there. Entries that were trimmed away before
	// the group read them are derived from the stream totals instead.
	trimmed := info.EntriesAdded - n
	now, terr := b.rdb.Time(ctx).Result()
	for _, g := range groups {
		gs := ports.EventBusGroupStats{Name: g.Name, Lag: g.Lag, Pending: g.Pending}
		if trimmed > 0 && info.FirstEntry.ID != "" && idLess(g.LastDeliveredID, info.FirstEntry.ID) {
			if lost := trimmed - g.EntriesRead; lost > 0 {
				gs.Lost = lost
			}
		}
		if g.Pending > 0 && terr == nil {
			if p, perr := b.rdb.XPending(ctx, b.stream(), g.Name).Result(); perr == nil && p.Lower != "" {
				ms, _, _ := strings.Cut(p.Lower, "-")
				if t, cerr := strconv.ParseInt(ms, 10, 64); cerr == nil {
					gs.OldestPending = now.Sub(time.UnixMilli(t))
				}
			}
		}
		out.Groups = append(out.Groups, gs)
	}
	return out, nil
}

func (b *Bus) deliveries(ctx context.Context, group, id string) int64 {
	p, err := b.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: b.stream(), Group: group, Start: id, End: id, Count: 1}).Result()
	if err != nil || len(p) == 0 {
		return 0
	}
	return p[0].RetryCount
}

func (b *Bus) park(ctx context.Context, group string, m redis.XMessage, reason string) {
	bg := context.WithoutCancel(ctx)
	vals := map[string]any{"reason": reason, "group": group, "orig_id": m.ID}
	for k, v := range m.Values {
		vals[k] = v
	}
	_ = b.rdb.XAdd(bg, &redis.XAddArgs{Stream: b.dead(), MaxLen: 10000, Approx: true, Values: vals}).Err()
	_ = b.rdb.XAck(bg, b.stream(), group, m.ID).Err()
}
