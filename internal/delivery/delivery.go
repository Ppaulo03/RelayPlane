// Package delivery moves tenant-facing events from the event bus to the tenants' webhook destinations:
//
//	EventBus --FanOut--> webhook_deliveries --Dispatcher--> signed POST (at-least-once, retries, DLQ)
//
// Guarantees: a delivery row is created once per (subscription, event) however many times the bus redelivers the
// event; a delivery is retried with backoff until it succeeds or lands in the DLQ; a destination that keeps failing
// is shielded by a circuit breaker so it cannot consume the worker; the consumer must dedupe by event id.
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// FanOut is the EventBus consumer that turns one event into one delivery per matching subscription.
type FanOut struct {
	Repos   ports.Repositories
	Log     *slog.Logger
	Now     func() time.Time
	Metrics *observability.Metrics

	ErasureKey []byte // keys the erasure tombstones (events.ErasureSubject)

	Batch int           // events claimed per pass (default 100)
	Lease time.Duration // how long a claimed event is exclusive (default 10s: a worker that dies leaves it to another)
	Poll  time.Duration // pause when idle (default 100ms)
}

func (f *FanOut) defaults() {
	if f.Batch <= 0 {
		f.Batch = 100
	}
	if f.Lease <= 0 {
		f.Lease = 10 * time.Second
	}
	if f.Poll <= 0 {
		f.Poll = 100 * time.Millisecond
	}
	if f.Log == nil {
		f.Log = slog.Default()
	}
}

// Run turns the accepted events of the event outbox into the tenant's deliveries until ctx is cancelled. It reads the DATABASE, not the
// broker: the outbox row stays until the deliveries exist, so a broker that loses what it was given loses nothing the tenant is owed.
func (f *FanOut) Run(ctx context.Context) {
	f.defaults()
	for ctx.Err() == nil {
		n, err := f.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			f.Log.WarnContext(ctx, "webhook fan-out pass failed", "error", err)
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(f.Poll):
			}
		}
	}
}

// RunOnce claims and fans out one batch, oldest first; it returns how many events it finished. An event that fails stops the batch (the
// ones after it wait for their turn) and is claimed again when its lease expires: Handle is idempotent, so doing it twice is safe.
func (f *FanOut) RunOnce(ctx context.Context) (int, error) {
	f.defaults()
	evs, err := f.Repos.Events.ClaimForFanOut(ctx, f.Batch, f.Lease)
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	done := make([]string, 0, len(evs))
	var herr error
	for _, ev := range evs {
		if herr = f.Handle(ctx, ev); herr != nil {
			break
		}
		done = append(done, ev.EventID)
	}
	at := time.Now()
	if f.Now != nil {
		at = f.Now()
	}
	if err := f.Repos.Events.MarkFannedOut(context.WithoutCancel(ctx), done, at.UTC()); err != nil {
		return len(done), err
	}
	return len(done), herr
}

// Handle creates the deliveries of one event for the tenant's subscriptions. Enqueue is idempotent, so running it again for the same event
// (a retry after a failure, a worker that died after creating them) is always safe.
func (f *FanOut) Handle(ctx context.Context, ev events.Event) error {
	if !subscription.TenantFacing(ev.EventType) || ev.TenantID == "" {
		return nil
	}
	// the contact was erased after this message was accepted: the event was already on its way, it must not bring the data back
	if gone, err := ports.ErasedEvent(ctx, f.Repos.Erasures, f.ErasureKey, ev); err != nil {
		return err
	} else if gone {
		f.Log.InfoContext(ctx, "event of an erased contact dropped before delivery", "event_id", ev.EventID)
		return nil
	}
	subs, err := f.Repos.Subscriptions.ListActive(ctx, ev.TenantID)
	if err != nil {
		return err
	}
	now := time.Now()
	if f.Now != nil {
		now = f.Now()
	}
	var ds []subscription.Delivery
	for _, s := range subs {
		if !s.Matches(ev) {
			continue
		}
		ds = append(ds, subscription.Delivery{ID: ids.New("dlv"), SubscriptionID: s.ID, TenantID: ev.TenantID, InstanceID: ev.InstanceID,
			EventID: ev.EventID, EventType: ev.EventType, Event: ev, Status: subscription.DeliveryPending, CreatedAt: now.UTC(), NextAttemptAt: now.UTC()})
	}
	if len(ds) == 0 {
		return nil
	}
	if _, err = f.Repos.Deliveries.Enqueue(ctx, ds); err != nil {
		return err
	}
	// write first, ask after: an erasure that landed while the deliveries were being created marked the contact BEFORE it deleted, so
	// either it deleted them or this check sees the mark
	if gone, err := ports.ErasedEvent(ctx, f.Repos.Erasures, f.ErasureKey, ev); err != nil {
		return err
	} else if gone {
		_, err := f.Repos.Deliveries.DeleteByEvent(ctx, ev.EventID)
		return err
	}
	return nil
}

// Dispatcher claims due deliveries and POSTs them.
type Dispatcher struct {
	Repos     ports.Repositories
	Sender    ports.WebhookSender
	ServerKey []byte
	// ErasureKey keys the erasure tombstones: a delivery of a contact erased after it was claimed is dropped before it is sent.
	ErasureKey []byte
	Retry      subscription.RetryPolicy
	Metrics    *observability.Metrics
	Log        *slog.Logger
	Now        func() time.Time
	// Rand returns a number in [0,1) for retry jitter (default math/rand).
	Rand func() float64

	BatchSize   int // deliveries claimed per pass (default 4x Concurrency: 32 POSTs of at most 5 s on 8 workers finish in 20 s, inside the 30 s lease)
	Concurrency int // parallel POSTs (default 8)
	// Lease is how long a claimed delivery is exclusive (default 30s). It is also how long the deliveries of a worker that DIED wait
	// for another one (and, through the per-instance order, everything behind them): keep it a few times the POST timeout, no more.
	Lease          time.Duration
	RequestTimeout time.Duration // per POST (default 5s)
	Poll           time.Duration // pause when idle (default 250ms)
	// MaxInFlightPerSubscription caps the POSTs one subscription can have going at the same time (default 32), so a
	// consumer that answers slowly cannot take every dispatcher slot from the others.
	MaxInFlightPerSubscription int

	Breaker *Breaker
}

func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *Dispatcher) defaults() {
	if d.Concurrency <= 0 {
		d.Concurrency = 8
	}
	if d.BatchSize <= 0 {
		d.BatchSize = 4 * d.Concurrency
	}
	if d.Lease <= 0 {
		d.Lease = 30 * time.Second
	}
	if d.RequestTimeout <= 0 {
		d.RequestTimeout = 5 * time.Second
	}
	if d.Poll <= 0 {
		d.Poll = 250 * time.Millisecond
	}
	if d.MaxInFlightPerSubscription <= 0 {
		d.MaxInFlightPerSubscription = 32
	}
	if d.Rand == nil {
		d.Rand = rand.Float64
	}
	if d.Breaker == nil {
		d.Breaker = NewBreaker(5, 30*time.Second, 5*time.Minute)
	}
	if len(d.Retry.Schedule) == 0 {
		d.Retry = subscription.DefaultRetry()
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Metrics == nil {
		d.Metrics = observability.NewMetrics()
	}
}

// Run dispatches until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	d.defaults()
	for ctx.Err() == nil {
		n, err := d.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			d.Log.WarnContext(ctx, "webhook dispatch pass failed", "error", err)
		}
		if n == 0 {
			select {
			case <-ctx.Done():
			case <-time.After(d.Poll):
			}
		}
	}
}

// RunOnce claims and processes one batch; it returns how many deliveries were handled.
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	d.defaults()
	claimed, err := d.Repos.Deliveries.ClaimDueWith(ctx, d.now(), d.Lease, d.BatchSize, d.MaxInFlightPerSubscription)
	if err != nil || len(claimed) == 0 {
		return 0, err
	}
	sem := make(chan struct{}, d.Concurrency)
	var wg sync.WaitGroup
	for _, dl := range claimed {
		sem <- struct{}{}
		wg.Add(1)
		go func(dl subscription.Delivery) {
			defer func() { <-sem; wg.Done() }()
			d.process(ctx, dl)
		}(dl)
	}
	wg.Wait()
	d.Metrics.WebhookBreakersOpen.Set(float64(d.Breaker.OpenCount(d.now())))
	return len(claimed), nil
}

func (d *Dispatcher) process(ctx context.Context, dl subscription.Delivery) {
	ctx = observability.With(ctx, observability.KeyTenantID, dl.TenantID, observability.KeyInstanceID, dl.InstanceID)
	ctx, span := observability.Start(ctx, "webhook.deliver")
	defer span.End()
	rctx := context.WithoutCancel(ctx) // bookkeeping must survive a shutdown that interrupts the POST

	sub, err := d.Repos.Subscriptions.GetByID(rctx, dl.SubscriptionID)
	if err != nil || !sub.Active {
		_ = d.Repos.Deliveries.MarkDead(rctx, dl.ID, "subscription removed or inactive")
		d.Metrics.WebhookDeliveries.WithLabelValues("dead").Inc()
		return
	}
	// the subscription may have been paused after this delivery was claimed: pausing means NOTHING is sent. Give the lease back and wait
	if sub.Paused {
		_ = d.Repos.Deliveries.Postpone(rctx, dl.ID, d.now())
		d.Metrics.WebhookDeliveries.WithLabelValues("postponed").Inc()
		return
	}
	// the contact may have been erased after this delivery was claimed (the erasure deleted the row, but this worker holds a copy of the text):
	// nothing may go out after the erasure returned. A POST already on the wire cannot be recalled; that limit is part of the contract.
	if gone, err := ports.ErasedEvent(rctx, d.Repos.Erasures, d.ErasureKey, dl.Event); err != nil {
		_ = d.Repos.Deliveries.Postpone(rctx, dl.ID, d.now()) // cannot tell: do not send, ask again
		return
	} else if gone {
		_, _ = d.Repos.Deliveries.DeleteByEvent(rctx, dl.EventID)
		d.Metrics.WebhookDeliveries.WithLabelValues("erased").Inc()
		return
	}
	// the circuit is per SUBSCRIPTION: two tenants that happen to use the same host (a shared automation service) must not trip each other
	host := sub.ID
	now := d.now()
	if until, open := d.Breaker.Check(host, now); open {
		// the destination is known to be failing: wait without spending the delivery's retry budget
		_ = d.Repos.Deliveries.Postpone(rctx, dl.ID, until)
		d.Metrics.WebhookDeliveries.WithLabelValues("postponed").Inc()
		return
	}

	wire := dl.Event
	wire.SchemaVersion, wire.Sequence, wire.ObservedAt, wire.AcceptedAt = events.SchemaVersion, dl.Sequence, nil, nil // internal fields never leave
	body, err := json.Marshal(wire)
	if err != nil {
		_ = d.Repos.Deliveries.MarkDead(rctx, dl.ID, "event not serializable: "+err.Error())
		return
	}
	ts := now.Unix()
	var sigs []string
	for _, sec := range subscription.SigningSecrets(d.ServerKey, *sub, now) {
		sigs = append(sigs, subscription.Sign(sec, ts, body))
	}
	req := ports.WebhookRequest{URL: sub.URL, Body: body, Timeout: d.RequestTimeout, Headers: map[string]string{
		"Content-Type":               "application/json",
		"User-Agent":                 "RelayPlane-Webhooks/1",
		subscription.HeaderEventID:   dl.EventID,
		subscription.HeaderEventType: string(dl.EventType),
		subscription.HeaderTimestamp: strconv.FormatInt(ts, 10),
		subscription.HeaderSignature: subscription.SignatureHeader(sigs...),
		subscription.HeaderAttempt:   strconv.Itoa(dl.Attempts + 1),
		subscription.HeaderClaim:     strconv.Itoa(max(dl.Claims, 1)),
	}}
	// link the consumer's trace to ours: the event's own trace (a message status carries the trace of the send), or this
	// delivery's span when the event started at the provider (an inbound message has no upstream trace)
	if tp := dl.Event.TraceParent; tp != "" {
		req.Headers["traceparent"] = tp
	} else if tp := observability.TraceParent(ctx); tp != "" {
		req.Headers["traceparent"] = tp
	}
	start := time.Now()
	status, serr := d.Sender.Send(ctx, req)
	d.Metrics.WebhookLatency.Observe(time.Since(start).Seconds())

	switch {
	case serr == nil && status >= 200 && status < 300:
		d.Breaker.Success(host)
		if err := d.Repos.Deliveries.MarkDelivered(rctx, dl.ID, d.now()); err != nil {
			d.Log.ErrorContext(ctx, "could not record a delivered webhook (it will be sent again)", "delivery_id", dl.ID, "error", err)
			return
		}
		d.Metrics.WebhookDeliveries.WithLabelValues("delivered").Inc()
		// first: the healthy path. recovered: no failed attempt, but a worker died holding the delivery and its lease had to
		// expire (that latency is the lease, bounded and reported apart). retry: it failed and was sent again after a backoff.
		attempt := "retry"
		switch {
		case dl.Attempts == 0 && dl.Claims <= 1:
			attempt = "first"
		case dl.Attempts == 0:
			attempt = "recovered"
		}
		// the origin of an outbound status is the database clock: a few hundred milliseconds of skew against this host must
		// not make the sample disappear, so a "negative" lag counts as zero (the skew is the measurement error)
		lag := d.now().Sub(dl.Event.LagOrigin(dl.CreatedAt))
		if lag < 0 {
			lag = 0
		}
		d.Metrics.EventDeliveryLag.WithLabelValues(string(dl.EventType), attempt).Observe(lag.Seconds())
	case errors.Is(serr, errs.ErrDestinationBlocked):
		// permanent: no retry can make a forbidden destination acceptable
		_ = d.Repos.Deliveries.MarkDead(rctx, dl.ID, serr.Error())
		d.Metrics.WebhookDeliveries.WithLabelValues("dead").Inc()
		d.Log.WarnContext(ctx, "webhook destination refused", "subscription_id", sub.ID, "error", serr)
	default:
		d.Breaker.Failure(host, d.now())
		msg := failureText(status, serr)
		if delay, ok := d.Retry.Next(dl.Attempts+1, d.Rand()); ok {
			_ = d.Repos.Deliveries.MarkRetry(rctx, dl.ID, d.now().Add(delay), msg)
			d.Metrics.WebhookDeliveries.WithLabelValues("retry").Inc()
			return
		}
		_ = d.Repos.Deliveries.MarkDead(rctx, dl.ID, msg)
		d.Metrics.WebhookDeliveries.WithLabelValues("dead").Inc()
		d.Log.WarnContext(ctx, "webhook delivery exhausted its retries (DLQ)", "subscription_id", sub.ID, "delivery_id", dl.ID, "error", msg)
	}
}

func failureText(status int, err error) string {
	if err != nil {
		t := err.Error()
		if len(t) > 300 {
			t = t[:300]
		}
		return "transport error: " + t
	}
	return fmt.Sprintf("http %d", status)
}

// ---- circuit breaker ----

// Breaker opens a destination after `threshold` consecutive failures and keeps it open for an exponentially
// growing cool-down (base .. max). While open, deliveries are postponed without counting an attempt, so one dead
// endpoint cannot burn the retry budgets of everything queued for it nor occupy the dispatcher's concurrency.
// State is per process (each worker protects itself); that is deliberate: it needs no coordination.
type Breaker struct {
	threshold int
	base, max time.Duration
	mu        sync.Mutex
	hosts     map[string]*breakerState
}

type breakerState struct {
	failures int
	trips    int
	openTill time.Time
}

// NewBreaker returns a breaker.
func NewBreaker(threshold int, base, max time.Duration) *Breaker {
	return &Breaker{threshold: threshold, base: base, max: max, hosts: map[string]*breakerState{}}
}

// Check reports whether the destination is open and until when. After the cool-down it lets requests through again
// ("half-open"): the next success closes it, the next failure re-opens it for longer.
func (b *Breaker) Check(host string, now time.Time) (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.hosts[host]
	if s == nil || !now.Before(s.openTill) {
		return time.Time{}, false
	}
	return s.openTill, true
}

// Failure records a failed attempt.
func (b *Breaker) Failure(host string, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.hosts[host]
	if s == nil {
		s = &breakerState{}
		b.hosts[host] = s
	}
	s.failures++
	if s.failures >= b.threshold {
		cool := b.base << s.trips
		if cool > b.max || cool <= 0 {
			cool = b.max
		}
		s.trips++
		s.openTill = now.Add(cool)
		s.failures = b.threshold - 1 // half-open: one more failure re-opens it
	}
}

// Success closes the destination.
func (b *Breaker) Success(host string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.hosts, host)
}

// OpenCount is how many destinations are currently open.
func (b *Breaker) OpenCount(now time.Time) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, s := range b.hosts {
		if now.Before(s.openTill) {
			n++
		}
	}
	return n
}
