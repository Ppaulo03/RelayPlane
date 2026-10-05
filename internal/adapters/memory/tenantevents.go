package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

type outboxEvent struct {
	ev         events.Event
	created    time.Time
	published  time.Time
	fanout     time.Time // the tenant's deliveries exist
	leaseUntil time.Time // a fan-out worker holds it until then
	projected  time.Time // the catalog applied it (born set when the catalog has nothing to learn from the event)
	projLease  time.Time // a projector holds it until then
}

// emitOutbound records the tenant-facing event of a message that just entered m.Status. It runs inside the
// caller's critical section, i.e. atomically with the status change (the in-memory analogue of the SQL transaction).
func (s *Store) emitOutbound(m *messaging.Message) {
	if !m.Status.NotifiesTenant() {
		return
	}
	provider := ""
	if i, ok := s.instances[m.InstanceID]; ok {
		provider = i.Provider
	}
	s.queueEvent(messaging.OutboundStatusEvent(*m, provider))
}

// queueEvent adds an event to the outbox unless it is already there. The caller holds s.mu.
func (s *Store) queueEvent(ev events.Event) {
	for _, e := range s.eventOutbox {
		if e.ev.EventID == ev.EventID {
			return
		}
	}
	oe := &outboxEvent{ev: ev, created: s.Now()}
	if !events.NeedsProjection(ev.EventType) {
		oe.projected = oe.created
	}
	s.eventOutbox = append(s.eventOutbox, oe)
}

// ---- events outbox ----

type eventsRepo struct{ s *Store }

func (r eventsRepo) ListUnpublished(_ context.Context, limit int) ([]events.Event, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []events.Event
	for _, e := range r.s.eventOutbox {
		if e.published.IsZero() {
			out = append(out, e.ev)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r eventsRepo) ClaimForFanOut(_ context.Context, limit int, lease time.Duration) ([]events.Event, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	now := r.s.Now()
	var out []events.Event
	for _, e := range r.s.eventOutbox {
		if !e.fanout.IsZero() || e.leaseUntil.After(now) {
			continue
		}
		e.leaseUntil = now.Add(lease)
		out = append(out, e.ev)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r eventsRepo) ClaimForProjection(_ context.Context, limit int, lease time.Duration) ([]events.Event, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	now := r.s.Now()
	var out []events.Event
	for _, e := range r.s.eventOutbox {
		if !e.projected.IsZero() || e.projLease.After(now) {
			continue
		}
		e.projLease = now.Add(lease)
		out = append(out, e.ev)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r eventsRepo) MarkProjected(_ context.Context, ids []string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	for _, e := range r.s.eventOutbox {
		if want[e.ev.EventID] && e.projected.IsZero() {
			e.projected, e.projLease = at, time.Time{}
		}
	}
	return nil
}

func (r eventsRepo) ProjectionStats(_ context.Context) (int64, time.Duration, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	var oldest time.Duration
	now := r.s.Now()
	for _, e := range r.s.eventOutbox {
		if !e.projected.IsZero() {
			continue
		}
		n++
		if age := now.Sub(e.created); age > oldest {
			oldest = age
		}
	}
	return n, oldest, nil
}

func (r eventsRepo) MarkFannedOut(_ context.Context, ids []string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	for _, e := range r.s.eventOutbox {
		if want[e.ev.EventID] && e.fanout.IsZero() {
			e.fanout, e.leaseUntil = at, time.Time{}
		}
	}
	return nil
}

func (r eventsRepo) MarkPublished(_ context.Context, ids []string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	for _, e := range r.s.eventOutbox {
		if want[e.ev.EventID] && e.published.IsZero() {
			e.published = at
		}
	}
	return nil
}

func (r eventsRepo) Purge(_ context.Context, before time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var keep []*outboxEvent
	var n int64
	for _, e := range r.s.eventOutbox {
		if !e.published.IsZero() && !e.fanout.IsZero() && !e.projected.IsZero() && e.published.Before(before) {
			n++
			continue
		}
		keep = append(keep, e)
	}
	r.s.eventOutbox = keep
	return n, nil
}

func (r eventsRepo) PendingStats(_ context.Context) (int64, time.Duration, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	var oldest time.Duration
	now := r.s.Now()
	for _, e := range r.s.eventOutbox {
		if !e.fanout.IsZero() {
			continue
		}
		n++
		if age := now.Sub(e.created); age > oldest {
			oldest = age
		}
	}
	return n, oldest, nil
}

func (r eventsRepo) EraseContact(_ context.Context, tenantID, number string) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var keep []*outboxEvent
	var n int64
	for _, e := range r.s.eventOutbox {
		if e.ev.TenantID == tenantID && payloadFrom(e.ev.Payload) == number {
			n++
			continue
		}
		keep = append(keep, e)
	}
	r.s.eventOutbox = keep
	return n, nil
}

// ---- subscriptions ----

type subsRepo struct{ s *Store }

func (r subsRepo) Create(ctx context.Context, sub subscription.Subscription) error {
	return r.CreateIfBelow(ctx, sub, 0)
}

func (r subsRepo) CreateIfBelow(_ context.Context, sub subscription.Subscription, max int) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if max > 0 {
		n := 0
		for _, o := range r.s.subs {
			if o.TenantID == sub.TenantID {
				n++
			}
		}
		if n >= max {
			return errs.ErrConflict
		}
	}
	if _, ok := r.s.tenants[sub.TenantID]; !ok {
		return fmt.Errorf("%w: tenant %s", errs.ErrNotFound, sub.TenantID)
	}
	if _, dup := r.s.subs[sub.ID]; dup {
		return errs.ErrAlreadyExists
	}
	c := sub
	r.s.subs[sub.ID] = &c
	return nil
}

func (r subsRepo) Get(_ context.Context, tenantID, id string) (*subscription.Subscription, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	s, ok := r.s.subs[id]
	if !ok || s.TenantID != tenantID {
		return nil, errs.ErrNotFound
	}
	c := *s
	return &c, nil
}

func (r subsRepo) GetByID(_ context.Context, id string) (*subscription.Subscription, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	s, ok := r.s.subs[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	c := *s
	return &c, nil
}

func (r subsRepo) list(tenantID string, activeOnly bool) []subscription.Subscription {
	var out []subscription.Subscription
	for _, s := range r.s.subs {
		if s.TenantID == tenantID && (!activeOnly || s.Active) {
			out = append(out, *s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt) || (out[i].CreatedAt.Equal(out[j].CreatedAt) && out[i].ID < out[j].ID)
	})
	return out
}

func (r subsRepo) ListByTenant(_ context.Context, tenantID string) ([]subscription.Subscription, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.list(tenantID, false), nil
}

func (r subsRepo) ListActive(_ context.Context, tenantID string) ([]subscription.Subscription, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.list(tenantID, true), nil
}

func (r subsRepo) SetPaused(_ context.Context, tenantID, id string, paused bool) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	s, ok := r.s.subs[id]
	if !ok || s.TenantID != tenantID {
		return errs.ErrNotFound
	}
	s.Paused = paused
	return nil
}

func (r subsRepo) RotateSecret(_ context.Context, tenantID, id string, at time.Time) (int, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	s, ok := r.s.subs[id]
	if !ok || s.TenantID != tenantID {
		return 0, errs.ErrNotFound
	}
	s.SecretVersion++
	s.RotatedAt = at
	return s.SecretVersion, nil
}

func (r subsRepo) Delete(_ context.Context, tenantID, id string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	s, ok := r.s.subs[id]
	if !ok || s.TenantID != tenantID {
		return errs.ErrNotFound
	}
	delete(r.s.subs, id)
	for k := range r.s.deliverySeq {
		if strings.HasPrefix(k, id+"|") {
			delete(r.s.deliverySeq, k)
		}
	}
	for did, d := range r.s.deliveries {
		if d.SubscriptionID == id {
			delete(r.s.deliveries, did)
		}
	}
	return nil
}

// ---- deliveries ----

type deliveryRow struct {
	subscription.Delivery
	leaseUntil time.Time
}

type deliveriesRepo struct{ s *Store }

func (r deliveriesRepo) Enqueue(_ context.Context, ds []subscription.Delivery) (int, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	n := 0
	for _, d := range ds {
		if _, ok := r.s.subs[d.SubscriptionID]; !ok {
			continue
		}
		dup := false
		for _, e := range r.s.deliveries {
			if e.SubscriptionID == d.SubscriptionID && e.EventID == d.EventID {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		if d.Status != subscription.DeliveryDead { // a delivery is born PENDING, or DEAD when it is over the subscription's backlog limit
			d.Status = subscription.DeliveryPending
		}
		d.Sequence = r.s.nextDeliverySeq(d.SubscriptionID, d.InstanceID)
		if d.CreatedAt.IsZero() {
			d.CreatedAt = r.s.Now()
		}
		if d.NextAttemptAt.IsZero() {
			d.NextAttemptAt = d.CreatedAt
		}
		r.s.deliveries[d.ID] = &deliveryRow{Delivery: d}
		n++
	}
	return n, nil
}

func (r deliveriesRepo) ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]subscription.Delivery, error) {
	return r.ClaimDueWith(ctx, now, lease, limit, 0)
}

func (r deliveriesRepo) ClaimDueWith(_ context.Context, now time.Time, lease time.Duration, limit, perSubscription int) ([]subscription.Delivery, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var due []*deliveryRow
	for _, d := range r.s.deliveries {
		if d.Status != subscription.DeliveryPending || d.NextAttemptAt.After(now) || d.leaseUntil.After(now) {
			continue
		}
		if sub, ok := r.s.subs[d.SubscriptionID]; ok && sub.Paused {
			continue // the consumer asked us to hold deliveries
		}
		due = append(due, d)
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].CreatedAt.Equal(due[j].CreatedAt) {
			return due[i].CreatedAt.Before(due[j].CreatedAt)
		}
		return due[i].ID < due[j].ID
	})
	inFlight := map[string]bool{}
	perSub := map[string]int{}
	for _, d := range r.s.deliveries {
		if d.Status == subscription.DeliveryPending && d.leaseUntil.After(now) {
			inFlight[d.SubscriptionID+"|"+d.InstanceID] = true
			perSub[d.SubscriptionID]++
		}
	}
	// within a (subscription, instance) the delivery order is the sequence order, whatever the creation times say
	head := map[string]*deliveryRow{}
	for _, d := range due {
		key := d.SubscriptionID + "|" + d.InstanceID
		if h, ok := head[key]; !ok || d.Sequence < h.Sequence {
			head[key] = d
		}
	}
	var out []subscription.Delivery
	for _, d := range due {
		key := d.SubscriptionID + "|" + d.InstanceID
		if head[key] != d {
			continue
		}
		if inFlight[key] {
			continue // best-effort ordering: one delivery per (subscription, instance) at a time
		}
		if perSubscription > 0 && perSub[d.SubscriptionID] >= perSubscription {
			continue // this consumer already has its share of POSTs in flight
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		d.leaseUntil = now.Add(lease)
		d.Claims++
		inFlight[key] = true
		perSub[d.SubscriptionID]++
		out = append(out, d.Delivery)
	}
	return out, nil
}

func (r deliveriesRepo) Backlog(_ context.Context, tenantID string, now time.Time) (map[string]ports.Backlog, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	out := map[string]ports.Backlog{}
	for _, d := range r.s.deliveries {
		if d.TenantID != tenantID || d.Status != subscription.DeliveryPending {
			continue
		}
		b := out[d.SubscriptionID]
		b.Pending++
		if age := now.Sub(d.CreatedAt); age > b.OldestPending {
			b.OldestPending = age
		}
		out[d.SubscriptionID] = b
	}
	return out, nil
}

func (r deliveriesRepo) row(id string) (*deliveryRow, error) {
	d, ok := r.s.deliveries[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	return d, nil
}

func (r deliveriesRepo) MarkDelivered(_ context.Context, id string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	d, err := r.row(id)
	if err != nil {
		return err
	}
	d.Status, d.DeliveredAt, d.leaseUntil = subscription.DeliveryDelivered, at, time.Time{}
	return nil
}

func (r deliveriesRepo) MarkRetry(_ context.Context, id string, next time.Time, lastErr string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	d, err := r.row(id)
	if err != nil {
		return err
	}
	d.Attempts++
	d.NextAttemptAt, d.LastError, d.leaseUntil = next, lastErr, time.Time{}
	return nil
}

func (r deliveriesRepo) MarkDead(_ context.Context, id string, lastErr string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	d, err := r.row(id)
	if err != nil {
		return err
	}
	d.Attempts++
	d.Status, d.LastError, d.leaseUntil = subscription.DeliveryDead, lastErr, time.Time{}
	return nil
}

func (r deliveriesRepo) Postpone(_ context.Context, id string, until time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	d, err := r.row(id)
	if err != nil {
		return err
	}
	d.NextAttemptAt, d.leaseUntil = until, time.Time{}
	if d.Claims > 0 {
		d.Claims-- // postponing is not a claim that led anywhere
	}
	return nil
}

func (r deliveriesRepo) List(_ context.Context, tenantID, subscriptionID string, status subscription.DeliveryStatus, limit int) ([]subscription.Delivery, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if s, ok := r.s.subs[subscriptionID]; !ok || s.TenantID != tenantID {
		return nil, errs.ErrNotFound
	}
	var out []subscription.Delivery
	for _, d := range r.s.deliveries {
		if d.SubscriptionID == subscriptionID && (status == "" || d.Status == status) {
			out = append(out, d.Delivery)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r deliveriesRepo) Requeue(_ context.Context, tenantID, id string, now time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	d, err := r.row(id)
	if err != nil || d.TenantID != tenantID {
		return errs.ErrNotFound
	}
	if d.Status != subscription.DeliveryDead {
		return fmt.Errorf("%w: only DEAD deliveries can be requeued (is %s)", errs.ErrConflict, d.Status)
	}
	d.Status, d.Attempts, d.NextAttemptAt, d.LastError, d.leaseUntil = subscription.DeliveryPending, 0, now, "", time.Time{}
	return nil
}

func (r deliveriesRepo) PurgeDelivered(_ context.Context, before time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for id, d := range r.s.deliveries {
		if d.Status == subscription.DeliveryDelivered && d.DeliveredAt.Before(before) {
			delete(r.s.deliveries, id)
			n++
		}
	}
	return n, nil
}

func (r deliveriesRepo) Counts(_ context.Context, now time.Time) (ports.DeliveryCounts, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var c ports.DeliveryCounts
	var oldest time.Time
	for _, d := range r.s.deliveries {
		switch d.Status {
		case subscription.DeliveryPending:
			c.Pending++
			if oldest.IsZero() || d.CreatedAt.Before(oldest) {
				oldest = d.CreatedAt
			}
		case subscription.DeliveryDelivered:
			c.Delivered++
		case subscription.DeliveryDead:
			c.Dead++
		}
	}
	if !oldest.IsZero() {
		c.OldestPending = now.Sub(oldest)
	}
	return c, nil
}
