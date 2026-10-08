package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

// emitOutbound writes the tenant-facing event of a message that just entered m.Status, inside the caller's
// transaction: the status change and its event are one atomic fact.
func emitOutbound(ctx context.Context, tx pgx.Tx, m *messaging.Message) error {
	if !m.Status.NotifiesTenant() {
		return nil
	}
	var provider string
	if err := tx.QueryRow(ctx, `SELECT provider FROM instances WHERE id=$1`, m.InstanceID).Scan(&provider); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	ev := messaging.OutboundStatusEvent(*m, provider)
	return insertEventOutbox(ctx, tx, ev)
}

// insertEventOutbox queues a tenant-facing event for publication, inside the caller's transaction.
func insertEventOutbox(ctx context.Context, tx pgx.Tx, ev events.Event) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	var projected *time.Time // an event the catalog has nothing to learn from is born projected
	if !events.NeedsProjection(ev.EventType) {
		now := time.Now().UTC()
		projected = &now
	}
	_, err = tx.Exec(ctx, `INSERT INTO event_outbox(event_id,tenant_id,instance_id,event,projected_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING`,
		ev.EventID, ev.TenantID, ev.InstanceID, raw, projected)
	return err
}

// ---- events outbox ----

type eventsRepo struct{ s *Store }

func (r eventsRepo) ListUnpublished(ctx context.Context, limit int) ([]events.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.s.pool.Query(ctx, `SELECT event FROM event_outbox WHERE published_at IS NULL ORDER BY seq LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.Event
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var ev events.Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, fmt.Errorf("corrupt event_outbox row: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (r eventsRepo) ClaimForFanOut(ctx context.Context, limit int, lease time.Duration) ([]events.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	now := time.Now()
	rows, err := r.s.pool.Query(ctx, `
WITH due AS (
    SELECT seq FROM event_outbox
    WHERE fanout_at IS NULL AND (fanout_lease_until IS NULL OR fanout_lease_until <= $1)
    ORDER BY seq LIMIT $2 FOR UPDATE SKIP LOCKED
)
UPDATE event_outbox o SET fanout_lease_until = $1 + make_interval(secs => $3)
FROM due WHERE o.seq = due.seq
  -- another claimer may have finished (or leased) the row since this statement's snapshot: the eligibility is checked again at update time
  AND o.fanout_at IS NULL AND (o.fanout_lease_until IS NULL OR o.fanout_lease_until <= $1)
RETURNING o.seq, o.event`, now, limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type claimed struct {
		seq int64
		ev  events.Event
	}
	var got []claimed
	for rows.Next() {
		var c claimed
		var raw []byte
		if err := rows.Scan(&c.seq, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &c.ev); err != nil {
			return nil, fmt.Errorf("corrupt event_outbox row %d: %w", c.seq, err)
		}
		got = append(got, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(got, func(i, j int) bool { return got[i].seq < got[j].seq }) // RETURNING does not promise an order
	out := make([]events.Event, len(got))
	for i, c := range got {
		out[i] = c.ev
	}
	return out, nil
}

func (r eventsRepo) ClaimForProjection(ctx context.Context, limit int, lease time.Duration) ([]events.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	now := time.Now()
	rows, err := r.s.pool.Query(ctx, `
WITH due AS (
    SELECT seq FROM event_outbox
    WHERE projected_at IS NULL AND (projection_lease_until IS NULL OR projection_lease_until <= $1)
    ORDER BY seq LIMIT $2 FOR UPDATE SKIP LOCKED
)
UPDATE event_outbox o SET projection_lease_until = $1 + make_interval(secs => $3)
FROM due WHERE o.seq = due.seq
  AND o.projected_at IS NULL AND (o.projection_lease_until IS NULL OR o.projection_lease_until <= $1)
RETURNING o.seq, o.event`, now, limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type claimed struct {
		seq int64
		ev  events.Event
	}
	var got []claimed
	for rows.Next() {
		var c claimed
		var raw []byte
		if err := rows.Scan(&c.seq, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &c.ev); err != nil {
			return nil, fmt.Errorf("corrupt event_outbox row %d: %w", c.seq, err)
		}
		got = append(got, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(got, func(i, j int) bool { return got[i].seq < got[j].seq })
	out := make([]events.Event, len(got))
	for i, c := range got {
		out[i] = c.ev
	}
	return out, nil
}

func (r eventsRepo) MarkProjected(ctx context.Context, ids []string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.s.pool.Exec(ctx, `UPDATE event_outbox SET projected_at=$2, projection_lease_until=NULL WHERE event_id = ANY($1) AND projected_at IS NULL`, ids, at)
	return err
}

func (r eventsRepo) ProjectionStats(ctx context.Context) (int64, time.Duration, error) {
	var n int64
	var age *float64
	err := r.s.pool.QueryRow(ctx, `SELECT count(*), extract(epoch FROM now() - min(created_at)) FROM event_outbox WHERE projected_at IS NULL`).Scan(&n, &age)
	if err != nil || age == nil {
		return n, 0, err
	}
	return n, time.Duration(*age * float64(time.Second)), nil
}

func (r eventsRepo) MarkFannedOut(ctx context.Context, ids []string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.s.pool.Exec(ctx, `UPDATE event_outbox SET fanout_at=$2, fanout_lease_until=NULL WHERE event_id = ANY($1) AND fanout_at IS NULL`, ids, at)
	return err
}

func (r eventsRepo) MarkPublished(ctx context.Context, ids []string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.s.pool.Exec(ctx, `UPDATE event_outbox SET published_at=$2 WHERE event_id = ANY($1) AND published_at IS NULL`, ids, at)
	return err
}

func (r eventsRepo) Purge(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM event_outbox WHERE published_at IS NOT NULL AND fanout_at IS NOT NULL AND projected_at IS NOT NULL AND published_at < $1`, before)
	return tag.RowsAffected(), err
}

func (r eventsRepo) EraseContact(ctx context.Context, tenantID, number string) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM event_outbox WHERE tenant_id=$1 AND (event #>> '{payload,from}') = $2`, tenantID, number)
	return tag.RowsAffected(), err
}

func (r eventsRepo) PendingStats(ctx context.Context) (int64, time.Duration, error) {
	var n int64
	var age *float64
	err := r.s.pool.QueryRow(ctx, `SELECT count(*), extract(epoch FROM now() - min(created_at)) FROM event_outbox WHERE fanout_at IS NULL`).Scan(&n, &age)
	if err != nil || age == nil {
		return n, 0, err
	}
	return n, time.Duration(*age * float64(time.Second)), nil
}

// ---- subscriptions ----

type subsRepo struct{ s *Store }

const subCols = `id,tenant_id,url,event_types,instance_ids,secret_version,rotated_at,active,created_at,exclude_groups,paused`

func scanSub(row pgx.Row) (*subscription.Subscription, error) {
	var s subscription.Subscription
	var types []string
	var rotated *time.Time
	if err := row.Scan(&s.ID, &s.TenantID, &s.URL, &types, &s.InstanceIDs, &s.SecretVersion, &rotated, &s.Active, &s.CreatedAt, &s.ExcludeGroups, &s.Paused); err != nil {
		return nil, notFound(err)
	}
	for _, t := range types {
		s.EventTypes = append(s.EventTypes, events.Type(t))
	}
	s.RotatedAt = zeroIfNil(rotated)
	return &s, nil
}

func (r subsRepo) Create(ctx context.Context, s subscription.Subscription) error {
	return insertSubscription(ctx, r.s.pool, s)
}

// CreateIfBelow serialises the creations of one tenant on its row, so the count and the insert cannot interleave.
func (r subsRepo) CreateIfBelow(ctx context.Context, s subscription.Subscription, max int) error {
	if max <= 0 {
		return insertSubscription(ctx, r.s.pool, s)
	}
	return r.s.withTx(ctx, func(tx pgx.Tx) error {
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM tenants WHERE id=$1 FOR UPDATE`, s.TenantID).Scan(&one); err != nil {
			return fmt.Errorf("%w: tenant %s", errs.ErrNotFound, s.TenantID)
		}
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM subscriptions WHERE tenant_id=$1`, s.TenantID).Scan(&n); err != nil {
			return err
		}
		if n >= max {
			return errs.ErrConflict
		}
		return insertSubscription(ctx, tx, s)
	})
}

func insertSubscription(ctx context.Context, db execer, s subscription.Subscription) error {
	types := make([]string, len(s.EventTypes))
	for i, t := range s.EventTypes {
		types[i] = string(t)
	}
	ids := s.InstanceIDs
	if ids == nil {
		ids = []string{}
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	_, err := db.Exec(ctx, `INSERT INTO subscriptions(id,tenant_id,url,event_types,instance_ids,secret_version,active,created_at,exclude_groups) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		s.ID, s.TenantID, s.URL, types, ids, s.SecretVersion, s.Active, s.CreatedAt, s.ExcludeGroups)
	if name, code := constraint(err); err != nil {
		switch {
		case code == "23505":
			return errs.ErrAlreadyExists
		case code == "23503":
			return fmt.Errorf("%w: tenant %s (%s)", errs.ErrNotFound, s.TenantID, name)
		}
	}
	return err
}

func (r subsRepo) Get(ctx context.Context, tenantID, id string) (*subscription.Subscription, error) {
	return scanSub(r.s.pool.QueryRow(ctx, `SELECT `+subCols+` FROM subscriptions WHERE id=$1 AND tenant_id=$2`, id, tenantID))
}

func (r subsRepo) GetByID(ctx context.Context, id string) (*subscription.Subscription, error) {
	return scanSub(r.s.pool.QueryRow(ctx, `SELECT `+subCols+` FROM subscriptions WHERE id=$1`, id))
}

func (r subsRepo) list(ctx context.Context, tenantID string, activeOnly bool) ([]subscription.Subscription, error) {
	q := `SELECT ` + subCols + ` FROM subscriptions WHERE tenant_id=$1`
	if activeOnly {
		q += ` AND active`
	}
	rows, err := r.s.pool.Query(ctx, q+` ORDER BY created_at, id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []subscription.Subscription
	for rows.Next() {
		s, err := scanSub(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

func (r subsRepo) ListByTenant(ctx context.Context, tenantID string) ([]subscription.Subscription, error) {
	return r.list(ctx, tenantID, false)
}

func (r subsRepo) ListActive(ctx context.Context, tenantID string) ([]subscription.Subscription, error) {
	return r.list(ctx, tenantID, true)
}

func (r subsRepo) SetPaused(ctx context.Context, tenantID, id string, paused bool) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE subscriptions SET paused=$3 WHERE id=$1 AND tenant_id=$2`, id, tenantID, paused)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func (r subsRepo) RotateSecret(ctx context.Context, tenantID, id string, at time.Time) (int, error) {
	var v int
	err := r.s.pool.QueryRow(ctx, `UPDATE subscriptions SET secret_version=secret_version+1, rotated_at=$3 WHERE id=$1 AND tenant_id=$2 RETURNING secret_version`,
		id, tenantID, at).Scan(&v)
	return v, notFound(err)
}

func (r subsRepo) Delete(ctx context.Context, tenantID, id string) error {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM subscriptions WHERE id=$1 AND tenant_id=$2`, id, tenantID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// ---- deliveries ----

type deliveriesRepo struct{ s *Store }

const delCols = `id,subscription_id,tenant_id,instance_id,event_id,event_type,event,sequence,claims,status,attempts,next_attempt_at,last_error,created_at,delivered_at`

func scanDelivery(row pgx.Row) (*subscription.Delivery, error) {
	var d subscription.Delivery
	var raw []byte
	var typ, st string
	var delivered *time.Time
	if err := row.Scan(&d.ID, &d.SubscriptionID, &d.TenantID, &d.InstanceID, &d.EventID, &typ, &raw, &d.Sequence, &d.Claims, &st, &d.Attempts, &d.NextAttemptAt, &d.LastError, &d.CreatedAt, &delivered); err != nil {
		return nil, notFound(err)
	}
	d.EventType, d.Status, d.DeliveredAt = events.Type(typ), subscription.DeliveryStatus(st), zeroIfNil(delivered)
	if err := json.Unmarshal(raw, &d.Event); err != nil {
		return nil, fmt.Errorf("corrupt webhook_deliveries row %s: %w", d.ID, err)
	}
	return &d, nil
}

func (r deliveriesRepo) Enqueue(ctx context.Context, ds []subscription.Delivery) (int, error) {
	n := 0
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		n = 0
		for _, d := range ds {
			raw, err := json.Marshal(d.Event)
			if err != nil {
				return err
			}
			if d.CreatedAt.IsZero() {
				d.CreatedAt = time.Now().UTC()
			}
			if d.NextAttemptAt.IsZero() {
				d.NextAttemptAt = d.CreatedAt
			}
			// the counter row is locked for the rest of the transaction: concurrent fan-outs of the same (subscription,
			// instance) queue up here, so sequences are assigned in commit order and never skip
			var last int64
			err = tx.QueryRow(ctx, `INSERT INTO delivery_sequences(subscription_id,instance_id) VALUES($1,$2)
				ON CONFLICT (subscription_id,instance_id) DO UPDATE SET last = delivery_sequences.last RETURNING last`,
				d.SubscriptionID, d.InstanceID).Scan(&last)
			if err != nil {
				if _, code := constraint(err); code == "23503" {
					continue // the subscription vanished between fan-out and insert
				}
				return err
			}
			status := "PENDING"
			if d.Status == subscription.DeliveryDead { // born in the DLQ: over the subscription's backlog limit
				status = "DEAD"
			}
			tag, err := tx.Exec(ctx, `INSERT INTO webhook_deliveries(id,subscription_id,tenant_id,instance_id,event_id,event_type,event,sequence,status,last_error,next_attempt_at,created_at)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (subscription_id,event_id) DO NOTHING`,
				d.ID, d.SubscriptionID, d.TenantID, d.InstanceID, d.EventID, string(d.EventType), raw, last+1, status, d.LastError, d.NextAttemptAt, d.CreatedAt)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 1 {
				if _, err := tx.Exec(ctx, `UPDATE delivery_sequences SET last=$3 WHERE subscription_id=$1 AND instance_id=$2`, d.SubscriptionID, d.InstanceID, last+1); err != nil {
					return err
				}
			}
			n += int(tag.RowsAffected())
		}
		return nil
	})
	return n, err
}

func (r deliveriesRepo) ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]subscription.Delivery, error) {
	return r.ClaimDueWith(ctx, now, lease, limit, 0)
}

func (r deliveriesRepo) ClaimDueWith(ctx context.Context, now time.Time, lease time.Duration, limit, perSubscription int) ([]subscription.Delivery, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.s.pool.Query(ctx, `
WITH heads AS (
    -- the head of each (subscription, instance): one delivery in flight per pair, in sequence order; paused subscriptions wait
    SELECT DISTINCT ON (d.subscription_id, d.instance_id) d.id, d.created_at, d.subscription_id
    FROM webhook_deliveries d
    WHERE d.status='PENDING' AND d.next_attempt_at <= $1 AND (d.lease_until IS NULL OR d.lease_until <= $1)
      AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.id=d.subscription_id AND s.paused)
      AND NOT EXISTS (SELECT 1 FROM webhook_deliveries o
                       WHERE o.subscription_id=d.subscription_id AND o.instance_id=d.instance_id AND o.id<>d.id
                         AND o.status='PENDING' AND o.lease_until > $1)
    ORDER BY d.subscription_id, d.instance_id, d.sequence, d.created_at, d.id
), ranked AS (
    SELECT h.id, h.created_at,
           row_number() OVER (PARTITION BY h.subscription_id ORDER BY h.created_at, h.id) AS rn,
           (SELECT count(*) FROM webhook_deliveries o WHERE o.subscription_id=h.subscription_id AND o.status='PENDING' AND o.lease_until > $1) AS inflight
    FROM heads h
), picked AS (
    -- no subscription may hold more than $4 POSTs in flight (0: no ceiling)
    SELECT id, created_at FROM ranked WHERE $4 <= 0 OR rn + inflight <= $4 ORDER BY created_at, id LIMIT $3
), locked AS (
    SELECT w.id FROM webhook_deliveries w JOIN picked h ON h.id=w.id ORDER BY h.created_at, h.id FOR UPDATE OF w SKIP LOCKED
)
UPDATE webhook_deliveries x SET lease_until = $1 + make_interval(secs => $2), claims = x.claims + 1
FROM locked WHERE x.id = locked.id
  -- the claimers share no lock until here: another one may have FINISHED this delivery since this statement's snapshot (delivered it,
  -- or scheduled a retry for later, both of which clear the lease). The row is re-read at update time, so the whole eligibility is
  -- checked again: a lease alone would revive a delivered event or ignore a retry backoff.
  AND x.status = 'PENDING' AND x.next_attempt_at <= $1 AND (x.lease_until IS NULL OR x.lease_until <= $1)
RETURNING `+prefixCols("x", delCols), now, lease.Seconds(), limit, perSubscription)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []subscription.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sortDeliveries(out)
	return out, nil
}

func (r deliveriesRepo) Backlog(ctx context.Context, tenantID string, now time.Time) (map[string]ports.Backlog, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT subscription_id, count(*), min(created_at) FROM webhook_deliveries WHERE tenant_id=$1 AND status='PENDING' GROUP BY subscription_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]ports.Backlog{}
	for rows.Next() {
		var id string
		var b ports.Backlog
		var oldest time.Time
		if err := rows.Scan(&id, &b.Pending, &oldest); err != nil {
			return nil, err
		}
		b.OldestPending = now.Sub(oldest)
		out[id] = b
	}
	return out, rows.Err()
}

func (r deliveriesRepo) exec(ctx context.Context, q string, args ...any) error {
	tag, err := r.s.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func (r deliveriesRepo) MarkDelivered(ctx context.Context, id string, at time.Time) error {
	return r.exec(ctx, `UPDATE webhook_deliveries SET status='DELIVERED', delivered_at=$2, lease_until=NULL WHERE id=$1`, id, at)
}

func (r deliveriesRepo) MarkRetry(ctx context.Context, id string, next time.Time, lastErr string) error {
	return r.exec(ctx, `UPDATE webhook_deliveries SET attempts=attempts+1, next_attempt_at=$2, last_error=$3, lease_until=NULL WHERE id=$1`, id, next, lastErr)
}

func (r deliveriesRepo) MarkDead(ctx context.Context, id string, lastErr string) error {
	return r.exec(ctx, `UPDATE webhook_deliveries SET attempts=attempts+1, status='DEAD', last_error=$2, lease_until=NULL WHERE id=$1`, id, lastErr)
}

func (r deliveriesRepo) Postpone(ctx context.Context, id string, until time.Time) error {
	return r.exec(ctx, `UPDATE webhook_deliveries SET next_attempt_at=$2, lease_until=NULL, claims=GREATEST(claims-1,0) WHERE id=$1`, id, until) // postponing is not a claim that led anywhere
}

func (r deliveriesRepo) List(ctx context.Context, tenantID, subscriptionID string, status subscription.DeliveryStatus, limit int) ([]subscription.Delivery, error) {
	var one int
	if err := r.s.pool.QueryRow(ctx, `SELECT 1 FROM subscriptions WHERE id=$1 AND tenant_id=$2`, subscriptionID, tenantID).Scan(&one); err != nil {
		return nil, notFound(err)
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.s.pool.Query(ctx, `SELECT `+delCols+` FROM webhook_deliveries WHERE subscription_id=$1 AND ($2='' OR status=$2) ORDER BY created_at DESC, id DESC LIMIT $3`,
		subscriptionID, string(status), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []subscription.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

func (r deliveriesRepo) Requeue(ctx context.Context, tenantID, id string, now time.Time) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE webhook_deliveries SET status='PENDING', attempts=0, next_attempt_at=$3, last_error='', lease_until=NULL
		WHERE id=$1 AND tenant_id=$2 AND status='DEAD'`, id, tenantID, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var st string
		if err := r.s.pool.QueryRow(ctx, `SELECT status FROM webhook_deliveries WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&st); err != nil {
			return notFound(err)
		}
		return fmt.Errorf("%w: only DEAD deliveries can be requeued (is %s)", errs.ErrConflict, st)
	}
	return nil
}

func (r deliveriesRepo) PurgeDelivered(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM webhook_deliveries WHERE status='DELIVERED' AND delivered_at < $1`, before)
	return tag.RowsAffected(), err
}

func (r deliveriesRepo) Counts(ctx context.Context, now time.Time) (ports.DeliveryCounts, error) {
	var c ports.DeliveryCounts
	var oldest *time.Time
	err := r.s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status='PENDING'), count(*) FILTER (WHERE status='DELIVERED'), count(*) FILTER (WHERE status='DEAD'),
		min(created_at) FILTER (WHERE status='PENDING') FROM webhook_deliveries`).Scan(&c.Pending, &c.Delivered, &c.Dead, &oldest)
	if err == nil && oldest != nil {
		c.OldestPending = now.Sub(*oldest)
	}
	return c, err
}
