package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO event_outbox(event_id,tenant_id,instance_id,event) VALUES($1,$2,$3,$4) ON CONFLICT (event_id) DO NOTHING`,
		ev.EventID, ev.TenantID, ev.InstanceID, raw)
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

func (r eventsRepo) MarkPublished(ctx context.Context, ids []string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.s.pool.Exec(ctx, `UPDATE event_outbox SET published_at=$2 WHERE event_id = ANY($1) AND published_at IS NULL`, ids, at)
	return err
}

func (r eventsRepo) Purge(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM event_outbox WHERE published_at IS NOT NULL AND published_at < $1`, before)
	return tag.RowsAffected(), err
}

// ---- subscriptions ----

type subsRepo struct{ s *Store }

const subCols = `id,tenant_id,url,event_types,instance_ids,secret_version,rotated_at,active,created_at`

func scanSub(row pgx.Row) (*subscription.Subscription, error) {
	var s subscription.Subscription
	var types []string
	var rotated *time.Time
	if err := row.Scan(&s.ID, &s.TenantID, &s.URL, &types, &s.InstanceIDs, &s.SecretVersion, &rotated, &s.Active, &s.CreatedAt); err != nil {
		return nil, notFound(err)
	}
	for _, t := range types {
		s.EventTypes = append(s.EventTypes, events.Type(t))
	}
	s.RotatedAt = zeroIfNil(rotated)
	return &s, nil
}

func (r subsRepo) Create(ctx context.Context, s subscription.Subscription) error {
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
	_, err := r.s.pool.Exec(ctx, `INSERT INTO subscriptions(id,tenant_id,url,event_types,instance_ids,secret_version,active,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		s.ID, s.TenantID, s.URL, types, ids, s.SecretVersion, s.Active, s.CreatedAt)
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

func (r subsRepo) CountByTenant(ctx context.Context, tenantID string) (int, error) {
	var n int
	err := r.s.pool.QueryRow(ctx, `SELECT count(*) FROM subscriptions WHERE tenant_id=$1`, tenantID).Scan(&n)
	return n, err
}

// ---- deliveries ----

type deliveriesRepo struct{ s *Store }

const delCols = `id,subscription_id,tenant_id,instance_id,event_id,event_type,event,status,attempts,next_attempt_at,last_error,created_at,delivered_at`

func scanDelivery(row pgx.Row) (*subscription.Delivery, error) {
	var d subscription.Delivery
	var raw []byte
	var typ, st string
	var delivered *time.Time
	if err := row.Scan(&d.ID, &d.SubscriptionID, &d.TenantID, &d.InstanceID, &d.EventID, &typ, &raw, &st, &d.Attempts, &d.NextAttemptAt, &d.LastError, &d.CreatedAt, &delivered); err != nil {
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
			tag, err := tx.Exec(ctx, `INSERT INTO webhook_deliveries(id,subscription_id,tenant_id,instance_id,event_id,event_type,event,status,next_attempt_at,created_at)
				VALUES($1,$2,$3,$4,$5,$6,$7,'PENDING',$8,$9) ON CONFLICT (subscription_id,event_id) DO NOTHING`,
				d.ID, d.SubscriptionID, d.TenantID, d.InstanceID, d.EventID, string(d.EventType), raw, d.NextAttemptAt, d.CreatedAt)
			if err != nil {
				if _, code := constraint(err); code == "23503" {
					continue // the subscription vanished between fan-out and insert
				}
				return err
			}
			n += int(tag.RowsAffected())
		}
		return nil
	})
	return n, err
}

func (r deliveriesRepo) ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]subscription.Delivery, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.s.pool.Query(ctx, `
WITH heads AS (
    SELECT id, created_at FROM (
        SELECT DISTINCT ON (d.subscription_id, d.instance_id) d.id, d.created_at
        FROM webhook_deliveries d
        WHERE d.status='PENDING' AND d.next_attempt_at <= $1 AND (d.lease_until IS NULL OR d.lease_until <= $1)
          AND NOT EXISTS (SELECT 1 FROM webhook_deliveries o
                           WHERE o.subscription_id=d.subscription_id AND o.instance_id=d.instance_id AND o.id<>d.id
                             AND o.status='PENDING' AND o.lease_until > $1)
        ORDER BY d.subscription_id, d.instance_id, d.created_at, d.id
    ) first ORDER BY created_at, id LIMIT $3
), locked AS (
    SELECT w.id FROM webhook_deliveries w JOIN heads h ON h.id=w.id ORDER BY h.created_at, h.id FOR UPDATE OF w SKIP LOCKED
)
UPDATE webhook_deliveries x SET lease_until = $1 + make_interval(secs => $2)
FROM locked WHERE x.id = locked.id AND (x.lease_until IS NULL OR x.lease_until <= $1)
RETURNING `+prefixCols("x", delCols), now, lease.Seconds(), limit)
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
	return r.exec(ctx, `UPDATE webhook_deliveries SET next_attempt_at=$2, lease_until=NULL WHERE id=$1`, id, until)
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
