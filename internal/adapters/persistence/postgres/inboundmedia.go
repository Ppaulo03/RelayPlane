package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/ports"
)

type inboundMediaRepo struct{ s *Store }

const inboundCols = `id,tenant_id,instance_id,event_id,event,ref,stage,attempts,next_attempt_at,last_error,created_at,done_at`

func scanInbound(row pgx.Row) (*media.InboundJob, error) {
	var j media.InboundJob
	var ev, ref []byte
	var stage string
	var done *time.Time
	if err := row.Scan(&j.ID, &j.TenantID, &j.InstanceID, &j.EventID, &ev, &ref, &stage, &j.Attempts, &j.NextAttemptAt, &j.LastError, &j.CreatedAt, &done); err != nil {
		return nil, notFound(err)
	}
	j.Stage, j.DoneAt, j.Ref = media.InboundStage(stage), zeroIfNil(done), ref
	if err := json.Unmarshal(ev, &j.Event); err != nil {
		return nil, fmt.Errorf("corrupt inbound_media row %s: %w", j.ID, err)
	}
	return &j, nil
}

func (r inboundMediaRepo) Enqueue(ctx context.Context, j media.InboundJob) (bool, error) {
	ev, err := json.Marshal(j.Event)
	if err != nil {
		return false, err
	}
	ref := []byte(j.Ref)
	if len(ref) == 0 {
		ref = []byte("{}")
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now().UTC()
	}
	if j.NextAttemptAt.IsZero() {
		j.NextAttemptAt = j.CreatedAt
	}
	tag, err := r.s.pool.Exec(ctx, `INSERT INTO inbound_media(id,tenant_id,instance_id,event_id,event,ref,next_attempt_at,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (event_id) DO NOTHING`,
		j.ID, j.TenantID, j.InstanceID, j.EventID, ev, ref, j.NextAttemptAt, j.CreatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (r inboundMediaRepo) ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]media.InboundJob, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := r.s.pool.Query(ctx, `
WITH due AS (
    SELECT id FROM inbound_media
    WHERE stage <> 'DONE' AND next_attempt_at <= $1 AND (lease_until IS NULL OR lease_until <= $1)
    ORDER BY created_at, id LIMIT $3 FOR UPDATE SKIP LOCKED
)
UPDATE inbound_media m SET lease_until = $1 + make_interval(secs => $2)
FROM due WHERE m.id = due.id
RETURNING `+prefixCols("m", inboundCols), now, lease.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []media.InboundJob
	for rows.Next() {
		j, err := scanInbound(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

func (r inboundMediaRepo) exec(ctx context.Context, q string, args ...any) error {
	tag, err := r.s.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func (r inboundMediaRepo) Resolve(ctx context.Context, id string, ev events.Event) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	// the decryption material is not needed any more: drop it as soon as the outcome is known
	return r.exec(ctx, `UPDATE inbound_media SET event=$2, ref='{}', stage='PUBLISH', attempts=0, lease_until=NULL, last_error='', next_attempt_at=now() WHERE id=$1 AND stage <> 'DONE'`, id, raw)
}

func (r inboundMediaRepo) Retry(ctx context.Context, id string, next time.Time, lastErr string) error {
	return r.exec(ctx, `UPDATE inbound_media SET attempts=attempts+1, next_attempt_at=$2, last_error=$3, lease_until=NULL WHERE id=$1`, id, next, lastErr)
}

func (r inboundMediaRepo) Done(ctx context.Context, id string, at time.Time) error {
	return r.exec(ctx, `UPDATE inbound_media SET stage='DONE', done_at=$2, lease_until=NULL, ref='{}' WHERE id=$1`, id, at)
}

func (r inboundMediaRepo) Purge(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM inbound_media WHERE stage='DONE' AND done_at < $1`, before)
	return tag.RowsAffected(), err
}

func (r inboundMediaRepo) Counts(ctx context.Context, now time.Time) (ports.InboundMediaCounts, error) {
	var c ports.InboundMediaCounts
	var oldest *time.Time
	err := r.s.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE stage='DOWNLOAD'), count(*) FILTER (WHERE stage='PUBLISH'),
		min(created_at) FILTER (WHERE stage <> 'DONE') FROM inbound_media`).Scan(&c.Download, &c.Publish, &oldest)
	if oldest != nil {
		c.OldestPending = now.Sub(*oldest)
	}
	return c, err
}
