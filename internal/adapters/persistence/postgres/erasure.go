package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/ports"
)

// ---- messages ----

func (r msgRepo) EraseRecipient(ctx context.Context, tenantID, number string, at time.Time) (ports.ErasedMessages, error) {
	var out ports.ErasedMessages
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
WITH old AS (
    SELECT id, status FROM outbound_messages WHERE tenant_id=$1 AND recipient=$2 AND erased_at IS NULL FOR UPDATE
)
UPDATE outbound_messages m SET recipient='', payload='{}'::jsonb, error_message='', erased_at=$3, updated_at=$3,
       status     = CASE WHEN old.status='QUEUED' THEN 'FAILED' ELSE m.status END,
       error_code = CASE WHEN old.status='QUEUED' THEN 'ERASED' ELSE m.error_code END
FROM old WHERE m.id = old.id
RETURNING m.id, old.status`, tenantID, number, at)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id, was string
			if err := rows.Scan(&id, &was); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
			out.Anonymized++
			if was == "QUEUED" {
				out.Cancelled++
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		// the copy of the command (it holds the text and the recipient) goes too, unless the message is still in the
		// provider's hands: recovery may need to rebuild it, and it is purged after resolution
		_, err = tx.Exec(ctx, `UPDATE outbox SET command='{}'::jsonb WHERE message_id = ANY($1)
			AND message_id IN (SELECT id FROM outbound_messages WHERE status NOT IN ('QUEUED','DISPATCHING'))`, ids)
		return err
	})
	return out, err
}

func (r msgRepo) ScrubTerminalBefore(ctx context.Context, before, at time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 500
	}
	var n int64
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
UPDATE outbound_messages SET recipient='', payload='{}'::jsonb, error_message='', erased_at=$2
WHERE id IN (SELECT id FROM outbound_messages WHERE erased_at IS NULL AND created_at < $1
             AND status IN ('ACCEPTED','DELIVERED','READ','FAILED','UNKNOWN') ORDER BY created_at LIMIT $3 FOR UPDATE SKIP LOCKED)
RETURNING id`, before, at, limit)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		n = int64(len(ids))
		_, err = tx.Exec(ctx, `UPDATE outbox SET command='{}'::jsonb WHERE message_id = ANY($1) AND dispatched_at IS NOT NULL`, ids)
		return err
	})
	return n, err
}

// ---- blobs ----

func (r blobRepo) ListBySubject(ctx context.Context, tenantID, subject string) ([]media.Blob, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT `+blobCols+` FROM blob_metadata WHERE tenant_id=$1 AND subject=$2 AND status<>'DELETED'`, tenantID, subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []media.Blob
	for rows.Next() {
		b, err := scanBlob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

// ---- deliveries and inbound media jobs ----

func (r deliveriesRepo) PurgeDead(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM webhook_deliveries WHERE status='DEAD' AND created_at < $1`, before)
	return tag.RowsAffected(), err
}

func (r deliveriesRepo) PurgePending(ctx context.Context, before, now time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM webhook_deliveries WHERE status='PENDING' AND created_at < $1 AND (lease_until IS NULL OR lease_until <= $2)`, before, now)
	return tag.RowsAffected(), err
}

func (r deliveriesRepo) EraseContact(ctx context.Context, tenantID, number string) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM webhook_deliveries WHERE tenant_id=$1 AND (event #>> '{payload,from}') = $2`, tenantID, number)
	return tag.RowsAffected(), err
}

func (r inboundMediaRepo) EraseContact(ctx context.Context, tenantID, number string) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM inbound_media WHERE tenant_id=$1 AND (event #>> '{payload,from}') = $2`, tenantID, number)
	return tag.RowsAffected(), err
}
