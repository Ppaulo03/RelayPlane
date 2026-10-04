package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

type erasureRepo struct{ s *Store }

func subjectOf(number string) string {
	sum := sha256.Sum256([]byte(number))
	return hex.EncodeToString(sum[:])
}

func (r erasureRepo) Mark(ctx context.Context, tenantID, number string, at time.Time) error {
	_, err := r.s.pool.Exec(ctx, `INSERT INTO contact_erasures(tenant_id,subject,erased_at) VALUES($1,$2,$3)
		ON CONFLICT (tenant_id,subject) DO UPDATE SET erased_at = GREATEST(contact_erasures.erased_at, EXCLUDED.erased_at)`,
		tenantID, subjectOf(number), at)
	return err
}

func (r erasureRepo) Erased(ctx context.Context, ev events.Event) (bool, error) {
	number := ev.ContactNumber()
	if number == "" || ev.AcceptedAt == nil {
		return false, nil
	}
	var erased bool
	err := r.s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM contact_erasures WHERE tenant_id=$1 AND subject=$2 AND erased_at >= $3)`,
		ev.TenantID, subjectOf(number), *ev.AcceptedAt).Scan(&erased)
	return erased, err
}

func (r erasureRepo) Purge(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM contact_erasures WHERE erased_at < $1`, before)
	return tag.RowsAffected(), err
}

func (r deliveriesRepo) DeleteByEvent(ctx context.Context, eventID string) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM webhook_deliveries WHERE event_id=$1`, eventID)
	return tag.RowsAffected(), err
}

func (r inboundMediaRepo) Drop(ctx context.Context, id string) error {
	_, err := r.s.pool.Exec(ctx, `DELETE FROM inbound_media WHERE id=$1`, id)
	return err
}
