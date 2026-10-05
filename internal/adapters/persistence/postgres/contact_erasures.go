package postgres

import (
	"context"
	"time"
)

type erasureRepo struct{ s *Store }

func (r erasureRepo) Mark(ctx context.Context, tenantID, subject string, at time.Time) error {
	_, err := r.s.pool.Exec(ctx, `INSERT INTO contact_erasures(tenant_id,subject,erased_at) VALUES($1,$2,$3)
		ON CONFLICT (tenant_id,subject) DO UPDATE SET erased_at = GREATEST(contact_erasures.erased_at, EXCLUDED.erased_at)`,
		tenantID, subject, at)
	return err
}

func (r erasureRepo) Erased(ctx context.Context, tenantID, subject string, acceptedAt time.Time) (bool, error) {
	var erased bool
	err := r.s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM contact_erasures WHERE tenant_id=$1 AND subject=$2 AND erased_at >= $3)`,
		tenantID, subject, acceptedAt).Scan(&erased)
	return erased, err
}

func (r deliveriesRepo) DeleteByEvent(ctx context.Context, eventID string) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM webhook_deliveries WHERE event_id=$1`, eventID)
	return tag.RowsAffected(), err
}

func (r inboundMediaRepo) Drop(ctx context.Context, id string) error {
	_, err := r.s.pool.Exec(ctx, `DELETE FROM inbound_media WHERE id=$1`, id)
	return err
}
