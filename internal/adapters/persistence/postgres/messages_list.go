package postgres

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

func (r msgRepo) ListByStatus(ctx context.Context, tenantID string, status messaging.Status, instanceID string, limit int) ([]messaging.Message, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := r.s.pool.Query(ctx, `SELECT `+msgCols+` FROM outbound_messages
		WHERE tenant_id=$1 AND status=$2 AND ($3='' OR instance_id=$3)
		ORDER BY instance_id, sequence_no, created_at LIMIT $4`, tenantID, string(status), instanceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []messaging.Message
	for rows.Next() {
		m, err := scanMsg(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	return out, rows.Err()
}

func (r msgRepo) UnknownStats(ctx context.Context) (int64, time.Duration, error) {
	var n int64
	var age *float64
	// the age is measured with the database clock: skew between the hosts cannot make it look shorter or longer
	err := r.s.pool.QueryRow(ctx, `SELECT count(*), extract(epoch FROM now() - min(updated_at)) FROM outbound_messages WHERE status='UNKNOWN'`).Scan(&n, &age)
	if err != nil || age == nil {
		return n, 0, err
	}
	return n, time.Duration(*age * float64(time.Second)), nil
}
