package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
)

type apiKeyRepo struct{ s *Store }

const apiKeyCols = `id,tenant_id,name,key_prefix,key_hash,created_at,expires_at,last_used_at,revoked_at`

func scanAPIKey(row pgx.Row) (*instance.APIKey, error) {
	var k instance.APIKey
	var exp, used, rev *time.Time
	if err := row.Scan(&k.ID, &k.TenantID, &k.Name, &k.Prefix, &k.KeyHash, &k.CreatedAt, &exp, &used, &rev); err != nil {
		return nil, notFound(err)
	}
	k.ExpiresAt, k.LastUsedAt, k.RevokedAt = zeroIfNil(exp), zeroIfNil(used), zeroIfNil(rev)
	return &k, nil
}

func nullable(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (r apiKeyRepo) Create(ctx context.Context, k instance.APIKey) error {
	if k.CreatedAt.IsZero() {
		k.CreatedAt = time.Now().UTC()
	}
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		// the tenant row is the lock that serialises concurrent creations, so the limit cannot be overshot
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM tenants WHERE id=$1 FOR UPDATE`, k.TenantID).Scan(&one); err != nil {
			return notFound(err)
		}
		var active int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM api_keys WHERE tenant_id=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2)`,
			k.TenantID, k.CreatedAt).Scan(&active); err != nil {
			return err
		}
		if active >= instance.MaxActiveAPIKeys {
			return errs.ErrConflict
		}
		_, err := tx.Exec(ctx, `INSERT INTO api_keys(`+apiKeyCols+`) VALUES($1,$2,$3,$4,$5,$6,$7,NULL,NULL)`,
			k.ID, k.TenantID, k.Name, k.Prefix, k.KeyHash, k.CreatedAt, nullable(k.ExpiresAt))
		return err
	})
	if _, code := constraint(err); code == "23505" {
		return errs.ErrAlreadyExists
	}
	return err
}

func (r apiKeyRepo) FindActiveByHash(ctx context.Context, hash string, now time.Time) (*instance.APIKey, error) {
	return scanAPIKey(r.s.pool.QueryRow(ctx, `SELECT `+apiKeyCols+` FROM api_keys WHERE key_hash=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2)`, hash, now))
}

func (r apiKeyRepo) List(ctx context.Context, tenantID string) ([]instance.APIKey, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT `+apiKeyCols+` FROM api_keys WHERE tenant_id=$1 ORDER BY created_at DESC, id DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []instance.APIKey
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *k)
	}
	return out, rows.Err()
}

func (r apiKeyRepo) Revoke(ctx context.Context, tenantID, id string, at time.Time) error {
	return r.s.withTx(ctx, func(tx pgx.Tx) error {
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM tenants WHERE id=$1 FOR UPDATE`, tenantID).Scan(&one); err != nil {
			return notFound(err)
		}
		var revoked *time.Time
		if err := tx.QueryRow(ctx, `SELECT revoked_at FROM api_keys WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&revoked); err != nil {
			return notFound(err)
		}
		if revoked != nil {
			return nil // already revoked
		}
		var others int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM api_keys WHERE tenant_id=$1 AND id<>$2 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $3)`,
			tenantID, id, at).Scan(&others); err != nil {
			return err
		}
		if others == 0 {
			return errs.ErrConflict // the last usable key: revoking it would lock the tenant out
		}
		_, err := tx.Exec(ctx, `UPDATE api_keys SET revoked_at=$2 WHERE id=$1`, id, at)
		return err
	})
}

func (r apiKeyRepo) Touch(ctx context.Context, id string, at time.Time, minAge time.Duration) error {
	_, err := r.s.pool.Exec(ctx, `UPDATE api_keys SET last_used_at=$2 WHERE id=$1 AND (last_used_at IS NULL OR last_used_at < $3)`, id, at, at.Add(-minAge))
	return err
}
