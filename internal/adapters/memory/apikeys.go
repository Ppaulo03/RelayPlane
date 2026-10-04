package memory

import (
	"context"
	"sort"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
)

type apiKeyRepo struct{ s *Store }

func (r apiKeyRepo) Create(_ context.Context, k instance.APIKey) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if _, ok := r.s.tenants[k.TenantID]; !ok {
		return errs.ErrNotFound
	}
	if k.CreatedAt.IsZero() {
		k.CreatedAt = r.s.Now()
	}
	active := 0
	for _, x := range r.s.apiKeys {
		if x.KeyHash == k.KeyHash || x.ID == k.ID {
			return errs.ErrAlreadyExists
		}
		if x.TenantID == k.TenantID && x.Active(k.CreatedAt) {
			active++
		}
	}
	if active >= instance.MaxActiveAPIKeys {
		return errs.ErrConflict
	}
	r.s.apiKeys[k.ID] = &k
	return nil
}

func (r apiKeyRepo) FindActiveByHash(_ context.Context, hash string, now time.Time) (*instance.APIKey, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, k := range r.s.apiKeys {
		if k.KeyHash == hash && k.Active(now) {
			c := *k
			return &c, nil
		}
	}
	return nil, errs.ErrNotFound
}

func (r apiKeyRepo) List(_ context.Context, tenantID string) ([]instance.APIKey, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []instance.APIKey
	for _, k := range r.s.apiKeys {
		if k.TenantID == tenantID {
			out = append(out, *k)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

func (r apiKeyRepo) Revoke(_ context.Context, tenantID, id string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	k, ok := r.s.apiKeys[id]
	if !ok || k.TenantID != tenantID {
		return errs.ErrNotFound
	}
	if !k.RevokedAt.IsZero() {
		return nil
	}
	others := 0
	for _, x := range r.s.apiKeys {
		if x.TenantID == tenantID && x.ID != id && x.Active(at) {
			others++
		}
	}
	if others == 0 {
		return errs.ErrConflict
	}
	k.RevokedAt = at
	return nil
}

func (r apiKeyRepo) Touch(_ context.Context, id string, at time.Time, minAge time.Duration) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if k, ok := r.s.apiKeys[id]; ok && (k.LastUsedAt.IsZero() || k.LastUsedAt.Before(at.Add(-minAge))) {
		k.LastUsedAt = at
	}
	return nil
}
