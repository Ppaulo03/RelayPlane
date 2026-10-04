package contracttest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
)

// Several keys per tenant: rotate without downtime, revoke, never lock a tenant out, bound the number, show last use.
func apiKeysContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.tenant(t, "t2")
	repo := fx.r.APIKeys
	// later than the initial key, which the fixture created a moment ago
	now := time.Now().UTC().Add(time.Minute).Truncate(time.Millisecond)

	// a tenant is born with one key, the one its creation returned
	initial, err := repo.FindActiveByHash(ctx, "hash-t1", now)
	if err != nil || initial.TenantID != "t1" || initial.Name != "initial" {
		t.Fatalf("initial key: %+v %v", initial, err)
	}

	// rotation: the new key works while the old one still does
	next := instance.APIKey{ID: "key_new", TenantID: "t1", Name: "agent-prod", Prefix: "rpk_ab12", KeyHash: "hash-new", CreatedAt: now}
	if err := repo.Create(ctx, next); err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"hash-t1", "hash-new"} {
		if k, err := repo.FindActiveByHash(ctx, h, now); err != nil || k.TenantID != "t1" {
			t.Fatalf("both keys are valid during the transition (%s): %v", h, err)
		}
	}
	if err := repo.Create(ctx, instance.APIKey{ID: "key_dup", TenantID: "t1", KeyHash: "hash-new", CreatedAt: now}); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Errorf("a hash is unique across tenants: %v", err)
	}
	if err := repo.Create(ctx, instance.APIKey{ID: "key_x", TenantID: "t_missing", KeyHash: "hash-x", CreatedAt: now}); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("unknown tenant: %v", err)
	}

	// the listing never carries the secret, only the prefix, and shows revoked keys too
	list, _ := repo.List(ctx, "t1")
	if len(list) != 2 || list[0].ID != "key_new" || list[0].Prefix != "rpk_ab12" {
		t.Fatalf("list (newest first): %+v", list)
	}
	if other, _ := repo.List(ctx, "t2"); len(other) != 1 {
		t.Errorf("tenants only see their own keys: %+v", other)
	}

	// revoke the old key: it stops working at once, the new one keeps working
	if err := repo.Revoke(ctx, "t1", initial.ID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FindActiveByHash(ctx, "hash-t1", now.Add(2*time.Second)); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("a revoked key must not authenticate: %v", err)
	}
	if _, err := repo.FindActiveByHash(ctx, "hash-new", now.Add(2*time.Second)); err != nil {
		t.Errorf("the new key keeps working: %v", err)
	}
	if err := repo.Revoke(ctx, "t1", initial.ID, now.Add(3*time.Second)); err != nil {
		t.Errorf("revoking twice is idempotent: %v", err)
	}
	// ...and a tenant can never revoke its way into a lock-out
	if err := repo.Revoke(ctx, "t1", "key_new", now.Add(4*time.Second)); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("the last usable key cannot be revoked: %v", err)
	}
	// a key of another tenant is indistinguishable from a missing one
	if err := repo.Revoke(ctx, "t2", "key_new", now); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("cross-tenant revoke: %v", err)
	}

	// expiry
	if err := repo.Create(ctx, instance.APIKey{ID: "key_short", TenantID: "t1", KeyHash: "hash-short", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FindActiveByHash(ctx, "hash-short", now.Add(30*time.Minute)); err != nil {
		t.Errorf("before expiry: %v", err)
	}
	if _, err := repo.FindActiveByHash(ctx, "hash-short", now.Add(2*time.Hour)); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("an expired key must not authenticate: %v", err)
	}
	// an expired key does not count as usable: revoking the other would lock the tenant out
	if err := repo.Revoke(ctx, "t1", "key_new", now.Add(2*time.Hour)); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("an expired key is not a way back in: %v", err)
	}

	// last use is recorded, but only once a minute
	if err := repo.Touch(ctx, "key_new", now.Add(5*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := repo.Touch(ctx, "key_new", now.Add(20*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	lst, _ := repo.List(ctx, "t1")
	for _, k := range lst {
		if k.ID == "key_new" && !k.LastUsedAt.Equal(now.Add(5*time.Second)) {
			t.Errorf("a second use within the minute must not write: %v", k.LastUsedAt)
		}
	}
	if err := repo.Touch(ctx, "key_new", now.Add(2*time.Minute), time.Minute); err != nil {
		t.Fatal(err)
	}
	lst, _ = repo.List(ctx, "t1")
	for _, k := range lst {
		if k.ID == "key_new" && !k.LastUsedAt.Equal(now.Add(2*time.Minute)) {
			t.Errorf("a use after the minute is recorded: %v", k.LastUsedAt)
		}
	}

	// the number of keys held at once is bounded
	for i := 0; i < instance.MaxActiveAPIKeys+2; i++ {
		err := repo.Create(ctx, instance.APIKey{ID: fmt.Sprintf("key_%02d", i), TenantID: "t2", KeyHash: fmt.Sprintf("hash-%02d", i), CreatedAt: now})
		if i < instance.MaxActiveAPIKeys-1 && err != nil { // t2 already holds its initial key
			t.Fatalf("key %d: %v", i, err)
		}
		if i >= instance.MaxActiveAPIKeys-1 && !errors.Is(err, errs.ErrConflict) {
			t.Fatalf("key %d must hit the limit of %d active keys: %v", i, instance.MaxActiveAPIKeys, err)
		}
	}
}
