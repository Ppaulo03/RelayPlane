package contracttest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/relayplane/relayplane/internal/core/errs"
)

// The limit of subscriptions per tenant and the insert are ONE step: requests that arrive together at the limit cannot all get in.
func subscriptionLimitContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.tenant(t, "t2")
	const max = 5
	var created, refused atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := fx.r.Subscriptions.CreateIfBelow(ctx, newSub(fmt.Sprintf("sub_c%d", i), "t1"), max)
			switch {
			case err == nil:
				created.Add(1)
			case errors.Is(err, errs.ErrConflict):
				refused.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if created.Load() != max || refused.Load() != 24-max {
		t.Fatalf("exactly %d of 24 concurrent creations may succeed: %d created, %d refused", max, created.Load(), refused.Load())
	}
	if l, _ := fx.r.Subscriptions.ListByTenant(ctx, "t1"); len(l) != max {
		t.Errorf("the tenant holds %d, not %d", len(l), max)
	}
	// the limit is per tenant
	if err := fx.r.Subscriptions.CreateIfBelow(ctx, newSub("sub_other", "t2"), max); err != nil {
		t.Errorf("another tenant is not affected: %v", err)
	}
	// 0 means no limit
	if err := fx.r.Subscriptions.CreateIfBelow(ctx, newSub("sub_free", "t1"), 0); err != nil {
		t.Errorf("no limit: %v", err)
	}
	if err := fx.r.Subscriptions.CreateIfBelow(ctx, newSub("sub_ghost", "no_such_tenant"), max); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("an unknown tenant: %v", err)
	}
}
