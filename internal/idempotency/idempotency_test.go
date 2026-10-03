package idempotency_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/idempotency"
)

type result struct {
	ID string `json:"id"`
}

func svc() *idempotency.Service {
	return idempotency.NewService(memory.NewStore().Repositories().Idempotency)
}

var seq atomic.Int64

func newID() string { return fmt.Sprintf("res_%d", seq.Add(1)) }

func TestINV03_RepeatedOperationReturnsSameResult(t *testing.T) {
	s, ctx := svc(), context.Background()
	var runs atomic.Int32
	fn := func(_ context.Context, id string) (result, error) { runs.Add(1); return result{ID: id}, nil }
	a, replayed, err := idempotency.Do(ctx, s, "t1", "k", "create_instance", "h1", newID, fn)
	if err != nil || replayed {
		t.Fatalf("first: %v %v", replayed, err)
	}
	b, replayed, err := idempotency.Do(ctx, s, "t1", "k", "create_instance", "h1", newID, fn)
	if err != nil || !replayed || a != b {
		t.Fatalf("replay: %+v %+v %v %v", a, b, replayed, err)
	}
	if runs.Load() != 1 {
		t.Fatalf("operation ran %d times", runs.Load())
	}
}

func TestDifferentPayloadSameKeyIsAnError(t *testing.T) {
	s, ctx := svc(), context.Background()
	fn := func(_ context.Context, id string) (result, error) { return result{ID: id}, nil }
	_, _, _ = idempotency.Do(ctx, s, "t1", "k", "send_message", "h1", newID, fn)
	_, _, err := idempotency.Do(ctx, s, "t1", "k", "send_message", "OTHER", newID, fn)
	if !errors.Is(err, errs.ErrIdempotencyConflict) {
		t.Fatalf("got %v", err)
	}
	_, _, err = idempotency.Do(ctx, s, "t1", "k", "delete_instance", "h1", newID, fn)
	if !errors.Is(err, errs.ErrIdempotencyConflict) {
		t.Fatalf("same key for another operation must conflict, got %v", err)
	}
}

func TestKeysAreTenantScoped(t *testing.T) {
	s, ctx := svc(), context.Background()
	fn := func(_ context.Context, id string) (result, error) { return result{ID: id}, nil }
	a, _, _ := idempotency.Do(ctx, s, "t1", "k", "op", "h", newID, fn)
	b, replayed, _ := idempotency.Do(ctx, s, "t2", "k", "op", "h", newID, fn)
	if replayed || a == b {
		t.Fatal("another tenant must not see t1's result")
	}
}

func TestNoKeyRunsEveryTime(t *testing.T) {
	s, ctx := svc(), context.Background()
	var runs atomic.Int32
	fn := func(_ context.Context, id string) (result, error) { runs.Add(1); return result{ID: id}, nil }
	for i := 0; i < 3; i++ {
		_, _, _ = idempotency.Do(ctx, s, "t1", "", "op", "h", newID, fn)
	}
	if runs.Load() != 3 {
		t.Fatalf("runs=%d", runs.Load())
	}
}

func TestConcurrentDuplicatesRunOnce(t *testing.T) {
	s, ctx := svc(), context.Background()
	var runs atomic.Int32
	release := make(chan struct{})
	fn := func(_ context.Context, id string) (result, error) {
		runs.Add(1)
		<-release
		return result{ID: id}, nil
	}
	var wg sync.WaitGroup
	var inProgress, ok atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := idempotency.Do(ctx, s, "t1", "k", "op", "h", newID, fn)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, errs.ErrInProgress):
				inProgress.Add(1)
			default:
				t.Errorf("unexpected %v", err)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if runs.Load() != 1 || ok.Load() != 1 || inProgress.Load() != 7 {
		t.Fatalf("runs=%d ok=%d inProgress=%d", runs.Load(), ok.Load(), inProgress.Load())
	}
}

func TestRejectedRequestsFreeTheKey(t *testing.T) {
	s, ctx := svc(), context.Background()
	_, _, err := idempotency.Do(ctx, s, "t1", "k", "op", "h", newID, func(context.Context, string) (result, error) {
		return result{}, fmt.Errorf("%w: bad", errs.ErrInvalidArgument)
	})
	if !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatal(err)
	}
	r, replayed, err := idempotency.Do(ctx, s, "t1", "k", "op", "h2", newID, func(_ context.Context, id string) (result, error) { return result{ID: id}, nil })
	if err != nil || replayed || r.ID == "" {
		t.Fatalf("a corrected request must be able to reuse the key: %v %v", replayed, err)
	}
}

func TestCrashedClaimIsResumedWithSameResourceID(t *testing.T) {
	store := memory.NewStore().Repositories().Idempotency
	s, ctx := idempotency.NewService(store), context.Background()
	s.StaleAfter = 20 * time.Millisecond
	var firstID string
	_, _, err := idempotency.Do(ctx, s, "t1", "k", "create_instance", "h", newID, func(_ context.Context, id string) (result, error) {
		firstID = id
		return result{}, errors.New("process died after creating the resource") // not a "rejected" error: claim stays
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	if _, _, err := idempotency.Do(ctx, s, "t1", "k", "create_instance", "h", newID, func(_ context.Context, id string) (result, error) { return result{ID: id}, nil }); !errors.Is(err, errs.ErrInProgress) {
		t.Fatalf("fresh claim must not be stolen: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	r, _, err := idempotency.Do(ctx, s, "t1", "k", "create_instance", "h", newID, func(_ context.Context, id string) (result, error) { return result{ID: id}, nil })
	if err != nil || r.ID != firstID {
		t.Fatalf("resume must reuse the original resource id %q, got %q (%v)", firstID, r.ID, err)
	}
}

func TestHashRequestStable(t *testing.T) {
	a := idempotency.HashRequest(map[string]any{"a": 1, "b": []string{"x"}})
	b := idempotency.HashRequest(map[string]any{"b": []string{"x"}, "a": 1})
	if a != b || a == idempotency.HashRequest(map[string]any{"a": 2}) {
		t.Fatal("hash must be order independent and content sensitive")
	}
}
