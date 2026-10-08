package contracttest

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/ports"
)

// ---------------- BlobStore ----------------

// BlobFactory returns a fresh BlobStore.
type BlobFactory func(t *testing.T) ports.BlobStore

// BlobStoreContract is the behavioural contract of ports.BlobStore.
func BlobStoreContract(t *testing.T, factory BlobFactory) {
	ctx := context.Background()
	b := factory(t)
	t.Run("DeclaredSizeIsExact", func(t *testing.T) {
		// the negative cases only mean something on a backend that demonstrably accepts a valid upload
		if err := b.Put(ctx, "t3/media/size/valid", strings.NewReader("abc"), 3, "text/plain"); err != nil {
			t.Fatalf("a body that matches its declared size must be accepted: %v", err)
		}
		if err := b.Put(ctx, "t3/media/size/longer", strings.NewReader("0123456789"), 3, "text/plain"); !errors.Is(err, errs.ErrInvalidArgument) {
			t.Errorf("a longer body must be refused as an invalid argument, got %v", err)
		}
		if _, err := b.Stat(ctx, "t3/media/size/longer"); err == nil {
			t.Error("the truncated object of a refused upload exists")
		}
		if err := b.Put(ctx, "t3/media/size/shorter", strings.NewReader("ab"), 5, "text/plain"); err == nil {
			t.Error("a shorter body was accepted")
		}
		if _, err := b.Stat(ctx, "t3/media/size/shorter"); err == nil {
			t.Error("the object of a truncated upload exists")
		}
	})
	key := "t1/media/m1/hello.txt"
	if err := b.Put(ctx, key, strings.NewReader("hello world"), 11, "text/plain"); err != nil {
		t.Fatalf("put: %v", err)
	}
	r, err := b.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(r)
	_ = r.Close()
	if string(data) != "hello world" {
		t.Errorf("get: %q", data)
	}
	info, err := b.Stat(ctx, key)
	if err != nil || info.Size != 11 {
		t.Errorf("stat: %+v %v", info, err)
	}
	if _, err := b.Stat(ctx, "t1/none"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("stat missing: %v", err)
	}
	if _, err := b.Get(ctx, "t1/none"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("get missing: %v", err)
	}
	_ = b.Put(ctx, "t1/media/m2/b.txt", strings.NewReader("b"), 1, "text/plain")
	_ = b.Put(ctx, "t2/media/m3/c.txt", strings.NewReader("c"), 1, "text/plain")
	var listed []string
	if err := b.List(ctx, "t1/", func(o ports.ObjectInfo) error { listed = append(listed, o.Key); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 {
		t.Errorf("tenant-prefix listing must not leak other tenants: %v", listed)
	}
	for _, op := range []ports.SignedOp{ports.SignedGet, ports.SignedPut} {
		u, err := b.SignedURL(ctx, key, op, time.Minute)
		if err != nil || u == "" {
			t.Errorf("signed %s url: %q %v", op, u, err)
		}
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Errorf("delete must be idempotent: %v", err)
	}
	if _, err := b.Stat(ctx, key); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("deleted object still there: %v", err)
	}
}

// ---------------- Locker ----------------

// LockerFactory returns a fresh Locker.
type LockerFactory func(t *testing.T) ports.Locker

// LockerContract is the behavioural contract of ports.Locker.
func LockerContract(t *testing.T, factory LockerFactory) {
	ctx := context.Background()
	l := factory(t)
	a, ok, err := l.TryLock(ctx, "res", 300*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("first lock: %v %v", ok, err)
	}
	if _, ok, _ := l.TryLock(ctx, "res", time.Second); ok {
		t.Fatal("lock must be exclusive")
	}
	if _, ok, _ := l.TryLock(ctx, "other", time.Second); !ok {
		t.Fatal("different names are independent")
	}
	if err := a.Extend(ctx, 300*time.Millisecond); err != nil {
		t.Errorf("extend: %v", err)
	}
	if err := a.Release(ctx); err != nil {
		t.Fatal(err)
	}
	b, ok, _ := l.TryLock(ctx, "res", 100*time.Millisecond)
	if !ok {
		t.Fatal("released lock must be available")
	}
	time.Sleep(250 * time.Millisecond)
	if err := b.Extend(ctx, time.Second); err == nil {
		t.Error("an expired lease cannot be extended")
	}
	c, ok, _ := l.TryLock(ctx, "res", time.Second)
	if !ok {
		t.Fatal("expired lock must be available")
	}
	if err := b.Release(ctx); err != nil { // stale holder releasing must not free c's lock
		t.Errorf("stale release: %v", err)
	}
	if _, ok, _ := l.TryLock(ctx, "res", time.Second); ok {
		t.Error("a stale holder released someone else's lock")
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, _ := l.TryLock(ctx, "race", time.Second); ok {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Errorf("concurrent TryLock winners: %d", winners.Load())
	}
	_ = c.Release(ctx)
}
