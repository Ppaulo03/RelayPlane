package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/ports"
)

// ---------------- EventBus ----------------

// Bus is an in-memory ports.EventBus. Every consumer group sees every event;
// events whose handler keeps failing are parked in DeadEvents after MaxDeliveries.
type Bus struct {
	mu     sync.Mutex
	log    []events.Event
	groups map[string]*busGroup

	MaxDeliveries int
	RetryDelay    time.Duration
	dead          []events.Event
}

type busGroup struct {
	mu     sync.Mutex // one in-flight handler per group
	offset int
}

// NewBus returns an empty bus.
func NewBus() *Bus {
	return &Bus{groups: map[string]*busGroup{}, MaxDeliveries: 5, RetryDelay: 5 * time.Millisecond}
}

func (b *Bus) Publish(_ context.Context, ev events.Event) error {
	b.mu.Lock()
	b.log = append(b.log, ev)
	b.mu.Unlock()
	return nil
}

// Published returns a snapshot of every event ever published (tests).
func (b *Bus) Published() []events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]events.Event(nil), b.log...)
}

// DeadEvents returns events parked after exhausting deliveries.
func (b *Bus) DeadEvents() []events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]events.Event(nil), b.dead...)
}

// Stats implements ports.EventBusInspector. The in-memory bus never trims.
func (b *Bus) Stats(_ context.Context) (ports.EventBusStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := ports.EventBusStats{Length: int64(len(b.log)), Retention: int64(len(b.log)) + 1}
	for name, g := range b.groups {
		st.Groups = append(st.Groups, ports.EventBusGroupStats{Name: name, Lag: int64(len(b.log) - g.offset)})
	}
	sort.Slice(st.Groups, func(i, j int) bool { return st.Groups[i].Name < st.Groups[j].Name })
	return st, nil
}

func (b *Bus) Subscribe(ctx context.Context, group string, h ports.EventHandler) error {
	b.mu.Lock()
	g := b.groups[group]
	if g == nil {
		g = &busGroup{}
		b.groups[group] = g
	}
	b.mu.Unlock()
	for ctx.Err() == nil {
		g.mu.Lock()
		b.mu.Lock()
		var ev *events.Event
		if g.offset < len(b.log) {
			e := b.log[g.offset]
			ev = &e
		}
		b.mu.Unlock()
		if ev == nil {
			g.mu.Unlock()
			sleep(ctx, 2*time.Millisecond)
			continue
		}
		var err error
		for attempt := 1; attempt <= b.MaxDeliveries; attempt++ {
			if err = h(ctx, *ev); err == nil || ctx.Err() != nil {
				break
			}
			sleep(ctx, b.RetryDelay)
		}
		if err != nil && ctx.Err() != nil {
			g.mu.Unlock()
			return ctx.Err()
		}
		if err != nil {
			b.mu.Lock()
			b.dead = append(b.dead, *ev)
			b.mu.Unlock()
		}
		b.mu.Lock()
		g.offset++
		b.mu.Unlock()
		g.mu.Unlock()
	}
	return ctx.Err()
}

// ---------------- BlobStore ----------------

// Blob is an in-memory ports.BlobStore.
type Blob struct {
	mu      sync.Mutex
	objects map[string]blobObj
}

type blobObj struct {
	data []byte
	ct   string
	mod  time.Time
}

// NewBlob returns an empty store.
func NewBlob() *Blob { return &Blob{objects: map[string]blobObj{}} }

func (b *Blob) Put(_ context.Context, key string, r io.Reader, _ int64, contentType string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.objects[key] = blobObj{data: data, ct: contentType, mod: time.Now()}
	b.mu.Unlock()
	return nil
}

func (b *Blob) Get(_ context.Context, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.objects[key]
	if !ok {
		return nil, errs.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

func (b *Blob) Stat(_ context.Context, key string) (*ports.ObjectInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.objects[key]
	if !ok {
		return nil, errs.ErrNotFound
	}
	return &ports.ObjectInfo{Key: key, Size: int64(len(o.data)), ContentType: o.ct, LastModified: o.mod}, nil
}

func (b *Blob) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	delete(b.objects, key)
	b.mu.Unlock()
	return nil
}

func (b *Blob) SignedURL(_ context.Context, key string, op ports.SignedOp, ttl time.Duration) (string, error) {
	return fmt.Sprintf("memory://blob/%s?op=%s&ttl=%d", key, op, int(ttl.Seconds())), nil
}

func (b *Blob) List(_ context.Context, prefix string, fn func(ports.ObjectInfo) error) error {
	b.mu.Lock()
	var infos []ports.ObjectInfo
	for k, o := range b.objects {
		if strings.HasPrefix(k, prefix) {
			infos = append(infos, ports.ObjectInfo{Key: k, Size: int64(len(o.data)), ContentType: o.ct, LastModified: o.mod})
		}
	}
	b.mu.Unlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
	for _, i := range infos {
		if err := fn(i); err != nil {
			return err
		}
	}
	return nil
}

// Keys lists all stored keys (tests).
func (b *Blob) Keys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for k := range b.objects {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Age backdates an object's modification time (cleanup tests).
func (b *Blob) Age(key string, d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if o, ok := b.objects[key]; ok {
		o.mod = o.mod.Add(-d)
		b.objects[key] = o
	}
}

// ---------------- Locker ----------------

// Locker is an in-memory ports.Locker.
type Locker struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
	seq   int64
}

type lockEntry struct {
	token  int64
	expiry time.Time
}

// NewLocker returns an empty locker.
func NewLocker() *Locker { return &Locker{locks: map[string]*lockEntry{}} }

type lease struct {
	l     *Locker
	name  string
	token int64
}

func (l *Locker) TryLock(_ context.Context, name string, ttl time.Duration) (ports.Lease, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if e, ok := l.locks[name]; ok && e.expiry.After(now) {
		return nil, false, nil
	}
	l.seq++
	l.locks[name] = &lockEntry{token: l.seq, expiry: now.Add(ttl)}
	return &lease{l: l, name: name, token: l.seq}, true, nil
}

func (s *lease) Extend(_ context.Context, ttl time.Duration) error {
	s.l.mu.Lock()
	defer s.l.mu.Unlock()
	e, ok := s.l.locks[s.name]
	if !ok || e.token != s.token || !e.expiry.After(time.Now()) {
		return fmt.Errorf("lease %q lost", s.name)
	}
	e.expiry = time.Now().Add(ttl)
	return nil
}

func (s *lease) Release(_ context.Context) error {
	s.l.mu.Lock()
	defer s.l.mu.Unlock()
	if e, ok := s.l.locks[s.name]; ok && e.token == s.token {
		delete(s.l.locks, s.name)
	}
	return nil
}
