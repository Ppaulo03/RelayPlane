// Package systemtest wires the real application services, worker and
// reconciler against the in-memory adapters to verify the platform invariants
// end to end without external infrastructure.
package systemtest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/delivery"
	"github.com/relayplane/relayplane/internal/idempotency"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
	"github.com/relayplane/relayplane/internal/reconciler"
	"github.com/relayplane/relayplane/internal/worker"
)

const (
	ProviderKey = "evolution-v2"
	InlineLimit = 1024
	WebhookTok  = "secret"
)

// Env is a full in-memory RelayPlane.
type Env struct {
	outboxOnce sync.Once
	T          *testing.T
	Store      *memory.Store // nil with real infrastructure
	Repos      ports.Repositories
	Queue      ports.CommandQueue
	QueueFault *FaultyQueue // wraps the queue: tests can make publishes fail or vanish
	Bus        *RecBus
	Blob       ports.BlobStore
	Locker     ports.Locker
	Provider   *memory.FakeProvider
	Metrics    *observability.Metrics
	App        *app.App
	Worker     *worker.Outbound
	Projector  *worker.Projector
	Reconciler *reconciler.Reconciler
	Tenant     string // primary tenant id
	Tenant2    string

	// Webhooks: the tenant-facing event delivery pipeline with a controllable receiver in place of the network.
	Receiver    *Receiver
	FanOut      *delivery.FanOut
	Dispatcher  *delivery.Dispatcher
	MediaIngest *worker.MediaIngestor

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Backend is the set of infrastructure ports an Env runs on.
type Backend struct {
	Store  *memory.Store
	Repos  ports.Repositories
	Queue  ports.CommandQueue
	Bus    ports.EventBus
	Blob   ports.BlobStore
	Locker ports.Locker
}

// RealBackend, when set (integration build), builds PostgreSQL+Redis+S3
// backends. It is used when RELAYPLANE_SYSTEMTEST_BACKEND=real.
var RealBackend func(t *testing.T) *Backend

func memoryBackend() *Backend {
	st := memory.NewStore()
	return &Backend{Store: st, Repos: st.Repositories(),
		Queue: memory.NewQueue(memory.QueueConfig{Partitions: 8, InlineMaxBytes: InlineLimit, DefaultRetryDelay: time.Millisecond}),
		Bus:   memory.NewBus(), Blob: memory.NewBlob(), Locker: memory.NewLocker()}
}

// RecBus records everything published through it.
type RecBus struct {
	ports.EventBus
	mu  sync.Mutex
	log []events.Event
	// Down makes Publish fail (the broker is unavailable).
	Down atomic.Bool
	// Drop makes Publish "succeed" and lose the event (the broker answered OK and then lost its data, e.g. a Redis wiped before any
	// consumer read the entry).
	Drop atomic.Bool
}

// Publish implements ports.EventBus.
func (b *RecBus) Publish(ctx context.Context, ev events.Event) error {
	if b.Down.Load() {
		return errors.New("event bus unavailable")
	}
	if b.Drop.Load() {
		return nil
	}
	if err := b.EventBus.Publish(ctx, ev); err != nil {
		return err
	}
	b.mu.Lock()
	b.log = append(b.log, ev)
	b.mu.Unlock()
	return nil
}

// Published returns the events published so far.
func (b *RecBus) Published() []events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]events.Event(nil), b.log...)
}

// FaultyQueue decorates a CommandQueue with broker faults.
type FaultyQueue struct {
	ports.CommandQueue
	Down atomic.Bool // Publish fails (broker unreachable)
	Drop atomic.Bool // Publish "succeeds" but the broker loses the command
}

// Publish implements ports.CommandQueue.
func (q *FaultyQueue) Publish(ctx context.Context, cmd ports.Command) error {
	if q.Down.Load() {
		return errors.New("broker unreachable")
	}
	if q.Drop.Load() {
		return nil
	}
	return q.CommandQueue.Publish(ctx, cmd)
}

// NewEnv builds an environment with nodes node-01 and node-02 (capacity 10,
// READY) and two tenants.
func NewEnv(t *testing.T) *Env {
	t.Helper()
	e := &Env{T: t}
	var be *Backend
	if os.Getenv("RELAYPLANE_SYSTEMTEST_BACKEND") == "real" && RealBackend != nil {
		be = RealBackend(t)
	} else {
		be = memoryBackend()
	}
	e.Store, e.Repos, e.Queue, e.Blob, e.Locker = be.Store, be.Repos, be.Queue, be.Blob, be.Locker
	e.QueueFault = &FaultyQueue{CommandQueue: be.Queue}
	e.Queue = e.QueueFault
	e.Bus = &RecBus{EventBus: be.Bus}
	e.Provider = memory.NewFakeProvider()
	e.Metrics = observability.NewMetrics()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	reg := app.NewProviderRegistry()
	reg.Register(ProviderKey, e.Provider, memory.FakeWebhook{ProviderKey: ProviderKey, Token: WebhookTok})

	cfg := app.DefaultConfig()
	cfg.MediaPolicy.InlineMaxBytes = InlineLimit
	cfg.MigrationVerifyTimeout = 300 * time.Millisecond
	cfg.Subscriptions = app.SubscriptionConfig{ServerKey: SubscriptionKey, AllowInsecureURLs: true, AllowPrivateDestinations: true}
	idem := idempotency.NewService(e.Repos.Idempotency)
	idem.StaleAfter = 50 * time.Millisecond
	e.App = app.New(app.Deps{Repos: e.Repos, Providers: reg, Queue: e.Queue, Bus: e.Bus, Blob: e.Blob, Locker: e.Locker,
		Idem: idem, Metrics: e.Metrics, Log: log, Cfg: cfg})

	e.Worker = worker.NewOutbound(e.Repos, reg, e.Blob, e.Metrics, log)
	e.Worker.GlobalPolicy = messaging.RatePolicy{} // unlimited unless a test sets one
	e.Worker.Retry = messaging.RetrySchedule{0, 5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}
	e.Worker.BarrierRecheck = 10 * time.Millisecond
	e.Projector = worker.NewProjector(e.Repos, log)
	e.Projector.Metrics = e.Metrics

	rc := reconciler.DefaultConfig()
	rc.InstanceInterval = 0
	rc.Policy.ConnectGrace = 0
	rc.Policy.CreateStuckAfter = 0
	rc.CallTimeout = 2 * time.Second
	e.Reconciler = reconciler.New(e.App, rc, log)

	e.Receiver = &Receiver{}
	e.FanOut = &delivery.FanOut{Repos: e.Repos, Log: log, Metrics: e.Metrics}
	e.Dispatcher = &delivery.Dispatcher{Repos: e.Repos, Sender: e.Receiver, ServerKey: SubscriptionKey, Metrics: e.Metrics, Log: log,
		Retry: subscription.RetryPolicy{Schedule: []time.Duration{20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}},
		Poll:  5 * time.Millisecond, Breaker: delivery.NewBreaker(1000, time.Millisecond, time.Millisecond)}

	e.MediaIngest = &worker.MediaIngestor{Repos: e.Repos, Providers: reg, Blob: e.Blob, ErasureKey: e.App.Deps.Cfg.ErasureKey, Metrics: e.Metrics, Log: log,
		MaxBytes: cfg.EffectiveInboundMaxBytes(), TTL: cfg.EffectiveInboundTTL(), Policy: cfg.MediaPolicy,
		Poll: 5 * time.Millisecond, MaxAttempts: 4, Backoff: func(int) time.Duration { return 5 * time.Millisecond }}

	e.ctx, e.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() {
		e.cancel()
		e.wg.Wait()
		for _, v := range e.Receiver.SchemaViolations() {
			t.Errorf("a delivered webhook body breaks the published event schema: %s", v)
		}
	})

	for _, id := range []string{"node-01", "node-02"} {
		e.AddNode(id, 10)
	}
	e.Tenant = e.MustTenant("acme")
	e.Tenant2 = e.MustTenant("globex")
	return e
}

// AddNode registers a READY node.
func (e *Env) AddNode(id string, capacity int) {
	e.T.Helper()
	ctx := context.Background()
	if err := e.Repos.Nodes.Upsert(ctx, routing.Node{ID: id, Provider: ProviderKey, Endpoint: "http://" + id, Capacity: capacity}); err != nil {
		e.T.Fatal(err)
	}
	if _, err := e.Repos.Nodes.SetStatus(ctx, id, routing.NodeReady); err != nil {
		e.T.Fatal(err)
	}
}

// MustTenant creates a tenant and returns its id.
func (e *Env) MustTenant(name string) string {
	e.T.Helper()
	tn, _, err := e.App.Tenants.Create(context.Background(), name)
	if err != nil {
		e.T.Fatal(err)
	}
	return tn.ID
}

// StartWorkers launches n outbound consumers and the projector.
func (e *Env) StartWorkers(n int) {
	for i := 0; i < n; i++ {
		e.wg.Add(1)
		go func() { defer e.wg.Done(); _ = e.Queue.Consume(e.ctx, e.Worker.Handle) }()
	}
}

// Flush publishes, now, the events waiting in the event outbox (an accepted inbound event reaches the bus through it).
func (e *Env) Flush() {
	for {
		n, err := e.App.EventOutbox.PublishPending(e.ctx, 100)
		if err != nil || n == 0 {
			return
		}
	}
}

// StartOutbox runs the outbox dispatcher loop (what the reconciler binary does every second). Calling it twice starts one loop.
func (e *Env) StartOutbox() {
	e.outboxOnce.Do(e.startOutbox)
}

func (e *Env) startOutbox() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		t := time.NewTicker(20 * time.Millisecond)
		defer t.Stop()
		for {
			_, _ = e.App.Outbox.DispatchPending(e.ctx, 100)
			_, _ = e.App.EventOutbox.PublishPending(e.ctx, 100)
			select {
			case <-e.ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// StartMedia runs the inbound attachment resolver (what the worker binary does).
func (e *Env) StartMedia() {
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.MediaIngest.Run(e.ctx) }()
}

// StartProjector launches the event projector consumer.
func (e *Env) StartProjector() {
	e.StartOutbox() // inbound events reach the bus through the outbox, as in production
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.Projector.Run(e.ctx) }() // from the database, as the worker binary does
}

// Stop halts background consumers (simulating a crash/restart).
func (e *Env) Stop() {
	e.cancel()
	e.wg.Wait()
	e.ctx, e.cancel = context.WithCancel(context.Background())
}

// CreateInstance creates an instance and (optionally) pairs it so it is CONNECTED.
func (e *Env) CreateInstance(tenant, name string, connect bool) *instance.Instance {
	e.T.Helper()
	res, _, err := e.App.Instances.Create(context.Background(), tenant, app.CreateInstanceInput{Name: name}, "")
	if err != nil {
		e.T.Fatalf("create instance: %v", err)
	}
	if connect {
		e.Connect(res.ID)
	}
	i, err := e.Repos.Instances.Get(context.Background(), res.ID)
	if err != nil {
		e.T.Fatal(err)
	}
	return i
}

// Connect simulates the user scanning the QR and the reconciler observing it.
func (e *Env) Connect(instanceID string) {
	e.T.Helper()
	cur, err := e.Repos.Instances.Get(context.Background(), instanceID)
	if err != nil {
		e.T.Fatal(err)
	}
	e.Provider.SetStateOn(cur.NodeID, instanceID, instance.Connected)
	if _, _, err := e.Reconciler.ReconcileInstance(context.Background(), instanceID); err != nil {
		e.T.Fatalf("reconcile: %v", err)
	}
	i, _ := e.Repos.Instances.Get(context.Background(), instanceID)
	if i.ObservedState != instance.Connected {
		e.T.Fatalf("instance %s observed %s, want CONNECTED", instanceID, i.ObservedState)
	}
}

// SendText accepts a text message via the API service.
func (e *Env) SendText(tenant, instanceID, text, key string) (app.SendResult, bool, error) {
	return e.App.Messages.Send(context.Background(), tenant, app.SendInput{
		InstanceID: instanceID, To: "5562999999999", Type: messaging.TypeText, Payload: app.SendPayload{Text: text}}, key)
}

// WaitMessage waits until the message reaches one of the statuses.
func (e *Env) WaitMessage(id string, want ...messaging.Status) *messaging.Message {
	e.T.Helper()
	var m *messaging.Message
	Eventually(e.T, 10*time.Second, "message "+id+" -> "+string(want[0]), func() bool {
		var err error
		m, err = e.Repos.Messages.Get(context.Background(), id)
		if err != nil {
			return false
		}
		for _, w := range want {
			if m.Status == w {
				return true
			}
		}
		return false
	})
	return m
}

// Eventually polls cond until it holds or the deadline passes.
func Eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
