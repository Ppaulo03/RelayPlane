// Package bootstrap is the composition root: the only place (besides cmd/)
// that knows concrete adapters and wires them into the application.
package bootstrap

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/relayplane/relayplane/internal/adapters/blob/s3"
	"github.com/relayplane/relayplane/internal/adapters/lock/redislock"
	"github.com/relayplane/relayplane/internal/adapters/messaging/redisstreams"
	"github.com/relayplane/relayplane/internal/adapters/persistence/postgres"
	evolution "github.com/relayplane/relayplane/internal/adapters/providers/evolution/v2"
	"github.com/relayplane/relayplane/internal/adapters/webhookout"
	apihttp "github.com/relayplane/relayplane/internal/api/http"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/config"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/delivery"
	"github.com/relayplane/relayplane/internal/idempotency"
	"github.com/relayplane/relayplane/internal/observability"
)

// Runtime holds the wired application and its resources.
type Runtime struct {
	Cfg     config.Config
	Log     *slog.Logger
	Metrics *observability.Metrics
	App     *app.App
	Store   *postgres.Store
	Redis   *redis.Client
	Queue   *redisstreams.Queue
	Bus     *redisstreams.Bus
	Blob    *s3.Store

	// FanOut and Dispatcher deliver tenant-facing events to webhook subscriptions (run by the worker).
	FanOut     *delivery.FanOut
	Dispatcher *delivery.Dispatcher

	shutdownTrace func(context.Context) error
}

// New connects every dependency. service names the binary for logs/traces.
func New(ctx context.Context, cfg config.Config, service string) (*Runtime, error) {
	log := observability.NewLogger(os.Stdout, cfg.LogLevel, service)
	slog.SetDefault(log)
	shutdown, err := observability.InitTracing(ctx, "relayplane-"+service)
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}
	rt := &Runtime{Cfg: cfg, Log: log, Metrics: observability.NewMetrics(), shutdownTrace: shutdown}

	if rt.Store, err = postgres.Open(ctx, cfg.DatabaseURL); err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	if cfg.AutoMigrate {
		if err := rt.Store.Migrate(ctx); err != nil {
			return nil, fmt.Errorf("migrate: %w", err)
		}
		log.Info("database migrations applied")
	}
	ropts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL: %w", err)
	}
	rt.Redis = redis.NewClient(ropts)
	if err := rt.Redis.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	if rt.Queue, err = redisstreams.NewQueue(ctx, rt.Redis, redisstreams.QueueConfig{
		Partitions: cfg.CommandPartitions, InlineMaxBytes: cfg.MediaInlineMaxBytes, LeaseTTL: cfg.CommandLeaseTTL}); err != nil {
		return nil, fmt.Errorf("command queue: %w", err)
	}
	rt.Bus = redisstreams.NewBus(rt.Redis, redisstreams.BusConfig{MaxLen: int64(cfg.EventBusRetention)})
	if rt.Blob, err = s3.New(ctx, s3.Config{Endpoint: cfg.BlobEndpoint, PublicEndpoint: cfg.BlobPublicEndpoint, AccessKey: cfg.BlobAccessKey,
		SecretKey: cfg.BlobSecretKey, Bucket: cfg.BlobBucket, UseSSL: cfg.BlobUseSSL, PublicUseSSL: cfg.BlobPublicUseSSL, LifecycleDays: cfg.BlobLifecycleDays}); err != nil {
		return nil, fmt.Errorf("blob store: %w", err)
	}

	// providers
	nodes := evolution.StaticNodes{}
	for _, n := range cfg.Nodes {
		if n.Provider == "" || n.Provider == evolution.ProviderKey {
			nodes[n.ID] = evolution.NodeInfo{BaseURL: n.Endpoint, APIKey: n.APIKey}
		}
	}
	reg := app.NewProviderRegistry()
	reg.Register(evolution.ProviderKey,
		evolution.New(evolution.Config{Nodes: nodes, WebhookBaseURL: cfg.WebhookBaseURL, WebhookSecret: cfg.WebhookSecret, AllowedVersions: cfg.EvolutionAllowedVersions}),
		evolution.Webhook{Secret: cfg.WebhookSecret})

	acfg := app.DefaultConfig()
	acfg.DefaultProvider = cfg.DefaultProvider
	acfg.MediaPolicy = media.DefaultPolicy()
	acfg.MediaPolicy.InlineMaxBytes = cfg.MediaInlineMaxBytes
	acfg.MediaPolicy.MaxBytes = cfg.MediaMaxBytes
	acfg.MediaTTL = cfg.MediaTTL
	acfg.PendingTTL = cfg.MediaPendingTTL
	acfg.InboundMediaMaxBytes, acfg.InboundMediaTTL = cfg.InboundMediaMaxBytes, cfg.InboundMediaTTL
	acfg.MigrationVerifyTimeout = cfg.MigrationVerifyTimeout
	acfg.DefaultRate = messaging.RatePolicy{MinInterval: cfg.RateMinInterval, Burst: cfg.RateBurst, MaxPerMinute: cfg.RateMaxPerMinute,
		MaxConcurrent: cfg.RateMaxConcurrent, Cooldown: cfg.RateCooldown}
	subKey := subscriptionKey(cfg)
	erKey := erasureKey(cfg)
	acfg.ErasureKey = erKey
	acfg.Subscriptions = app.SubscriptionConfig{ServerKey: subKey, MaxPerTenant: cfg.WebhooksMaxPerTenant,
		AllowInsecureURLs: cfg.WebhooksAllowInsecure, AllowPrivateDestinations: cfg.WebhooksAllowPrivate}

	repos := rt.Store.Repositories()
	idem := idempotency.NewService(repos.Idempotency)
	if cfg.IdempotencyTTL > 0 {
		idem.TTL = cfg.IdempotencyTTL
	}
	rt.App = app.New(app.Deps{Repos: repos, Providers: reg, Queue: rt.Queue, Bus: rt.Bus, Blob: rt.Blob,
		Locker: redislock.New(rt.Redis, ""), Idem: idem, Metrics: rt.Metrics, Log: log, Cfg: acfg})

	rt.FanOut = &delivery.FanOut{Repos: repos, Log: log, Metrics: rt.Metrics, ErasureKey: erKey}
	rt.Dispatcher = &delivery.Dispatcher{Repos: repos, ServerKey: subKey, ErasureKey: erKey, Metrics: rt.Metrics, Log: log,
		Sender:         webhookout.New(webhookout.Config{AllowPrivate: cfg.WebhooksAllowPrivate, AllowInsecure: cfg.WebhooksAllowInsecure}),
		RequestTimeout: cfg.WebhookDeliveryTimeout, Concurrency: cfg.WebhookDeliveryWorkers, MaxInFlightPerSubscription: cfg.WebhookMaxInFlightPerSub}
	return rt, nil
}

// subscriptionKey is the server key that derives webhook signing secrets: SUBSCRIPTION_SECRET when set,
// otherwise derived from WEBHOOK_SECRET with domain separation (so the two uses never share key material).
func subscriptionKey(cfg config.Config) []byte {
	if cfg.SubscriptionSecret != "" {
		return []byte(cfg.SubscriptionSecret)
	}
	m := hmac.New(sha256.New, []byte(cfg.WebhookSecret))
	m.Write([]byte("relayplane/subscription-server-key/v1"))
	return m.Sum(nil)
}

// erasureKey keys the tombstones of erased contacts: ERASURE_KEY when set, otherwise derived from WEBHOOK_SECRET with domain separation.
func erasureKey(cfg config.Config) []byte {
	if cfg.ErasureKey != "" {
		return []byte(cfg.ErasureKey)
	}
	m := hmac.New(sha256.New, []byte(cfg.WebhookSecret))
	m.Write([]byte("relayplane/erasure-key/v1"))
	return m.Sum(nil)
}

// SeedNodes registers the configured nodes in the catalog (idempotent; keeps
// runtime state such as status and active_instances).
func (rt *Runtime) SeedNodes(ctx context.Context) error {
	for _, n := range rt.Cfg.Nodes {
		p := n.Provider
		if p == "" {
			p = evolution.ProviderKey
		}
		if err := rt.App.Nodes.Register(ctx, routing.Node{ID: n.ID, Provider: p, Endpoint: n.Endpoint, Capacity: n.Capacity}); err != nil {
			return fmt.Errorf("register node %s: %w", n.ID, err)
		}
	}
	return nil
}

// ReadyChecks are the readiness dependencies of the gateway.
func (rt *Runtime) ReadyChecks() []apihttp.ReadyCheck {
	return []apihttp.ReadyCheck{
		{Name: "postgres", Check: rt.Store.Ping},
		{Name: "command_queue", Check: func(ctx context.Context) error { _, err := rt.Queue.Depth(ctx); return err }},
		{Name: "event_bus", Check: func(ctx context.Context) error { return rt.Redis.Ping(ctx).Err() }},
		{Name: "blob_store", Check: rt.Blob.Ping},
	}
}

// ServeOps exposes /metrics and health for binaries without a public API.
func (rt *Runtime) ServeOps(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", rt.Metrics.Handler())
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		for _, c := range rt.ReadyChecks() {
			if err := c.Check(r.Context()); err != nil {
				w.WriteHeader(503)
				return
			}
		}
		w.WriteHeader(200)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			rt.Log.Error("ops server failed", "error", err)
		}
	}()
}

// Close releases resources.
func (rt *Runtime) Close(ctx context.Context) {
	if rt.shutdownTrace != nil {
		_ = rt.shutdownTrace(ctx)
	}
	if rt.Redis != nil {
		_ = rt.Redis.Close()
	}
	if rt.Store != nil {
		rt.Store.Close()
	}
}
