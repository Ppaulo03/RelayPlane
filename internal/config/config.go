// Package config loads RelayPlane settings from environment variables.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Node describes one provider node known to the deployment. The API key is a
// secret and is never persisted in the catalog.
type Node struct {
	ID       string `json:"id"`
	Provider string `json:"provider"`
	Endpoint string `json:"endpoint"`
	APIKey   string `json:"api_key"`
	Capacity int    `json:"capacity"`
}

// Config is the full runtime configuration.
type Config struct {
	Env      string
	LogLevel slog.Level

	HTTPPort string
	OpsPort  string // metrics/health port of worker and reconciler

	DatabaseURL string
	AutoMigrate bool
	RedisURL    string

	// EventBusRetention is how many events the bus keeps (approximate stream cap). A consumer group
	// that falls further behind than this loses events: size it for the worst consumer outage.
	EventBusRetention int
	CommandPartitions int
	CommandLeaseTTL   time.Duration

	BlobEndpoint       string
	BlobPublicEndpoint string
	BlobBucket         string
	BlobAccessKey      string
	BlobSecretKey      string
	BlobUseSSL         bool
	BlobPublicUseSSL   bool
	BlobLifecycleDays  int

	MediaInlineMaxBytes int
	MediaMaxBytes       int64
	MediaTTL            time.Duration
	MediaPendingTTL     time.Duration // an upload must finish within this window

	AdminAPIKey    string
	WebhookSecret  string
	WebhookBaseURL string

	DefaultProvider string
	Nodes           []Node
	// EvolutionAllowedVersions overrides the adapter's tested-version allow-list.
	EvolutionAllowedVersions []string

	RateMinInterval   time.Duration
	RateBurst         int
	RateMaxPerMinute  int
	RateMaxConcurrent int
	RateCooldown      time.Duration

	ReconcilerEnabled  bool
	ReconcilerInterval time.Duration
	InstanceInterval   time.Duration
	NodeOfflineAfter   time.Duration

	MigrationVerifyTimeout time.Duration
	// UnknownBarrierTimeout: how long an UNKNOWN message holds back later messages of the
	// same instance. Default 0 = until it is resolved (strict ordering); a positive value is
	// an explicit availability-over-ordering choice.
	UnknownBarrierTimeout time.Duration

	// IdempotencyTTL is how long Idempotency-Keys are remembered. A client may safely retry a send with
	// the same key only inside this window.
	IdempotencyTTL time.Duration

	// Tenant webhook subscriptions. SubscriptionSecret derives the per-subscription signing secrets; when
	// empty it is derived from WEBHOOK_SECRET (with domain separation).
	SubscriptionSecret        string
	WebhooksMaxPerTenant      int
	WebhooksAllowInsecure     bool // http:// destinations (default: development only)
	WebhooksAllowPrivate      bool // loopback/private destinations (default: development only)
	WebhookDeliveryTimeout    time.Duration
	WebhookDeliveryWorkers    int
	WebhookDeliveredRetention time.Duration
}

// Load reads the environment. Secrets have no defaults: a missing required
// value is reported by Validate rather than silently replaced.
func Load() (Config, error) {
	c := Config{
		Env: get("APP_ENV", "development"), HTTPPort: get("HTTP_PORT", "8080"), OpsPort: get("OPS_PORT", "9090"),
		DatabaseURL: os.Getenv("DATABASE_URL"), AutoMigrate: getBool("AUTO_MIGRATE", false), RedisURL: get("REDIS_URL", "redis://localhost:6379/0"),
		EventBusRetention: getInt("EVENT_BUS_RETENTION", 100000), CommandPartitions: getInt("COMMAND_PARTITIONS", 32), CommandLeaseTTL: getDur("COMMAND_LEASE_TTL", 15*time.Second),
		BlobEndpoint: os.Getenv("BLOB_STORE_ENDPOINT"), BlobPublicEndpoint: os.Getenv("BLOB_STORE_PUBLIC_ENDPOINT"),
		BlobBucket: get("BLOB_STORE_BUCKET", "relayplane-media"), BlobAccessKey: os.Getenv("BLOB_STORE_ACCESS_KEY"), BlobSecretKey: os.Getenv("BLOB_STORE_SECRET_KEY"),
		BlobUseSSL: getBool("BLOB_STORE_USE_SSL", false), BlobPublicUseSSL: getBool("BLOB_STORE_PUBLIC_USE_SSL", false), BlobLifecycleDays: getInt("BLOB_STORE_LIFECYCLE_DAYS", 7),
		MediaInlineMaxBytes: getInt("MEDIA_INLINE_MAX_BYTES", 262144), MediaMaxBytes: int64(getInt("MEDIA_MAX_BYTES", 100<<20)), MediaTTL: getDur("MEDIA_DEFAULT_TTL", 24*time.Hour), MediaPendingTTL: getDur("MEDIA_PENDING_TTL", 30*time.Minute),
		AdminAPIKey: os.Getenv("ADMIN_API_KEY"), WebhookSecret: os.Getenv("WEBHOOK_SECRET"), WebhookBaseURL: os.Getenv("WEBHOOK_BASE_URL"),
		DefaultProvider: get("DEFAULT_PROVIDER", "evolution-v2"),
		RateMinInterval: getDur("RATE_MIN_INTERVAL", time.Second), RateBurst: getInt("RATE_BURST", 1), RateMaxPerMinute: getInt("RATE_MAX_PER_MINUTE", 30),
		RateMaxConcurrent: getInt("RATE_MAX_CONCURRENT", 1), RateCooldown: getDur("RATE_COOLDOWN", time.Minute),
		ReconcilerEnabled: getBool("RECONCILER_ENABLED", true), ReconcilerInterval: getDur("RECONCILER_INTERVAL", 10*time.Second),
		InstanceInterval: getDur("RECONCILER_INSTANCE_INTERVAL", 30*time.Second), NodeOfflineAfter: getDur("NODE_OFFLINE_AFTER", time.Minute),
		MigrationVerifyTimeout: getDur("MIGRATION_VERIFY_TIMEOUT", 10*time.Minute),
		UnknownBarrierTimeout:  getDur("UNKNOWN_BARRIER_TIMEOUT", 0),
		IdempotencyTTL:         getDur("IDEMPOTENCY_TTL", 24*time.Hour),
		SubscriptionSecret:     os.Getenv("SUBSCRIPTION_SECRET"),
		WebhooksMaxPerTenant:   getInt("WEBHOOKS_MAX_PER_TENANT", 10),
		WebhookDeliveryTimeout: getDur("WEBHOOK_DELIVERY_TIMEOUT", 5*time.Second),
		WebhookDeliveryWorkers: getInt("WEBHOOK_DELIVERY_WORKERS", 8),

		WebhookDeliveredRetention: getDur("WEBHOOK_DELIVERED_RETENTION", 7*24*time.Hour),
	}
	dev := c.Env != "production"
	c.WebhooksAllowInsecure = getBool("WEBHOOKS_ALLOW_INSECURE", dev)
	c.WebhooksAllowPrivate = getBool("WEBHOOKS_ALLOW_PRIVATE_DESTINATIONS", dev)
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(get("LOG_LEVEL", "info"))); err != nil {
		return c, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	c.LogLevel = lvl
	if raw := os.Getenv("PROVIDER_NODES"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.Nodes); err != nil {
			return c, fmt.Errorf("PROVIDER_NODES: %w", err)
		}
	}
	for i := range c.Nodes {
		if c.Nodes[i].Provider == "" {
			c.Nodes[i].Provider = c.DefaultProvider
		}
	}
	if v := os.Getenv("EVOLUTION_ALLOWED_VERSIONS"); v != "" {
		c.EvolutionAllowedVersions = strings.Split(v, ",")
	}
	return c, nil
}

// Validate checks the settings every role needs. All roles resolve provider
// nodes (gateway/reconciler to create and probe, workers to send), so a process
// that could never reach a provider must not start "healthy".
func (c Config) Validate() error {
	var missing []string
	need := func(name, v string) {
		if v == "" {
			missing = append(missing, name)
		}
	}
	need("DATABASE_URL", c.DatabaseURL)
	need("BLOB_STORE_ENDPOINT", c.BlobEndpoint)
	need("BLOB_STORE_ACCESS_KEY", c.BlobAccessKey)
	need("BLOB_STORE_SECRET_KEY", c.BlobSecretKey)
	need("WEBHOOK_SECRET", c.WebhookSecret)
	if len(missing) > 0 {
		return fmt.Errorf("missing required configuration: %s", strings.Join(missing, ", "))
	}
	if c.Env == "production" && c.SubscriptionSecret != "" && len(c.SubscriptionSecret) < 16 {
		return fmt.Errorf("SUBSCRIPTION_SECRET must have at least 16 characters in production")
	}
	if c.IdempotencyTTL != 0 && c.IdempotencyTTL < time.Minute {
		return fmt.Errorf("IDEMPOTENCY_TTL must be at least 1m")
	}
	if c.Env == "production" && len(c.WebhookSecret) < 16 {
		return fmt.Errorf("WEBHOOK_SECRET must have at least 16 characters in production")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("PROVIDER_NODES must list at least one provider node")
	}
	seen := map[string]bool{}
	for _, n := range c.Nodes {
		switch {
		case n.ID == "", n.Endpoint == "", n.Provider == "", n.APIKey == "", n.Capacity <= 0:
			return fmt.Errorf("PROVIDER_NODES entry %+v needs id, provider, endpoint, api_key and capacity > 0", redacted(n))
		case seen[n.ID]:
			return fmt.Errorf("PROVIDER_NODES: duplicate node id %q (nodes are singletons with unique identities)", n.ID)
		}
		seen[n.ID] = true
	}
	return nil
}

func redacted(n Node) Node { n.APIKey = "[REDACTED]"; return n }

func get(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func getDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
