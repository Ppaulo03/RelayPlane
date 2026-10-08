// Package observability provides Prometheus metrics, structured logging with
// context-carried identifiers, and OpenTelemetry tracing helpers.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Metrics groups every collector exported by RelayPlane (prefix relayplane_).
type Metrics struct {
	Registry *prometheus.Registry

	InstancesTotal         *prometheus.GaugeVec // by observed state
	InstancesConnected     prometheus.Gauge
	ProviderNodesTotal     *prometheus.GaugeVec // by status
	ProviderNodeHealth     *prometheus.GaugeVec // by node, 1 = READY
	OutboundQueueDepth     prometheus.Gauge
	OutboundRetryTotal     *prometheus.CounterVec // by class
	OutboundDLQTotal       prometheus.Counter
	OutboundMessages       *prometheus.CounterVec // by terminal status
	InboundEventsTotal     *prometheus.CounterVec // by event type
	InboundDuplicates      prometheus.Counter
	OwnershipViolation     prometheus.Counter
	StaleCommandTotal      prometheus.Counter
	EpochMismatchTotal     prometheus.Counter
	ReconciliationTotal    *prometheus.CounterVec // by action
	ReconciliationFail     prometheus.Counter
	ReconciliationDrift    prometheus.Counter
	RateLimitWait          prometheus.Histogram
	BlobBytes              *prometheus.CounterVec   // by op (put|get)
	EventDeliveryLag       *prometheus.HistogramVec // seconds from "RelayPlane knows" to the consumer's 2xx, by event type and attempt
	WebhookBacklogOverflow prometheus.Counter       // deliveries created DEAD because their subscription was over its backlog limit
	EventProjectionPending prometheus.Gauge         // events the catalog has not applied yet
	EventProjectionOldest  prometheus.Gauge         // seconds the oldest of them has waited
	EventOutboxPending     prometheus.Gauge         // accepted events not yet on the bus
	EventOutboxOldest      prometheus.Gauge         // seconds the oldest of them has waited
	UnknownMessages        prometheus.Gauge         // messages waiting for a decision (UNKNOWN)
	UnknownOldest          prometheus.Gauge         // seconds the oldest UNKNOWN has been waiting
	RetentionApplied       *prometheus.CounterVec   // records changed by retention (messages|dead_deliveries)
	RateLimited            prometheus.Counter       // API requests refused because the tenant spent its budget
	InboundMedia           *prometheus.CounterVec   // inbound attachments by outcome (ready|too_large|type_not_allowed|unsupported|expired|download_failed|retry)
	InboundMediaPending    *prometheus.GaugeVec     // jobs waiting by stage (download|publish)
	BlobCleanupTotal       *prometheus.CounterVec   // by reason
	ProviderLatency        *prometheus.HistogramVec
	HTTPRequests           *prometheus.CounterVec
	HTTPLatency            *prometheus.HistogramVec
	MigrationBlockedTotal  prometheus.Counter
	BarrierDeferrals       *prometheus.CounterVec // by reason (unknown|unresolved)
	OutboxPublished        prometheus.Counter
	BusConsumerLag         *prometheus.GaugeVec // by group
	BusOldestPending       *prometheus.GaugeVec // by group, seconds
	BusEventsLost          *prometheus.GaugeVec // by group: events trimmed before the group read them

	WebhookDeliveries    *prometheus.CounterVec // by result (delivered|retry|dead|postponed)
	WebhookLatency       prometheus.Histogram   // seconds per delivery attempt
	WebhookPending       prometheus.Gauge       // deliveries waiting (incl. retries)
	WebhookDead          prometheus.Gauge       // deliveries in the DLQ
	WebhookOldestPending prometheus.Gauge       // age (s) of the oldest pending delivery
	WebhookBreakersOpen  prometheus.Gauge       // destinations whose circuit is open
}

// NewMetrics registers all collectors on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Metrics{Registry: reg}

	m.InstancesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relayplane_instances_total", Help: "Instances by observed state."}, []string{"state"})
	m.InstancesConnected = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_instances_connected", Help: "Instances currently CONNECTED."})
	m.ProviderNodesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relayplane_provider_nodes_total", Help: "Provider nodes by status."}, []string{"status"})
	m.ProviderNodeHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relayplane_provider_node_health", Help: "1 when the node is READY, else 0."}, []string{"node", "provider"})
	m.OutboundQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_outbound_queue_depth", Help: "Unacknowledged outbound commands."})
	m.OutboundRetryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_outbound_retry_total", Help: "Outbound retries by failure class."}, []string{"class"})
	m.OutboundDLQTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_outbound_dlq_total", Help: "Outbound commands dead-lettered."})
	m.OutboundMessages = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_outbound_messages_total", Help: "Outbound messages by resulting status."}, []string{"status"})
	m.InboundEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_inbound_events_total", Help: "Canonical inbound events published."}, []string{"type"})
	m.InboundDuplicates = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_inbound_duplicates_total", Help: "Inbound events suppressed as duplicates."})
	m.OwnershipViolation = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_ownership_violation_total", Help: "Webhooks claiming a node that does not own the instance."})
	m.StaleCommandTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_stale_command_total", Help: "Commands rejected for an outdated assignment."})
	m.EpochMismatchTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_assignment_epoch_mismatch_total", Help: "Operations rejected because of an assignment epoch mismatch."})
	m.ReconciliationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_reconciliation_total", Help: "Reconciliation passes by action."}, []string{"action"})
	m.ReconciliationFail = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_reconciliation_failure_total", Help: "Reconciliation passes that failed."})
	m.ReconciliationDrift = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_reconciliation_drift_total", Help: "Instances found with desired != observed."})
	m.RateLimitWait = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "relayplane_rate_limit_wait_seconds", Help: "Time sends were delayed by rate limiting.", Buckets: prometheus.ExponentialBuckets(0.05, 2, 12)})
	m.EventDeliveryLag = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relayplane_event_delivery_lag_seconds",
		Help:    "Seconds from the moment RelayPlane learned of an event to the consumer's 2xx answer. attempt=first is the healthy-path latency (target p95 < 2s); attempt=recovered waited for the lease of a dead worker; attempt=retry includes the backoff.",
		Buckets: []float64{0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300}}, []string{"event_type", "attempt"})
	m.WebhookBacklogOverflow = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_webhook_backlog_overflow_total", Help: "Deliveries created DEAD (in the DLQ) because their subscription already had its maximum of deliveries waiting."})
	m.EventProjectionPending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_event_projection_pending", Help: "Events (delivery receipts, session state changes) the catalog has not applied yet."})
	m.EventProjectionOldest = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_event_projection_oldest_seconds", Help: "How long the oldest unapplied receipt or state change has waited: growing means no projector is running."})
	m.EventOutboxPending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_event_outbox_pending", Help: "Accepted events (inbound messages, statuses) whose deliveries for the tenant do not exist yet."})
	m.EventOutboxOldest = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_event_outbox_oldest_seconds", Help: "How long the oldest accepted event has waited for its tenant deliveries: growing means no worker is fanning the outbox out."})
	m.UnknownMessages = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_unknown_messages", Help: "Outbound messages in UNKNOWN: they wait for a decision (resolve) and hold back the later messages of their instance."})
	m.UnknownOldest = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_unknown_oldest_seconds", Help: "How long the oldest UNKNOWN message has been waiting."})
	m.RetentionApplied = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_retention_applied_total", Help: "Records anonymized or deleted by retention."}, []string{"kind"})
	m.RateLimited = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_api_rate_limited_total", Help: "API requests refused with 429."})
	m.InboundMedia = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_inbound_media_total", Help: "Inbound attachments by outcome."}, []string{"outcome"})
	m.InboundMediaPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "relayplane_inbound_media_pending", Help: "Inbound attachments waiting to be resolved or published, by stage."}, []string{"stage"})
	m.BlobBytes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_blob_bytes", Help: "Bytes moved through the blob store."}, []string{"op"})
	m.BlobCleanupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_blob_cleanup_total", Help: "Blobs removed by lifecycle cleanup."}, []string{"reason"})
	m.ProviderLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relayplane_provider_request_seconds", Help: "Provider call latency.", Buckets: prometheus.DefBuckets}, []string{"op", "outcome"})
	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_http_requests_total", Help: "HTTP requests."}, []string{"route", "method", "code"})
	m.HTTPLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relayplane_http_request_seconds", Help: "HTTP latency.", Buckets: prometheus.DefBuckets}, []string{"route"})
	m.MigrationBlockedTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_migration_blocked_total", Help: "Migrations blocked because fencing could not be confirmed."})

	m.BarrierDeferrals = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_outbound_barrier_deferrals_total", Help: "Dispatches deferred by the per-instance ordering barrier."}, []string{"reason"})
	m.OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_outbox_published_total", Help: "Commands published from the transactional outbox."})
	m.WebhookDeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_webhook_deliveries_total", Help: "Webhook delivery attempts by result."}, []string{"result"})
	m.WebhookLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "relayplane_webhook_delivery_seconds", Help: "Latency of webhook POSTs.", Buckets: prometheus.DefBuckets})
	m.WebhookPending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_webhook_pending", Help: "Webhook deliveries waiting to be (re)tried."})
	m.WebhookDead = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_webhook_dead", Help: "Webhook deliveries in the DLQ."})
	m.WebhookOldestPending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_webhook_oldest_pending_seconds", Help: "Age of the oldest pending webhook delivery."})
	m.WebhookBreakersOpen = prometheus.NewGauge(prometheus.GaugeOpts{Name: "relayplane_webhook_circuit_open", Help: "Destinations whose circuit breaker is open."})
	for _, c := range []prometheus.Collector{m.WebhookDeliveries, m.WebhookLatency, m.WebhookPending, m.WebhookDead, m.WebhookOldestPending, m.WebhookBreakersOpen,
		m.InstancesTotal, m.InstancesConnected, m.ProviderNodesTotal, m.ProviderNodeHealth, m.OutboundQueueDepth,
		m.OutboundRetryTotal, m.OutboundDLQTotal, m.OutboundMessages, m.InboundEventsTotal, m.InboundDuplicates,
		m.OwnershipViolation, m.StaleCommandTotal, m.EpochMismatchTotal, m.ReconciliationTotal, m.ReconciliationFail,
		m.ReconciliationDrift, m.RateLimitWait, m.BlobBytes, m.EventProjectionPending, m.EventProjectionOldest, m.WebhookBacklogOverflow, m.EventOutboxPending, m.EventOutboxOldest, m.UnknownMessages, m.UnknownOldest, m.EventDeliveryLag, m.RetentionApplied, m.RateLimited, m.InboundMedia, m.InboundMediaPending, m.BlobCleanupTotal, m.ProviderLatency, m.HTTPRequests,
		m.HTTPLatency, m.MigrationBlockedTotal, m.BarrierDeferrals, m.OutboxPublished,
	} {
		f(c)
	}
	return m
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
