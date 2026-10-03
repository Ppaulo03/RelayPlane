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

	InstancesTotal        *prometheus.GaugeVec // by observed state
	InstancesConnected    prometheus.Gauge
	ProviderNodesTotal    *prometheus.GaugeVec // by status
	ProviderNodeHealth    *prometheus.GaugeVec // by node, 1 = READY
	OutboundQueueDepth    prometheus.Gauge
	OutboundRetryTotal    *prometheus.CounterVec // by class
	OutboundDLQTotal      prometheus.Counter
	OutboundMessages      *prometheus.CounterVec // by terminal status
	InboundEventsTotal    *prometheus.CounterVec // by event type
	InboundDuplicates     prometheus.Counter
	OwnershipViolation    prometheus.Counter
	StaleCommandTotal     prometheus.Counter
	EpochMismatchTotal    prometheus.Counter
	ReconciliationTotal   *prometheus.CounterVec // by action
	ReconciliationFail    prometheus.Counter
	ReconciliationDrift   prometheus.Counter
	RateLimitWait         prometheus.Histogram
	BlobBytes             *prometheus.CounterVec // by op (put|get)
	BlobCleanupTotal      *prometheus.CounterVec // by reason
	ProviderLatency       *prometheus.HistogramVec
	HTTPRequests          *prometheus.CounterVec
	HTTPLatency           *prometheus.HistogramVec
	MigrationBlockedTotal prometheus.Counter
	BarrierDeferrals      *prometheus.CounterVec // by reason (unknown|unresolved)
	OutboxPublished       prometheus.Counter
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
	m.BlobBytes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_blob_bytes", Help: "Bytes moved through the blob store."}, []string{"op"})
	m.BlobCleanupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_blob_cleanup_total", Help: "Blobs removed by lifecycle cleanup."}, []string{"reason"})
	m.ProviderLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relayplane_provider_request_seconds", Help: "Provider call latency.", Buckets: prometheus.DefBuckets}, []string{"op", "outcome"})
	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_http_requests_total", Help: "HTTP requests."}, []string{"route", "method", "code"})
	m.HTTPLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relayplane_http_request_seconds", Help: "HTTP latency.", Buckets: prometheus.DefBuckets}, []string{"route"})
	m.MigrationBlockedTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_migration_blocked_total", Help: "Migrations blocked because fencing could not be confirmed."})

	m.BarrierDeferrals = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relayplane_outbound_barrier_deferrals_total", Help: "Dispatches deferred by the per-instance ordering barrier."}, []string{"reason"})
	m.OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{Name: "relayplane_outbox_published_total", Help: "Commands published from the transactional outbox."})
	for _, c := range []prometheus.Collector{
		m.InstancesTotal, m.InstancesConnected, m.ProviderNodesTotal, m.ProviderNodeHealth, m.OutboundQueueDepth,
		m.OutboundRetryTotal, m.OutboundDLQTotal, m.OutboundMessages, m.InboundEventsTotal, m.InboundDuplicates,
		m.OwnershipViolation, m.StaleCommandTotal, m.EpochMismatchTotal, m.ReconciliationTotal, m.ReconciliationFail,
		m.ReconciliationDrift, m.RateLimitWait, m.BlobBytes, m.BlobCleanupTotal, m.ProviderLatency, m.HTTPRequests,
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
