package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/relayplane/relayplane/internal/ports"
)

func TestRecordBusStatsExposesLagAndTrimRisk(t *testing.T) {
	m := NewMetrics()
	m.RecordBusStats(ports.EventBusStats{Length: 900, Retention: 1000, Groups: []ports.EventBusGroupStats{
		{Name: "projector", Lag: 600, Pending: 2, OldestPending: 90 * time.Second},
		{Name: "billing", Lag: 10},
	}})
	if got := testutil.ToFloat64(m.BusTrimRisk); got != 0.6 {
		t.Fatalf("trim risk %v, want the worst group (600/1000)", got)
	}
	if got := testutil.ToFloat64(m.BusConsumerLag.WithLabelValues("projector")); got != 600 {
		t.Fatalf("lag %v", got)
	}
	if got := testutil.ToFloat64(m.BusOldestPending.WithLabelValues("projector")); got != 90 {
		t.Fatalf("oldest pending %v", got)
	}
	// a group that disappears must not linger in the metrics
	m.RecordBusStats(ports.EventBusStats{Length: 1, Retention: 1000, Groups: []ports.EventBusGroupStats{{Name: "billing"}}})
	if n := testutil.CollectAndCount(m.BusConsumerLag); n != 1 {
		t.Fatalf("stale group series kept: %d", n)
	}
	if got := testutil.ToFloat64(m.BusTrimRisk); got != 0 {
		t.Fatalf("trim risk %v", got)
	}
}

func TestTrimRiskReachesOneWhenAConsumerIsOvertaken(t *testing.T) {
	s := ports.EventBusStats{Retention: 100, Groups: []ports.EventBusGroupStats{{Name: "g", Lag: 250, Lost: 150}}}
	if s.TrimRisk() < 1 {
		t.Fatal(s.TrimRisk())
	}
	if (ports.EventBusStats{}).TrimRisk() != 0 || !strings.Contains("x", "x") {
		t.Fatal("empty stats carry no risk")
	}
}
