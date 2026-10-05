package systemtest

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	eventschema "github.com/relayplane/relayplane/docs/events"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

// SubscriptionKey is the server key that derives subscription secrets in tests.
var SubscriptionKey = []byte("systemtest-subscription-key")

// Received is one request the fake tenant endpoint got.
type Received struct {
	URL         string
	EventID     string
	EventType   string
	Body        []byte
	Signature   string
	Timestamp   int64
	Attempt     string
	Claim       string
	Traceparent string
	At          time.Time
	Status      int // what the endpoint answered
}

// Receiver is the tenant's webhook endpoint (and the WebhookSender) in tests.
type Receiver struct {
	mu   sync.Mutex
	reqs []Received
	// Behave decides the HTTP status for the n-th request (1-based) to a URL; nil answers 200.
	Behave func(n int, r Received) (status int, err error)
	// schemaErrs collects every body that does not satisfy docs/events/events.schema.json: the whole system suite doubles
	// as a conformance test of what really goes over the wire.
	schemaErrs []string
}

// SchemaViolations lists the delivered bodies that break the published event contract.
func (r *Receiver) SchemaViolations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.schemaErrs...)
}

// Send implements ports.WebhookSender.
func (r *Receiver) Send(_ context.Context, req ports.WebhookRequest) (int, error) {
	ts, _ := strconv.ParseInt(req.Headers[subscription.HeaderTimestamp], 10, 64)
	rec := Received{URL: req.URL, EventID: req.Headers[subscription.HeaderEventID], EventType: req.Headers[subscription.HeaderEventType],
		Body: append([]byte(nil), req.Body...), Signature: req.Headers[subscription.HeaderSignature], Timestamp: ts,
		Attempt: req.Headers[subscription.HeaderAttempt], Claim: req.Headers[subscription.HeaderClaim], Traceparent: req.Headers["traceparent"], At: time.Now()}
	if err := eventschema.Validate(req.Body); err != nil {
		r.mu.Lock()
		r.schemaErrs = append(r.schemaErrs, fmt.Sprintf("%v: %s", err, req.Body))
		r.mu.Unlock()
	}
	r.mu.Lock()
	n := 0
	for _, x := range r.reqs {
		if x.URL == req.URL {
			n++
		}
	}
	n++
	behave := r.Behave
	r.mu.Unlock()
	status, err := 200, error(nil)
	if behave != nil {
		status, err = behave(n, rec)
	}
	rec.Status = status
	r.mu.Lock()
	r.reqs = append(r.reqs, rec)
	r.mu.Unlock()
	return status, err
}

// All returns every request received so far.
func (r *Receiver) All() []Received {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Received(nil), r.reqs...)
}

// Accepted returns the requests the endpoint answered with 2xx.
func (r *Receiver) Accepted(url string) []Received {
	var out []Received
	for _, x := range r.All() {
		if x.URL == url && x.Status >= 200 && x.Status < 300 {
			out = append(out, x)
		}
	}
	return out
}

// DistinctEvents is the set of event ids the endpoint accepted (the consumer dedupes by event id).
func (r *Receiver) DistinctEvents(url string) map[string]int {
	out := map[string]int{}
	for _, x := range r.Accepted(url) {
		out[x.EventID]++
	}
	return out
}

// StartWebhooks runs the fan-out consumer and the delivery dispatcher (what the worker binary does), and the outbox publisher that
// puts accepted events on the bus (what the reconciler binary does): inbound events reach the bus through it, as in production.
func (e *Env) StartWebhooks() {
	e.StartOutbox()
	e.StartWebhookConsumers()
}

// StartWebhookConsumers runs ONLY the fan-out consumer and the delivery dispatcher, with the outbox publisher left alone: for a test
// that plays the publisher itself (a crash between "published" and "marked published").
func (e *Env) StartWebhookConsumers() {
	e.wg.Add(2)
	go func() { defer e.wg.Done(); e.FanOut.Run(e.ctx) }() // from the database, as the worker binary does
	go func() { defer e.wg.Done(); e.Dispatcher.Run(e.ctx) }()
}
