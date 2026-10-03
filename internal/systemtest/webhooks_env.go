package systemtest

import (
	"context"
	"strconv"
	"sync"
	"time"

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
}

// Send implements ports.WebhookSender.
func (r *Receiver) Send(_ context.Context, req ports.WebhookRequest) (int, error) {
	ts, _ := strconv.ParseInt(req.Headers[subscription.HeaderTimestamp], 10, 64)
	rec := Received{URL: req.URL, EventID: req.Headers[subscription.HeaderEventID], EventType: req.Headers[subscription.HeaderEventType],
		Body: append([]byte(nil), req.Body...), Signature: req.Headers[subscription.HeaderSignature], Timestamp: ts,
		Attempt: req.Headers[subscription.HeaderAttempt], Traceparent: req.Headers["traceparent"], At: time.Now()}
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

// StartWebhooks runs the fan-out consumer and the delivery dispatcher (what the worker binary does).
func (e *Env) StartWebhooks() {
	e.wg.Add(2)
	go func() { defer e.wg.Done(); _ = e.Bus.Subscribe(e.ctx, "webhook-fanout", e.FanOut.Handle) }()
	go func() { defer e.wg.Done(); e.Dispatcher.Run(e.ctx) }()
}
