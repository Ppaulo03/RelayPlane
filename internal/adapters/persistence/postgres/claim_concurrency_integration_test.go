//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

// Several dispatchers claim at the same time (every worker runs one). A delivery must be leased by exactly ONE of them: a second
// lease would send the same event twice at once and break the per-instance order.
func TestConcurrentDispatchersNeverClaimTheSameDeliveryTwice(t *testing.T) {
	s := openStore(t)
	repos := s.Repositories()
	ctx := context.Background()
	if err := repos.Tenants.Create(ctx, instance.Tenant{ID: "t1", Name: "t1", APIKeyHash: "h"}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Subscriptions.Create(ctx, subscription.Subscription{ID: "sub_1", TenantID: "t1", URL: "https://a.example.com/h", SecretVersion: 1, Active: true}); err != nil {
		t.Fatal(err)
	}
	const n = 400
	now := time.Now().UTC()
	var ds []subscription.Delivery
	for i := 0; i < n; i++ { // one instance each: every delivery is a head, so they all compete
		id := fmt.Sprintf("d%03d", i)
		ds = append(ds, subscription.Delivery{ID: id, SubscriptionID: "sub_1", TenantID: "t1", InstanceID: fmt.Sprintf("inst_%03d", i), EventID: "e" + id,
			EventType: events.MessageReceived, Event: events.Event{EventID: "e" + id, EventType: events.MessageReceived, TenantID: "t1", Payload: map[string]any{}},
			Status: subscription.DeliveryPending, CreatedAt: now, NextAttemptAt: now})
	}
	if _, err := repos.Deliveries.Enqueue(ctx, ds); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	claimedBy := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for empty := 0; empty < 3; {
				got, err := repos.Deliveries.ClaimDueWith(ctx, time.Now(), time.Minute, 16, 0)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, d := range got {
					claimedBy[d.ID]++
				}
				mu.Unlock()
				if len(got) == 0 {
					empty++
				} else {
					empty = 0
				}
			}
		}()
	}
	wg.Wait()
	dup := 0
	for id, c := range claimedBy {
		if c != 1 {
			dup++
			t.Errorf("%s was leased %d times", id, c)
		}
	}
	if len(claimedBy) != n || dup != 0 {
		t.Errorf("%d of %d deliveries claimed, %d of them more than once", len(claimedBy), n, dup)
	}
}

// The dispatchers also FINISH what they claim, all the time: the next claimer's snapshot is then older than the row. A delivery
// that was just delivered, or just scheduled for a retry later, must not be leased again by a claimer that still saw it as due
// (that re-sent delivered events and ignored the retry backoff, and got worse with more workers).
func TestAClaimNeverRevivesADeliveryThatWasJustFinished(t *testing.T) {
	s := openStore(t)
	repos := s.Repositories()
	ctx := context.Background()
	if err := repos.Tenants.Create(ctx, instance.Tenant{ID: "t1", Name: "t1", APIKeyHash: "h"}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Subscriptions.Create(ctx, subscription.Subscription{ID: "sub_1", TenantID: "t1", URL: "https://a.example.com/h", SecretVersion: 1, Active: true}); err != nil {
		t.Fatal(err)
	}
	const n = 300
	now := time.Now().UTC()
	var ds []subscription.Delivery
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("d%03d", i)
		ds = append(ds, subscription.Delivery{ID: id, SubscriptionID: "sub_1", TenantID: "t1", InstanceID: fmt.Sprintf("inst_%03d", i), EventID: "e" + id,
			EventType: events.MessageReceived, Event: events.Event{EventID: "e" + id, EventType: events.MessageReceived, TenantID: "t1", Payload: map[string]any{}},
			Status: subscription.DeliveryPending, CreatedAt: now, NextAttemptAt: now})
	}
	if _, err := repos.Deliveries.Enqueue(ctx, ds); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for empty := 0; empty < 20; {
				got, err := repos.Deliveries.ClaimDueWith(ctx, time.Now(), time.Minute, 8, 0)
				if err != nil {
					t.Error(err)
					return
				}
				for _, d := range got {
					mu.Lock()
					claimed[d.ID]++
					c := claimed[d.ID]
					mu.Unlock()
					// every third one "fails" and goes back for a retry in an hour, the others are delivered, at once
					if (d.ID[len(d.ID)-1]-'0')%3 == 0 && c == 1 {
						_ = repos.Deliveries.MarkRetry(ctx, d.ID, time.Now().Add(time.Hour), "http 500")
					} else {
						_ = repos.Deliveries.MarkDelivered(ctx, d.ID, time.Now())
					}
				}
				if len(got) == 0 {
					empty++
					time.Sleep(time.Millisecond)
				} else {
					empty = 0
				}
			}
		}()
	}
	wg.Wait()
	again := 0
	for id, c := range claimed {
		if c > 1 {
			again++
			if again <= 5 {
				t.Errorf("%s was leased %d times although it had been delivered or scheduled for later", id, c)
			}
		}
	}
	if again > 0 {
		t.Errorf("%d of %d deliveries were leased more than once", again, len(claimed))
	}
	if len(claimed) != n {
		t.Errorf("%d of %d deliveries were claimed", len(claimed), n)
	}
}

// The same pattern is used by the inbound attachment queue: finished jobs must not be claimed again.
func TestInboundMediaJobsAreNeverClaimedTwice(t *testing.T) {
	s := openStore(t)
	repos := s.Repositories()
	ctx := context.Background()
	if err := repos.Tenants.Create(ctx, instance.Tenant{ID: "t1", Name: "t1", APIKeyHash: "h"}); err != nil {
		t.Fatal(err)
	}
	const n = 300
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("med_%03d", i)
		if _, err := repos.InboundMedia.Enqueue(ctx, media.InboundJob{ID: id, TenantID: "t1", InstanceID: "inst_1", EventID: "evt_" + id, CreatedAt: now, NextAttemptAt: now,
			Event: events.Event{EventID: "evt_" + id, EventType: events.MessageReceived, TenantID: "t1", Payload: map[string]any{}}}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for empty := 0; empty < 20; {
				got, err := repos.InboundMedia.ClaimDue(ctx, time.Now(), time.Minute, 8)
				if err != nil {
					t.Error(err)
					return
				}
				for _, j := range got {
					mu.Lock()
					claimed[j.ID]++
					mu.Unlock()
					_ = repos.InboundMedia.Done(ctx, j.ID, time.Now())
				}
				if len(got) == 0 {
					empty++
					time.Sleep(time.Millisecond)
				} else {
					empty = 0
				}
			}
		}()
	}
	wg.Wait()
	for id, c := range claimed {
		if c > 1 {
			t.Errorf("%s was claimed %d times", id, c)
		}
	}
	if len(claimed) != n {
		t.Errorf("%d of %d jobs were claimed", len(claimed), n)
	}
}
