package contracttest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/media"
)

func inboundJob(id, eventID string, at time.Time) media.InboundJob {
	return media.InboundJob{ID: id, TenantID: "t1", InstanceID: "inst_1", EventID: eventID, CreatedAt: at, NextAttemptAt: at,
		Ref: json.RawMessage(`{"key":"secret-material"}`),
		Event: events.Event{EventID: eventID, EventType: events.MessageReceived, TenantID: "t1", InstanceID: "inst_1", Timestamp: at,
			Payload: map[string]any{"media": map[string]any{"media_id": id, "status": "PENDING"}}}}
}

// The queue that resolves inbound attachments before their message.received event is delivered.
func inboundMediaContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	repo := fx.r.InboundMedia
	now := time.Now().UTC().Truncate(time.Millisecond)

	if ok, err := repo.Enqueue(ctx, inboundJob("med_1", "evt_1", now)); err != nil || !ok {
		t.Fatalf("enqueue: %v %v", ok, err)
	}
	// a provider retry of the same webhook produces the same event id: nothing new
	if ok, err := repo.Enqueue(ctx, inboundJob("med_1b", "evt_1", now)); err != nil || ok {
		t.Fatalf("ENQUEUE IS IDEMPOTENT per event: %v %v", ok, err)
	}
	if _, err := repo.Enqueue(ctx, inboundJob("med_2", "evt_2", now.Add(time.Millisecond))); err != nil {
		t.Fatal(err)
	}

	got, err := repo.ClaimDue(ctx, now.Add(time.Second), time.Minute, 10)
	if err != nil || len(got) != 2 || got[0].ID != "med_1" || got[1].ID != "med_2" {
		t.Fatalf("claim oldest first: %+v %v", got, err)
	}
	if got[0].Stage != media.StageDownload || string(got[0].Ref) == "" || got[0].Event.EventID != "evt_1" {
		t.Errorf("the job travels with its event and reference: %+v", got[0])
	}
	if again, _ := repo.ClaimDue(ctx, now.Add(time.Second), time.Minute, 10); len(again) != 0 {
		t.Fatalf("a leased job is not claimed twice: %d", len(again))
	}
	if after, _ := repo.ClaimDue(ctx, now.Add(2*time.Minute), time.Minute, 10); len(after) != 2 {
		t.Fatalf("an expired lease releases the job (a crashed worker): %d", len(after))
	}

	// a failed attempt is counted and delayed
	if err := repo.Retry(ctx, "med_2", now.Add(time.Hour), "provider down"); err != nil {
		t.Fatal(err)
	}
	if due, _ := repo.ClaimDue(ctx, now.Add(3*time.Minute), time.Minute, 10); len(due) != 1 || due[0].ID != "med_1" {
		t.Fatalf("a delayed job waits for its time: %+v", due)
	}
	later, _ := repo.ClaimDue(ctx, now.Add(2*time.Hour), time.Minute, 10)
	var j2 media.InboundJob
	for _, j := range later {
		if j.ID == "med_2" {
			j2 = j
		}
	}
	if j2.Attempts != 1 || j2.LastError != "provider down" {
		t.Errorf("retry bookkeeping: %+v", j2)
	}

	// resolve: the final event replaces the pending one, the decryption material is dropped, the job moves to PUBLISH
	final := inboundJob("med_1", "evt_1", now).Event
	final.Payload = map[string]any{"media": map[string]any{"media_id": "med_1", "status": "READY"}}
	if err := repo.Resolve(ctx, "med_1", final); err != nil {
		t.Fatal(err)
	}
	pub, _ := repo.ClaimDue(ctx, now.Add(3*time.Hour), time.Minute, 10)
	var j1 media.InboundJob
	for _, j := range pub {
		if j.ID == "med_1" {
			j1 = j
		}
	}
	m, _ := j1.Event.Payload.(map[string]any)["media"].(map[string]any)
	if j1.Stage != media.StagePublish || m["status"] != "READY" || j1.Attempts != 0 {
		t.Errorf("resolved job: %+v", j1)
	}
	if s := string(j1.Ref); s != "" && s != "{}" && s != "null" {
		t.Errorf("the download reference (it can hold keys) must be gone once the outcome is known: %q", s)
	}

	if c, _ := repo.Counts(ctx, now.Add(3*time.Hour)); c.Publish != 1 || c.Download != 1 {
		t.Errorf("counts: %+v", c)
	}
	if err := repo.Complete(ctx, "med_1", j1.Event, now.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// the resolved event is in the event outbox the moment the job is DONE: the broker is not the only copy
	if got, err := fx.r.Events.ClaimForFanOut(ctx, 10, time.Minute); err != nil || len(got) != 1 || got[0].EventID != "evt_1" {
		t.Errorf("Complete must queue the resolved event in the outbox: %v %v", got, err)
	}
	if err := repo.Complete(ctx, "med_1", j1.Event, now.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if left, _ := fx.r.Events.ClaimForFanOut(ctx, 10, time.Minute); len(left) != 0 {
		t.Errorf("completing twice queues the event once (the first claim still holds its lease): %v", left)
	}
	if rest, _ := repo.ClaimDue(ctx, now.Add(4*time.Hour), time.Minute, 10); len(rest) != 1 || rest[0].ID != "med_2" {
		t.Errorf("a done job is never claimed again: %+v", rest)
	}
	if n, _ := repo.Purge(ctx, now.Add(24*time.Hour)); n != 1 {
		t.Errorf("purge of finished jobs: %d", n)
	}
}
