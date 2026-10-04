package contracttest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

func personMsg(id, tenant, instance, to, text string) messaging.Message {
	m := outboxMsg(id, instance)
	m.TenantID, m.Recipient = tenant, to
	m.Payload = json.RawMessage(fmt.Sprintf(`{"text":%q}`, text))
	return m
}

func commandsOf(t *testing.T, fx fixture, instanceID string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := fx.r.Messages.ListOutbox(context.Background(), instanceID, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		out[e.MessageID] = string(e.Command)
	}
	return out
}

func walk(t *testing.T, fx fixture, id string, path ...messaging.Status) {
	t.Helper()
	from := messaging.StatusQueued
	for _, to := range path {
		if _, err := fx.r.Messages.Transition(context.Background(), id, []messaging.Status{from}, to, ports.MessagePatch{}); err != nil {
			t.Fatalf("%s %s -> %s: %v", id, from, to, err)
		}
		from = to
	}
}

// An erasure request must leave nothing of the person in what RelayPlane keeps; retention must do the same on a clock.
func erasureContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.tenant(t, "t2")
	fx.node(t, "node-01", 10)
	fx.instance(t, "inst_1", "t1")
	fx.instance(t, "inst_2", "t2")
	now := time.Now().UTC().Truncate(time.Millisecond)
	const ana, bia = "5562111111111", "5562222222222"

	build := func(id, to string) func(int64) ([]byte, error) {
		return func(seq int64) ([]byte, error) {
			return []byte(fmt.Sprintf(`{"message_id":%q,"to":%q,"text":"secret to %s"}`, id, to, to)), nil
		}
	}
	for _, c := range []struct{ id, tenant, inst, to string }{
		{"m_ana1", "t1", "inst_1", ana}, {"m_ana2", "t1", "inst_1", ana}, {"m_ana3", "t1", "inst_1", ana},
		{"m_bia", "t1", "inst_1", bia}, {"m_ana_other_tenant", "t2", "inst_2", ana},
	} {
		if _, err := fx.r.Messages.CreateWithOutbox(ctx, personMsg(c.id, c.tenant, c.inst, c.to, "secret to "+c.to), build(c.id, c.to)); err != nil {
			t.Fatal(err)
		}
	}
	// m_ana1 was sent and read, m_ana2 is in the provider's hands, m_ana3 has not left yet
	walk(t, fx, "m_ana1", messaging.StatusDispatching, messaging.StatusAccepted, messaging.StatusRead)
	walk(t, fx, "m_ana2", messaging.StatusDispatching)

	res, err := fx.r.Messages.EraseRecipient(ctx, "t1", ana, now)
	if err != nil || res.Anonymized != 3 || res.Cancelled != 1 {
		t.Fatalf("erase: %+v %v", res, err)
	}
	for _, id := range []string{"m_ana1", "m_ana2", "m_ana3"} {
		m, _ := fx.r.Messages.Get(ctx, id)
		if m.Recipient != "" || string(m.Payload) != "{}" || m.ErasedAt.IsZero() || m.ErrorMessage != "" {
			t.Errorf("%s must hold nothing of the person: %+v", id, m)
		}
	}
	if m, _ := fx.r.Messages.Get(ctx, "m_ana1"); m.Status != messaging.StatusRead || m.SequenceNo == 0 {
		t.Errorf("the ledger survives (status and sequence): %+v", m)
	}
	if m, _ := fx.r.Messages.Get(ctx, "m_ana3"); m.Status != messaging.StatusFailed || m.ErrorCode != "ERASED" {
		t.Errorf("a message that had not been sent is cancelled, not sent later: %+v", m)
	}
	if m, _ := fx.r.Messages.Get(ctx, "m_ana2"); m.Status != messaging.StatusDispatching {
		t.Errorf("a message in the provider's hands keeps its status: %+v", m)
	}
	cmds := commandsOf(t, fx, "inst_1")
	if cmds["m_ana1"] != "{}" || cmds["m_ana3"] != "{}" {
		t.Errorf("the stored copy of the command must be gone: %v", cmds)
	}
	// nobody else is touched
	if m, _ := fx.r.Messages.Get(ctx, "m_bia"); m.Recipient != bia || string(m.Payload) == "{}" {
		t.Errorf("another contact: %+v", m)
	}
	if m, _ := fx.r.Messages.Get(ctx, "m_ana_other_tenant"); m.Recipient != ana || string(m.Payload) == "{}" {
		t.Errorf("the same number at another tenant is another tenant's data: %+v", m)
	}
	if again, _ := fx.r.Messages.EraseRecipient(ctx, "t1", ana, now); again.Anonymized != 0 {
		t.Errorf("erasing twice is harmless: %+v", again)
	}

	// retention: finished messages older than the cutoff lose their content; newer and unfinished ones keep it
	// (stores stamp created_at themselves, so the cutoff is taken between two messages instead of back-dating one)
	if _, err := fx.r.Messages.CreateWithOutbox(ctx, personMsg("m_old", "t1", "inst_1", bia, "old text"), build("m_old", bia)); err != nil {
		t.Fatal(err)
	}
	walk(t, fx, "m_old", messaging.StatusDispatching, messaging.StatusAccepted, messaging.StatusDelivered)
	time.Sleep(30 * time.Millisecond)
	if _, err := fx.r.Messages.CreateWithOutbox(ctx, personMsg("m_new", "t1", "inst_1", bia, "new text"), build("m_new", bia)); err != nil {
		t.Fatal(err)
	}
	walk(t, fx, "m_new", messaging.StatusDispatching, messaging.StatusAccepted, messaging.StatusDelivered)
	// the cutoff is the midpoint of the stores' own timestamps: a database clock may differ from the test's
	mOld, _ := fx.r.Messages.Get(ctx, "m_old")
	mNew, _ := fx.r.Messages.Get(ctx, "m_new")
	cutoff := mOld.CreatedAt.Add(mNew.CreatedAt.Sub(mOld.CreatedAt) / 2)
	n, err := fx.r.Messages.ScrubTerminalBefore(ctx, cutoff, now, 100)
	if err != nil || n != 1 {
		t.Fatalf("retention scrubbed %d: %v", n, err)
	}
	if m, _ := fx.r.Messages.Get(ctx, "m_old"); m.Recipient != "" || string(m.Payload) != "{}" || m.Status != messaging.StatusDelivered || m.ErasedAt.IsZero() {
		t.Errorf("an old finished message loses its content and keeps its ledger: %+v", m)
	}
	if m, _ := fx.r.Messages.Get(ctx, "m_new"); m.Recipient != bia || string(m.Payload) == "{}" {
		t.Errorf("a message newer than the cutoff is kept: %+v", m)
	}
	if m, _ := fx.r.Messages.Get(ctx, "m_bia"); m.Recipient != bia {
		t.Errorf("a message that is not finished is kept whatever its age: %+v", m)
	}
	if n, _ := fx.r.Messages.ScrubTerminalBefore(ctx, cutoff, now, 100); n != 0 {
		t.Errorf("retention is idempotent: %d", n)
	}

	// deliveries: the contact's events go, whatever their status; the DLQ has a retention
	if err := fx.r.Subscriptions.Create(ctx, newSub("sub_1", "t1")); err != nil {
		t.Fatal(err)
	}
	inbound := func(id, from string, at time.Time) subscription.Delivery {
		d := newDelivery(id, "sub_1", "t1", "inst_1", "ev_"+id, at)
		d.Event.Payload = map[string]any{"from": from, "text": "secret " + id, "type": "text", "provider_message_id": id}
		return d
	}
	if _, err := fx.r.Deliveries.Enqueue(ctx, []subscription.Delivery{inbound("d_ana1", ana, now), inbound("d_ana2", ana, now.Add(time.Millisecond)),
		inbound("d_bia", bia, now.Add(2*time.Millisecond)), inbound("d_dead", bia, now.Add(-40*24*time.Hour))}); err != nil {
		t.Fatal(err)
	}
	if err := fx.r.Deliveries.MarkDead(ctx, "d_ana2", "boom"); err != nil { // the DLQ holds the user's text too
		t.Fatal(err)
	}
	if err := fx.r.Deliveries.MarkDead(ctx, "d_dead", "boom"); err != nil {
		t.Fatal(err)
	}
	if n, err := fx.r.Deliveries.PurgeDead(ctx, now.Add(-30*24*time.Hour)); err != nil || n != 1 {
		t.Fatalf("dead-letter retention: %d %v", n, err)
	}
	if n, err := fx.r.Deliveries.EraseContact(ctx, "t1", ana); err != nil || n != 2 {
		t.Fatalf("erase deliveries (pending and dead alike): %d %v", n, err)
	}
	left := 0
	for _, st := range []subscription.DeliveryStatus{subscription.DeliveryPending, subscription.DeliveryDelivered, subscription.DeliveryDead} {
		ds, _ := fx.r.Deliveries.List(ctx, "t1", "sub_1", st, 100)
		for _, d := range ds {
			left++
			if d.ID != "d_bia" {
				t.Errorf("%s survived", d.ID)
			}
		}
	}
	if left != 1 {
		t.Errorf("only the other contact's delivery is left: %d", left)
	}
	if n, _ := fx.r.Deliveries.EraseContact(ctx, "t2", bia); n != 0 {
		t.Errorf("another tenant erases nothing here: %d", n)
	}

	// inbound attachment jobs and stored attachments are found by contact
	job := inboundJob("med_ana", "evt_ana", now)
	job.Event.Payload = map[string]any{"from": ana, "media": map[string]any{"media_id": "med_ana"}}
	if _, err := fx.r.InboundMedia.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if n, err := fx.r.InboundMedia.EraseContact(ctx, "t1", ana); err != nil || n != 1 {
		t.Fatalf("erase inbound media jobs: %d %v", n, err)
	}
	for _, b := range []media.Blob{
		{ID: "blob_ana", TenantID: "t1", ObjectKey: "t1/media/blob_ana/a.ogg", ContentType: "audio/ogg", Size: 3, SHA256: "x", Subject: ana, Status: media.BlobReady, ExpiresAt: now.Add(time.Hour)},
		{ID: "blob_bia", TenantID: "t1", ObjectKey: "t1/media/blob_bia/b.ogg", ContentType: "audio/ogg", Size: 3, SHA256: "x", Subject: bia, Status: media.BlobReady, ExpiresAt: now.Add(time.Hour)},
		{ID: "blob_upload", TenantID: "t1", ObjectKey: "t1/media/blob_upload/u.pdf", ContentType: "application/pdf", Size: 3, SHA256: "x", Status: media.BlobReady, ExpiresAt: now.Add(time.Hour)},
	} {
		if err := fx.r.Blobs.Create(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	got, err := fx.r.Blobs.ListBySubject(ctx, "t1", ana)
	if err != nil || len(got) != 1 || got[0].ID != "blob_ana" {
		t.Fatalf("attachments of a contact: %+v %v", got, err)
	}
	if other, _ := fx.r.Blobs.ListBySubject(ctx, "t2", ana); len(other) != 0 {
		t.Errorf("another tenant: %+v", other)
	}
	_ = fx.r.Blobs.MarkDeleted(ctx, "blob_ana", now)
	if gone, _ := fx.r.Blobs.ListBySubject(ctx, "t1", ana); len(gone) != 0 {
		t.Errorf("a deleted attachment is not listed: %+v", gone)
	}
}
