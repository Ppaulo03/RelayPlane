package systemtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/ports"
)

// Gateway dies after creating the provider session but before persisting the
// outcome: the reconciler adopts the existing session instead of duplicating it.
func TestFailure_GatewayDiesAfterProviderCreate(t *testing.T) {
	e := NewEnv(t)
	ctx := context.Background()
	// the provider-side create already happened...
	inst, err := e.Repos.Instances.CreateWithPlacement(ctx, ports.PlacementRequest{
		Instance: instance.Instance{ID: "inst_crash", TenantID: e.Tenant, Name: "x", Provider: ProviderKey,
			DesiredState: instance.DesiredConnected, ObservedState: instance.Allocating},
		Provider: ProviderKey, Choose: func(c []routing.Node) (string, error) { return c[0].ID, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Provider.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: inst.Assignment(), TenantID: e.Tenant, Name: "x"}); err != nil {
		t.Fatal(err)
	}
	// ...but the gateway died before recording anything (still ALLOCATING).
	if _, _, err := e.Reconciler.ReconcileInstance(ctx, inst.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Repos.Instances.Get(ctx, inst.ID)
	if got.ObservedState != instance.AwaitingPairing {
		t.Fatalf("existing provider session must be adopted, observed=%s", got.ObservedState)
	}
	if got.ProviderInstanceID == "" {
		t.Error("provider_instance_id must be recorded on adoption")
	}
	// Provision itself is also safe to re-run (AlreadyExists is adopted)
	if _, err := e.App.Instances.Provision(ctx, *got); err != nil {
		t.Fatalf("re-provision: %v", err)
	}
}

// Interrupted provisioning where the provider never received the create is resumed.
func TestFailure_ProvisioningResumedByReconciler(t *testing.T) {
	e := NewEnv(t)
	e.Provider.FailNext(memory.FailUnavailable) // node down during create
	res, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: "x"}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != instance.Creating {
		t.Fatalf("status %s, want CREATING (accepted, not failed)", res.Status)
	}
	op, _ := e.Repos.Operations.Get(bg, res.OperationID)
	if op.Status != instance.OpRunning {
		t.Fatalf("operation %+v", op)
	}
	if _, _, err := e.Reconciler.ReconcileInstance(bg, res.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Repos.Instances.Get(bg, res.ID)
	if got.ObservedState != instance.AwaitingPairing {
		t.Fatalf("observed %s", got.ObservedState)
	}
	op, _ = e.Repos.Operations.Get(bg, res.OperationID)
	if op.Status != instance.OpSucceeded {
		t.Fatalf("operation %+v", op)
	}
}

// A permanent provider failure during create fails the instance and frees the slot.
func TestFailure_PermanentCreateFailureReleasesCapacity(t *testing.T) {
	e := NewEnv(t)
	e.Provider.FailNext(memory.FailAuth)
	res, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: "x"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != instance.Failed {
		t.Fatalf("status %s", res.Status)
	}
	op, _ := e.Repos.Operations.Get(bg, res.OperationID)
	if op.Status != instance.OpFailed || op.ErrorCode != "PROVIDER_AUTH_FAILED" {
		t.Fatalf("op %+v", op)
	}
	for _, n := range []string{"node-01", "node-02"} {
		node, _ := e.Repos.Nodes.Get(bg, n)
		if node.ActiveInstances != 0 {
			t.Errorf("node %s leaked capacity", n)
		}
	}
}

// Worker dies after sending but before ACK: the redelivered command must not resend.
func TestFailure_WorkerCrashAfterSendBeforeAck(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	r, _, err := e.SendText(e.Tenant, inst.ID, "once", "")
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the crash window: the claim was taken (DISPATCHING) and the
	// message may or may not have been sent when the process died.
	if _, err := e.Repos.Messages.Transition(bg, r.MessageID, []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{BumpAttempt: true}); err != nil {
		t.Fatal(err)
	}
	e.StartWorkers(1) // redelivery of the un-acked command
	m := e.WaitMessage(r.MessageID, messaging.StatusUnknown)
	if m.ErrorCode != "WORKER_CRASH" {
		t.Errorf("code %q", m.ErrorCode)
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(e.Provider.Sent()); n != 0 {
		t.Fatalf("an interrupted dispatch must never be blindly resent (sent %d)", n)
	}
	if d, _ := e.Queue.Depth(bg); d != 0 {
		t.Errorf("command not acked, depth %d", d)
	}
}

// Redelivery of an already processed command (broker at-least-once) is a no-op.
func TestFailure_RedeliveryIsIdempotent(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	r, _, _ := e.SendText(e.Tenant, inst.ID, "hello", "")
	e.StartWorkers(1)
	e.WaitMessage(r.MessageID, messaging.StatusAccepted)
	// the broker redelivers the same command
	m, _ := e.Repos.Messages.Get(bg, r.MessageID)
	env := messaging.Envelope{MessageID: m.ID, TenantID: m.TenantID, InstanceID: m.InstanceID,
		Assignment:   ownership.Assignment{InstanceID: m.InstanceID, NodeID: m.NodeID, Epoch: m.AssignmentEpoch},
		PartitionKey: m.InstanceID, Type: messaging.TypeText, To: m.Recipient, Payload: messaging.Payload{Text: "hello"}}
	for i := 0; i < 3; i++ {
		res, err := e.Worker.Handle(bg, ports.Command{ID: m.ID, PartitionKey: m.InstanceID, Payload: env, Attempt: 1})
		if err != nil || res.Disposition != ports.Ack {
			t.Fatalf("redelivery: %+v %v", res, err)
		}
	}
	if n := len(e.Provider.Sent()); n != 1 {
		t.Fatalf("message sent %d times", n)
	}
}

// Provider timeout after the request may have been executed => UNKNOWN, no auto-retry.
func TestFailure_ProviderTimeoutIsAmbiguousAndNotRetried(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.FailNext(memory.FailAmbiguous)
	r, _, _ := e.SendText(e.Tenant, inst.ID, "maybe", "")
	e.StartWorkers(1)
	m := e.WaitMessage(r.MessageID, messaging.StatusUnknown)
	if m.ErrorCode != "AMBIGUOUS_DISPATCH" {
		t.Errorf("code %q", m.ErrorCode)
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(e.Provider.Sent()); n != 0 {
		t.Errorf("an ambiguous dispatch must not be retried (sent %d)", n)
	}
	if got, _ := e.Repos.Messages.Get(bg, r.MessageID); got.AttemptCount != 1 {
		t.Errorf("attempts %d", got.AttemptCount)
	}
}

// Provider 5xx/unavailable: retried with backoff, then dead-lettered; never infinite.
func TestFailure_ProviderUnavailableRetriesThenDLQ(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.FailNext(memory.FailUnavailable, memory.FailUnavailable, memory.FailUnavailable, memory.FailUnavailable, memory.FailUnavailable)
	r, _, _ := e.SendText(e.Tenant, inst.ID, "doomed", "")
	e.StartWorkers(1)
	m := e.WaitMessage(r.MessageID, messaging.StatusFailed)
	if m.ErrorCode != "RETRIES_EXHAUSTED" || m.AttemptCount != 5 {
		t.Fatalf("code=%s attempts=%d", m.ErrorCode, m.AttemptCount)
	}
	Eventually(t, 5*time.Second, "dlq entry", func() bool { d, _ := e.Queue.DeadLetters(bg, 10); return len(d) == 1 })
	if len(e.Provider.Sent()) != 0 {
		t.Error("nothing should have been sent")
	}
	if testutilCounter(e, "relayplane_outbound_retry_total") != 4 || testutilCounter(e, "relayplane_outbound_dlq_total") != 1 {
		t.Errorf("metrics: retry=%v dlq=%v", testutilCounter(e, "relayplane_outbound_retry_total"), testutilCounter(e, "relayplane_outbound_dlq_total"))
	}
}

// A transient failure that recovers within the retry budget still delivers the message once.
func TestFailure_RetryThenSuccess(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.FailNext(memory.FailUnavailable, memory.FailUnavailable)
	r, _, _ := e.SendText(e.Tenant, inst.ID, "eventually", "")
	e.StartWorkers(2)
	m := e.WaitMessage(r.MessageID, messaging.StatusAccepted)
	if m.AttemptCount != 3 || len(e.Provider.Sent()) != 1 {
		t.Fatalf("attempts=%d sent=%d", m.AttemptCount, len(e.Provider.Sent()))
	}
}

// Instance socket disconnected while the node HTTP stays healthy: sends are
// retried until the session is back, in order, without loss.
func TestFailure_SocketDownThenRecovers(t *testing.T) {
	e := NewEnv(t)
	e.Worker.Retry = messaging.RetrySchedule{20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}
	inst := e.CreateInstance(e.Tenant, "a", true)
	var ids []string
	for i := 0; i < 3; i++ {
		r, _, _ := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%d", i), "")
		ids = append(ids, r.MessageID)
	}
	e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Disconnected)
	if _, _, err := e.Reconciler.ReconcileInstance(bg, inst.ID); err != nil { // observe drop and reconnect
		t.Fatal(err)
	}
	e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Disconnected)
	cur, _ := e.Repos.Instances.Get(bg, inst.ID)
	_, _ = e.Repos.Instances.SetObserved(bg, inst.ID, cur.AssignmentEpoch, instance.Disconnected, time.Now())
	e.StartWorkers(1)
	time.Sleep(60 * time.Millisecond)
	if len(e.Provider.Sent()) != 0 {
		t.Fatal("must not send while the session is down")
	}
	e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Connected)
	_, _, _ = e.Reconciler.ReconcileInstance(bg, inst.ID)
	for _, id := range ids {
		e.WaitMessage(id, messaging.StatusAccepted)
	}
	var order []string
	for _, s := range e.Provider.Sent() {
		order = append(order, s.Message.Text)
	}
	if fmt.Sprint(order) != "[m0 m1 m2]" {
		t.Fatalf("order after recovery: %v", order)
	}
}

// Blob removed before the worker consumed the command.
func TestFailure_BlobRemovedBeforeConsumption(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	data := []byte("some document bytes")
	sum := sha256.Sum256(data)
	ticket, _ := e.App.Media.CreateUpload(bg, e.Tenant, app.UploadRequest{ContentType: "application/pdf", Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), Filename: "a.pdf"})
	if _, err := e.App.Media.Upload(bg, e.Tenant, ticket.MediaID, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	r, _, err := e.App.Messages.Send(bg, e.Tenant, app.SendInput{InstanceID: inst.ID, To: "5562999999999", Type: messaging.TypeDocument, Payload: app.SendPayload{MediaID: ticket.MediaID}}, "")
	if err != nil {
		t.Fatal(err)
	}
	_ = e.Blob.Delete(bg, ticket.ObjectKey) // lifecycle policy / operator removed it
	e.StartWorkers(1)
	m := e.WaitMessage(r.MessageID, messaging.StatusFailed)
	if m.ErrorCode != "MEDIA_UNAVAILABLE" {
		t.Errorf("code %q", m.ErrorCode)
	}
	if len(e.Provider.Sent()) != 0 {
		t.Error("nothing may be sent without its media")
	}
}

// A corrupted object (checksum mismatch) is never sent.
func TestFailure_BlobChecksumMismatch(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	data := []byte("good content")
	sum := sha256.Sum256(data)
	ticket, _ := e.App.Media.CreateUpload(bg, e.Tenant, app.UploadRequest{ContentType: "application/pdf", Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), Filename: "a.pdf"})
	if _, err := e.App.Media.Upload(bg, e.Tenant, ticket.MediaID, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	r, _, _ := e.App.Messages.Send(bg, e.Tenant, app.SendInput{InstanceID: inst.ID, To: "5562999999999", Type: messaging.TypeDocument, Payload: app.SendPayload{MediaID: ticket.MediaID}}, "")
	_ = e.Blob.Put(bg, ticket.ObjectKey, bytes.NewReader([]byte("evil content")), int64(len(data)), "application/pdf") // same size, other bytes
	e.StartWorkers(1)
	m := e.WaitMessage(r.MessageID, messaging.StatusFailed)
	if m.ErrorCode != "MEDIA_INVALID" || len(e.Provider.Sent()) != 0 {
		t.Fatalf("code=%s sent=%d", m.ErrorCode, len(e.Provider.Sent()))
	}
}

// Media upload with a wrong checksum is rejected and the object removed.
func TestMedia_UploadValidation(t *testing.T) {
	e := NewEnv(t)
	data := []byte("12345")
	wrong := sha256.Sum256([]byte("other"))
	tk, err := e.App.Media.CreateUpload(bg, e.Tenant, app.UploadRequest{ContentType: "application/pdf", Size: 5, SHA256: hex.EncodeToString(wrong[:]), Filename: "a.pdf"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.App.Media.Upload(bg, e.Tenant, tk.MediaID, bytes.NewReader(data)); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("checksum mismatch: %v", err)
	}
	if _, err := e.Blob.Stat(bg, tk.ObjectKey); err == nil {
		t.Error("rejected upload left an object behind")
	}
	if _, err := e.App.Media.Upload(bg, e.Tenant, tk.MediaID, bytes.NewReader(append(data, 'x'))); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("size mismatch: %v", err)
	}
	for name, req := range map[string]app.UploadRequest{
		"bad type":  {ContentType: "application/x-msdownload", Size: 5, SHA256: hex.EncodeToString(wrong[:])},
		"too large": {ContentType: "application/pdf", Size: 1 << 40, SHA256: hex.EncodeToString(wrong[:])},
		"bad sha":   {ContentType: "application/pdf", Size: 5, SHA256: "xyz"},
	} {
		if _, err := e.App.Media.CreateUpload(bg, e.Tenant, req); !errors.Is(err, errs.ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// Tenants can never reference each other's objects or instances.
func TestTenantIsolation(t *testing.T) {
	e := NewEnv(t)
	a := e.CreateInstance(e.Tenant, "a", true)
	if _, err := e.App.Instances.Get(bg, e.Tenant2, a.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("get: %v", err)
	}
	if _, _, err := e.SendText(e.Tenant2, a.ID, "hi", ""); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("send via other tenant: %v", err)
	}
	if _, err := e.App.Instances.Reconnect(bg, e.Tenant2, a.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("reconnect: %v", err)
	}
	if _, _, err := e.App.Migrations.Start(bg, e.Tenant2, a.ID, app.MigrateInput{}, ""); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("migrate: %v", err)
	}
	if _, _, err := e.App.Instances.Delete(bg, e.Tenant2, a.ID, ""); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("delete: %v", err)
	}
	data := []byte("x")
	sum := sha256.Sum256(data)
	tk, _ := e.App.Media.CreateUpload(bg, e.Tenant, app.UploadRequest{ContentType: "application/pdf", Size: 1, SHA256: hex.EncodeToString(sum[:]), Filename: "a.pdf"})
	_, _ = e.App.Media.Upload(bg, e.Tenant, tk.MediaID, bytes.NewReader(data))
	b := e.CreateInstance(e.Tenant2, "b", true)
	_, _, err := e.App.Messages.Send(bg, e.Tenant2, app.SendInput{InstanceID: b.ID, To: "5562999999999", Type: messaging.TypeDocument, Payload: app.SendPayload{MediaID: tk.MediaID}}, "")
	if !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("referencing another tenant's media: %v", err)
	}
	if _, err := e.App.Media.Get(bg, e.Tenant2, tk.MediaID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("media get: %v", err)
	}
	r, _, _ := e.SendText(e.Tenant, a.ID, "hi", "")
	if _, err := e.App.Messages.Get(bg, e.Tenant2, r.MessageID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("message get: %v", err)
	}
	if _, err := e.App.Instances.GetOperation(bg, e.Tenant2, "op_create_"+a.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("operation get: %v", err)
	}
}

// Webhook claiming a node that does not own the instance is an ownership violation.
func TestWebhook_OwnershipViolationAndAuth(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	other := "node-02"
	if inst.NodeID == other {
		other = "node-01"
	}
	_, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(other, inst.AssignmentEpoch, recvEv(inst.ID, "w1")))
	if !errors.Is(err, errs.ErrOwnershipViolation) {
		t.Fatalf("want OWNERSHIP_VIOLATION, got %v", err)
	}
	if testutilCounter(e, "relayplane_ownership_violation_total") != 1 {
		t.Error("metric missing")
	}
	var violation bool
	for _, ev := range e.Bus.Published() {
		if ev.EventType == events.OwnershipViolation {
			violation = true
		}
	}
	if !violation {
		t.Error("violation must be visible on the event bus")
	}
	// right node, stale epoch (old owner still talking after a migration)
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch+5, recvEv(inst.ID, "w2"))); !errors.Is(err, errs.ErrOwnershipViolation) {
		t.Fatalf("stale epoch claim: %v", err)
	}
	// nothing was published for rejected requests
	for _, ev := range e.Bus.Published() {
		if ev.EventType == events.MessageReceived {
			t.Fatal("rejected webhook leaked an event")
		}
	}
	// bad credentials
	bad, _ := json.Marshal(memory.FakeWebhookBody{Node: inst.NodeID, Token: "nope"})
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, ports.InboundRequest{Body: bad}); !errors.Is(err, errs.ErrUnauthenticated) {
		t.Fatalf("auth: %v", err)
	}
	// unknown instance is ignored (no retry storm)
	res, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv("inst_ghost", "w3")))
	if err != nil || res.Ignored != 1 {
		t.Fatalf("unknown instance: %+v %v", res, err)
	}
}

// Late webhooks (older than the catalog state) must not regress observed_state;
// receipts update messages monotonically.
func TestProjector_LateAndDuplicateEvents(t *testing.T) {
	e := NewEnv(t)
	e.StartProjector()
	e.StartWorkers(1)
	inst := e.CreateInstance(e.Tenant, "a", true)
	r, _, _ := e.SendText(e.Tenant, inst.ID, "hi", "")
	m := e.WaitMessage(r.MessageID, messaging.StatusAccepted)

	post := func(evs ...memory.FakeWebhookEv) {
		if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, evs...)); err != nil {
			t.Fatal(err)
		}
	}
	post(statusEv(inst.ID, m.ProviderMessageID, "read"))
	e.WaitMessage(r.MessageID, messaging.StatusRead)
	post(statusEv(inst.ID, m.ProviderMessageID, "delivered")) // arrives late
	time.Sleep(100 * time.Millisecond)
	if got, _ := e.Repos.Messages.Get(bg, r.MessageID); got.Status != messaging.StatusRead {
		t.Fatalf("a late delivered receipt regressed the message to %s", got.Status)
	}

	// instance.status_changed applies quickly; a stale one is ignored
	now := time.Now()
	post(memory.FakeWebhookEv{InstanceID: inst.ID, Type: events.InstanceStatusChanged, ProviderMessageID: fmt.Sprint(now.UnixMilli()), State: "DISCONNECTED",
		Timestamp: now.Add(time.Second), Payload: json.RawMessage(`{"state":"DISCONNECTED"}`)})
	Eventually(t, 5*time.Second, "projected DISCONNECTED", func() bool {
		i, _ := e.Repos.Instances.Get(bg, inst.ID)
		return i.ObservedState == instance.Disconnected
	})
	post(memory.FakeWebhookEv{InstanceID: inst.ID, Type: events.InstanceStatusChanged, ProviderMessageID: "old", State: "CONNECTED",
		Timestamp: now.Add(-time.Hour), Payload: json.RawMessage(`{"state":"CONNECTED"}`)})
	time.Sleep(100 * time.Millisecond)
	if i, _ := e.Repos.Instances.Get(bg, inst.ID); i.ObservedState != instance.Disconnected {
		t.Fatalf("a late status event overwrote newer state: %s", i.ObservedState)
	}
}

// A rolled back transaction leaves nothing behind and the retry succeeds.
func TestFailure_DatabaseRollbackDuringPlacement(t *testing.T) {
	e := NewEnv(t)
	if e.Store == nil {
		t.Skip("fault injection needs the in-memory store (PostgreSQL rollback is covered by the repository contract)")
	}
	fail := true
	e.Store.BeforeCommit = func(op string) error {
		if op == "create_with_placement" && fail {
			fail = false
			return errors.New("simulated serialization failure")
		}
		return nil
	}
	if _, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: "x"}, "k"); err == nil {
		t.Fatal("expected failure")
	}
	for _, n := range []string{"node-01", "node-02"} {
		node, _ := e.Repos.Nodes.Get(bg, n)
		if node.ActiveInstances != 0 {
			t.Fatalf("rollback leaked capacity on %s", n)
		}
	}
	if l, _ := e.App.Instances.List(bg, e.Tenant); len(l) != 0 {
		t.Fatalf("rollback left %d instances", len(l))
	}
	time.Sleep(80 * time.Millisecond) // the in-flight claim of the failed attempt goes stale
	res, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: "x"}, "k")
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := e.App.Instances.List(bg, e.Tenant); len(l) != 1 || l[0].ID != res.ID {
		t.Fatalf("retry result: %+v", l)
	}
}

// Reconciler restart: two reconciler runs never double-apply an action.
func TestFailure_ReconcilerRestartIsIdempotent(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Disconnected)
	e.Provider.ConnectLeadsTo = instance.Connected
	for i := 0; i < 3; i++ { // "restarts"
		if _, _, err := e.Reconciler.ReconcileInstance(bg, inst.ID); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := e.Repos.Instances.Get(bg, inst.ID)
	if got.ObservedState != instance.Connected {
		t.Fatalf("observed %s", got.ObservedState)
	}
	changes := 0
	for _, ev := range e.Bus.Published() {
		if ev.EventType == events.InstanceStatusChanged && ev.InstanceID == inst.ID {
			changes++
		}
	}
	if changes != 1 { // only the initial pairing changed the catalog; the repeats were no-ops
		t.Errorf("status changes announced: %d", changes)
	}
	if e.Provider.StateOn(inst.NodeID, inst.ID) != instance.Connected {
		t.Error("the session should have been reconnected exactly once")
	}
}

// Rate policy hierarchy is honoured by the worker: a rate-limited instance is
// deferred (not blocked) and other instances keep flowing.
func TestRateLimit_DefersWithoutBlockingOthers(t *testing.T) {
	e := NewEnv(t)
	a := e.CreateInstance(e.Tenant, "a", true)
	b := e.CreateInstance(e.Tenant, "b", true)
	e.Worker.GlobalPolicy = messaging.RatePolicy{MinInterval: 300 * time.Millisecond, Burst: 1}
	var ra []string
	for i := 0; i < 3; i++ {
		r, _, _ := e.SendText(e.Tenant, a.ID, fmt.Sprintf("a%d", i), "")
		ra = append(ra, r.MessageID)
	}
	rb, _, _ := e.SendText(e.Tenant, b.ID, "b0", "")
	start := time.Now()
	e.StartWorkers(2)
	e.WaitMessage(rb.MessageID, messaging.StatusAccepted)
	if time.Since(start) > 250*time.Millisecond {
		t.Errorf("instance b waited for a's rate limit: %v", time.Since(start))
	}
	e.WaitMessage(ra[2], messaging.StatusAccepted)
	if time.Since(start) < 500*time.Millisecond {
		t.Errorf("3 sends with a 300ms interval finished in %v", time.Since(start))
	}
	var order []string
	for _, s := range e.Provider.Sent() {
		if s.Assignment.InstanceID == a.ID {
			order = append(order, s.Message.Text)
		}
	}
	if fmt.Sprint(order) != "[a0 a1 a2]" {
		t.Errorf("order under rate limiting: %v", order)
	}
	if testutilCounter(e, "relayplane_rate_limit_wait_seconds") < 1 {
		t.Error("rate limit wait metric missing")
	}
}

// Instance-level policy overrides tenant and global.
func TestRateLimit_PolicyHierarchyInWorker(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Worker.GlobalPolicy = messaging.RatePolicy{MinInterval: time.Hour, Burst: 1} // would block forever
	// override at instance level through the store (a tenant/instance policy admin API would write this)
	_ = e.Repos.Instances.SetRatePolicy(bg, inst.ID, &messaging.RatePolicy{MinInterval: time.Millisecond, Burst: 10})
	var ids []string
	for i := 0; i < 5; i++ {
		r, _, _ := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%d", i), "")
		ids = append(ids, r.MessageID)
	}
	e.StartWorkers(1)
	for _, id := range ids {
		e.WaitMessage(id, messaging.StatusAccepted)
	}
}
