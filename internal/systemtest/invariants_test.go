package systemtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

var bg = context.Background()

// INV-01: an instance has at most one active owner (verified through the
// service layer across a full migration; the repository contract proves the
// storage-level guarantee).
func TestINV01_SingleOwnerThroughMigration(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	res, _, err := e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitMigration(t, e, res.OperationID, instance.OpSucceeded, instance.OpRunning)
	recs, _ := e.Repos.Instances.Assignments(bg, inst.ID)
	open := 0
	for i, r := range recs {
		if r.ReleasedAt == nil {
			open++
		}
		if r.Epoch != int64(i+1) {
			t.Errorf("epochs must be monotonic and gapless: %+v", recs)
		}
	}
	if open != 1 || len(recs) != 2 {
		t.Fatalf("INV-01 violated: %+v", recs)
	}
}

// waitMigration waits for the migration operation to reach a status.
func waitMigration(t *testing.T, e *Env, opID string, want ...instance.OperationStatus) *instance.Operation {
	t.Helper()
	var op *instance.Operation
	Eventually(t, 10*time.Second, "migration "+opID, func() bool {
		_ = e.App.Migrations.Drive(bg, opID)
		op, _ = e.Repos.Operations.Get(bg, opID)
		for _, w := range want {
			if op == nil {
				continue
			}
			if op.Status == w && (w != instance.OpRunning || op.Step == string(ownership.StepVerifyConn)) {
				return true
			}
			// "reached VERIFY_CONNECTION": the new owner exists, possibly already waiting for its QR scan
			if w == instance.OpRunning && op.Status == instance.OpAwaitingPairing && op.Step == string(ownership.StepVerifyConn) {
				return true
			}
		}
		return false
	})
	return op
}

// finishMigration pairs the new owner (the user scans the QR on the new node) and
// lets the migration's own VERIFY step observe it. While a migration is active the
// reconciler deliberately stays out of the way, so this is the only way forward.
func finishMigration(t *testing.T, e *Env, instanceID, opID string) {
	t.Helper()
	waitMigration(t, e, opID, instance.OpSucceeded, instance.OpRunning) // reach VERIFY_CONNECTION: the new owner exists
	cur, _ := e.Repos.Instances.Get(bg, instanceID)
	e.Provider.SetStateOn(cur.NodeID, instanceID, instance.Connected)
	waitMigration(t, e, opID, instance.OpSucceeded)
}

// INV-02 + INV-08: a command accepted under an old epoch is never dispatched.
func TestINV02_INV08_StaleCommandNeverReachesProvider(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	// accept messages under epoch 1 but do not run workers yet
	var ids []string
	for i := 0; i < 3; i++ {
		r, _, err := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%d", i), "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.MessageID)
	}
	// migrate: epoch becomes 2
	mr, _, err := e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitMigration(t, e, mr.OperationID, instance.OpSucceeded, instance.OpRunning)
	cur, _ := e.Repos.Instances.Get(bg, inst.ID)
	if cur.AssignmentEpoch != 2 {
		t.Fatalf("epoch %d", cur.AssignmentEpoch)
	}
	finishMigration(t, e, inst.ID, mr.OperationID) // pair the new owner

	e.StartWorkers(2)
	for _, id := range ids {
		m := e.WaitMessage(id, messaging.StatusFailed)
		if m.ErrorCode != "STALE_COMMAND" {
			t.Errorf("message %s: code %q", id, m.ErrorCode)
		}
	}
	if n := len(e.Provider.Sent()); n != 0 {
		t.Fatalf("INV-08: %d stale commands reached the provider", n)
	}
	// a fresh message under epoch 2 flows
	r, _, _ := e.SendText(e.Tenant, inst.ID, "fresh", "")
	e.WaitMessage(r.MessageID, messaging.StatusAccepted)
	sent := e.Provider.Sent()
	if len(sent) != 1 || sent[0].Assignment.Epoch != 2 {
		t.Fatalf("sent: %+v", sent)
	}
	if testutilCounter(e, "relayplane_stale_command_total") < 3 {
		t.Error("stale_command_total metric not incremented")
	}
}

// INV-03: repeating an idempotent operation returns the same result.
func TestINV03_IdempotentOperations(t *testing.T) {
	e := NewEnv(t)
	in := app.CreateInstanceInput{Name: "main"}
	a, replayed, err := e.App.Instances.Create(bg, e.Tenant, in, "key-1")
	if err != nil || replayed {
		t.Fatal(err, replayed)
	}
	b, replayed, err := e.App.Instances.Create(bg, e.Tenant, in, "key-1")
	if err != nil || !replayed || a != b {
		t.Fatalf("create replay: %+v %+v %v %v", a, b, replayed, err)
	}
	list, _ := e.App.Instances.List(bg, e.Tenant)
	if len(list) != 1 {
		t.Fatalf("duplicate create produced %d instances", len(list))
	}
	if _, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: "other"}, "key-1"); !errors.Is(err, errs.ErrIdempotencyConflict) {
		t.Fatalf("same key, different payload must fail: %v", err)
	}
	// another tenant using the same key is independent
	c, _, err := e.App.Instances.Create(bg, e.Tenant2, in, "key-1")
	if err != nil || c.ID == a.ID {
		t.Fatalf("tenant scoping: %+v %v", c, err)
	}

	// send message
	e.Connect(a.ID)
	s1, replayed, err := e.SendText(e.Tenant, a.ID, "hello", "order-1")
	if err != nil || replayed {
		t.Fatal(err)
	}
	s2, replayed, err := e.SendText(e.Tenant, a.ID, "hello", "order-1")
	if err != nil || !replayed || s1 != s2 {
		t.Fatalf("send replay: %+v %+v %v %v", s1, s2, replayed, err)
	}
	if d, _ := e.Queue.Depth(bg); d != 1 {
		t.Fatalf("a replayed send must not enqueue twice, depth=%d", d)
	}
	if _, _, err := e.SendText(e.Tenant, a.ID, "different", "order-1"); !errors.Is(err, errs.ErrIdempotencyConflict) {
		t.Fatalf("got %v", err)
	}

	// delete
	d1, _, err := e.App.Instances.Delete(bg, e.Tenant, a.ID, "del-1")
	if err != nil {
		t.Fatal(err)
	}
	d2, replayed, err := e.App.Instances.Delete(bg, e.Tenant, a.ID, "del-1")
	if err != nil || !replayed || d1 != d2 {
		t.Fatalf("delete replay: %+v %+v %v %v", d1, d2, replayed, err)
	}

	// migration
	e2 := e.CreateInstance(e.Tenant, "m", true)
	m1, _, err := e.App.Migrations.Start(bg, e.Tenant, e2.ID, app.MigrateInput{}, "mig-1")
	if err != nil {
		t.Fatal(err)
	}
	m2, replayed, err := e.App.Migrations.Start(bg, e.Tenant, e2.ID, app.MigrateInput{}, "mig-1")
	if err != nil || !replayed || m1.OperationID != m2.OperationID {
		t.Fatalf("migration replay: %+v %+v %v %v", m1, m2, replayed, err)
	}
}

// INV-04: a DRAINING node receives no new instances.
func TestINV04_DrainingNodeGetsNoNewInstances(t *testing.T) {
	e := NewEnv(t)
	if _, err := e.App.Nodes.Drain(bg, "node-01"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		inst := e.CreateInstance(e.Tenant, fmt.Sprintf("i%d", i), false)
		if inst.NodeID != "node-02" {
			t.Fatalf("instance placed on %s", inst.NodeID)
		}
	}
	if _, err := e.App.Nodes.Drain(bg, "node-02"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: "x"}, ""); !errors.Is(err, errs.ErrNoCapacity) {
		t.Fatalf("want ErrNoCapacity, got %v", err)
	}
	// existing instances stay where they are (no rebalance)
	list, _ := e.App.Instances.List(bg, e.Tenant)
	for _, i := range list {
		if i.NodeID != "node-02" || i.AssignmentEpoch != 1 {
			t.Errorf("draining must not move sessions: %+v", i)
		}
	}
	if _, err := e.App.Nodes.Resume(bg, "node-01"); err != nil {
		t.Fatal(err)
	}
	if inst := e.CreateInstance(e.Tenant, "after", false); inst.NodeID != "node-01" {
		t.Errorf("resumed node should be used again, got %s", inst.NodeID)
	}
}

func inboundBody(node string, epoch int64, evs ...memory.FakeWebhookEv) ports.InboundRequest {
	b, _ := json.Marshal(memory.FakeWebhookBody{Node: node, Epoch: epoch, Token: WebhookTok, Events: evs})
	return ports.InboundRequest{Body: b}
}

func recvEv(instID, pmid string) memory.FakeWebhookEv {
	return memory.FakeWebhookEv{InstanceID: instID, Type: events.MessageReceived, ProviderMessageID: pmid, Timestamp: time.Now(),
		Payload: json.RawMessage(`{"provider_message_id":"` + pmid + `","from":"5562","type":"text","text":"hi"}`)}
}

func statusEv(instID, pmid, state string) memory.FakeWebhookEv {
	return memory.FakeWebhookEv{InstanceID: instID, Type: events.MessageStatus, ProviderMessageID: pmid, State: state, Timestamp: time.Now(),
		Payload: json.RawMessage(`{"provider_message_id":"` + pmid + `","status":"` + state + `"}`)}
}

// INV-05: duplicate events do not produce duplicate effects.
func TestINV05_DuplicateEventsAreDeduplicated(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	base := len(e.Bus.Published()) // events already emitted while setting up
	req := inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "wamid1"))
	r1, err := e.App.Inbound.Handle(bg, ProviderKey, req)
	if err != nil || r1.Published != 1 {
		t.Fatalf("%+v %v", r1, err)
	}
	r2, err := e.App.Inbound.Handle(bg, ProviderKey, req)
	if err != nil || r2.Published != 0 || r2.Duplicates != 1 {
		t.Fatalf("duplicate: %+v %v", r2, err)
	}
	if n := len(e.Bus.Published()) - base; n != 1 {
		t.Fatalf("published %d events", n)
	}

	// sent/delivered/read are distinct facts
	for _, st := range []string{"sent", "delivered", "read"} {
		r, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, statusEv(inst.ID, "wamid2", st)))
		if err != nil || r.Published != 1 {
			t.Fatalf("%s: %+v %v", st, r, err)
		}
	}
	if n := len(e.Bus.Published()) - base; n != 4 {
		t.Fatalf("want 4 events, got %d", n)
	}

	// concurrent duplicates: exactly one publish
	var wg sync.WaitGroup
	var published atomic.Int32
	req = inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "wamid-race"))
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.App.Inbound.Handle(bg, ProviderKey, req)
			if err != nil {
				t.Error(err)
			}
			published.Add(int32(r.Published))
		}()
	}
	wg.Wait()
	if published.Load() != 1 {
		t.Fatalf("concurrent duplicates published %d times", published.Load())
	}
	// event ids are deterministic so downstream can also dedupe
	var ids = map[string]int{}
	for _, ev := range e.Bus.Published() {
		ids[ev.EventID]++
	}
	for id, n := range ids {
		if n != 1 {
			t.Errorf("event %s published %d times", id, n)
		}
	}
}

// INV-07: commands for the same instance are dispatched in order, even with
// several workers and provider jitter; different instances run in parallel.
func TestINV07_OrderingPerInstance(t *testing.T) {
	e := NewEnv(t)
	const instances, perInst = 5, 30
	var insts []*instance.Instance
	for i := 0; i < instances; i++ {
		insts = append(insts, e.CreateInstance(e.Tenant, fmt.Sprintf("i%d", i), true))
	}
	var mu sync.Mutex
	active := map[string]int{}
	var overlap atomic.Int32
	var cur, peak atomic.Int32
	e.Provider.OnSend = func(a ownership.Assignment, m messaging.OutboundMessage) {
		mu.Lock()
		active[a.InstanceID]++
		if active[a.InstanceID] > 1 {
			overlap.Add(1)
		}
		mu.Unlock()
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(time.Duration(m.Text[len(m.Text)-1]%3) * time.Millisecond) // jitter
		cur.Add(-1)
		mu.Lock()
		active[a.InstanceID]--
		mu.Unlock()
	}
	want := map[string][]string{}
	for k := 0; k < perInst; k++ {
		for _, in := range insts {
			txt := fmt.Sprintf("%s-%03d", in.ID, k)
			if _, _, err := e.SendText(e.Tenant, in.ID, txt, ""); err != nil {
				t.Fatal(err)
			}
			want[in.ID] = append(want[in.ID], txt)
		}
	}
	e.StartWorkers(4)
	Eventually(t, 30*time.Second, "all sent", func() bool { return len(e.Provider.Sent()) == instances*perInst })
	got := map[string][]string{}
	for _, s := range e.Provider.Sent() {
		got[s.Assignment.InstanceID] = append(got[s.Assignment.InstanceID], s.Message.Text)
	}
	for id, w := range want {
		if fmt.Sprint(got[id]) != fmt.Sprint(w) {
			t.Fatalf("INV-07: instance %s dispatched out of order:\n got %v\nwant %v", id, got[id], w)
		}
	}
	if overlap.Load() != 0 {
		t.Fatalf("INV-07: %d overlapping dispatches for the same instance", overlap.Load())
	}
	if peak.Load() < 2 {
		t.Errorf("different instances should dispatch in parallel (peak %d)", peak.Load())
	}
}

// INV-09: a new owner is never activated before the old one is fenced.
func TestINV09_FencingFailureBlocksMigration(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.FailNext(memory.FailUnavailable) // fencing impossible: Disconnect fails once
	res, _, err := e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
	if err != nil {
		t.Fatal(err)
	}
	op := waitMigration(t, e, res.OperationID, instance.OpBlocked)
	if op.Step != string(ownership.StepBlocked) || op.ErrorCode != "FENCING_FAILED" {
		t.Fatalf("op %+v", op)
	}
	cur, _ := e.Repos.Instances.Get(bg, inst.ID)
	if cur.NodeID != inst.NodeID || cur.AssignmentEpoch != 1 {
		t.Fatalf("INV-09: ownership changed without fencing: %+v", cur)
	}
	other := "node-02"
	if inst.NodeID == "node-02" {
		other = "node-01"
	}
	if e.Provider.HasOn(other, inst.ID) {
		t.Fatal("INV-09: new owner was started although the old owner was not fenced")
	}
	// the repository itself refuses to reassign while blocked
	if _, err := e.Repos.Instances.Reassign(bg, ports.ReassignRequest{InstanceID: inst.ID, ExpectedEpoch: 1, NewNodeID: other, OperationID: res.OperationID}); !errors.Is(err, errs.ErrFencingRequired) {
		t.Fatalf("reassign while blocked: %v", err)
	}
	if testutilCounter(e, "relayplane_migration_blocked_total") < 1 {
		t.Error("migration_blocked metric missing")
	}
	// restart of the request resumes the same operation; fencing now succeeds
	again, _, err := e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
	if err != nil || again.OperationID != res.OperationID {
		t.Fatalf("resume: %+v %v", again, err)
	}
	waitMigration(t, e, res.OperationID, instance.OpSucceeded, instance.OpRunning)
	cur, _ = e.Repos.Instances.Get(bg, inst.ID)
	if cur.AssignmentEpoch != 2 || cur.NodeID != other {
		t.Fatalf("after resume: %+v", cur)
	}
	if e.Provider.StateOn(inst.NodeID, inst.ID) == instance.Connected {
		t.Fatal("old owner must stay fenced")
	}
}

// INV-10: node health and instance health are independent.
func TestINV10_NodeAndInstanceHealthAreIndependent(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	// node healthy, socket dead
	e.Provider.SetState(inst.ID, instance.Disconnected)
	e.Reconciler.ProbeNodes(bg)
	n, _ := e.Repos.Nodes.Get(bg, inst.NodeID)
	if n.Status != routing.NodeReady {
		t.Fatalf("node must stay READY, is %s", n.Status)
	}
	if _, _, err := e.Reconciler.ReconcileInstance(bg, inst.ID); err != nil {
		t.Fatal(err)
	}
	// reconciler asked the provider to reconnect (grace is 0 in tests)
	got, _ := e.Repos.Instances.Get(bg, inst.ID)
	if got.ObservedState == instance.Disconnected && e.Provider.StateOn(inst.NodeID, inst.ID) == instance.Disconnected {
		t.Fatal("reconciler should have attempted to reconnect the session")
	}

	// node unreachable, instance state untouched, ownership untouched (no failover)
	e.Provider.SetNodeHealthy(inst.NodeID, false)
	e.Reconciler.Cfg.NodeOfflineAfter = -time.Second
	e.Reconciler.ProbeNodes(bg)
	n, _ = e.Repos.Nodes.Get(bg, inst.NodeID)
	if n.Status != routing.NodeOffline {
		t.Fatalf("node should be OFFLINE, is %s", n.Status)
	}
	before, _ := e.Repos.Instances.Get(bg, inst.ID)
	e.Provider.FailNext(memory.FailUnavailable)
	_, _, _ = e.Reconciler.ReconcileInstance(bg, inst.ID)
	after, _ := e.Repos.Instances.Get(bg, inst.ID)
	if after.NodeID != before.NodeID || after.AssignmentEpoch != before.AssignmentEpoch || after.ObservedState != before.ObservedState {
		t.Fatalf("a node outage must not change instance state or ownership: %+v -> %+v", before, after)
	}
	// and the offline node receives nothing new
	for i := 0; i < 3; i++ {
		x := e.CreateInstance(e.Tenant, fmt.Sprintf("new%d", i), false)
		if x.NodeID == before.NodeID {
			t.Fatal("OFFLINE node received a new instance")
		}
	}
}

// INV-11: binary payloads above the inline limit never go through the broker.
func TestINV11_MediaUsesClaimCheck(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)

	// oversized inline content is refused before it reaches the queue
	big := string(bytes.Repeat([]byte("A"), InlineLimit*2))
	if _, _, err := e.SendText(e.Tenant, inst.ID, big, ""); !errors.Is(err, errs.ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge, got %v", err)
	}
	if d, _ := e.Queue.Depth(bg); d != 0 {
		t.Fatalf("oversized payload reached the broker (depth %d)", d)
	}
	// the broker adapter enforces the limit on its own as a second barrier
	if err := e.Queue.Publish(bg, ports.Command{ID: "x", PartitionKey: inst.ID, Payload: map[string]string{"b64": big}}); !errors.Is(err, errs.ErrPayloadTooLarge) {
		t.Fatalf("queue accepted a big payload: %v", err)
	}

	// a 1 MiB document travels as a reference
	data := bytes.Repeat([]byte("PDF!"), 256*1024)
	sum := sha256.Sum256(data)
	ticket, err := e.App.Media.CreateUpload(bg, e.Tenant, app.UploadRequest{ContentType: "application/pdf", Size: int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]), Filename: "doc.pdf"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.App.Media.Upload(bg, e.Tenant, ticket.MediaID, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	res, _, err := e.App.Messages.Send(bg, e.Tenant, app.SendInput{InstanceID: inst.ID, To: "5562999999999", Type: messaging.TypeDocument,
		Payload: app.SendPayload{MediaID: ticket.MediaID, Caption: "contract"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int
	e.Provider.OnSend = func(a ownership.Assignment, m messaging.OutboundMessage) {
		if m.Media == nil || m.Media.URL == "" || m.Media.Size != int64(len(data)) {
			t.Errorf("provider got no usable attachment: %+v", m.Media)
		}
		sizes = append(sizes, len(m.Text))
	}
	e.StartWorkers(1)
	e.WaitMessage(res.MessageID, messaging.StatusAccepted)
	if len(e.Provider.Sent()) != 1 {
		t.Fatal("document not sent")
	}
	stored, _ := e.Repos.Messages.Get(bg, res.MessageID)
	if len(stored.Payload) > InlineLimit {
		t.Errorf("stored payload carries %d bytes", len(stored.Payload))
	}
	var p messaging.Payload
	_ = json.Unmarshal(stored.Payload, &p)
	if p.Media == nil || p.Media.ObjectKey == "" || p.Media.SHA256 == "" {
		t.Errorf("claim check missing: %+v", p)
	}
}

// INV-12: the reconciler converges observed_state towards desired_state.
func TestINV12_ReconcilerConverges(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)

	// socket dies silently: no event reaches the catalog
	e.Provider.SetState(inst.ID, instance.Disconnected)
	e.Provider.ConnectLeadsTo = instance.Connected
	Eventually(t, 5*time.Second, "converge to CONNECTED", func() bool {
		_, _, _ = e.Reconciler.ReconcileInstance(bg, inst.ID)
		i, _ := e.Repos.Instances.Get(bg, inst.ID)
		return i.ObservedState == instance.Connected
	})
	var changed bool
	for _, ev := range e.Bus.Published() {
		if ev.EventType == events.InstanceStatusChanged && ev.InstanceID == inst.ID {
			changed = true
		}
	}
	if !changed {
		t.Error("reconciliation must announce instance.status_changed")
	}
	if testutilCounter(e, "relayplane_reconciliation_drift_total") < 1 {
		t.Error("drift metric missing")
	}

	// desired DISCONNECTED converges too
	_ = e.Repos.Instances.UpdateDesired(bg, inst.ID, instance.DesiredDisconnected)
	Eventually(t, 5*time.Second, "converge to disconnected", func() bool {
		_, _, _ = e.Reconciler.ReconcileInstance(bg, inst.ID)
		i, _ := e.Repos.Instances.Get(bg, inst.ID)
		return !i.Drifted()
	})

	// desired DELETED converges: provider session removed, ownership released
	_ = e.Repos.Instances.UpdateDesired(bg, inst.ID, instance.DesiredDeleted)
	Eventually(t, 5*time.Second, "converge to deleted", func() bool {
		_, _, _ = e.Reconciler.ReconcileInstance(bg, inst.ID)
		i, _ := e.Repos.Instances.Get(bg, inst.ID)
		return i.ObservedState == instance.Deleted
	})
	if e.Provider.Has(inst.ID) {
		t.Error("provider session still exists after delete")
	}
	n, _ := e.Repos.Nodes.Get(bg, inst.NodeID)
	if n.ActiveInstances != 0 {
		t.Errorf("capacity leaked: %d", n.ActiveInstances)
	}
}
