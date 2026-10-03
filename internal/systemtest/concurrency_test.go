package systemtest

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/ports"
)

func TestConcurrent_DuplicateCreateWithSameKeyYieldsOneInstance(t *testing.T) {
	e := NewEnv(t)
	var wg sync.WaitGroup
	var ok, inProgress atomic.Int32
	var mu sync.Mutex
	ids := map[string]bool{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: "main"}, "same-key")
			switch {
			case err == nil:
				ok.Add(1)
				mu.Lock()
				ids[res.ID] = true
				mu.Unlock()
			case errors.Is(err, errs.ErrInProgress):
				inProgress.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		}()
	}
	wg.Wait()
	list, _ := e.App.Instances.List(bg, e.Tenant)
	if len(list) != 1 || len(ids) != 1 {
		t.Fatalf("instances=%d distinct ids=%d", len(list), len(ids))
	}
	if ok.Load() < 1 {
		t.Fatal("nobody succeeded")
	}
}

func TestConcurrent_CreatesNeverOverbookCapacity(t *testing.T) {
	e := NewEnv(t)
	for _, id := range []string{"node-01", "node-02"} {
		_ = e.Repos.Nodes.Upsert(bg, routing.Node{ID: id, Provider: ProviderKey, Endpoint: "http://" + id, Capacity: 3})
	}
	var wg sync.WaitGroup
	var ok, noCap atomic.Int32
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := e.App.Instances.Create(bg, e.Tenant, app.CreateInstanceInput{Name: fmt.Sprintf("i%d", i)}, fmt.Sprintf("k%d", i))
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, errs.ErrNoCapacity):
				noCap.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 6 || noCap.Load() != 19 {
		t.Fatalf("ok=%d noCap=%d, want 6 successes (total capacity)", ok.Load(), noCap.Load())
	}
	nodes, _ := e.Repos.Nodes.List(bg)
	for _, n := range nodes {
		if n.ActiveInstances != 3 {
			t.Errorf("node %s has %d", n.ID, n.ActiveInstances)
		}
	}
}

func TestConcurrent_DuplicateSendWithSameKeyEnqueuesOnce(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := e.SendText(e.Tenant, inst.ID, "pay", "order-77")
			if err != nil && !errors.Is(err, errs.ErrInProgress) {
				t.Errorf("unexpected %v", err)
			}
		}()
	}
	wg.Wait()
	e.StartWorkers(3)
	Eventually(t, 5*time.Second, "sent", func() bool { return len(e.Provider.Sent()) >= 1 })
	time.Sleep(100 * time.Millisecond)
	if n := len(e.Provider.Sent()); n != 1 {
		t.Fatalf("sent %d times", n)
	}
}

func TestConcurrent_SameCommandDeliveredToManyWorkersSendsOnce(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	r, _, _ := e.SendText(e.Tenant, inst.ID, "hi", "")
	m, _ := e.Repos.Messages.Get(bg, r.MessageID)
	env := messaging.Envelope{MessageID: m.ID, TenantID: m.TenantID, InstanceID: m.InstanceID, PartitionKey: m.InstanceID,
		Assignment: inst.Assignment(), Type: messaging.TypeText, To: m.Recipient, Payload: messaging.Payload{Text: "hi"}}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ { // a broker that duplicates the command to many consumers
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.Worker.Handle(bg, ports.Command{ID: m.ID, PartitionKey: m.InstanceID, Payload: env, Attempt: 1})
		}()
	}
	wg.Wait()
	if n := len(e.Provider.Sent()); n != 1 {
		t.Fatalf("sent %d times", n)
	}
}

func TestConcurrent_MigrationRequestsYieldOneOperation(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ops := map[string]bool{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, _, err := e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
			if err != nil {
				t.Errorf("start: %v", err)
				return
			}
			mu.Lock()
			ops[res.OperationID] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(ops) != 1 {
		t.Fatalf("%d migration operations were created", len(ops))
	}
	for id := range ops {
		waitMigration(t, e, id, instance.OpSucceeded, instance.OpRunning)
	}
	cur, _ := e.Repos.Instances.Get(bg, inst.ID)
	if cur.AssignmentEpoch != 2 {
		t.Fatalf("epoch %d: the migration ran more than once", cur.AssignmentEpoch)
	}
}

func TestConcurrent_SendsDuringMigrationNeverReachWrongOwner(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.StartWorkers(3)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _, err := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%d", i), "")
			if err != nil && !errors.Is(err, errs.ErrConflict) {
				t.Errorf("send: %v", err)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(30 * time.Millisecond)
	res, _, err := e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitMigration(t, e, res.OperationID, instance.OpSucceeded, instance.OpRunning)
	finishMigration(t, e, inst.ID, res.OperationID)
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	cur, _ := e.Repos.Instances.Get(bg, inst.ID)
	// Every message that reached the provider did so under the assignment that
	// was current at the time; none may reach the fenced old owner afterwards.
	var firstNew time.Time
	for _, s := range e.Provider.Sent() {
		if s.Assignment.Epoch == cur.AssignmentEpoch && firstNew.IsZero() {
			firstNew = s.At
		}
	}
	for _, s := range e.Provider.Sent() {
		if s.Assignment.Epoch < cur.AssignmentEpoch && !firstNew.IsZero() && s.At.After(firstNew) {
			t.Fatalf("a command for the old epoch was dispatched after the new owner became active")
		}
	}
}

// ---- Definition of Done: the complete first-version flow ----

func TestE2E_DefinitionOfDone(t *testing.T) {
	e := NewEnv(t)
	e.StartWorkers(2)
	e.StartProjector()

	// create tenant (API key authentication)
	tn, key, err := e.App.Tenants.Create(bg, "Comercial SA")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := e.App.Tenants.Authenticate(bg, key); err != nil || got.ID != tn.ID {
		t.Fatalf("auth: %v", err)
	}
	if _, err := e.App.Tenants.Authenticate(bg, "wrong"); !errors.Is(err, errs.ErrUnauthenticated) {
		t.Fatal("wrong key must not authenticate")
	}

	// create instance -> automatic placement -> created on the node
	res, _, err := e.App.Instances.Create(bg, tn.ID, app.CreateInstanceInput{Name: "Comercial"}, "tenant-main-whatsapp")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != instance.AwaitingPairing {
		t.Fatalf("status %s", res.Status)
	}
	inst, _ := e.Repos.Instances.Get(bg, res.ID)
	if inst.NodeID == "" || inst.AssignmentEpoch != 1 || !e.Provider.HasOn(inst.NodeID, inst.ID) {
		t.Fatalf("placement/creation: %+v", inst)
	}
	// duplicate create -> idempotent
	again, replayed, _ := e.App.Instances.Create(bg, tn.ID, app.CreateInstanceInput{Name: "Comercial"}, "tenant-main-whatsapp")
	if !replayed || again != res {
		t.Fatalf("not idempotent: %+v", again)
	}

	// obtain QR
	pc, err := e.App.Instances.Pairing(bg, tn.ID, inst.ID, app.PairingQR)
	if err != nil || pc.QRCode == "" {
		t.Fatalf("qr: %+v %v", pc, err)
	}

	// connect (user scans) -> reconcile CONNECTED
	e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Connected)
	if _, _, err := e.Reconciler.ReconcileInstance(bg, inst.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.Repos.Instances.Get(bg, inst.ID); got.ObservedState != instance.Connected {
		t.Fatalf("observed %s", got.ObservedState)
	}

	// enqueue two messages -> worker dispatch, ordered
	m1, _, err := e.App.Messages.Send(bg, tn.ID, app.SendInput{InstanceID: inst.ID, To: "5562999999999", Type: messaging.TypeText, Payload: app.SendPayload{Text: "first"}}, "order-1")
	if err != nil || m1.Status != messaging.StatusQueued {
		t.Fatalf("%+v %v", m1, err)
	}
	m2, _, _ := e.App.Messages.Send(bg, tn.ID, app.SendInput{InstanceID: inst.ID, To: "5562999999999", Type: messaging.TypeText, Payload: app.SendPayload{Text: "second"}}, "order-2")
	acc1 := e.WaitMessage(m1.MessageID, messaging.StatusAccepted)
	e.WaitMessage(m2.MessageID, messaging.StatusAccepted)
	sent := e.Provider.Sent()
	if len(sent) != 2 || sent[0].Message.Text != "first" || sent[1].Message.Text != "second" {
		t.Fatalf("sent: %+v", sent)
	}

	// inbound webhook -> normalize -> canonical event; duplicate deduplicated
	hook := inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "wamid-in-1"), statusEv(inst.ID, acc1.ProviderMessageID, "delivered"))
	hr, err := e.App.Inbound.Handle(bg, ProviderKey, hook)
	if err != nil || hr.Published != 2 {
		t.Fatalf("%+v %v", hr, err)
	}
	if hr2, _ := e.App.Inbound.Handle(bg, ProviderKey, hook); hr2.Duplicates != 2 || hr2.Published != 0 {
		t.Fatalf("duplicate webhook: %+v", hr2)
	}
	var received *events.Event
	for _, ev := range e.Bus.Published() {
		if ev.EventType == events.MessageReceived {
			ev := ev
			received = &ev
		}
	}
	if received == nil || received.TenantID != tn.ID || received.InstanceID != inst.ID || received.Provider != ProviderKey ||
		received.EventID == "" || received.Payload == nil {
		t.Fatalf("canonical event: %+v", received)
	}
	raw, _ := json.Marshal(received.Payload)
	for _, forbidden := range []string{"messages.upsert", "evolution", "remoteJid", "baileys"} {
		if containsFold(string(raw), forbidden) {
			t.Errorf("provider-specific content %q leaked into the canonical event: %s", forbidden, raw)
		}
	}
	e.WaitMessage(m1.MessageID, messaging.StatusDelivered) // projector applied the receipt

	// stale command rejected, draining honoured (covered in depth by INV tests)
	if _, err := e.App.Nodes.Drain(bg, inst.NodeID); err != nil {
		t.Fatal(err)
	}
	x := e.CreateInstance(tn.ID, "second", false)
	if x.NodeID == inst.NodeID {
		t.Fatal("draining node received a new instance")
	}

	// worker crash is recoverable: stop workers, queue a message, restart workers
	e.Stop()
	m3, _, _ := e.App.Messages.Send(bg, tn.ID, app.SendInput{InstanceID: inst.ID, To: "5562999999999", Type: messaging.TypeText, Payload: app.SendPayload{Text: "after crash"}}, "")
	e.StartWorkers(1)
	e.WaitMessage(m3.MessageID, messaging.StatusAccepted)
}

func containsFold(s, sub string) bool {
	return len(sub) > 0 && (indexFold(s, sub) >= 0)
}

func indexFold(s, sub string) int {
	ls, lsub := []rune(s), []rune(sub)
	for i := 0; i+len(lsub) <= len(ls); i++ {
		match := true
		for j := range lsub {
			a, b := ls[i+j], lsub[j]
			if a >= 'A' && a <= 'Z' {
				a += 32
			}
			if b >= 'A' && b <= 'Z' {
				b += 32
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
