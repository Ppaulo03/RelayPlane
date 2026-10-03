package contracttest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/ports"
)

func outboxMsg(id, instanceID string) messaging.Message {
	return messaging.Message{ID: id, TenantID: "t1", InstanceID: instanceID, NodeID: "node-01", AssignmentEpoch: 1,
		PartitionKey: instanceID, Recipient: "5562", Type: messaging.TypeText, Payload: json.RawMessage(`{"text":"hi"}`), Status: messaging.StatusQueued}
}

func buildFor(id string) func(seq int64) ([]byte, error) {
	return func(seq int64) ([]byte, error) {
		return []byte(fmt.Sprintf(`{"message_id":%q,"sequence_no":%d}`, id, seq)), nil
	}
}

// OutboxContract covers the transactional outbox, the per-instance sequence and
// the ordering barrier queries (run as part of RepositoryContract).
func outboxContract(t *testing.T, f RepoFactory) {
	setup := func(t *testing.T) (fixture, context.Context) {
		fx := newFixture(t, f)
		fx.tenant(t, "t1")
		fx.node(t, "node-01", 10)
		fx.instance(t, "inst_1", "t1")
		fx.instance(t, "inst_2", "t1")
		return fx, context.Background()
	}

	t.Run("sequence is gapless and unique under concurrency", func(t *testing.T) {
		fx, ctx := setup(t)
		const n = 30
		var wg sync.WaitGroup
		seqs := make(chan int64, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("msg_%02d", i)
				seq, err := fx.r.Messages.CreateWithOutbox(ctx, outboxMsg(id, "inst_1"), buildFor(id))
				if err != nil {
					t.Error(err)
					return
				}
				seqs <- seq
			}(i)
		}
		wg.Wait()
		close(seqs)
		var got []int
		for s := range seqs {
			got = append(got, int(s))
		}
		sort.Ints(got)
		for i, s := range got {
			if s != i+1 {
				t.Fatalf("INV-07: sequence must be 1..%d without gaps or duplicates, got %v", n, got)
			}
		}
		entries, _ := fx.r.Messages.ListOutbox(ctx, "inst_1", 100)
		if len(entries) != n {
			t.Fatalf("outbox entries: %d", len(entries))
		}
		for i, e := range entries {
			if e.Sequence != int64(i+1) {
				t.Fatalf("outbox must list in sequence order: %+v", entries[:i+1])
			}
			var env struct {
				ID  string `json:"message_id"`
				Seq int64  `json:"sequence_no"`
			}
			if err := json.Unmarshal(e.Command, &env); err != nil || env.ID != e.MessageID || env.Seq != e.Sequence {
				t.Fatalf("the command must carry the sequence allocated with it: %s (%v)", e.Command, err)
			}
			m, _ := fx.r.Messages.Get(ctx, e.MessageID)
			if m.SequenceNo != e.Sequence {
				t.Fatalf("message %s has sequence %d, outbox %d", m.ID, m.SequenceNo, e.Sequence)
			}
		}
		// the other instance has its own counter
		seq, err := fx.r.Messages.CreateWithOutbox(ctx, outboxMsg("msg_other", "inst_2"), buildFor("msg_other"))
		if err != nil || seq != 1 {
			t.Fatalf("sequences are per instance: %d %v", seq, err)
		}
	})

	t.Run("failed accept consumes no sequence and writes nothing", func(t *testing.T) {
		fx, ctx := setup(t)
		if _, err := fx.r.Messages.CreateWithOutbox(ctx, outboxMsg("a", "inst_1"), buildFor("a")); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("build failed")
		if _, err := fx.r.Messages.CreateWithOutbox(ctx, outboxMsg("b", "inst_1"), func(int64) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
			t.Fatalf("got %v", err)
		}
		if _, err := fx.r.Messages.Get(ctx, "b"); !errors.Is(err, errs.ErrNotFound) {
			t.Fatalf("a failed accept left a message behind: %v", err)
		}
		if _, err := fx.r.Messages.CreateWithOutbox(ctx, outboxMsg("a", "inst_1"), buildFor("a")); !errors.Is(err, errs.ErrAlreadyExists) {
			t.Fatalf("duplicate id: %v", err)
		}
		seq, err := fx.r.Messages.CreateWithOutbox(ctx, outboxMsg("c", "inst_1"), buildFor("c"))
		if err != nil || seq != 2 {
			t.Fatalf("sequence after failed attempts must continue without a gap: %d %v", seq, err)
		}
		if es, _ := fx.r.Messages.ListOutbox(ctx, "inst_1", 10); len(es) != 2 {
			t.Fatalf("outbox: %+v", es)
		}
	})

	t.Run("dispatch bookkeeping", func(t *testing.T) {
		fx, ctx := setup(t)
		for i := 1; i <= 3; i++ {
			id := fmt.Sprintf("m%d", i)
			_, _ = fx.r.Messages.CreateWithOutbox(ctx, outboxMsg(id, "inst_1"), buildFor(id))
		}
		_, _ = fx.r.Messages.CreateWithOutbox(ctx, outboxMsg("x1", "inst_2"), buildFor("x1"))
		ids, _ := fx.r.Messages.ListInstancesWithPendingOutbox(ctx, 10)
		if len(ids) != 2 {
			t.Fatalf("pending instances: %v", ids)
		}
		now := time.Now()
		if err := fx.r.Messages.MarkOutboxDispatched(ctx, "inst_1", 1, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fx.r.Messages.MarkOutboxDispatched(ctx, "inst_1", 99, now); !errors.Is(err, errs.ErrNotFound) {
			t.Fatalf("unknown entry: %v", err)
		}
		es, _ := fx.r.Messages.ListOutbox(ctx, "inst_1", 10)
		if len(es) != 2 || es[0].Sequence != 2 {
			t.Fatalf("dispatched entries must leave the pending list: %+v", es)
		}
		// m1 was dispatched an hour ago but is still QUEUED: the broker lost it
		stuck, _ := fx.r.Messages.ListStuckOutbox(ctx, now.Add(-time.Minute), 10)
		if len(stuck) != 1 || stuck[0].MessageID != "m1" {
			t.Fatalf("stuck: %+v", stuck)
		}
		_, _ = fx.r.Messages.Transition(ctx, "m1", []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{})
		if err := fx.r.Messages.ResetOutbox(ctx, "inst_1", 1); err != nil {
			t.Fatal(err)
		}
		if es, _ := fx.r.Messages.ListOutbox(ctx, "inst_1", 10); len(es) != 3 || es[0].Sequence != 1 {
			t.Fatalf("a reset entry is pending again, in order: %+v", es)
		}
		if err := fx.r.Messages.MarkOutboxDispatched(ctx, "inst_1", 1, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := fx.r.Messages.ResetOutbox(ctx, "inst_1", 99); !errors.Is(err, errs.ErrNotFound) {
			t.Fatalf("reset of an unknown entry: %v", err)
		}
		if stuck, _ := fx.r.Messages.ListStuckOutbox(ctx, now.Add(-time.Minute), 10); len(stuck) != 0 {
			t.Fatalf("a message that is being processed is not stuck: %+v", stuck)
		}
		if n, err := fx.r.Messages.PurgeOutbox(ctx, now.Add(-time.Minute)); err != nil || n != 1 {
			t.Fatalf("purge: %d %v", n, err)
		}
		if n, _ := fx.r.Messages.PurgeOutbox(ctx, now.Add(time.Hour)); n != 0 {
			t.Fatalf("undispatched entries must never be purged: %d", n)
		}
	})

	t.Run("ordering barrier query", func(t *testing.T) {
		fx, ctx := setup(t)
		for i := 1; i <= 4; i++ {
			id := fmt.Sprintf("m%d", i)
			_, _ = fx.r.Messages.CreateWithOutbox(ctx, outboxMsg(id, "inst_1"), buildFor(id))
		}
		move := func(id string, from, to messaging.Status) {
			if _, err := fx.r.Messages.Transition(ctx, id, []messaging.Status{from}, to, ports.MessagePatch{}); err != nil {
				t.Fatalf("%s %s->%s: %v", id, from, to, err)
			}
		}
		first := func(seq int64, unknownTimeout time.Duration) *messaging.Message {
			m, err := fx.r.Messages.FirstUnresolvedBefore(ctx, "inst_1", seq, unknownTimeout)
			if errors.Is(err, errs.ErrNotFound) {
				return nil
			}
			if err != nil {
				t.Fatal(err)
			}
			return m
		}
		if first(1, 0) != nil {
			t.Fatal("nothing precedes sequence 1")
		}
		if m := first(2, 0); m == nil || m.ID != "m1" {
			t.Fatalf("a QUEUED predecessor blocks: %+v", m)
		}
		move("m1", messaging.StatusQueued, messaging.StatusDispatching)
		if m := first(2, 0); m == nil || m.ID != "m1" {
			t.Fatalf("a DISPATCHING predecessor blocks: %+v", m)
		}
		move("m1", messaging.StatusDispatching, messaging.StatusAccepted)
		if first(2, 0) != nil {
			t.Fatal("an ACCEPTED predecessor does not block")
		}
		move("m2", messaging.StatusQueued, messaging.StatusDispatching)
		move("m2", messaging.StatusDispatching, messaging.StatusUnknown)
		if m := first(4, 0); m == nil || m.ID != "m2" || m.Status != messaging.StatusUnknown {
			t.Fatalf("UNKNOWN is an ordering barrier: %+v", m)
		}
		if m := first(3, 0); m == nil || m.ID != "m2" {
			t.Fatalf("barrier also holds the immediate successor: %+v", m)
		}
		if first(2, 0) != nil {
			t.Fatal("a message is never blocked by itself or by later ones")
		}
		// the barrier can be released after a timeout: an UNKNOWN older than the cutoff no longer blocks
		if m := first(4, time.Hour); m == nil || m.ID != "m2" {
			t.Fatalf("a recent UNKNOWN still blocks: %+v", m)
		}
		time.Sleep(30 * time.Millisecond)
		if m := first(4, 10*time.Millisecond); m != nil && m.ID == "m2" {
			t.Fatalf("an UNKNOWN older than the timeout must not block: %+v", m)
		}
		// resolution (FAILED definitively did not go out) lifts it; m3 (QUEUED) is the next predecessor of m4
		if _, err := fx.r.Messages.Transition(ctx, "m2", []messaging.Status{messaging.StatusUnknown}, messaging.StatusFailed, ports.MessagePatch{ErrorCode: "NOT_SENT"}); err != nil {
			t.Fatal(err)
		}
		if m := first(4, 0); m == nil || m.ID != "m3" {
			t.Fatalf("after resolving m2 the next unresolved predecessor is m3: %+v", m)
		}
	})
}
