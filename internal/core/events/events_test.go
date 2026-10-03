package events

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDedupeKeyDistinguishesStates(t *testing.T) { // INV-05 (key shape)
	sent := DedupeKey("i1", MessageStatus, "wamid1", "sent")
	del := DedupeKey("i1", MessageStatus, "wamid1", "delivered")
	read := DedupeKey("i1", MessageStatus, "wamid1", "read")
	if sent == del || del == read || sent == read {
		t.Fatal("sent/delivered/read must never collapse")
	}
	if DedupeKey("i1", MessageStatus, "wamid1", "read") != read {
		t.Fatal("key must be deterministic")
	}
	if DedupeKey("i2", MessageStatus, "wamid1", "read") == read {
		t.Fatal("different instances must not collide")
	}
}

func TestEventIDDeterministic(t *testing.T) {
	k := DedupeKey("i1", MessageReceived, "m1", "")
	if EventIDFor(k) != EventIDFor(k) || EventIDFor(k) == EventIDFor(k+"x") {
		t.Fatal("event id must be a stable function of the dedupe key")
	}
}

func TestEventCarriesTheSourceAssignment(t *testing.T) {
	raw, _ := json.Marshal(Event{EventID: "e", InstanceID: "i", SourceAssignment: &SourceAssignment{NodeID: "node-01", Epoch: 10}})
	if !strings.Contains(string(raw), `"source_assignment":{"node_id":"node-01","epoch":10}`) {
		t.Fatalf("%s", raw)
	}
	var back Event
	if err := json.Unmarshal(raw, &back); err != nil || back.SourceAssignment == nil || back.SourceAssignment.Epoch != 10 {
		t.Fatalf("round trip: %+v %v", back, err)
	}
}
