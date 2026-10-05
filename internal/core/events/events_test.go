package events

import (
	"crypto/sha256"
	"encoding/hex"
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

func TestErasureSubjectIsKeyedAndNotAPlainHash(t *testing.T) {
	key := []byte("k1")
	a := ErasureSubject(key, "5562988887777")
	if a == "" || a != ErasureSubject(key, "5562988887777") {
		t.Fatal("the subject of a number is stable for one key")
	}
	if a == ErasureSubject([]byte("k2"), "5562988887777") {
		t.Error("another key, another subject: the table alone must not be enough to try numbers")
	}
	sum := sha256.Sum256([]byte("5562988887777"))
	if a == hex.EncodeToString(sum[:]) {
		t.Error("it must not be a plain SHA-256 of the number (a phone number has little entropy)")
	}
	if ErasureSubject(key, "5562988887777") == ErasureSubject(key, "5562988887778") {
		t.Error("different numbers, different subjects")
	}
}

func TestOnlyInboundMessagesAndTheirDeletionsAreAboutAContact(t *testing.T) {
	p := map[string]any{"from": "5562988887777"}
	for _, typ := range []Type{MessageReceived, MessageDeleted} {
		if got := (Event{EventType: typ, Payload: p}).ContactNumber(); got != "5562988887777" {
			t.Errorf("%s is about its author: %q", typ, got)
		}
	}
	for _, typ := range []Type{MessageStatus, MessageOutboundStatus, InstanceStatusChanged} {
		if got := (Event{EventType: typ, Payload: p}).ContactNumber(); got != "" {
			t.Errorf("%s is about nobody: %q", typ, got)
		}
	}
}
