package events

import "testing"

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
