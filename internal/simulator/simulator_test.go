package simulator

import (
	"encoding/json"
	"testing"
)

// The control API says [] for "nothing sent yet", not null: a client that iterates the answer must not need a special case.
func TestSentMessagesIsNeverNil(t *testing.T) {
	s := New(Config{})
	got := s.SentMessages("nobody")
	if got == nil {
		t.Fatal("an unknown instance has no messages: an empty list, not nil")
	}
	raw, _ := json.Marshal(map[string]any{"sent": got})
	if string(raw) != `{"sent":[]}` {
		t.Errorf("the control API must answer an empty list: %s", raw)
	}
}
