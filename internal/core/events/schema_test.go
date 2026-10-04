package events_test

import (
	"encoding/json"
	"strings"
	"testing"

	eventschema "github.com/relayplane/relayplane/docs/events"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

// The schema in docs/events is the contract consumers compile against. These tests keep it and the code in step: a
// change that is not written in the schema (or the other way round) fails here, and so does anything that would break a
// consumer silently.

func TestExamplesValidateAgainstTheSchema(t *testing.T) {
	ex, err := eventschema.Examples()
	if err != nil || len(ex) == 0 {
		t.Fatalf("examples: %v", err)
	}
	for name, body := range ex {
		if err := eventschema.Validate(body); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestEveryTenantFacingEventTypeHasAnExample(t *testing.T) {
	ex, _ := eventschema.Examples()
	have := map[events.Type]bool{}
	for _, body := range ex {
		var e struct {
			Type events.Type `json:"event_type"`
		}
		_ = json.Unmarshal(body, &e)
		have[e.Type] = true
	}
	for _, typ := range subscription.TenantFacingTypes() {
		if !have[typ] {
			t.Errorf("no example (and so no schema coverage) for %s", typ)
		}
	}
}

// The examples are decoded by the Go types and written back: what the code emits must still satisfy the schema, which
// forbids undeclared fields, so a field added to a struct without a schema entry fails this test.
func TestGoTypesRoundTripThroughTheSchema(t *testing.T) {
	ex, _ := eventschema.Examples()
	for name, body := range ex {
		var ev events.Event
		if err := json.Unmarshal(body, &ev); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		back, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		if err := eventschema.Validate(back); err != nil {
			t.Errorf("%s re-marshalled by the Go types no longer matches the schema: %v\n%s", name, err, back)
		}
		if ev.SchemaVersion != events.SchemaVersion {
			t.Errorf("%s: schema_version %d, code says %d", name, ev.SchemaVersion, events.SchemaVersion)
		}
	}
}

func TestSchemaRejectsBreakingShapes(t *testing.T) {
	ex, _ := eventschema.Examples()
	base := ex["message.received.json"]
	mutate := func(f func(m map[string]any)) []byte {
		var m map[string]any
		_ = json.Unmarshal(base, &m)
		f(m)
		out, _ := json.Marshal(m)
		return out
	}
	payload := func(m map[string]any) map[string]any { return m["payload"].(map[string]any) }
	for name, body := range map[string][]byte{
		"missing sequence":          mutate(func(m map[string]any) { delete(m, "sequence") }),
		"sequence 0":                mutate(func(m map[string]any) { m["sequence"] = 0 }),
		"wrong schema_version":      mutate(func(m map[string]any) { m["schema_version"] = 2 }),
		"missing from":              mutate(func(m map[string]any) { delete(payload(m), "from") }),
		"undeclared payload field":  mutate(func(m map[string]any) { payload(m)["surprise"] = 1 }),
		"undeclared envelope field": mutate(func(m map[string]any) { m["surprise"] = 1 }),
		"unknown event type":        mutate(func(m map[string]any) { m["event_type"] = "message.exploded" }),
		"bad timestamp":             mutate(func(m map[string]any) { m["timestamp"] = "yesterday" }),
		"status enum":               []byte(strings.Replace(string(ex["message.outbound_status.json"]), `"ACCEPTED"`, `"SENT"`, 1)),
	} {
		if err := eventschema.Validate(body); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
}
