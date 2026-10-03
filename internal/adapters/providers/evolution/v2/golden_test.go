package v2_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/relayplane/relayplane/internal/adapters/providers/evolution/v2"
	"github.com/relayplane/relayplane/internal/core/events"
)

// testdata/real holds webhooks recorded from a REAL Evolution 2.3.7 node (Baileys 7.0.0-rc13) with a real number and then
// sanitized (tools/spike). They are the ground truth for what the node sends; the adapter must normalize each of them.
// A new file must be classified here, which forces someone to decide what it means.
func TestRealWebhooksNormalize(t *testing.T) {
	type want struct {
		typ    events.Type // "" = the adapter must drop it
		kind   string      // message.received: payload type
		state  string      // status events
		group  bool
		quoted bool
	}
	cases := map[string]want{
		"upsert-conversation-direct-plain":    {typ: events.MessageReceived, kind: "text"},
		"upsert-conversation-direct-quoted":   {typ: events.MessageReceived, kind: "text", quoted: true},
		"upsert-conversation-group-plain":     {typ: events.MessageReceived, kind: "text", group: true},
		"upsert-conversation-group-quoted":    {typ: events.MessageReceived, kind: "text", group: true, quoted: true},
		"upsert-audioMessage-direct-plain":    {typ: events.MessageReceived, kind: "audio"},
		"upsert-imageMessage-direct-plain":    {typ: events.MessageReceived, kind: "image"},
		"upsert-documentMessage-direct-plain": {typ: events.MessageReceived, kind: "document"},
		"upsert-videoMessage-direct-plain":    {typ: events.MessageReceived, kind: "video"},
		// an EDIT arrives like this and its new text is not readable: consumers must treat it as "the user changed something"
		"upsert-secretEncryptedMessage-direct-plain": {typ: events.MessageReceived, kind: "secretEncrypted"},
		"upsert-secretEncryptedMessage-group-plain":  {typ: events.MessageReceived, kind: "secretEncrypted", group: true},
		"update-SERVER_ACK":                          {typ: events.MessageStatus, state: "sent"},
		"update-DELIVERY_ACK":                        {typ: events.MessageStatus, state: "delivered"},
		"update-READ":                                {typ: events.MessageStatus, state: "read"},
		"update-PLAYED":                              {typ: events.MessageStatus, state: "read"},
		"messages-delete":                            {typ: events.MessageDeleted},
		"messages-edited":                            {}, // the REVOKE companion of messages.delete: no extra information
		"send-message":                               {}, // echo of our own send
		"connection-open-200":                        {typ: events.InstanceStatusChanged, state: "CONNECTED"},
		"connection-connecting-200":                  {typ: events.InstanceStatusChanged, state: "CONNECTING"},
		"connection-close-401":                       {typ: events.InstanceStatusChanged, state: "LOGGED_OUT"},
		"connection-refused-428":                     {}, // seen while pairing; not a state RelayPlane models
	}

	files, err := filepath.Glob("testdata/real/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no real fixtures: %v", err)
	}
	w := v2.Webhook{Secret: "s"}
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".json")
		c, known := cases[name]
		if !known {
			t.Errorf("%s: unclassified fixture; add the expectation to golden_test.go", name)
			continue
		}
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			got, err := w.Normalize(hook("s", "node-01", 1, string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			if c.typ == "" {
				if len(got) != 0 {
					t.Fatalf("must be dropped: %+v", got)
				}
				return
			}
			if len(got) != 1 || got[0].Type != c.typ {
				t.Fatalf("want one %s, got %+v", c.typ, got)
			}
			switch pl := got[0].Payload.(type) {
			case events.MessageReceivedPayload:
				if pl.Type != c.kind || pl.Group != c.group || (pl.ReplyToProviderMessageID != "") != c.quoted {
					t.Errorf("%+v", pl)
				}
				if strings.Contains(pl.From, "@") || strings.HasSuffix(pl.From, "lid") || len(pl.From) < 10 {
					t.Errorf("from must be a phone number: %q", pl.From)
				}
				if c.group && (!strings.HasSuffix(pl.ChatID, "@g.us") || pl.SenderLID == "" || !strings.HasPrefix(pl.From, "55")) {
					t.Errorf("group sender not resolved: %+v", pl)
				}
				if !c.group && pl.ChatID != "" {
					t.Errorf("chat_id is for groups only: %+v", pl)
				}
			case events.MessageStatusPayload:
				if pl.Status != c.state {
					t.Errorf("%+v", pl)
				}
			case events.InstanceStatusChangedPayload:
				if pl.State != c.state {
					t.Errorf("%+v", pl)
				}
			case events.MessageDeletedPayload:
				if pl.ProviderMessageID == "" || !strings.HasPrefix(pl.From, "55") {
					t.Errorf("%+v", pl)
				}
			}
		})
	}
	for name := range cases {
		if _, err := os.Stat(filepath.Join("testdata/real", name+".json")); err != nil {
			t.Errorf("expected fixture %s is missing", name)
		}
	}
}
