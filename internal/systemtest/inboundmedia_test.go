package systemtest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
)

// An inbound message with an attachment is delivered to the tenant only after RelayPlane resolved the attachment: stored
// and downloadable (READY), refused on purpose (REJECTED) or lost (FAILED). It is never left half-resolved.

type mediaFixture struct {
	t    *testing.T
	e    *Env
	inst *instance.Instance
}

func newMediaFixture(t *testing.T) *mediaFixture { return newMediaFixtureWith(t, nil) }

// newMediaFixtureWith lets a test configure the environment BEFORE the background loops start (changing the dispatcher or
// the receiver afterwards would race with them).
func newMediaFixtureWith(t *testing.T, configure func(*Env)) *mediaFixture {
	e := NewEnv(t)
	if configure != nil {
		configure(e)
	}
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.StartOutbox()
	e.StartWebhooks()
	return &mediaFixture{t: t, e: e, inst: inst}
}

// receive plays the provider posting an inbound message that carries an attachment.
func (f *mediaFixture) receive(id string, m memory.FakeWebhookMedia) {
	f.t.Helper()
	payload := fmt.Sprintf(`{"provider_message_id":%q,"from":"5562988887777","type":%q,"text":"olha"}`, id, m.Kind)
	ev := memory.FakeWebhookEv{InstanceID: f.inst.ID, Type: events.MessageReceived, ProviderMessageID: id, Timestamp: time.Now().UTC(),
		Payload: json.RawMessage(payload), Media: &m}
	if _, err := f.e.App.Inbound.Handle(bg, ProviderKey, inboundBody(f.inst.NodeID, f.inst.AssignmentEpoch, ev)); err != nil {
		f.t.Fatal(err)
	}
}

// receiveFrom plays a given contact writing: a text, or an attachment with an optional caption.
func (f *mediaFixture) receiveFrom(from, id, text string, m *memory.FakeWebhookMedia) {
	f.t.Helper()
	typ := "text"
	if m != nil {
		typ = m.Kind
	}
	payload := fmt.Sprintf(`{"provider_message_id":%q,"from":%q,"type":%q,"text":%q}`, id, from, typ, text)
	ev := memory.FakeWebhookEv{InstanceID: f.inst.ID, Type: events.MessageReceived, ProviderMessageID: id, Timestamp: time.Now().UTC(),
		Payload: json.RawMessage(payload), Media: m}
	if _, err := f.e.App.Inbound.Handle(bg, ProviderKey, inboundBody(f.inst.NodeID, f.inst.AssignmentEpoch, ev)); err != nil {
		f.t.Fatal(err)
	}
}

func attachmentOf(kind, mime, name string, content []byte) *memory.FakeWebhookMedia {
	m := attachment(kind, mime, name, content, nil)
	return &m
}

func mediaIDOf(t *testing.T, body []byte) string {
	t.Helper()
	var env struct {
		Payload events.MessageReceivedPayload `json:"payload"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Payload.Media == nil {
		t.Fatalf("no media in %s: %v", body, err)
	}
	return env.Payload.Media.MediaID
}

func attachment(kind, mime, name string, content []byte, mod func(*memory.FakeMediaRef)) memory.FakeWebhookMedia {
	ref := memory.NewFakeMediaRef(content, mime, name)
	if mod != nil {
		mod(&ref)
	}
	return memory.FakeWebhookMedia{Kind: kind, MimeType: mime, Size: int64(len(content)), Filename: name, Ref: ref.JSON()}
}

// delivered returns the media description of the n-th message.received the tenant got (waiting for it).
func (f *mediaFixture) delivered(n int) (events.MessageMedia, Received) {
	f.t.Helper()
	Eventually(f.t, 15*time.Second, fmt.Sprintf("message.received #%d delivered", n), func() bool { return len(f.e.Receiver.Accepted(hookURL)) >= n })
	rec := f.e.Receiver.Accepted(hookURL)[n-1]
	var env struct {
		Payload events.MessageReceivedPayload `json:"payload"`
	}
	if err := json.Unmarshal(rec.Body, &env); err != nil {
		f.t.Fatal(err)
	}
	if env.Payload.Media == nil {
		f.t.Fatalf("a message with an attachment arrived without media: %s", rec.Body)
	}
	return *env.Payload.Media, rec
}

func (f *mediaFixture) settle() { time.Sleep(150 * time.Millisecond) }

func TestInboundMedia_ReadyIsStoredDownloadableAndDeliveredOnce(t *testing.T) {
	f := newMediaFixture(t)
	content := []byte("OggS....a voice note")
	f.receive("WA-AUD-1", attachment("audio", "audio/ogg; codecs=opus", "", content, nil))

	// nothing is delivered while the resolver is not running: the event is held, not published half-done
	f.settle()
	if n := len(f.e.Receiver.All()); n != 0 {
		t.Fatalf("a message with media must wait for its attachment, %d deliveries happened", n)
	}
	f.e.StartMedia()
	m, rec := f.delivered(1)
	if m.Status != events.MediaReady || m.Reason != "" || m.MediaID == "" || m.Kind != "audio" || m.Size != int64(len(content)) || m.MimeType != "audio/ogg" {
		t.Fatalf("media: %+v", m)
	}

	// the tenant downloads exactly what the provider had
	b, r, err := f.e.App.Media.Open(bg, f.e.Tenant, m.MediaID)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	sum := sha256.Sum256(content)
	if string(got) != string(content) || b.SHA256 != hex.EncodeToString(sum[:]) || b.Size != int64(len(content)) || b.ContentType != "audio/ogg; codecs=opus" {
		t.Fatalf("stored media: %+v %q", b, got)
	}
	if !b.ExpiresAt.After(time.Now().Add(24 * time.Hour)) {
		t.Errorf("an inbound attachment is kept for days, not the 24h of an upload: %v", b.ExpiresAt)
	}
	// the decryption reference never reaches the tenant
	for _, banned := range []string{"content_b64", "ref"} {
		if containsKey(rec.Body, banned) {
			t.Errorf("%q leaked into the tenant event: %s", banned, rec.Body)
		}
	}

	// another tenant cannot see it
	if _, _, err := f.e.App.Media.Open(bg, f.e.Tenant2, m.MediaID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("tenant isolation: %v", err)
	}
	f.settle()
	if n := len(f.e.Receiver.Accepted(hookURL)); n != 1 {
		t.Errorf("delivered %d times", n)
	}
}

func containsKey(body []byte, key string) bool {
	var v any
	_ = json.Unmarshal(body, &v)
	var walk func(any) bool
	walk = func(x any) bool {
		switch t := x.(type) {
		case map[string]any:
			for k, c := range t {
				if k == key || walk(c) {
					return true
				}
			}
		case []any:
			for _, c := range t {
				if walk(c) {
					return true
				}
			}
		}
		return false
	}
	return walk(v)
}

func TestInboundMedia_RefusedOnPurposeWithoutDownloading(t *testing.T) {
	f := newMediaFixture(t)
	f.e.StartMedia()

	big := attachment("video", "video/mp4", "film.mp4", []byte("x"), nil)
	big.Size = 30 << 20 // announced size above the 25 MiB default
	f.receive("WA-BIG", big)
	m, _ := f.delivered(1)
	if m.Status != events.MediaRejected || m.Reason != "too_large" || m.Size != 30<<20 {
		t.Errorf("too large: %+v", m)
	}

	f.receive("WA-EXE", attachment("document", "application/x-msdownload", "setup.exe", []byte("MZ"), nil))
	if m, _ = f.delivered(2); m.Status != events.MediaRejected || m.Reason != "type_not_allowed" {
		t.Errorf("type: %+v", m)
	}

	// the provider itself refuses (the real size is above the limit even though it was not announced)
	f.receive("WA-BIG2", attachment("image", "image/jpeg", "a.jpg", []byte("x"), func(r *memory.FakeMediaRef) { r.Error = "too_large" }))
	if m, _ = f.delivered(3); m.Status != events.MediaRejected || m.Reason != "too_large" {
		t.Errorf("provider too large: %+v", m)
	}

	if n := f.e.Provider.MediaDownloadCalls(); n != 1 {
		t.Errorf("only the last one reached the provider (the first two are decided on what the sender announced): %d downloads", n)
	}
	if _, _, err := f.e.App.Media.Open(bg, f.e.Tenant, m.MediaID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("a refused attachment has nothing to download: %v", err)
	}
}

func TestInboundMedia_ALostAttachmentIsReportedNotRetriedForever(t *testing.T) {
	f := newMediaFixture(t)
	f.e.StartMedia()

	f.receive("WA-GONE", attachment("image", "image/jpeg", "a.jpg", []byte("x"), func(r *memory.FakeMediaRef) { r.Error = "missing" }))
	m, _ := f.delivered(1)
	if m.Status != events.MediaFailed || m.Reason != "expired" {
		t.Errorf("the provider no longer has it: %+v", m)
	}
	calls := f.e.Provider.MediaDownloadCalls()

	// the node answers but cannot fetch it: a couple of attempts, then FAILED
	f.receive("WA-BAD", attachment("image", "image/jpeg", "b.jpg", []byte("x"), func(r *memory.FakeMediaRef) { r.Error = "rejected" }))
	if m, _ = f.delivered(2); m.Status != events.MediaFailed || m.Reason != "download_failed" {
		t.Errorf("rejected: %+v", m)
	}
	if n := f.e.Provider.MediaDownloadCalls() - calls; n != 3 {
		t.Errorf("a rejecting node is tried 3 times, not forever: %d", n)
	}

	// a node that stays down: bounded by MaxAttempts
	calls = f.e.Provider.MediaDownloadCalls()
	f.receive("WA-DOWN", attachment("image", "image/jpeg", "c.jpg", []byte("x"), func(r *memory.FakeMediaRef) { r.Error = "unavailable" }))
	if m, _ = f.delivered(3); m.Status != events.MediaFailed || m.Reason != "download_failed" {
		t.Errorf("unavailable: %+v", m)
	}
	if n := f.e.Provider.MediaDownloadCalls() - calls; n != f.e.MediaIngest.MaxAttempts {
		t.Errorf("an unavailable node is tried MaxAttempts=%d times: %d", f.e.MediaIngest.MaxAttempts, n)
	}
}

func TestInboundMedia_ATransientProviderFailureIsRetried(t *testing.T) {
	f := newMediaFixture(t)
	f.e.StartMedia()
	f.receive("WA-FLAKY", attachment("document", "application/pdf", "doc.pdf", []byte("%PDF-1.7 ok"), func(r *memory.FakeMediaRef) { r.FailFirst = 2 }))
	m, _ := f.delivered(1)
	if m.Status != events.MediaReady {
		t.Fatalf("it recovers on the third attempt: %+v", m)
	}
	if m.Filename != "doc.pdf" {
		t.Errorf("filename: %+v", m)
	}
	if n := f.e.Provider.MediaDownloadCalls(); n != 3 {
		t.Errorf("downloads: %d", n)
	}
}

// A provider that retries the same webhook (it does, whenever our answer is slow or lost) must not produce a second job
// or a second delivery.
func TestInboundMedia_ProviderRetryOfTheSameWebhookIsIdempotent(t *testing.T) {
	f := newMediaFixture(t)
	f.e.StartMedia()
	m := attachment("image", "image/jpeg", "a.jpg", []byte("jpegbytes"), nil)
	for i := 0; i < 3; i++ {
		f.receive("WA-DUP", m)
	}
	first, _ := f.delivered(1)
	f.settle()
	if n := len(f.e.Receiver.Accepted(hookURL)); n != 1 {
		t.Fatalf("delivered %d times", n)
	}
	if n := f.e.Provider.MediaDownloadCalls(); n != 1 {
		t.Errorf("downloaded %d times", n)
	}
	if first.Status != events.MediaReady {
		t.Errorf("%+v", first)
	}
}

func TestInboundMedia_LimitsAreAdvertised(t *testing.T) {
	e := NewEnv(t)
	l := e.App.Limits()
	if l.Media.InboundMaxBytes != 25<<20 || l.Media.InboundTTLSeconds != 7*24*3600 {
		t.Errorf("limits: %+v", l.Media)
	}
}
