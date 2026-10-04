package memory

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// FakeMediaRef is the self-describing download reference FakeWebhook hands over for an inbound attachment: the test says
// in it what the provider will do when asked for the bytes.
type FakeMediaRef struct {
	ContentB64 string `json:"content_b64"`
	Mime       string `json:"mime"`
	Filename   string `json:"filename"`
	// Error makes every download fail as: too_large | missing | rejected | unavailable.
	Error string `json:"error,omitempty"`
	// FailFirst makes the first N downloads of this reference fail as unavailable (a provider that recovers).
	FailFirst int `json:"fail_first,omitempty"`
	// Size is what the reader reports (default: len of the content).
	Size int64 `json:"size,omitempty"`
}

// NewFakeMediaRef encodes a reference for a test attachment.
func NewFakeMediaRef(content []byte, mime, filename string) FakeMediaRef {
	return FakeMediaRef{ContentB64: base64.StdEncoding.EncodeToString(content), Mime: mime, Filename: filename}
}

// JSON renders the reference.
func (r FakeMediaRef) JSON() json.RawMessage { b, _ := json.Marshal(r); return b }

var _ ports.MediaDownloader = (*FakeProvider)(nil)

// DownloadMedia serves the attachment described by a FakeMediaRef.
func (f *FakeProvider) DownloadMedia(_ context.Context, a ownership.Assignment, ref json.RawMessage, maxBytes int64) (*ports.DownloadedMedia, error) {
	f.enter("DownloadMedia", a)
	var r FakeMediaRef
	if err := json.Unmarshal(ref, &r); err != nil {
		return nil, fmt.Errorf("%w: unusable media reference", errs.ErrProviderRejected)
	}
	f.mu.Lock()
	if f.mediaFailures == nil {
		f.mediaFailures = map[string]int{}
	}
	f.mediaFailures[string(ref)]++
	n := f.mediaFailures[string(ref)]
	f.mu.Unlock()
	if n <= r.FailFirst {
		return nil, fmt.Errorf("%w: media service restarting", errs.ErrProviderUnavailable)
	}
	switch r.Error {
	case "too_large":
		return nil, fmt.Errorf("%w: too large", errs.ErrPayloadTooLarge)
	case "missing":
		return nil, fmt.Errorf("%w: the attachment is gone", errs.ErrNotFound)
	case "rejected":
		return nil, fmt.Errorf("%w: cannot fetch", errs.ErrProviderRejected)
	case "unavailable":
		return nil, fmt.Errorf("%w: down", errs.ErrProviderUnavailable)
	}
	raw, err := base64.StdEncoding.DecodeString(r.ContentB64)
	if err != nil {
		return nil, fmt.Errorf("%w: bad content", errs.ErrProviderRejected)
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("%w: too large", errs.ErrPayloadTooLarge)
	}
	size := r.Size
	if size == 0 {
		size = int64(len(raw))
	}
	return &ports.DownloadedMedia{Body: io.NopCloser(bytes.NewReader(raw)), Size: size, ContentType: r.Mime, Filename: r.Filename}, nil
}

// MediaDownloadCalls is how many downloads were attempted (tests).
func (f *FakeProvider) MediaDownloadCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.Method == "DownloadMedia" {
			n++
		}
	}
	return n
}
