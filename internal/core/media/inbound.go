package media

import (
	"encoding/json"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// InboundStage is where an inbound attachment is in its journey to the tenant.
type InboundStage string

const (
	// StageDownload: the bytes still have to be fetched from the provider and stored.
	StageDownload InboundStage = "DOWNLOAD"
	// StagePublish: the outcome is decided and recorded in Event; the event still has to reach the bus.
	StagePublish InboundStage = "PUBLISH"
	// StageDone: published.
	StageDone InboundStage = "DONE"
)

// InboundJob is one inbound message whose attachment is being resolved. Event is the message.received event as it will
// be published (the media status is rewritten when the outcome is known), so a crash at any point only repeats a step:
// event ids are deterministic and every step is idempotent. The event is not delivered before the media outcome is
// known, so a consumer never sees a half-resolved attachment.
type InboundJob struct {
	ID            string // the media id the tenant will use
	TenantID      string
	InstanceID    string
	EventID       string
	Event         events.Event
	Ref           json.RawMessage // provider-specific download reference (may hold decryption keys)
	Stage         InboundStage
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	DoneAt        time.Time
}
