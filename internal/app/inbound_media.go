package app

import (
	"strings"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/ports"
)

// MediaPending is the status carried by the event held in the queue until the attachment is resolved. It is internal:
// no tenant ever receives it.
const MediaPending = "PENDING"

// admitMedia decides what happens to the attachment of an inbound message and rewrites the event's media description:
//
//   - the provider cannot hand over bytes, the sender's declared size is above the limit, or the declared type is not
//     allowed: the attachment is REJECTED right away (no download) and the event is delivered as is;
//   - otherwise a download job is returned and the event must NOT be published yet: the ingestor publishes it once the
//     bytes are stored (READY) or the attempt is over (FAILED).
func (s *InboundService) admitMedia(inst *instance.Instance, ev *events.Event, in events.Inbound) *media.InboundJob {
	pl, ok := ev.Payload.(events.MessageReceivedPayload)
	if !ok || pl.Media == nil || in.Media == nil {
		return nil
	}
	m := *pl.Media
	m.MediaID = "med_" + strings.TrimPrefix(ev.EventID, "evt_")
	reject := func(reason string) *media.InboundJob {
		m.Status, m.Reason = events.MediaRejected, reason
		pl.Media = &m
		ev.Payload = pl
		s.d.Metrics.InboundMedia.WithLabelValues(reason).Inc()
		return nil
	}
	max := s.d.Cfg.EffectiveInboundMaxBytes()
	if max == 0 || !s.canDownload(inst) {
		return reject("unsupported")
	}
	if m.Size > max {
		return reject("too_large")
	}
	if m.MimeType != "" && !s.d.Cfg.MediaPolicy.TypeAllowed(m.MimeType) {
		return reject("type_not_allowed")
	}
	m.Status = MediaPending
	pl.Media = &m
	ev.Payload = pl
	return &media.InboundJob{ID: m.MediaID, TenantID: inst.TenantID, InstanceID: inst.ID, EventID: ev.EventID, Event: *ev, Ref: in.Media.Ref,
		CreatedAt: s.d.now().UTC()}
}

func (s *InboundService) canDownload(inst *instance.Instance) bool {
	p, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return false
	}
	_, ok := p.(ports.MediaDownloader)
	return ok
}
