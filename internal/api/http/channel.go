package http

import (
	nethttp "net/http"
	"time"

	"github.com/relayplane/relayplane/internal/ports"
)

// sendPresence shows "typing…" or "recording audio…" to a contact. It answers 202 at once: the node keeps the state for
// duration_ms and then pauses by itself.
func (s *Server) sendPresence(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in struct {
		To         string `json:"to"`
		State      string `json:"state"`
		DurationMS int64  `json:"duration_ms"`
	}
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	if err := s.App.Channel.SendPresence(r.Context(), p.TenantID, r.PathValue("id"), in.To, ports.PresenceState(in.State),
		time.Duration(in.DurationMS)*time.Millisecond); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, nethttp.StatusAccepted, map[string]any{"accepted": true})
}

// markRead marks messages the contact sent as read.
func (s *Server) markRead(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in struct {
		InstanceID         string   `json:"instance_id"`
		Chat               string   `json:"chat"`
		ProviderMessageIDs []string `json:"provider_message_ids"`
	}
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	if err := s.App.Channel.MarkRead(r.Context(), p.TenantID, in.InstanceID, in.Chat, in.ProviderMessageIDs); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, map[string]any{"read": len(in.ProviderMessageIDs)})
}
