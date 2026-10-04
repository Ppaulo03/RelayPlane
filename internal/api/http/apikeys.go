package http

import (
	nethttp "net/http"
	"time"

	"github.com/relayplane/relayplane/internal/core/instance"
)

type apiKeyView struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	// Current marks the key that authenticated this request (listings only).
	Current bool `json:"current,omitempty"`
}

func viewAPIKey(k instance.APIKey) apiKeyView {
	v := apiKeyView{ID: k.ID, Name: k.Name, Prefix: k.Prefix, CreatedAt: k.CreatedAt}
	if !k.ExpiresAt.IsZero() {
		t := k.ExpiresAt
		v.ExpiresAt = &t
	}
	if !k.LastUsedAt.IsZero() {
		t := k.LastUsedAt
		v.LastUsedAt = &t
	}
	if !k.RevokedAt.IsZero() {
		t := k.RevokedAt
		v.RevokedAt = &t
	}
	return v
}

type createKeyInput struct {
	Name             string `json:"name"`
	ExpiresInSeconds int64  `json:"expires_in_seconds"`
}

func (s *Server) issueAPIKey(w nethttp.ResponseWriter, r *nethttp.Request, tenantID string) {
	var in createKeyInput
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	k, secret, err := s.App.APIKeys.Create(r.Context(), tenantID, in.Name, time.Duration(in.ExpiresInSeconds)*time.Second)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	out := struct {
		apiKeyView
		APIKey string `json:"api_key"`
	}{viewAPIKey(*k), secret}
	writeJSON(w, 201, out)
}

// createAPIKey issues another key for the caller's tenant: rotation is "create, roll out, revoke the old one".
func (s *Server) createAPIKey(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	s.issueAPIKey(w, r, p.TenantID)
}

// createTenantAPIKey is the administrator's way back in for a tenant that lost its keys.
func (s *Server) createTenantAPIKey(w nethttp.ResponseWriter, r *nethttp.Request, _ Principal) {
	id := r.PathValue("id")
	if _, err := s.App.Tenants.Get(r.Context(), id); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	s.issueAPIKey(w, r, id)
}

func (s *Server) listAPIKeys(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	ks, err := s.App.APIKeys.List(r.Context(), p.TenantID)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	out := make([]apiKeyView, 0, len(ks))
	for _, k := range ks {
		v := viewAPIKey(k)
		v.Current = k.ID == p.KeyID
		out = append(out, v)
	}
	writeJSON(w, 200, map[string]any{"api_keys": out})
}

func (s *Server) revokeAPIKey(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	if err := s.App.APIKeys.Revoke(r.Context(), p.TenantID, r.PathValue("id")); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.WriteHeader(nethttp.StatusNoContent)
}
