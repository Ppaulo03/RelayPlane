package http

import (
	"errors"
	"io"
	nethttp "net/http"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/ports"
)

type principalFn = func(nethttp.ResponseWriter, *nethttp.Request, Principal)

// ---- views (public JSON shapes; no provider, node or epoch details) ----

type instanceView struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Status           string    `json:"status"`
	DesiredState     string    `json:"desired_state"`
	ObservedState    string    `json:"observed_state"`
	LastStatusChange time.Time `json:"last_status_change"`
	CreatedAt        time.Time `json:"created_at"`
}

func viewInstance(i instance.Instance) instanceView {
	return instanceView{ID: i.ID, Name: i.Name, Status: string(i.ObservedState), DesiredState: string(i.DesiredState),
		ObservedState: string(i.ObservedState), LastStatusChange: i.LastStatusChange, CreatedAt: i.CreatedAt}
}

type operationView struct {
	ID           string     `json:"id"`
	Type         string     `json:"type"`
	Status       string     `json:"status"`
	Step         string     `json:"step,omitempty"`
	InstanceID   string     `json:"instance_id,omitempty"`
	ErrorCode    string     `json:"error_code,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

func viewOperation(o instance.Operation) operationView {
	return operationView{ID: o.ID, Type: string(o.Type), Status: string(o.Status), Step: o.Step, InstanceID: o.InstanceID,
		ErrorCode: o.ErrorCode, ErrorMessage: o.ErrorMessage, CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt, CompletedAt: o.CompletedAt}
}

type messageView struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	To        string    `json:"to"`
	Type      string    `json:"type"`
	Attempts  int       `json:"attempts"`
	Sequence  int64     `json:"sequence_no"`
	ErrorCode string    `json:"error_code,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func viewMessage(m messaging.Message) messageView {
	return messageView{ID: m.ID, Status: string(m.Status), To: m.Recipient, Type: string(m.Type), Attempts: m.AttemptCount, Sequence: m.SequenceNo,
		ErrorCode: m.ErrorCode, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
}

func idemKey(r *nethttp.Request) string { return r.Header.Get("Idempotency-Key") }

func replayed(w nethttp.ResponseWriter, was bool) {
	if was {
		w.Header().Set("Idempotent-Replayed", "true")
	}
}

// ---- instances ----

func (s *Server) createInstance(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in app.CreateInstanceInput
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	res, was, err := s.App.Instances.Create(r.Context(), p.TenantID, in, idemKey(r))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	replayed(w, was)
	code := nethttp.StatusCreated
	if res.Status == instance.Creating || res.Status == instance.Allocating {
		code = nethttp.StatusAccepted
	}
	w.Header().Set("Location", "/api/v1/instances/"+res.ID)
	writeJSON(w, code, res)
}

func (s *Server) listInstances(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	list, err := s.App.Instances.List(r.Context(), p.TenantID)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	out := make([]instanceView, 0, len(list))
	for _, i := range list {
		out = append(out, viewInstance(i))
	}
	writeJSON(w, 200, map[string]any{"instances": out})
}

func (s *Server) getInstance(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	i, err := s.App.Instances.Get(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewInstance(*i))
}

func (s *Server) deleteInstance(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	res, was, err := s.App.Instances.Delete(r.Context(), p.TenantID, r.PathValue("id"), idemKey(r))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	replayed(w, was)
	writeJSON(w, nethttp.StatusAccepted, res)
}

func (s *Server) pairing(kind app.PairingKind) principalFn {
	return func(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
		pc, err := s.App.Instances.Pairing(r.Context(), p.TenantID, r.PathValue("id"), kind)
		if err != nil {
			writeError(w, r, s.Log, err)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		body := map[string]any{"expires_at": pc.ExpiresAt}
		if kind == app.PairingQR {
			body["qrcode"] = pc.QRCode
		}
		if pc.PairingCode != "" {
			body["pairing_code"] = pc.PairingCode
		}
		writeJSON(w, 200, body)
	}
}

func (s *Server) reconnect(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	res, err := s.App.Instances.Reconnect(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, res)
}

func (s *Server) logout(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	res, err := s.App.Instances.Logout(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, res)
}

func (s *Server) migrate(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in app.MigrateInput
	if r.ContentLength != 0 {
		if err := decode(r, &in); err != nil {
			writeError(w, r, s.Log, err)
			return
		}
	}
	res, was, err := s.App.Migrations.Start(r.Context(), p.TenantID, r.PathValue("id"), in, idemKey(r))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	replayed(w, was)
	writeJSON(w, nethttp.StatusAccepted, res)
}

type ratePolicyBody struct {
	MinIntervalMs int `json:"min_interval_ms"`
	Burst         int `json:"burst"`
	MaxPerMinute  int `json:"max_per_minute"`
	MaxConcurrent int `json:"max_concurrent"`
	CooldownMs    int `json:"cooldown_ms"`
}

func (b ratePolicyBody) policy() (*messaging.RatePolicy, error) {
	if b.MinIntervalMs < 0 || b.Burst < 0 || b.MaxPerMinute < 0 || b.MaxConcurrent < 0 || b.CooldownMs < 0 {
		return nil, errs.Wrap(errs.ErrInvalidArgument, "rate policy values cannot be negative")
	}
	return &messaging.RatePolicy{MinInterval: time.Duration(b.MinIntervalMs) * time.Millisecond, Burst: b.Burst,
		MaxPerMinute: b.MaxPerMinute, MaxConcurrent: b.MaxConcurrent, Cooldown: time.Duration(b.CooldownMs) * time.Millisecond}, nil
}

func (s *Server) setInstanceRatePolicy(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var b ratePolicyBody
	if err := decode(r, &b); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	pol, err := b.policy()
	if err == nil {
		var inst *instance.Instance
		if inst, err = s.App.Instances.Get(r.Context(), p.TenantID, r.PathValue("id")); err == nil {
			err = s.App.Deps.Repos.Instances.SetRatePolicy(r.Context(), inst.ID, pol)
		}
	}
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, b)
}

func (s *Server) setTenantRatePolicy(w nethttp.ResponseWriter, r *nethttp.Request, _ Principal) {
	var b ratePolicyBody
	if err := decode(r, &b); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	pol, err := b.policy()
	if err == nil {
		err = s.App.Deps.Repos.Tenants.SetRatePolicy(r.Context(), r.PathValue("id"), pol)
	}
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, b)
}

// ---- messages & operations ----

func (s *Server) sendMessage(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in app.SendInput
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	res, was, err := s.App.Messages.Send(r.Context(), p.TenantID, in, idemKey(r))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	replayed(w, was)
	writeJSON(w, nethttp.StatusAccepted, res)
}

func (s *Server) getMessage(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	m, err := s.App.Messages.Get(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewMessage(*m))
}

func (s *Server) resolveMessage(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in struct {
		Outcome string `json:"outcome"`
	}
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	m, err := s.App.Messages.Resolve(r.Context(), p.TenantID, r.PathValue("id"), app.ResolveOutcome(in.Outcome))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewMessage(*m))
}

func (s *Server) getOperation(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	o, err := s.App.Instances.GetOperation(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewOperation(*o))
}

// ---- media (claim check) ----

type mediaView struct {
	ID          string    `json:"id"`
	Status      string    `json:"status"`
	ContentType string    `json:"content_type"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	Filename    string    `json:"filename"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func viewMedia(b media.Blob) mediaView {
	return mediaView{ID: b.ID, Status: string(b.Status), ContentType: b.ContentType, Size: b.Size, SHA256: b.SHA256, Filename: b.Filename, ExpiresAt: b.ExpiresAt}
}

func (s *Server) createUpload(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in app.UploadRequest
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	t, err := s.App.Media.CreateUpload(r.Context(), p.TenantID, in)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 201, t)
}

func (s *Server) uploadContent(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	max := s.MaxUpload
	if max <= 0 {
		max = 100 << 20
	}
	b, err := s.App.Media.Upload(r.Context(), p.TenantID, r.PathValue("id"), io.LimitReader(r.Body, max+1))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewMedia(*b))
}

func (s *Server) getMedia(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	b, err := s.App.Media.Get(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewMedia(*b))
}

func (s *Server) deleteMedia(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	if err := s.App.Media.Delete(r.Context(), p.TenantID, r.PathValue("id")); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.WriteHeader(nethttp.StatusNoContent)
}

// ---- administration ----

type nodeView struct {
	ID              string     `json:"id"`
	Provider        string     `json:"provider"`
	ProviderVersion string     `json:"provider_version"`
	Capacity        int        `json:"capacity"`
	ActiveInstances int        `json:"active_instances"`
	Status          string     `json:"status"`
	HeartbeatAt     *time.Time `json:"heartbeat_at,omitempty"`
}

func viewNode(n routing.Node) nodeView {
	v := nodeView{ID: n.ID, Provider: n.Provider, ProviderVersion: n.ProviderVersion, Capacity: n.Capacity,
		ActiveInstances: n.ActiveInstances, Status: string(n.Status)}
	if !n.HeartbeatAt.IsZero() {
		v.HeartbeatAt = &n.HeartbeatAt
	}
	return v
}

func (s *Server) listNodes(w nethttp.ResponseWriter, r *nethttp.Request, _ Principal) {
	nodes, err := s.App.Nodes.List(r.Context())
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	out := make([]nodeView, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, viewNode(n))
	}
	writeJSON(w, 200, map[string]any{"nodes": out})
}

func (s *Server) drainNode(w nethttp.ResponseWriter, r *nethttp.Request, _ Principal) {
	n, err := s.App.Nodes.Drain(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewNode(*n))
}

func (s *Server) resumeNode(w nethttp.ResponseWriter, r *nethttp.Request, _ Principal) {
	n, err := s.App.Nodes.Resume(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewNode(*n))
}

func (s *Server) createTenant(w nethttp.ResponseWriter, r *nethttp.Request, _ Principal) {
	var in struct {
		Name string `json:"name"`
	}
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	t, key, err := s.App.Tenants.Create(r.Context(), in.Name)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 201, map[string]any{"id": t.ID, "name": t.Name, "api_key": key})
}

// ---- webhooks ----

func (s *Server) webhook(w nethttp.ResponseWriter, r *nethttp.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeError(w, r, s.Log, errs.Wrap(errs.ErrInvalidArgument, "unreadable body"))
		return
	}
	res, err := s.App.Inbound.Handle(r.Context(), r.PathValue("provider"),
		ports.InboundRequest{Header: r.Header, Query: r.URL.Query(), Body: body})
	if err != nil {
		switch {
		case errors.Is(err, errs.ErrOwnershipViolation):
			writeJSON(w, nethttp.StatusConflict, errorBody{apiError{Code: "ownership_violation", Message: "sender does not own this instance"}})
		case errors.Is(err, errs.ErrUnauthenticated):
			writeJSON(w, nethttp.StatusUnauthorized, errorBody{apiError{Code: "unauthenticated", Message: "invalid webhook credentials"}})
		case errors.Is(err, errs.ErrNotFound):
			writeJSON(w, nethttp.StatusNotFound, errorBody{apiError{Code: "not_found", Message: "unknown provider"}})
		case errors.Is(err, errs.ErrInvalidArgument):
			writeJSON(w, nethttp.StatusBadRequest, errorBody{apiError{Code: "invalid_argument", Message: "malformed webhook payload"}})
		default:
			s.Log.ErrorContext(r.Context(), "webhook failed", "error", err)
			writeJSON(w, nethttp.StatusServiceUnavailable, errorBody{apiError{Code: "unavailable", Message: "event could not be accepted; retry"}})
		}
		return
	}
	writeJSON(w, 200, res)
}
