package http

import (
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

type subscriptionView struct {
	ID            string    `json:"id"`
	URL           string    `json:"url"`
	EventTypes    []string  `json:"event_types"`
	InstanceIDs   []string  `json:"instance_ids"`
	ExcludeGroups bool      `json:"exclude_groups"`
	Active        bool      `json:"active"`
	CreatedAt     time.Time `json:"created_at"`
	// Secret is the signing secret. It is returned only when the subscription is created or its secret rotated.
	Secret string `json:"secret,omitempty"`
	// PreviousSecretValidUntil is when the previous secret stops being used to sign (rotation only).
	PreviousSecretValidUntil *time.Time `json:"previous_secret_valid_until,omitempty"`
}

func viewSubscription(s subscription.Subscription) subscriptionView {
	v := subscriptionView{ID: s.ID, URL: s.URL, EventTypes: []string{}, InstanceIDs: s.InstanceIDs, ExcludeGroups: s.ExcludeGroups, Active: s.Active, CreatedAt: s.CreatedAt}
	for _, t := range s.EventTypes {
		v.EventTypes = append(v.EventTypes, string(t))
	}
	if v.InstanceIDs == nil {
		v.InstanceIDs = []string{}
	}
	return v
}

type deliveryView struct {
	ID            string     `json:"id"`
	EventID       string     `json:"event_id"`
	EventType     string     `json:"event_type"`
	InstanceID    string     `json:"instance_id"`
	Sequence      int64      `json:"sequence"`
	Status        string     `json:"status"`
	Attempts      int        `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LastError     string     `json:"last_error,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	DeliveredAt   *time.Time `json:"delivered_at,omitempty"`
}

func viewDelivery(d subscription.Delivery) deliveryView {
	v := deliveryView{ID: d.ID, EventID: d.EventID, EventType: string(d.EventType), InstanceID: d.InstanceID, Sequence: d.Sequence, Status: string(d.Status),
		Attempts: d.Attempts, NextAttemptAt: d.NextAttemptAt, LastError: d.LastError, CreatedAt: d.CreatedAt}
	if !d.DeliveredAt.IsZero() {
		t := d.DeliveredAt
		v.DeliveredAt = &t
	}
	return v
}

func (s *Server) createSubscription(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	var in struct {
		URL           string   `json:"url"`
		EventTypes    []string `json:"event_types"`
		InstanceIDs   []string `json:"instance_ids"`
		ExcludeGroups bool     `json:"exclude_groups"`
	}
	if err := decode(r, &in); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	sv, was, err := s.App.Subscriptions.Create(r.Context(), p.TenantID, app.CreateSubscriptionInput{URL: in.URL, EventTypes: in.EventTypes, InstanceIDs: in.InstanceIDs,
		ExcludeGroups: in.ExcludeGroups}, idemKey(r))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	v := viewSubscription(sv.Subscription)
	v.Secret = sv.Secret // empty on a replay
	replayed(w, was)
	if was {
		writeJSON(w, 200, v)
		return
	}
	writeJSON(w, nethttp.StatusCreated, v)
}

func (s *Server) listSubscriptions(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	subs, err := s.App.Subscriptions.List(r.Context(), p.TenantID)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	out := make([]subscriptionView, 0, len(subs))
	for _, sub := range subs {
		out = append(out, viewSubscription(sub))
	}
	writeJSON(w, 200, map[string]any{"subscriptions": out})
}

func (s *Server) getSubscription(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	sub, err := s.App.Subscriptions.Get(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, 200, viewSubscription(*sub))
}

func (s *Server) deleteSubscription(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	if err := s.App.Subscriptions.Delete(r.Context(), p.TenantID, r.PathValue("id")); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.WriteHeader(nethttp.StatusNoContent)
}

func (s *Server) rotateSubscriptionSecret(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	sv, err := s.App.Subscriptions.RotateSecret(r.Context(), p.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	v := viewSubscription(sv.Subscription)
	v.Secret = sv.Secret
	until := sv.RotatedAt.Add(subscription.RotationGrace)
	v.PreviousSecretValidUntil = &until
	writeJSON(w, 200, v)
}

func (s *Server) listDeliveries(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	ds, err := s.App.Subscriptions.Deliveries(r.Context(), p.TenantID, r.PathValue("id"), subscription.DeliveryStatus(r.URL.Query().Get("status")), limit)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	out := make([]deliveryView, 0, len(ds))
	for _, d := range ds {
		out = append(out, viewDelivery(d))
	}
	writeJSON(w, 200, map[string]any{"deliveries": out})
}

func (s *Server) redeliver(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	if err := s.App.Subscriptions.Redeliver(r.Context(), p.TenantID, r.PathValue("id")); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.WriteHeader(nethttp.StatusAccepted)
}
