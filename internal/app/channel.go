package app

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/ports"
)

// ChannelService is what a conversation needs besides sending text: showing "typing…" and marking messages as read.
// Both act on a CONNECTED instance, go straight to the provider (they are ephemeral and have no ordering against the
// message queue) and are refused when the provider cannot do them.
type ChannelService struct {
	d Deps

	mu       sync.Mutex
	inFlight map[string]int // presence calls running per instance
}

const (
	// DefaultPresence is how long "typing…" is shown when the caller does not say.
	DefaultPresence = 3 * time.Second
	// MaxPresence bounds one presence call: the node keeps the call open for that long.
	MaxPresence = 25 * time.Second
	// MaxPresencePerInstance bounds the presence calls running at once for one instance.
	MaxPresencePerInstance = 4
	// MaxReadBatch bounds the messages marked as read by one request.
	MaxReadBatch = 50
)

func (s *ChannelService) connected(ctx context.Context, tenantID, instanceID string) (*instance.Instance, error) {
	inst, err := s.d.loadForTenant(ctx, tenantID, instanceID)
	if err != nil {
		return nil, err
	}
	if inst.DesiredState == instance.DesiredDeleted || inst.NodeID == "" {
		return nil, fmt.Errorf("%w: the instance cannot do this (it is being removed)", errs.ErrConflict)
	}
	if inst.ObservedState != instance.Connected {
		return nil, fmt.Errorf("%w: the instance is %s, not CONNECTED", errs.ErrConflict, inst.ObservedState)
	}
	return inst, nil
}

// SendPresence shows `state` to the contact for `duration` (the provider then pauses by itself) and returns at once: the
// call to the node takes about `duration`, and a typing indicator is best effort, so it runs in the background and a
// failure is logged and counted, not reported to the caller.
func (s *ChannelService) SendPresence(ctx context.Context, tenantID, instanceID, to string, state ports.PresenceState, duration time.Duration) error {
	if !validRecipient(to) {
		return fmt.Errorf("%w: invalid recipient", errs.ErrInvalidArgument)
	}
	if !state.Valid() {
		return fmt.Errorf("%w: state must be composing, recording or paused", errs.ErrInvalidArgument)
	}
	switch {
	case state == ports.PresencePaused:
		duration = 0
	case duration <= 0:
		duration = DefaultPresence
	case duration > MaxPresence:
		return fmt.Errorf("%w: duration_ms is at most %d", errs.ErrInvalidArgument, MaxPresence.Milliseconds())
	}
	inst, err := s.connected(ctx, tenantID, instanceID)
	if err != nil {
		return err
	}
	prov, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return err
	}
	sender, ok := prov.(ports.PresenceSender)
	if !ok || !prov.Capabilities(ctx).Presence {
		return fmt.Errorf("%w: this provider cannot show typing", errs.ErrCapabilityMissing)
	}
	s.mu.Lock()
	if s.inFlight == nil {
		s.inFlight = map[string]int{}
	}
	if s.inFlight[inst.ID] >= MaxPresencePerInstance {
		s.mu.Unlock()
		return fmt.Errorf("%w: too many typing indicators running for this instance", errs.ErrRateLimited)
	}
	s.inFlight[inst.ID]++
	s.mu.Unlock()

	a := inst.Assignment()
	go func() {
		defer func() { s.mu.Lock(); s.inFlight[inst.ID]--; s.mu.Unlock() }()
		cctx, cancel := context.WithTimeout(context.Background(), duration+MaxPresence)
		defer cancel()
		if err := sender.SendPresence(cctx, a, to, state, duration); err != nil {
			s.d.Log.WarnContext(cctx, "typing indicator failed", "instance_id", inst.ID, "state", state, "error", err)
		}
	}()
	return nil
}

// MarkRead marks messages the chat sent as read (the contact sees the blue ticks). Synchronous: it is quick and the caller
// wants to know it worked.
func (s *ChannelService) MarkRead(ctx context.Context, tenantID, instanceID, chat string, providerMessageIDs []string) error {
	switch {
	case !validRecipient(chat):
		return fmt.Errorf("%w: invalid chat", errs.ErrInvalidArgument)
	case len(providerMessageIDs) == 0 || len(providerMessageIDs) > MaxReadBatch:
		return fmt.Errorf("%w: give between 1 and %d provider message ids", errs.ErrInvalidArgument, MaxReadBatch)
	}
	for _, id := range providerMessageIDs {
		if id == "" || len(id) > 128 {
			return fmt.Errorf("%w: invalid provider message id", errs.ErrInvalidArgument)
		}
	}
	inst, err := s.connected(ctx, tenantID, instanceID)
	if err != nil {
		return err
	}
	prov, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return err
	}
	marker, ok := prov.(ports.ReadMarker)
	if !ok {
		return fmt.Errorf("%w: this provider cannot mark messages as read", errs.ErrCapabilityMissing)
	}
	return marker.MarkRead(ctx, inst.Assignment(), chat, providerMessageIDs)
}
