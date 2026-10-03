package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// SentMessage records a message that reached the (fake) provider.
type SentMessage struct {
	Assignment ownership.Assignment
	Message    messaging.OutboundMessage
	At         time.Time
}

// FakeProvider is a controllable in-memory MessagingProvider used by tests and
// as a second implementation for the ProviderContractSuite.
type FakeProvider struct {
	mu        sync.Mutex
	instances map[string]*fakeInst
	sent      []SentMessage
	nodes     map[string]bool // node id -> healthy
	failNext  []FailKind
	msgSeq    int

	// AutoPair makes freshly created instances come up CONNECTED.
	AutoPair bool
	// OnSend, when set, runs before a message is recorded (ordering/jitter tests).
	OnSend func(a ownership.Assignment, m messaging.OutboundMessage)
	// ConnectLeadsTo is the state a ConnectInstance call produces.
	ConnectLeadsTo instance.ObservedState
	// Caps are the advertised capabilities.
	Caps ports.ProviderCapabilities
	// BeforeCall, when set, runs (outside any lock) before a provider method executes;
	// tests use it to interleave other operations at an exact point.
	BeforeCall func(method string, a ownership.Assignment)

	calls []Call
}

// Call is one recorded provider call.
type Call struct {
	Method     string
	Assignment ownership.Assignment
}

// Calls returns every provider call so far, in order.
func (f *FakeProvider) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Call(nil), f.calls...)
}

func (f *FakeProvider) enter(method string, a ownership.Assignment) {
	f.mu.Lock()
	hook := f.BeforeCall
	f.mu.Unlock()
	if hook != nil {
		hook(method, a)
	}
	f.mu.Lock()
	f.calls = append(f.calls, Call{method, a})
	f.mu.Unlock()
}

type fakeInst struct {
	assignment ownership.Assignment
	state      instance.ObservedState
}

// FailKind selects an injected failure.
type FailKind string

const (
	FailUnavailable FailKind = "unavailable" // provider provably did not act
	FailAmbiguous   FailKind = "ambiguous"   // may have acted (send only)
	FailAuth        FailKind = "auth"
	FailNotFound    FailKind = "not_found"
)

// NewFakeProvider returns a provider with all capabilities enabled.
func NewFakeProvider() *FakeProvider {
	return &FakeProvider{
		instances:      map[string]*fakeInst{},
		nodes:          map[string]bool{},
		ConnectLeadsTo: instance.Connected,
		Caps: ports.ProviderCapabilities{QRCode: true, PairingCode: true, Groups: true, Media: true,
			Presence: true, Disconnect: true},
	}
}

// FailNext makes the next provider call (any method) fail with kind.
func (f *FakeProvider) FailNext(kinds ...FailKind) {
	f.mu.Lock()
	f.failNext = append(f.failNext, kinds...)
	f.mu.Unlock()
}

func (f *FakeProvider) injected(isSend bool) error {
	if len(f.failNext) == 0 {
		return nil
	}
	k := f.failNext[0]
	f.failNext = f.failNext[1:]
	switch k {
	case FailUnavailable:
		return fmt.Errorf("%w: injected", errs.ErrProviderUnavailable)
	case FailAmbiguous:
		return fmt.Errorf("%w: injected", errs.ErrAmbiguousDispatch)
	case FailAuth:
		return fmt.Errorf("%w: injected", errs.ErrAuthenticationFailed)
	case FailNotFound:
		return fmt.Errorf("%w: injected", errs.ErrInstanceNotFound)
	}
	return nil
}

// SetNodeHealthy controls ProbeNode results.
func (f *FakeProvider) SetNodeHealthy(node string, ok bool) {
	f.mu.Lock()
	f.nodes[node] = ok
	f.mu.Unlock()
}

// SetState forces the provider-side state of an instance (socket drops, pairing).
func (f *FakeProvider) SetState(instanceID string, st instance.ObservedState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, i := range f.instances {
		if i.assignment.InstanceID == instanceID {
			i.state = st
		}
	}
}

// SetStateOn sets the state of the session held by one specific node.
func (f *FakeProvider) SetStateOn(nodeID, instanceID string, st instance.ObservedState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i, ok := f.instances[key(nodeID, instanceID)]; ok {
		i.state = st
	}
}

// StateOn returns the state of the session held by a node ("" if none).
func (f *FakeProvider) StateOn(nodeID, instanceID string) instance.ObservedState {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i, ok := f.instances[key(nodeID, instanceID)]; ok {
		return i.state
	}
	return ""
}

func key(nodeID, instanceID string) string { return nodeID + "|" + instanceID }

// Has reports whether the provider holds the instance.
func (f *FakeProvider) Has(instanceID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, i := range f.instances {
		if i.assignment.InstanceID == instanceID {
			return true
		}
	}
	return false
}

// HasOn reports whether a specific node holds a session for the instance.
func (f *FakeProvider) HasOn(nodeID, instanceID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.instances[key(nodeID, instanceID)]
	return ok
}

// Forget removes an instance from the provider without any API call (simulates
// provider-side data loss or a crash before the create response was persisted).
func (f *FakeProvider) Forget(instanceID string) {
	f.mu.Lock()
	for k, i := range f.instances {
		if i.assignment.InstanceID == instanceID {
			delete(f.instances, k)
		}
	}
	f.mu.Unlock()
}

// Sent returns a copy of every message that reached the provider.
func (f *FakeProvider) Sent() []SentMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]SentMessage(nil), f.sent...)
}

func (f *FakeProvider) CreateInstance(_ context.Context, req ports.CreateInstanceRequest) (*ports.ProviderInstance, error) {
	f.enter("CreateInstance", req.Assignment)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(false); err != nil {
		return nil, err
	}
	a := req.Assignment
	if _, ok := f.instances[key(a.NodeID, a.InstanceID)]; ok {
		return nil, fmt.Errorf("%w: %s", errs.ErrInstanceAlreadyExists, a.InstanceID)
	}
	st := instance.AwaitingPairing
	if f.AutoPair {
		st = instance.Connected
	}
	f.instances[key(a.NodeID, a.InstanceID)] = &fakeInst{assignment: a, state: st}
	return &ports.ProviderInstance{ProviderInstanceID: "fake-" + a.InstanceID, State: st}, nil
}

func (f *FakeProvider) get(a ownership.Assignment) (*fakeInst, error) {
	i, ok := f.instances[key(a.NodeID, a.InstanceID)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errs.ErrInstanceNotFound, a.InstanceID)
	}
	if i.assignment.NodeID != a.NodeID || i.assignment.Epoch != a.Epoch {
		return nil, fmt.Errorf("%w: provider has epoch %d on %s", errs.ErrStaleAssignment, i.assignment.Epoch, i.assignment.NodeID)
	}
	return i, nil
}

func (f *FakeProvider) DeleteInstance(_ context.Context, a ownership.Assignment) error {
	f.enter("DeleteInstance", a)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(false); err != nil {
		return err
	}
	if _, err := f.get(a); err != nil {
		return err
	}
	delete(f.instances, key(a.NodeID, a.InstanceID))
	return nil
}

func (f *FakeProvider) GetInstanceState(_ context.Context, a ownership.Assignment) (*ports.InstanceState, error) {
	f.enter("GetInstanceState", a)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(false); err != nil {
		return nil, err
	}
	i, err := f.get(a)
	if err != nil {
		return nil, err
	}
	return &ports.InstanceState{State: i.state, Heartbeat: time.Now()}, nil
}

func (f *FakeProvider) GetPairingCode(_ context.Context, a ownership.Assignment) (*ports.PairingCode, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(false); err != nil {
		return nil, err
	}
	if !f.Caps.QRCode && !f.Caps.PairingCode {
		return nil, errs.ErrCapabilityMissing
	}
	i, err := f.get(a)
	if err != nil {
		return nil, err
	}
	if i.state != instance.AwaitingPairing {
		return nil, fmt.Errorf("%w: state %s", errs.ErrPairingUnavailable, i.state)
	}
	return &ports.PairingCode{QRCode: "qr:" + a.InstanceID, PairingCode: "ABCD-1234", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func (f *FakeProvider) SendMessage(_ context.Context, a ownership.Assignment, m messaging.OutboundMessage) (*ports.SendResult, error) {
	f.mu.Lock()
	if err := f.injected(true); err != nil {
		f.mu.Unlock()
		return nil, err
	}
	i, err := f.get(a)
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	if i.state != instance.Connected {
		f.mu.Unlock()
		return nil, fmt.Errorf("%w: instance not connected (%s)", errs.ErrProviderUnavailable, i.state)
	}
	hook := f.OnSend
	f.mu.Unlock()
	if hook != nil {
		hook(a, m) // outside the lock so jitter in tests does not serialise calls
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgSeq++
	f.sent = append(f.sent, SentMessage{Assignment: a, Message: m, At: time.Now()})
	return &ports.SendResult{ProviderMessageID: fmt.Sprintf("pm-%d", f.msgSeq), Status: messaging.StatusAccepted}, nil
}

func (f *FakeProvider) Capabilities(context.Context) ports.ProviderCapabilities { return f.Caps }

func (f *FakeProvider) ConnectInstance(_ context.Context, a ownership.Assignment) error {
	f.enter("ConnectInstance", a)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(false); err != nil {
		return err
	}
	i, err := f.get(a)
	if err != nil {
		return err
	}
	if i.state == instance.AwaitingPairing || i.state == instance.LoggedOut {
		return fmt.Errorf("%w: needs pairing", errs.ErrPairingUnavailable)
	}
	i.state = f.ConnectLeadsTo
	return nil
}

func (f *FakeProvider) Disconnect(_ context.Context, a ownership.Assignment) error {
	f.enter("Disconnect", a)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.injected(false); err != nil {
		return err
	}
	i, err := f.get(a)
	if err != nil {
		return err
	}
	i.state = instance.LoggedOut
	return nil
}

func (f *FakeProvider) ProbeNode(_ context.Context, nodeID string) (*ports.NodeProbe, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ok, set := f.nodes[nodeID]; set && !ok {
		return nil, fmt.Errorf("%w: node %s down", errs.ErrProviderUnavailable, nodeID)
	}
	return &ports.NodeProbe{Ready: true, Version: "fake-1"}, nil
}
