package contracttest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// Failure is a provider-agnostic failure the harness knows how to provoke
// using the provider's *native* failure modes.
type Failure string

const (
	Unavailable Failure = "unavailable" // connection refused / 503: provably not executed
	Ambiguous   Failure = "ambiguous"   // timeout/500 after the request may have been executed
	AuthFailed  Failure = "auth"        // 401/403
	NotFound    Failure = "not_found"   // 404
)

// ProviderHarness adapts a concrete provider (and its fake backend) to the suite.
type ProviderHarness struct {
	Provider ports.MessagingProvider
	NodeID   string
	// Inject arranges for the NEXT provider call to fail with f.
	Inject func(f Failure)
	// Pair simulates the user scanning the QR code: the instance becomes CONNECTED.
	Pair func(a ownership.Assignment)
	// Drop simulates the WhatsApp socket dying while the node HTTP API stays healthy.
	Drop func(a ownership.Assignment)
	// RejectsStale is true when the provider itself can refuse a stale assignment.
	RejectsStale bool
}

var providerSeq atomic.Int64

// ProviderContractSuite is the common behavioural suite every
// MessagingProvider implementation must pass. newHarness must return a fresh
// harness (fresh fake backend) per call.
func ProviderContractSuite(t *testing.T, newHarness func(t *testing.T) ProviderHarness) {
	newInst := func(t *testing.T) (ProviderHarness, ownership.Assignment, context.Context) {
		h := newHarness(t)
		a := ownership.Assignment{InstanceID: fmt.Sprintf("inst_contract_%d", providerSeq.Add(1)), NodeID: h.NodeID, Epoch: 1}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		t.Cleanup(cancel)
		return h, a, ctx
	}
	create := func(t *testing.T, h ProviderHarness, a ownership.Assignment, ctx context.Context) {
		t.Helper()
		pi, err := h.Provider.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a, TenantID: "t1", Name: "contract"})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if pi.ProviderInstanceID == "" || !pi.State.Valid() {
			t.Fatalf("create returned %+v", pi)
		}
	}
	connected := func(t *testing.T, h ProviderHarness, a ownership.Assignment, ctx context.Context) {
		t.Helper()
		create(t, h, a, ctx)
		h.Pair(a)
		st, err := h.Provider.GetInstanceState(ctx, a)
		if err != nil || st.State != instance.Connected {
			t.Fatalf("expected CONNECTED after pairing, got %+v %v", st, err)
		}
	}

	t.Run("CreateAndGetState", func(t *testing.T) {
		h, a, ctx := newInst(t)
		create(t, h, a, ctx)
		st, err := h.Provider.GetInstanceState(ctx, a)
		if err != nil || !st.State.Valid() {
			t.Fatalf("state: %+v %v", st, err)
		}
		if st.State == instance.Connected {
			t.Errorf("a brand-new instance cannot already be CONNECTED")
		}
	})

	t.Run("CreateTwiceIsAlreadyExists", func(t *testing.T) {
		h, a, ctx := newInst(t)
		create(t, h, a, ctx)
		_, err := h.Provider.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a, TenantID: "t1", Name: "contract"})
		if !errors.Is(err, errs.ErrInstanceAlreadyExists) {
			t.Fatalf("want ErrInstanceAlreadyExists (needed to adopt after a crash), got %v", err)
		}
	})

	t.Run("UnknownInstanceIsNotFound", func(t *testing.T) {
		h, a, ctx := newInst(t)
		if _, err := h.Provider.GetInstanceState(ctx, a); !errors.Is(err, errs.ErrInstanceNotFound) {
			t.Errorf("get: %v", err)
		}
		if err := h.Provider.DeleteInstance(ctx, a); !errors.Is(err, errs.ErrInstanceNotFound) {
			t.Errorf("delete: %v", err)
		}
	})

	t.Run("PairingMaterial", func(t *testing.T) {
		h, a, ctx := newInst(t)
		create(t, h, a, ctx)
		caps := h.Provider.Capabilities(ctx)
		pc, err := h.Provider.GetPairingCode(ctx, a)
		if !caps.QRCode && !caps.PairingCode {
			if !errors.Is(err, errs.ErrCapabilityMissing) {
				t.Errorf("without capabilities want ErrCapabilityMissing, got %v", err)
			}
			return
		}
		if err != nil || (pc.QRCode == "" && pc.PairingCode == "") {
			t.Fatalf("pairing: %+v %v", pc, err)
		}
		h.Pair(a)
		if _, err := h.Provider.GetPairingCode(ctx, a); !errors.Is(err, errs.ErrPairingUnavailable) {
			t.Errorf("connected instance has nothing to pair: %v", err)
		}
	})

	t.Run("SendText", func(t *testing.T) {
		h, a, ctx := newInst(t)
		connected(t, h, a, ctx)
		res, err := h.Provider.SendMessage(ctx, a, messaging.OutboundMessage{ID: "msg_1", To: "5562999999999", Type: messaging.TypeText, Text: "hello"})
		if err != nil || res.ProviderMessageID == "" {
			t.Fatalf("send: %+v %v", res, err)
		}
		if res.Status != messaging.StatusAccepted {
			t.Errorf("status %s", res.Status)
		}
	})

	t.Run("SendMedia", func(t *testing.T) {
		h, a, ctx := newInst(t)
		if !h.Provider.Capabilities(ctx).Media {
			t.Skip("provider has no media capability")
		}
		connected(t, h, a, ctx)
		res, err := h.Provider.SendMessage(ctx, a, messaging.OutboundMessage{ID: "msg_m", To: "5562999999999", Type: messaging.TypeDocument,
			Filename: "a.pdf", Caption: "doc",
			Media: &messaging.Attachment{ContentType: "application/pdf", Size: 4, URL: "http://blob.test/a.pdf?sig=1"}})
		if err != nil || res.ProviderMessageID == "" {
			t.Fatalf("send media: %+v %v", res, err)
		}
	})

	t.Run("SendWhileSocketDeadIsRetryableNotAmbiguous", func(t *testing.T) {
		h, a, ctx := newInst(t)
		connected(t, h, a, ctx)
		h.Drop(a)
		st, _ := h.Provider.GetInstanceState(ctx, a)
		if st == nil || st.State == instance.Connected {
			t.Fatalf("node healthy but socket dead must be visible per instance, got %+v", st)
		}
		_, err := h.Provider.SendMessage(ctx, a, messaging.OutboundMessage{ID: "m", To: "5562999999999", Type: messaging.TypeText, Text: "x"})
		if err == nil || errs.Classify(err) != errs.Retryable {
			t.Errorf("INV-10: dead socket => RETRYABLE, got %v (%s)", err, errs.Classify(err))
		}
		probe, perr := h.Provider.ProbeNode(ctx, h.NodeID)
		if perr != nil || !probe.Ready {
			t.Errorf("INV-10: node must stay healthy while the instance is disconnected: %+v %v", probe, perr)
		}
	})

	t.Run("ErrorTranslation", func(t *testing.T) {
		cases := []struct {
			name string
			f    Failure
			call func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error
			want error
			cls  errs.Class
		}{
			{"state unavailable", Unavailable, func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error {
				_, err := h.Provider.GetInstanceState(ctx, a)
				return err
			}, errs.ErrProviderUnavailable, errs.Retryable},
			{"state auth", AuthFailed, func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error {
				_, err := h.Provider.GetInstanceState(ctx, a)
				return err
			}, errs.ErrAuthenticationFailed, errs.NonRetryable},
			{"state not found", NotFound, func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error {
				_, err := h.Provider.GetInstanceState(ctx, a)
				return err
			}, errs.ErrInstanceNotFound, errs.NonRetryable},
			{"send unavailable", Unavailable, func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error {
				_, err := h.Provider.SendMessage(ctx, a, messaging.OutboundMessage{ID: "m", To: "5562", Type: messaging.TypeText, Text: "x"})
				return err
			}, errs.ErrProviderUnavailable, errs.Retryable},
			{"send ambiguous", Ambiguous, func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error {
				_, err := h.Provider.SendMessage(ctx, a, messaging.OutboundMessage{ID: "m", To: "5562", Type: messaging.TypeText, Text: "x"})
				return err
			}, errs.ErrAmbiguousDispatch, errs.Ambiguous},
			{"send auth", AuthFailed, func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error {
				_, err := h.Provider.SendMessage(ctx, a, messaging.OutboundMessage{ID: "m", To: "5562", Type: messaging.TypeText, Text: "x"})
				return err
			}, errs.ErrAuthenticationFailed, errs.NonRetryable},
			{"create unavailable", Unavailable, func(h ProviderHarness, a ownership.Assignment, ctx context.Context) error {
				_, err := h.Provider.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a, TenantID: "t1", Name: "n"})
				return err
			}, errs.ErrProviderUnavailable, errs.Retryable},
		}
		for _, c := range cases {
			c := c
			t.Run(c.name, func(t *testing.T) {
				h, a, ctx := newInst(t)
				if strings.HasPrefix(c.name, "send") || strings.HasPrefix(c.name, "state") {
					connected(t, h, a, ctx)
				}
				h.Inject(c.f)
				err := c.call(h, a, ctx)
				if !errors.Is(err, c.want) {
					t.Fatalf("want %v, got %v", c.want, err)
				}
				if errs.Classify(err) != c.cls {
					t.Errorf("class %s, want %s", errs.Classify(err), c.cls)
				}
			})
		}
	})

	t.Run("DisconnectConfirmsFencing", func(t *testing.T) {
		h, a, ctx := newInst(t)
		if !h.Provider.Capabilities(ctx).Disconnect {
			t.Skip("provider cannot fence")
		}
		connected(t, h, a, ctx)
		if err := h.Provider.Disconnect(ctx, a); err != nil {
			t.Fatalf("disconnect: %v", err)
		}
		st, err := h.Provider.GetInstanceState(ctx, a)
		if err != nil || st.State == instance.Connected || st.State == instance.Connecting {
			t.Fatalf("Disconnect must not return before the socket is closed: %+v %v", st, err)
		}
		if _, err := h.Provider.SendMessage(ctx, a, messaging.OutboundMessage{ID: "m", To: "5562", Type: messaging.TypeText, Text: "x"}); err == nil {
			t.Error("a fenced owner must not be able to send")
		}
	})

	t.Run("DeleteRemovesInstance", func(t *testing.T) {
		h, a, ctx := newInst(t)
		connected(t, h, a, ctx)
		if err := h.Provider.DeleteInstance(ctx, a); err != nil {
			t.Fatal(err)
		}
		if _, err := h.Provider.GetInstanceState(ctx, a); !errors.Is(err, errs.ErrInstanceNotFound) {
			t.Errorf("after delete: %v", err)
		}
	})

	t.Run("StaleAssignmentRejectedWhenSupported", func(t *testing.T) {
		h, a, ctx := newInst(t)
		if !h.RejectsStale {
			t.Skip("provider cannot reject stale assignments")
		}
		connected(t, h, a, ctx)
		old := a
		old.Epoch = 0
		if _, err := h.Provider.GetInstanceState(ctx, old); !errors.Is(err, errs.ErrStaleAssignment) {
			t.Errorf("want ErrStaleAssignment, got %v", err)
		}
	})

	t.Run("ProbeNode", func(t *testing.T) {
		h, _, ctx := newInst(t)
		p, err := h.Provider.ProbeNode(ctx, h.NodeID)
		if err != nil || !p.Ready {
			t.Errorf("probe: %+v %v", p, err)
		}
	})
}
