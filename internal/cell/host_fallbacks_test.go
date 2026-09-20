package cell

import (
	"context"
	"strings"
	"testing"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/service"
)

// TestCellHostFallbacks pins the no-host / partial-host contract: every
// capability group the host does not implement must fall back to the
// documented default behavior. This is the formalization of the
// pre-existing Config-nil semantics (see p01-cell-host-design §4).

type stubActor struct{ actor.Host }

func (stubActor) OnInit(ctx actor.Context) error { return nil }
func (stubActor) OnStart(ctx actor.Context) error { return nil }
func (stubActor) OnStop(ctx actor.Context) error  { return nil }

type hostTestRef struct{ aid id.ActorID }

func (r hostTestRef) ID() id.ActorID                        { return r.aid }
func (r hostTestRef) Service() (string, bool)               { return "", false }
func (r hostTestRef) Invoke(context.Context, string, any, ...map[string]string) *invoke.Call {
	return nil
}

var _ ref.Ref = hostTestRef{}

func hostTestID(slot uint16) id.ActorID {
	return id.NewCanonical(slot, 0, func() uint64 { return 7 }).Next()
}

func newFallbackCell(t *testing.T, host Host, svcReg service.Registry) *Cell {
	t.Helper()
	return New(Config{
		Self:            hostTestRef{aid: hostTestID(1)},
		Actor:           stubActor{},
		Props:           actor.PropsFromFunc(func() actor.Actor { return stubActor{} }),
		ServiceRegistry: svcReg,
		InvokeTable:     nil, // pins fallback #12: frames dropped, not panic
		Host:            host,
	})
}

func TestCellHostFallbacks_ServiceHost(t *testing.T) {
	svcReg := service.New()
	c := newFallbackCell(t, nil, svcReg)
	ctx := newStartContext(c)

	// #5/#6: without ServiceHost, LookupService only checks the local
	// registry and never resolves scoped services.
	owner := hostTestRef{aid: hostTestID(2)}
	if err := svcReg.Register("svca", owner); err != nil {
		t.Fatalf("seed register: %v", err)
	}
	if r, ok := ctx.LookupService("svca"); !ok || r == nil {
		t.Fatalf("LookupService via local registry = (%v, %v), want hit", r, ok)
	}
	if _, ok := ctx.LookupService("scopedonly"); ok {
		t.Fatal("LookupService resolved scoped-only name without host, want miss")
	}

	// #1: Expose falls back to ServiceRegistry.Register.
	if err := ctx.RegisterDomain("dom").Expose(); err != nil {
		t.Fatalf("Expose fallback: %v", err)
	}
	if _, ok := svcReg.Lookup("dom"); !ok {
		t.Fatal("Expose fallback did not register into local registry")
	}

	// #3: ExposeToChildren without host errors out.
	if err := ctx.RegisterDomain("dom").ExposeChildren(); err == nil {
		t.Fatal("ExposeChildren without host = nil error, want error")
	}

	// #2/#4: clearServices without host unregisters via the local
	// registry and clears the childServices bookkeeping without
	// touching any scoped directory.
	c.services = append(c.services, "dom")
	c.childServices = append(c.childServices, "kid")
	c.clearServices()
	if _, ok := svcReg.Lookup("dom"); ok {
		t.Fatal("clearServices fallback left service registered")
	}
	if len(c.childServices) != 0 {
		t.Fatalf("childServices = %v, want cleared", c.childServices)
	}
}

func TestCellHostFallbacks_TreeHost(t *testing.T) {
	c := newFallbackCell(t, nil, nil)
	ctx := newStartContext(c)

	// #7: Spawn without TreeHost errors.
	if _, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return stubActor{} }), "kid"); err == nil ||
		!strings.Contains(err.Error(), "not available") {
		t.Fatalf("Spawn without host = (%v), want not-available error", err)
	}

	// #8: Stop/Destroy without TreeHost return nil (no-op).
	if err := ctx.Stop(nil); err != nil {
		t.Fatalf("Stop fallback = %v, want nil", err)
	}
	if err := ctx.Destroy(nil); err != nil {
		t.Fatalf("Destroy fallback = %v, want nil", err)
	}

	// #9: LookupID without TreeHost returns (nil, false).
	if r, ok := ctx.LookupID(hostTestID(3)); ok || r != nil {
		t.Fatalf("LookupID fallback = (%v, %v), want (nil, false)", r, ok)
	}
}

func TestCellHostFallbacks_DeliveryHost(t *testing.T) {
	c := newFallbackCell(t, nil, nil)
	ctx := newStartContext(c)

	// #11a: After without DeliveryHost errors.
	if err := ctx.After(0, "tick", nil); err == nil {
		t.Fatal("After without host = nil error, want error")
	}
	// #11b: Watch without DeliveryHost errors.
	if err := ctx.Watch(hostTestRef{aid: hostTestID(4)}); err == nil {
		t.Fatal("Watch without host = nil error, want error")
	}
	// #11c: Unwatch without DeliveryHost is silent.
	ctx.Unwatch(hostTestRef{aid: hostTestID(4)})

	// #11d: notifyWatchers skips delivery but does not panic.
	c.notifyWatchers()

	// #12: reply routing without an invoke table drops frames.
	if c.replyReg.deliver(message.Frame{Kind: message.KindReply, CorID: 1}) {
		t.Fatal("reply deliver without table = true, want false")
	}

	// #13: root escalation without DeliveryHost is silently observed.
	c.notifyParentEscalation(context.Canceled)
}

// scriptFallbackOwner exercises the scriptRuntimeOwner host wiring: a
// cell without host capabilities must degrade script-context calls the
// same way the startContext fallbacks do, not panic.
func TestCellHostFallbacks_ScriptRuntime(t *testing.T) {
	c := newFallbackCell(t, nil, nil)
	o := newScriptRuntimeOwner(nil, c)
	adapter := &scriptContextAdapter{owner: o}

	if got := adapter.Spawn("kid", 0); got != "" {
		t.Fatalf("Spawn without treeHost = %q, want empty", got)
	}
	if got := adapter.Stop("01"); got {
		t.Fatal("Stop without treeHost = true, want false")
	}
	if got := adapter.Watch("01"); got {
		t.Fatal("Watch without host = true, want false")
	}
	if dr := adapter.LookupID("01"); dr == nil {
		t.Fatal("LookupID without treeHost = nil ActorRef, want dead ref")
	}
	if dr := adapter.LookupService("svc"); dr == nil {
		t.Fatal("LookupService without svcHost = nil ActorRef, want dead ref")
	}
	if got := adapter.Plan("01", "x", nil, 0); got != "" {
		t.Fatalf("Plan without treeHost = %q, want empty", got)
	}
}
// countingDeliveryHost implements only DeliveryHost — partial hosts
// must still work group-wise.
type countingDeliveryHost struct {
	delivered int
	escalated int
}

func (h *countingDeliveryHost) Deliver(target ref.Ref, env mailbox.Envelope) error {
	h.delivered++
	return nil
}

func (h *countingDeliveryHost) RootEscalated(reason error) {
	h.escalated++
}

func TestCellHost_PartialHostDelivery(t *testing.T) {
	h := &countingDeliveryHost{}
	c := newFallbackCell(t, h, nil)
	ctx := newStartContext(c)

	if err := ctx.Watch(hostTestRef{aid: hostTestID(5)}); err != nil {
		t.Fatalf("Watch via host: %v", err)
	}
	if h.delivered != 1 {
		t.Fatalf("host Deliver calls = %d, want 1", h.delivered)
	}

	// Root escalation path: a parent-less cell with a delivery host
	// routes to RootEscalated instead of delivering to a parent.
	c.notifyParentEscalation(context.Canceled)
	if h.escalated != 1 {
		t.Fatalf("RootEscalated calls = %d, want 1", h.escalated)
	}
	if h.delivered != 1 {
		t.Fatalf("Deliver after escalation = %d, want 1 (parent is nil: no parent delivery)", h.delivered)
	}
}
