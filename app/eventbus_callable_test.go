package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/events"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
)

type eventRootActor struct {
	actor.Host
	registered chan struct{}
}

func (a *eventRootActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := ctx.RegisterEventKind("public-turn", turnEvent{}, actor.Public()); err != nil {
		return err
	}
	if err := ctx.RegisterEventKind("admin-turn", turnEvent{}, actor.AdminOnly()); err != nil {
		return err
	}
	if err := ctx.RegisterDomain("testsvc").Expose(); err != nil {
		return err
	}
	if err := ctx.Register("test.emit", func(ctx actor.Context, req emitReq) error {
		return ctx.EmitEvent(req.Kind, turnEvent{Token: req.Token})
	}); err != nil {
		return err
	}
	if a.registered != nil {
		close(a.registered)
	}
	return nil
}

// eventInstanceActor registers events but does NOT call Expose, so its
// routing key is the actor ID (instance identity).
type eventInstanceActor struct {
	actor.Host
	registered chan struct{}
}

func (a *eventInstanceActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := ctx.RegisterEventKind("public-turn", turnEvent{}, actor.Public()); err != nil {
		return err
	}
	if err := ctx.Register("test.emit", func(ctx actor.Context, req emitReq) error {
		return ctx.EmitEvent(req.Kind, turnEvent{Token: req.Token})
	}); err != nil {
		return err
	}
	if a.registered != nil {
		close(a.registered)
	}
	return nil
}

type turnEvent struct {
	Token string `json:"token"`
}

type emitReq struct {
	Kind  string `json:"kind"`
	Token string `json:"token"`
}

// emitUntilCancelled tells test.emit on a ticker until ctx is cancelled. Used by
// streaming subscribe tests because the subscribe handler races with the emit
// handler — the bus has no replay, so a single pre-subscribe emit gets dropped.
func emitUntilCancelled(ctx context.Context, root ref.Ref, kind, token string) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c := root.Invoke(context.Background(), "test.emit", emitReq{Kind: kind, Token: token})
			_ = c.Close()
		}
	}
}

func TestEventSubscribeCallable_PublicEventViaService(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &eventRootActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	root := app.Self()
	stream := root.Invoke(context.Background(), eventSubscribeServiceCallID, eventSubscribeServiceReq{
		ServiceName: "testsvc",
		Kind:        "public-turn",
	})
	defer stream.Close()

	emitCtx, emitCancel := context.WithCancel(context.Background())
	defer emitCancel()
	go emitUntilCancelled(emitCtx, root, "public-turn", "hello")

	chunk, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	body, ok := chunk.([]byte)
	if !ok {
		t.Fatalf("chunk type = %T, want []byte", chunk)
	}
	var got turnEvent
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Token != "hello" {
		t.Fatalf("token = %q, want hello", got.Token)
	}
}

func TestEventSubscribeCallable_PublicEventViaInstance(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &eventInstanceActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	root := app.Self()
	stream := root.Invoke(context.Background(), eventSubscribeInstanceCallID, eventSubscribeInstanceReq{
		ActorId: root.ID().String(),
		Kind:    "public-turn",
	})
	defer stream.Close()

	emitCtx, emitCancel := context.WithCancel(context.Background())
	defer emitCancel()
	go emitUntilCancelled(emitCtx, root, "public-turn", "via-instance")

	chunk, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	body, ok := chunk.([]byte)
	if !ok {
		t.Fatalf("chunk type = %T, want []byte", chunk)
	}
	var got turnEvent
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Token != "via-instance" {
		t.Fatalf("token = %q, want via-instance", got.Token)
	}
}

func TestEventSubscribeCallable_AdminEventDeniedToGuest(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &eventRootActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	root := app.Self()
	stream := root.Invoke(context.Background(), eventSubscribeServiceCallID, eventSubscribeServiceReq{
		ServiceName: "testsvc",
		Kind:        "admin-turn",
	}, map[string]string{"gospore.caller_role": "guest"})
	defer stream.Close()

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected denial error, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagPolicyDenied) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagPolicyDenied)
	}
}

func TestEventSubscribeCallable_AdminEventAllowedToAdmin(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &eventRootActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	root := app.Self()
	stream := root.Invoke(context.Background(), eventSubscribeServiceCallID, eventSubscribeServiceReq{
		ServiceName: "testsvc",
		Kind:        "admin-turn",
	}, map[string]string{
		"gospore.caller_role":    "admin",
		"gospore.caller_kind":    id.IdentityToken.String(),
		"gospore.caller_subject": "alice",
	})
	defer stream.Close()

	emitCtx, emitCancel := context.WithCancel(context.Background())
	defer emitCancel()
	go emitUntilCancelled(emitCtx, root, "admin-turn", "secret")

	chunk, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	body, ok := chunk.([]byte)
	if !ok {
		t.Fatalf("chunk type = %T, want []byte", chunk)
	}
	var got turnEvent
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Token != "secret" {
		t.Fatalf("token = %q, want secret", got.Token)
	}
}

func TestEventVisibilityAllowed(t *testing.T) {
	admin := id.Identity{Kind: id.IdentityToken, Role: id.RoleAdmin, Subject: "alice"}
	guest := id.Identity{Kind: id.IdentityToken, Role: id.Role("guest"), Subject: "bob"}

	cases := []struct {
		name       string
		visibility actor.Visibility
		identity   id.Identity
		want       bool
	}{
		{name: "public guest", visibility: actor.VisibilityPublic, identity: guest, want: true},
		{name: "admin admin", visibility: actor.VisibilityAdmin, identity: admin, want: true},
		{name: "admin guest", visibility: actor.VisibilityAdmin, identity: guest, want: false},
		{name: "diagnostic admin", visibility: actor.VisibilityDiagnostic, identity: admin, want: true},
		{name: "diagnostic guest", visibility: actor.VisibilityDiagnostic, identity: guest, want: false},
		{name: "internal admin", visibility: actor.VisibilityInternal, identity: admin, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := eventVisibilityAllowed(tc.visibility, tc.identity); got != tc.want {
				t.Fatalf("eventVisibilityAllowed(%v, %+v) = %v, want %v", tc.visibility, tc.identity, got, tc.want)
			}
		})
	}
}

func TestEventSubscribeCallable_InstanceNotFound(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &eventRootActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	root := app.Self()
	stream := root.Invoke(context.Background(), eventSubscribeInstanceCallID, eventSubscribeInstanceReq{
		ActorId: "ffffffffffffffffffffffffffffffffffffffff",
		Kind:    "public-turn",
	})
	defer stream.Close()

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for unknown actor ID, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error = %q, want containing 'not found'", err)
	}
}

func TestEventSubscribeCallable_ServiceNotFound(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &eventRootActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	root := app.Self()
	stream := root.Invoke(context.Background(), eventSubscribeServiceCallID, eventSubscribeServiceReq{
		ServiceName: "nonexistent",
		Kind:        "public-turn",
	})
	defer stream.Close()

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for unknown service, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error = %q, want containing 'not found'", err)
	}
}

// TestEventStatsCallable_ListsLiveSubscriptions verifies gospore.events.stats
// reports an open subscription while it is live and drops it after the stream
// closes (the drain loop's deferred cancel unregisters it).
func TestEventStatsCallable_ListsLiveSubscriptions(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &eventRootActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	root := app.Self()
	stream := root.Invoke(context.Background(), eventSubscribeServiceCallID, eventSubscribeServiceReq{
		ServiceName: "testsvc",
		Kind:        "public-turn",
	})
	defer stream.Close()

	waitForSub := func(want int) []eventStatsSub {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			call := root.Invoke(context.Background(), eventStatsCallID, eventStatsReq{})
			v, err := call.Final(context.Background())
			call.Close()
			if err != nil {
				t.Fatalf("stats: %v", err)
			}
			resp, ok := v.(eventStatsResp)
			if !ok {
				t.Fatalf("stats type = %T", v)
			}
			if len(resp.Subscriptions) == want {
				return resp.Subscriptions
			}
			select {
			case <-deadline:
				t.Fatalf("subscriptions = %d, want %d (last: %+v)", len(resp.Subscriptions), want, resp.Subscriptions)
			case <-time.After(20 * time.Millisecond):
			}
		}
	}

	subs := waitForSub(1)
	s := subs[0]
	if s.Flavor != "service" || s.Match != "testsvc" || s.Kind != "public-turn" {
		t.Fatalf("entry = %+v", s)
	}
	if s.Created == "" {
		t.Fatal("Created not set")
	}

	// Cancelling the stream propagates a cancel frame to the server side,
	// ending drainSubscription whose deferred cancel unregisters the
	// subscription. (Close() alone only detaches the caller.)
	stream.Cancel()
	waitForSub(0)
}

func TestEventStatsCallable_ReportsRingEvictions(t *testing.T) {
	registered := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithEventRingCapacity(2),
		WithRootActor(func() actor.Actor { return &eventRootActor{registered: registered} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not register events")
	}

	// Always-write means every invoke (including the stats call itself,
	// which targets root) writes invoke records into root's ring —
	// reading root's own ring is self-referential. Pin the eviction
	// contract on a detached actor ID nobody invokes: three pushes into
	// a capacity-2 ring evict exactly one.
	impl := app.(*appImpl)
	root := app.Self()
	fakeID, err := id.Parse("03000000000000000000000000000003")
	if err != nil {
		t.Fatal(err)
	}

	stats := func() eventStatsResp {
		call := root.Invoke(context.Background(), eventStatsCallID, eventStatsReq{})
		v, err := call.Final(context.Background())
		call.Close()
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		resp, ok := v.(eventStatsResp)
		if !ok {
			t.Fatalf("stats type = %T", v)
		}
		for _, r := range resp.Rings {
			if r.ActorID == fakeID.String() {
				return resp
			}
		}
		return resp
	}

	stats() // drain any earlier state; fakeID has no ring yet

	for i := 0; i < 3; i++ {
		impl.eventsStore.Push(fakeID, events.Record{Kind: actor.WatchStarted})
	}

	resp := stats()
	var r eventStatsRing
	found := false
	for _, e := range resp.Rings {
		if e.ActorID == fakeID.String() {
			r, found = e, true
		}
	}
	if !found {
		t.Fatalf("no ring entry for the detached actor despite 3 pushes into capacity-2 ring: %+v", resp.Rings)
	}
	if r.Evicted != 1 || r.Capacity != 2 || r.Len != 2 {
		t.Fatalf("ring entry = %+v", r)
	}
}

var _ invoke.CallMode
