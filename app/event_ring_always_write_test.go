package app

import (
	"context"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/events"
	"github.com/qomos-w/gospore/id"
)

// ringHistoryActor registers an event kind and a callable so tests can
// drive both record sources (user EmitEvent + callable invocations).
type ringHistoryActor struct {
	actor.Host
}

type histTurnEvent struct {
	Note string `json:"note"`
}

func (a *ringHistoryActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := ctx.RegisterEventKind("turn", histTurnEvent{}); err != nil {
		return err
	}
	return actor.RegisterStateful[string, string](ctx, "hist.poke",
		func(ctx actor.Context, _ string) (string, error) {
			if err := ctx.EmitEvent("turn", histTurnEvent{Note: "poked"}); err != nil {
				return "", err
			}
			return "ok", nil
		})
}

// TestEventRing_AlwaysWritesIdle pins the §4.23 retention contract: ring
// history is written during subscriber-free ("idle") periods. Before the
// always-write decision the emit paths were gated on HasSubscribers, so
// lifecycle and invoke records only existed from the first subscription
// onward and gap replay across idle periods returned nothing.
func TestEventRing_AlwaysWritesIdle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a, err := New(
		WithNamespace("apphist"),
		WithRootActor(func() actor.Actor { return &ringHistoryActor{} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() { cancel(); <-done }()
	time.Sleep(150 * time.Millisecond)

	impl := a.(*appImpl)
	rootID := a.Self().ID()

	// No subscriber has ever existed. Lifecycle (WatchStarted) must
	// already be in the ring.
	if recs := impl.eventsStore.Recent(rootID, 0, []actor.WatchKind{actor.WatchStarted}, 10); len(recs) == 0 {
		t.Fatal("no WatchStarted records in ring after boot — idle-period writes missing")
	}

	// Invoke the callable with no subscriber: invoke records must land.
	call := a.Self().Invoke(ctx, "hist.poke", "")
	if _, err := call.Recv(); err != nil {
		t.Fatalf("hist.poke: %v", err)
	}
	if recs := impl.eventsStore.Recent(rootID, 0, []actor.WatchKind{actor.WatchInvokeInStarted}, 10); len(recs) == 0 {
		t.Fatal("no WatchInvokeInStarted records in ring — idle-period invoke writes missing")
	}
}

// TestEmitEvent_LandsInRing pins the user-event half of the contract:
// ctx.EmitEvent reaches the bus (existing tests) AND leaves a
// WatchUserEvent record carrying the registered kind name, so idle-period
// emissions are auditable and gap-replayable.
func TestEmitEvent_LandsInRing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a, err := New(
		WithNamespace("apphist2"),
		WithRootActor(func() actor.Actor { return &ringHistoryActor{} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() { cancel(); <-done }()
	time.Sleep(150 * time.Millisecond)

	call := a.Self().Invoke(ctx, "hist.poke", "")
	if _, err := call.Recv(); err != nil {
		t.Fatalf("hist.poke: %v", err)
	}

	impl := a.(*appImpl)
	recs := impl.eventsStore.Recent(a.Self().ID(), 0, []actor.WatchKind{actor.WatchUserEvent}, 10)
	if len(recs) == 0 {
		t.Fatal("no WatchUserEvent record in ring after EmitEvent")
	}
	if recs[0].Event != "turn" {
		t.Fatalf("user-event record Event = %q, want \"turn\"", recs[0].Event)
	}
	// Filtering by the new kind must not leak other kinds.
	for _, r := range recs {
		if r.Kind != actor.WatchUserEvent {
			t.Fatalf("kind filter leaked record of kind %v", r.Kind)
		}
	}
}

// TestWatchUserEvent_SubscribeValid pins that the new kind is accepted by
// the events store's per-entry kind validation.
func TestWatchUserEvent_SubscribeValid(t *testing.T) {
	s := events.NewStore(8)
	aid, err := id.Parse("01000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := s.Subscribe(aid, 0, []actor.WatchKind{actor.WatchUserEvent})
	if err != nil {
		t.Fatalf("Subscribe with WatchUserEvent: %v", err)
	}
	_ = sub.Close()
}
