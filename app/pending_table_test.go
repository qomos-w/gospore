package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/ref"
)

// slowCalleeActor blocks inside test.slow until released.
type slowCalleeActor struct {
	actor.Host
	release chan struct{}
}

func (a *slowCalleeActor) OnStart(ctx actor.Context) error {
	if err := ctx.RegisterDomain("slowcallee").Expose(); err != nil {
		return err
	}
	return ctx.Register("test.slow", func(c actor.Context, _ []byte) (string, error) {
		<-a.release
		return "done", nil
	})
}

// slowCallerActor's test.kick blocks awaiting a planner call to test.slow,
// keeping that invocation registered in the caller's pending table.
type slowCallerActor struct {
	actor.Host
}

func (a *slowCallerActor) OnStart(ctx actor.Context) error {
	return ctx.Register("test.kick", func(c actor.Context, _ []byte) (string, error) {
		svcRef, ok := c.LookupService("slowcallee")
		if !ok {
			return "", fmt.Errorf("slowcallee service not found")
		}
		callCtx, cancel := context.WithTimeout(c.Lifecycle(), 10*time.Second)
		defer cancel()
		result, err := c.Planner().Call(callCtx, svcRef, "test.slow", []byte("go")).Await()
		if err != nil {
			return "", err
		}
		switch v := result.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	})
}

type pendingRootActor struct {
	actor.Host
	CallerRef       ref.Ref
	CalleeRef       ref.Ref
	release         chan struct{}
	ready           chan struct{}
	waitForStarted  func(*testing.T)
}

func (a *pendingRootActor) OnStart(ctx actor.Context) error {
	callee, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &slowCalleeActor{release: a.release}
	}), "slowcallee")
	if err != nil {
		return err
	}
	caller, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &slowCallerActor{}
	}).WithPlanner(), "slowcaller")
	if err != nil {
		return err
	}
	a.CalleeRef = callee
	a.CallerRef = caller
	close(a.ready)
	return nil
}

// refs returns the spawned refs, waiting for OnStart to publish them.
// Reading the fields off the system lane without this handoff is a data race.
// waitForStarted, when set, additionally blocks until the child cells have
// completed their own OnStart (their callables are registered there).
func (a *pendingRootActor) refs(t *testing.T) (ref.Ref, ref.Ref) {
	t.Helper()
	select {
	case <-a.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("root did not spawn actors in time")
	}
	if a.waitForStarted != nil {
		a.waitForStarted(t)
	}
	return a.CallerRef, a.CalleeRef
}

func runPendingApp(t *testing.T) (*pendingRootActor, App, func()) {
	t.Helper()
	root := &pendingRootActor{release: make(chan struct{}), ready: make(chan struct{})}

	a, err := New(
		WithNamespace("test"),
		WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	impl := a.(*appImpl)
	root.waitForStarted = func(t *testing.T) {
		if err := impl.waitForCellStart(root.CallerRef); err != nil {
			t.Fatalf("caller start: %v", err)
		}
		if err := impl.waitForCellStart(root.CalleeRef); err != nil {
			t.Fatalf("callee start: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Wait for the root actor (and its spawned children) to start.
	root.refs(t)

	return root, a, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}
}

func fetchCellStats(t *testing.T, from ref.Ref, path string) CellStats {
	t.Helper()
	stream := from.Invoke(context.Background(), cellStatsCallID, cellStatsReq{ActorPath: path})
	val, err := stream.Recv()
	stream.Close()
	if err != nil {
		t.Fatalf("cellStats Recv(%s): %v", path, err)
	}
	return decodeCellStats(t, val)
}

// TestPerCellPendingTableAttribution verifies that a planner call issued by
// one actor is attributed to that actor's own pending table only. Under the
// former App-level shared table, gospore.cell.stats reported the same
// in-flight set for every actor, so the callee and root here would also show
// test.slow in flight.
func TestPerCellPendingTableAttribution(t *testing.T) {
	root, a, cleanup := runPendingApp(t)
	defer cleanup()

	self := a.Self()
	callerRef, calleeRef := root.refs(t)

	kickDone := make(chan struct{})
	var kickErr error
	go func() {
		defer close(kickDone)
		call := callerRef.Invoke(context.Background(), "test.kick", []byte("go"))
		_, kickErr = call.Final(context.Background())
		call.Close()
	}()

	callerPath := callerRef.ID().String()
	calleePath := calleeRef.ID().String()

	var callerStats CellStats
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		callerStats = fetchCellStats(t, self, callerPath)
		if callerStats.Invoke.InFlightByCall["test.slow"] == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if callerStats.Invoke.InFlightByCall["test.slow"] != 1 {
		t.Fatalf("caller never reported test.slow in flight: %+v (kickErr=%v)", callerStats.Invoke.InFlightByCall, kickErr)
	}
	if callerStats.PendingInvokes < 1 {
		t.Errorf("caller PendingInvokes = %d, want >= 1", callerStats.PendingInvokes)
	}

	calleeStats := fetchCellStats(t, self, calleePath)
	if got := calleeStats.Invoke.InFlightByCall["test.slow"]; got != 0 {
		t.Errorf("callee InFlightByCall[test.slow] = %d, want 0", got)
	}
	if calleeStats.PendingInvokes != 0 {
		t.Errorf("callee PendingInvokes = %d, want 0", calleeStats.PendingInvokes)
	}

	rootStats := fetchCellStats(t, self, self.ID().String())
	if got := rootStats.Invoke.InFlightByCall["test.slow"]; got != 0 {
		t.Errorf("root InFlightByCall[test.slow] = %d, want 0", got)
	}

	close(root.release)
	select {
	case <-kickDone:
	case <-time.After(5 * time.Second):
		t.Fatal("kick call did not finish after release")
	}
}
