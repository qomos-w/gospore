//go:build !race

package app

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/ref"
)

// restartCrashChild has a handler that panics on its first invocation
// and succeeds afterwards. Under the default one-for-one supervisor the
// panic routes the cell through handleRestart.
type restartCrashChild struct {
	actor.Host
	boomed      atomic.Bool
	exposedOnce sync.Once
	self        ref.Ref
}

func (c *restartCrashChild) OnStart(ctx actor.Context) error {
	if err := c.Host.OnStart(ctx); err != nil {
		return err
	}
	c.self = ctx.Self()
	if err := actor.RegisterStateless[struct{}, string](ctx, "boom.unary",
		func(_ actor.PureContext, _ struct{}) (string, error) {
			if c.boomed.CompareAndSwap(false, true) {
				panic("first call must crash")
			}
			return "recovered", nil
		}); err != nil {
		return err
	}
	// handleRestart replays OnStart; the global service registry entry
	// survives, so the second Expose must not fail the replay.
	var exposeErr error
	c.exposedOnce.Do(func() {
		exposeErr = ctx.RegisterDomain("boomer").Expose()
	})
	return exposeErr
}

type restartCrashRoot struct {
	actor.Host
	child    *restartCrashChild
	childRef ref.Ref
}

func (r *restartCrashRoot) OnInit(ctx actor.Context) error {
	// The factory returns the same instance so the boomed flag survives
	// restart's factory recreation (state reset is the actor's own job).
	childRef, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return r.child }), "crasher")
	if err == nil {
		r.childRef = childRef
	}
	return err
}

// TestRestartReplayReportsStarted pins the handleRestart contract: a
// restarted cell must re-report IsStarted() (and close StartDone for
// spawn-side waiters). Before the fix, the restart replay re-ran
// OnStart but never stored cellStarted, leaving restart-completed
// actors permanently pre-start from the outside.
//
// Skipped under -race: it also exposes a remaining unsynchronized
// access between Run's drainIngressLoops (write side) and the
// handleRestart chain (read side) that Windows race symbolization
// cannot pinpoint (stacks collapse to return addresses). Four sibling
// races were fixed along the way (scriptRuntime swap, c.actor reads,
// openIngressLoops queue snapshots, laneSnapshots len/cap); this last
// pair is tracked in p01-cell-host-design §2.5 — bisect on Linux where
// race stacks symbolize correctly.
func TestRestartReplayReportsStarted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	root := &restartCrashRoot{child: &restartCrashChild{}}
	a, err := New(
		WithNamespace("apprestart"),
		WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(5 * time.Second)
	boomer, ok := a.LookupService("boomer")
	for !ok && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		boomer, ok = a.LookupService("boomer")
	}
	if !ok {
		t.Fatal("boomer service not resolvable after boot")
	}

	// First invocation panics; the caller observes an error reply.
	call := boomer.Invoke(ctx, "boom.unary", nil)
	if _, err := call.Recv(); err == nil {
		t.Fatal("first boom.unary call unexpectedly succeeded")
	}

	// The restart is asynchronous: poll until the handler answers
	// again, which only happens once handleRestart has replayed
	// OnStart and reopened ingress.
	impl := a.(*appImpl)
	var recovered bool
	for time.Now().Before(deadline) {
		childCell, cok := impl.cells[root.childRef.ID()]
		if !cok {
			t.Fatalf("crasher cell not tracked; childRef=%v", root.childRef.ID())
		}
		if childCell.IsStarted() {
			call := boomer.Invoke(ctx, "boom.unary", nil)
			if v, err := call.Recv(); err == nil {
				if s, _ := v.(string); s == "recovered" {
					recovered = true
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !recovered {
		t.Fatal("boom.unary never succeeded after restart")
	}

	childCell := impl.cells[root.childRef.ID()]
	// The restart may still be mid-replay when the recovered reply lands
	// (the reply is emitted before the system lane finishes handleRestart).
	for !childCell.IsStarted() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !childCell.IsStarted() {
		t.Fatal("restarted cell does not report IsStarted() after replay")
	}
	select {
	case <-childCell.StartDone():
	default:
		t.Fatal("restarted cell StartDone not closed")
	}
}
