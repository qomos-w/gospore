package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
)

// wedgeInitChild blocks in OnInit until released. Terminal callbacks
// record exactly-once firing.
type wedgeInitChild struct {
	actor.Host
	release   chan struct{}
	inInit    atomic.Bool
	onStop    atomic.Int32
	onDestroy atomic.Int32
}

func (w *wedgeInitChild) OnInit(_ actor.Context) error {
	w.inInit.Store(true)
	<-w.release
	return nil
}

func (w *wedgeInitChild) OnStop(_ actor.Context) error {
	w.onStop.Add(1)
	return nil
}
func (w *wedgeInitChild) OnDestroy(_ actor.Context) error {
	w.onDestroy.Add(1)
	return nil
}

// wedgeInitRoot spawns the wedge child.
type wedgeInitRoot struct {
	actor.Host
	child *wedgeInitChild
}

func (w *wedgeInitRoot) OnInit(ctx actor.Context) error {
	if err := w.Host.OnInit(ctx); err != nil {
		return err
	}
	_, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return w.child }), "wedge")
	return err
}

// TestTerminateTimeoutForcesCleanup pins the abort-during-OnInit resource
// contract: when terminate's graceful Done() wait times out because
// systemLoop is wedged in OnInit while Run's drain already closed the
// queues, ForceCleanup must still run OnStop and OnDestroy — and exactly
// once even after the wedge releases and the message path replays its own
// Stop/Destroy handling.
func TestTerminateTimeoutForcesCleanup(t *testing.T) {
	child := &wedgeInitChild{release: make(chan struct{})}
	root := &wedgeInitRoot{child: child}

	runCtx, cancelRun := context.WithCancel(context.Background())
	a, err := New(
		WithNamespace("forceclean"),
		WithRootActor(func() actor.Actor { return root }),
		WithTerminateTimeout(150*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = a.Run(runCtx) }()

	// Wait until the wedge child is inside OnInit.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if child.inInit.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !child.inInit.Load() {
		t.Fatal("wedge child never entered OnInit")
	}

	// Cancel the app ctx: Run's drain closes the queues while systemLoop
	// is still blocked in the wedge child's OnInit. Shutdown waits for
	// Done via the LIFO destroy, times out per terminateTimeout, and
	// falls into ForceCleanup.
	cancelRun()
	_ = a.Shutdown(context.Background())

	// The LIFO destroy (root cell's Run teardown) reaches the wedge
	// child's terminate asynchronously: enqueue Destroy (never processed
	// — queues closed by drain), wait terminateTimeout, then ForceCleanup.
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		dl := time.Now().Add(5 * time.Second)
		for time.Now().Before(dl) {
			if cond() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s never fired during abort", what)
	}
	waitFor("OnStop via ForceCleanup", func() bool { return child.onStop.Load() == 1 })
	waitFor("OnDestroy via ForceCleanup", func() bool { return child.onDestroy.Load() == 1 })

	// Release the wedge and give the (still running) message path a
	// chance to replay Stop/Destroy — the exactly-once guards must hold.
	close(child.release)
	time.Sleep(200 * time.Millisecond)
	if got := child.onStop.Load(); got != 1 {
		t.Fatalf("OnStop fired %d times after message-path replay, want 1", got)
	}
	if got := child.onDestroy.Load(); got != 1 {
		t.Fatalf("OnDestroy fired %d times after message-path replay, want 1", got)
	}
}
