package integration

import (
	"context"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
)

// wedgedChild's OnInit never observes cancellation: it blocks on a channel
// only the test closes. This models downstream actors whose init waits on
// I/O that a cancelled boot will never satisfy.
type wedgedChild struct {
	actor.Host
	unblock <-chan struct{}
}

func (a *wedgedChild) OnInit(_ actor.Context) error {
	<-a.unblock
	return nil
}

type cancelDuringInitRoot struct {
	actor.Host
	unblock <-chan struct{}
}

func (r *cancelDuringInitRoot) OnInit(ctx actor.Context) error {
	_, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &wedgedChild{unblock: r.unblock}
	}), "wedged")
	return err
}

// TestRun_CancelDuringInit_Returns reproduces the early-cancel deadlock
// reported downstream: cancelling the context while Phase 1 (root init)
// is in flight parked Run forever — the root cell's Done never closes
// because recursive Spawn waits on a child whose OnInit wedged past the
// cancellation. Run (and the recursive spawn chain) must observe ctx
// cancellation and return promptly.
func TestRun_CancelDuringInit_Returns(t *testing.T) {
	unblock := make(chan struct{})
	defer close(unblock) // unwedge the leaked child after the assertion

	a, err := app.New(
		app.WithNamespace("itest"),
		app.WithRootActor(func() actor.Actor {
			return &cancelDuringInitRoot{unblock: unblock}
		}),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Enter the init window (root spawning the wedged child), then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil; cancelled boot should surface ctx.Err")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within 5s of context cancellation during init (Phase 1 deadlock)")
	}
}
