package app

import (
	"context"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
)

type plainRootActor struct{}

func (plainRootActor) OnInit(actor.Context) error { return nil }
func (plainRootActor) OnStart(actor.Context) error { return nil }
func (plainRootActor) OnStop(actor.Context) error  { return nil }
func (plainRootActor) Type() string                { return "plainroot" }

type slowOnInitActor struct {
	delay time.Duration
	enter chan struct{}
}

func (a *slowOnInitActor) OnInit(actor.Context) error {
	if a.enter != nil {
		close(a.enter)
	}
	time.Sleep(a.delay)
	return nil
}
func (a *slowOnInitActor) OnStart(actor.Context) error { return nil }
func (a *slowOnInitActor) OnStop(actor.Context) error  { return nil }
func (a *slowOnInitActor) Type() string                { return "slowoninit" }

type noopChildActor struct{}

func (noopChildActor) OnInit(actor.Context) error { return nil }
func (noopChildActor) OnStart(actor.Context) error {
	return nil
}
func (noopChildActor) OnStop(actor.Context) error { return nil }
func (noopChildActor) Type() string               { return "noopchild" }

// TestSpawnSlowOnInitSiblingDoesNotStallSpawns is the regression for the
// sporecode workspace freeze: a sibling whose OnInit takes seconds (cold
// disk IO, Defender scans) used to hold the tree write lock for its whole
// OnInit, so every concurrent spawn queued behind it and callers timed
// out ("project actor did not answer"). With the reserve/build split the
// lock is only held for topology reservation; unrelated spawns proceed.
func TestSpawnSlowOnInitSiblingDoesNotStallSpawns(t *testing.T) {
	a, err := New(
		WithNamespace("test"),
		WithRootActor(func() actor.Actor { return plainRootActor{} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	if err := a.WaitForAllCellsStart(5 * time.Second); err != nil {
		t.Fatalf("WaitForAllCellsStart: %v", err)
	}

	entered := make(chan struct{})
	go func() {
		_, _ = a.Spawn(actor.PropsFromFunc(func() actor.Actor {
			return &slowOnInitActor{delay: 3 * time.Second, enter: entered}
		}), "slow")
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow child never entered OnInit")
	}

	start := time.Now()
	fast, err := a.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return noopChildActor{}
	}), "fast")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("fast Spawn while sibling in OnInit: %v", err)
	}
	if fast == nil {
		t.Fatal("fast Spawn returned nil ref")
	}
	// The sibling still has ~3s of OnInit left; anything close to that
	// means the fast spawn queued behind the slow one's lock.
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("fast Spawn took %v while sibling OnInit (3s) was in flight — spawns still serialized", elapsed)
	}

	cancel()
	<-done
}
