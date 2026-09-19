package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/events"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/service"
)

// awaitRootStarted blocks until the App's root actor emits WatchStarted.
func awaitRootStarted(t *testing.T, a app.App, runFn func()) {
	t.Helper()
	sub, err := a.Events().Subscribe(a.Self().ID(), 0, []actor.WatchKind{actor.WatchStarted})
	if err != nil {
		t.Fatalf("awaitRootStarted: Events.Subscribe: %v", err)
	}
	defer sub.Close()
	runFn()

	type result struct {
		rec events.Record
		err error
	}
	ch := make(chan result, 1)
	go func() {
		rec, err := sub.Recv()
		ch <- result{rec: rec, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("awaitRootStarted: Recv: %v", r.err)
		}
		if r.rec.Kind != actor.WatchStarted {
			t.Fatalf("awaitRootStarted: got Kind=%v, want WatchStarted", r.rec.Kind)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("awaitRootStarted: WatchStarted did not arrive within 2s")
	}
}

func waitForCellStarted(t *testing.T, a app.App, r ref.Ref) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if _, ok := a.HandlerTableFor(r); ok {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("cell %s did not start", r.ID().String())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// invokeUnary retries calling callID on r until the handler is registered or
// the deadline expires. It returns the unary result.
func invokeUnary(t *testing.T, ctx context.Context, r ref.Ref, callID string, payload any, deadline time.Duration) any {
	t.Helper()
	d := time.After(deadline)
	for {
		stream := r.Invoke(ctx, callID, payload)
		if stream != nil {
			v, err := stream.Final(ctx)
			if err == nil {
				return v
			}
			if stream != nil {
				_ = stream.Close()
			}
			// Fall through to retry on error.
		}
		select {
		case <-d:
			t.Fatalf("invokeUnary %q did not succeed within %v", callID, deadline)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// parentProjectActor registers a callable, exposes it to descendants, and
// spawns a child that can report its scoped service resolution.
type parentProjectActor struct {
	actor.Host
	childReady chan ref.Ref
}

func (a *parentProjectActor) OnStart(ctx actor.Context) error {
	if err := ctx.Register("project.ping", func(_ actor.Context, _ []byte) ([]byte, error) {
		return []byte("pong"), nil
	}); err != nil {
		return err
	}
	if err := ctx.RegisterDomain("project").ExposeChildren(); err != nil {
		return err
	}
	child, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &childProjectActor{}
	}), "child")
	if err != nil {
		return err
	}
	if a.childReady != nil {
		a.childReady <- child
	}
	return nil
}

type childProjectActor struct {
	actor.Host
}

func (a *childProjectActor) OnStart(ctx actor.Context) error {
	return ctx.Register("child.report", func(ctx actor.Context, _ []byte) ([]byte, error) {
		if r, ok := ctx.LookupService("project"); ok {
			return []byte(r.ID().String()), nil
		}
		return nil, nil
	})
}

type rootSpawnsParent struct {
	actor.Host
	parentFactory func() actor.Actor
	parentReady   chan ref.Ref
	exposeName    string
	exposed       chan struct{}
}

func (r *rootSpawnsParent) OnStart(ctx actor.Context) error {
	if r.parentReady == nil {
		r.parentReady = make(chan ref.Ref, 1)
	}
	if r.exposeName != "" {
		if err := ctx.RegisterDomain(r.exposeName).Expose(); err != nil {
			return err
		}
		if r.exposed != nil {
			close(r.exposed)
		}
	}
	parentRef, err := ctx.Spawn(actor.PropsFromFunc(r.parentFactory), "parent")
	if err != nil {
		return err
	}
	r.parentReady <- parentRef
	return nil
}

func TestExposeToChildrenResolvesInDescendant(t *testing.T) {
	childReady := make(chan ref.Ref, 1)
	parentFactory := func() actor.Actor {
		return &parentProjectActor{childReady: childReady}
	}
	root := &rootSpawnsParent{parentFactory: parentFactory}

	a, err := app.New(
		app.WithNamespace("testscoped"),
		app.WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	awaitRootStarted(t, a, func() { go func() { done <- a.Run(ctx) }() })

	var parentRef ref.Ref
	select {
	case parentRef = <-root.parentReady:
	case <-time.After(2 * time.Second):
		t.Fatal("parent was not spawned")
	}
	waitForCellStarted(t, a, parentRef)

	var childRef ref.Ref
	select {
	case childRef = <-childReady:
	case <-time.After(2 * time.Second):
		t.Fatal("child was not spawned")
	}
	waitForCellStarted(t, a, childRef)

	resolvedID := invokeUnary(t, context.Background(), childRef, "child.report", nil, 2*time.Second)
	if resolvedID == nil {
		t.Fatal("child did not resolve scoped service")
	}
	if string(resolvedID.([]byte)) != parentRef.ID().String() {
		t.Fatalf("child resolved to %v, want parent %v", resolvedID, parentRef.ID().String())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("app did not shut down")
	}
}

type conflictingParentActor struct {
	actor.Host
	rootExposed chan struct{}
	errCh       chan error
}

func (a *conflictingParentActor) OnStart(ctx actor.Context) error {
	if a.rootExposed != nil {
		select {
		case <-a.rootExposed:
		case <-time.After(2 * time.Second):
			return errors.New("timed out waiting for root global expose")
		}
	}
	err := ctx.RegisterDomain("project").ExposeChildren()
	if a.errCh != nil {
		a.errCh <- err
	}
	return err
}

func TestExposeToChildrenConflictsWithGlobal(t *testing.T) {
	exposed := make(chan struct{})
	errCh := make(chan error, 1)
	parentFactory := func() actor.Actor {
		return &conflictingParentActor{rootExposed: exposed, errCh: errCh}
	}
	root := &rootSpawnsParent{
		parentFactory: parentFactory,
		exposeName:    "project",
		exposed:       exposed,
	}

	a, err := app.New(
		app.WithNamespace("testscopedconflict"),
		app.WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	awaitRootStarted(t, a, func() { go func() { done <- a.Run(ctx) }() })

	select {
	case err := <-errCh:
		if !errors.Is(err, service.ErrScopedConflict) {
			t.Fatalf("expected scoped conflict, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected scoped conflict, got none")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("app did not shut down")
	}
}
