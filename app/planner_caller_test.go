package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/ref"
)

// plannerCalleeActor exposes the "callee" service and reports its view of
// the caller.
type plannerCalleeActor struct {
	actor.Host
}

func (a *plannerCalleeActor) OnStart(ctx actor.Context) error {
	if err := ctx.RegisterDomain("callee").Expose(); err != nil {
		return err
	}
	if err := ctx.Register("test.who", func(c actor.Context, _ []byte) (string, error) {
		if c.Caller() == nil {
			return "<nil-caller>", nil
		}
		return c.Caller().ID().String(), nil
	}); err != nil {
		return err
	}
	return ctx.Register("test.names", func(_ actor.Context, _ []byte) (string, error) {
		return "README.md\napp.go", nil
	})
}

// plannerCallerActor calls the "callee" service through its planner and
// relays the caller identity the callee observed.
type plannerCallerActor struct {
	actor.Host
}

func (a *plannerCallerActor) OnStart(ctx actor.Context) error {
	if err := ctx.Register("test.kick", func(c actor.Context, _ []byte) (string, error) {
		svcRef, ok := c.LookupService("callee")
		if !ok {
			return "", fmt.Errorf("callee service not found")
		}
		callCtx, cancel := context.WithTimeout(c.Lifecycle(), 2*time.Second)
		defer cancel()
		result, err := c.Planner().Call(callCtx, svcRef, "test.who", []byte("go")).Await()
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
	}); err != nil {
		return err
	}
	return ctx.Register("test.rawtype", func(c actor.Context, _ []byte) (string, error) {
		svcRef, ok := c.LookupService("callee")
		if !ok {
			return "", fmt.Errorf("callee service not found")
		}
		callCtx, cancel := context.WithTimeout(c.Lifecycle(), 2*time.Second)
		defer cancel()
		result, err := c.Planner().Call(callCtx, svcRef, "test.names", []byte("go")).Await()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%T|%v", result, result), nil
	})
}

// plannerRootActor spawns the callee and the planner-enabled caller, keeping
// the caller ref for the test driver. ready signals the test goroutine after
// OnStart publishes the ref — reading CallerRef off the system lane without
// that handoff is a data race.
type plannerRootActor struct {
	actor.Host
	CallerRef            ref.Ref
	ready                chan struct{}
	waitForCallerStarted func(*testing.T)
}

func (a *plannerRootActor) OnStart(ctx actor.Context) error {
	if _, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return &plannerCalleeActor{} }), "callee"); err != nil {
		return err
	}
	caller, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return &plannerCallerActor{} }).WithPlanner(), "caller")
	if err != nil {
		return err
	}
	// The caller registers its callables in its own OnStart; a Spawn in
	// root OnStart only starts the child — it does not wait. The test
	// driver waits via waitForCellStart before issuing Invoke.
	a.CallerRef = caller
	close(a.ready)
	return nil
}

// Caller returns the spawned caller ref, waiting for OnStart to publish it
// and for the caller cell to finish registering its callables.
func (a *plannerRootActor) Caller(t *testing.T) ref.Ref {
	t.Helper()
	select {
	case <-a.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("root did not spawn caller actor in time")
	}
	a.waitForCallerStarted(t)
	return a.CallerRef
}

func runPlannerApp(t *testing.T) (*plannerRootActor, func()) {
	t.Helper()
	root := &plannerRootActor{ready: make(chan struct{})}

	a, err := New(
		WithNamespace("test"),
		WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	impl := a.(*appImpl)
	root.waitForCallerStarted = func(t *testing.T) {
		if err := impl.waitForCellStart(root.CallerRef); err != nil {
			t.Fatalf("caller start: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Wait for the root actor (and its spawned children) to start.
	root.Caller(t)

	return root, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}
}

// TestPlannerCallCarriesCallerIdentity verifies that a planner.Call issued
// from an actor's context delivers that actor as ctx.Caller() in the target
// handler. Regression test for the caller-identity loss where service calls
// arrived with the target itself as sender, breaking caller-scoped
// authorization in the target (e.g. worktree binding by caller).
func TestPlannerCallCarriesCallerIdentity(t *testing.T) {
	root, shutdown := runPlannerApp(t)
	defer shutdown()
	caller := root.Caller(t)

	stream := caller.Invoke(context.Background(), "test.kick", []byte("go"))
	if stream == nil {
		t.Fatal("Invoke returned nil stream")
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	stream.Close()

	want := caller.ID().String()
	s, ok := resp.(string)
	if !ok || s != want {
		t.Fatalf("callee observed caller %v (%T), want %q (string)", resp, resp, want)
	}
}

// TestPlannerCallStringReplyDecodesAsString verifies that the raw result of a
// planner.Call to a string-returning handler is a Go string on the caller
// side. String replies used to take a raw Value-mode fast path that surfaced
// as []byte across the transport (breaking `.(string)` assertions in callers,
// e.g. appmanager dev_gate's project.list round-trip); they must round-trip
// through the codec with the builtin string schema like every other type.
func TestPlannerCallStringReplyDecodesAsString(t *testing.T) {
	root, shutdown := runPlannerApp(t)
	defer shutdown()
	caller := root.Caller(t)

	stream := caller.Invoke(context.Background(), "test.rawtype", []byte("go"))
	if stream == nil {
		t.Fatal("Invoke returned nil stream")
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	stream.Close()

	s, ok := resp.(string)
	if !ok {
		t.Fatalf("test.rawtype reply = %v (%T), want string", resp, resp)
	}
	if s != "string|README.md\napp.go" {
		t.Fatalf("planner.Call result type/value = %q, want %q", s, "string|README.md\napp.go")
	}
}
