package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
)

type projectionRootActor struct {
	actor.Host
	Counter int `gospore:"component,public"`
	ready   chan struct{}
}

func (a *projectionRootActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := ctx.Register("test.inc", func(ctx actor.Context, req incReq) (incRes, error) {
		a.Counter += req.Delta
		return incRes{Value: a.Counter}, nil
	}); err != nil {
		return err
	}
	if a.ready != nil {
		close(a.ready)
	}
	return nil
}

type incReq struct {
	Delta int `json:"delta"`
}

type incRes struct {
	Value int `json:"value"`
}

func TestProjectionGetCallable(t *testing.T) {
	ready := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &projectionRootActor{ready: ready} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not start")
	}

	root := app.Self()
	path := root.ID().String()

	getStream := root.Invoke(context.Background(), projectionGetCallID, projectionGetReq{
		ActorPath: path,
		Component: "Counter",
		SchemaID:  0,
	})
	defer getStream.Close()

	val, err := getStream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	// Scalar components are returned as the raw value; initially Counter is 0.
	got := unpackScalarInt(t, val)
	if got != 0 {
		t.Fatalf("got %d, want 0", got)
	}
}

func TestProjectionWatchCallable(t *testing.T) {
	ready := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &projectionRootActor{ready: ready} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not start")
	}

	root := app.Self()
	path := root.ID().String()

	// Materialise the snapshot first so watch receives an initial full snapshot.
	getStream := root.Invoke(context.Background(), projectionGetCallID, projectionGetReq{
		ActorPath: path,
		Component: "Counter",
		SchemaID:  0,
	})
	if _, err := getStream.Recv(); err != nil {
		t.Fatalf("get Recv: %v", err)
	}
	_ = getStream.Close()

	watchStream := root.Invoke(context.Background(), projectionWatchCallID, projectionWatchReq{
		ActorPath: path,
		Component: "Counter",
		SchemaID:  0,
	})
	defer watchStream.Close()

	// Consume the initial full snapshot.
	chunk, err := watchStream.Recv()
	if err != nil {
		t.Fatalf("watch initial Recv: %v", err)
	}
	if got := unpackScalarInt(t, chunk); got != 0 {
		t.Fatalf("initial value = %d, want 0", got)
	}

	// Trigger a state change so projection emits a delta update.
	incStream := root.Invoke(context.Background(), "test.inc", incReq{Delta: 1})
	if _, err := incStream.Recv(); err != nil {
		t.Fatalf("inc Recv: %v", err)
	}
	_ = incStream.Close()

	chunk, err = watchStream.Recv()
	if err != nil {
		t.Fatalf("watch delta Recv: %v", err)
	}
	got := unpackScalarInt(t, chunk)
	if got != 1 {
		t.Fatalf("got %d, want 1", got)
	}
}

func unpackScalarInt(t *testing.T, val any) int {
	t.Helper()
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case []byte:
		var n int
		if err := json.Unmarshal(v, &n); err != nil {
			t.Fatalf("unmarshal scalar bytes %q: %v", v, err)
		}
		return n
	default:
		t.Fatalf("unexpected scalar type %T: %v", val, val)
		return 0
	}
}

// TestProjectionWatchDoesNotBlockOwnerLane guards the OwnerLane constraint:
// while a gospore.projection.watch stream is active (drain idle inside
// sub.Recv with no further updates), the root owner lane must keep serving
// other callables. gospore.cell.stats (stateless) and the stateful test.inc
// handler must both complete within the timeout, and the watch must still
// deliver the projection delta triggered by test.inc afterwards.
func TestProjectionWatchDoesNotBlockOwnerLane(t *testing.T) {
	ready := make(chan struct{})
	app, err := New(
		WithNamespace("testapp"),
		WithRuntimeSlot(1),
		WithRootActor(func() actor.Actor { return &projectionRootActor{ready: ready} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = app.Run(ctx) }()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("root actor did not start")
	}

	root := app.Self()
	path := root.ID().String()

	// Materialise the snapshot first so watch receives an initial full snapshot.
	getStream := root.Invoke(context.Background(), projectionGetCallID, projectionGetReq{
		ActorPath: path,
		Component: "Counter",
		SchemaID:  0,
	})
	if _, err := getStream.Recv(); err != nil {
		t.Fatalf("get Recv: %v", err)
	}
	_ = getStream.Close()

	// Open the watch and consume the initial full snapshot. From this point
	// the watch drain sits idle inside sub.Recv() — exactly the state that
	// must not occupy the root owner lane.
	watchStream := root.Invoke(context.Background(), projectionWatchCallID, projectionWatchReq{
		ActorPath: path,
		Component: "Counter",
		SchemaID:  0,
	})
	defer watchStream.Close()
	if chunk, err := watchStream.Recv(); err != nil {
		t.Fatalf("watch initial Recv: %v", err)
	} else if got := unpackScalarInt(t, chunk); got != 0 {
		t.Fatalf("initial value = %d, want 0", got)
	}

	// While the watch is active, gospore.cell.stats must stay responsive.
	statsDone := make(chan error, 1)
	go func() {
		s := root.Invoke(context.Background(), cellStatsCallID, cellStatsReq{ActorPath: path})
		_, err := s.Recv()
		_ = s.Close()
		statsDone <- err
	}()
	select {
	case err := <-statsDone:
		if err != nil {
			t.Fatalf("cell.stats Recv during active watch: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gospore.cell.stats blocked by active projection.watch")
	}

	// While the watch is active, a stateful root callable must stay responsive
	// — the direct owner-lane starvation probe.
	incDone := make(chan error, 1)
	go func() {
		s := root.Invoke(context.Background(), "test.inc", incReq{Delta: 1})
		_, err := s.Recv()
		_ = s.Close()
		incDone <- err
	}()
	select {
	case err := <-incDone:
		if err != nil {
			t.Fatalf("test.inc Recv during active watch: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stateful callable test.inc blocked by active projection.watch")
	}

	// The watch drain is still alive: the Counter change made by test.inc
	// must arrive on the watch stream as a delta.
	type deltaResult struct {
		val any
		err error
	}
	deltaCh := make(chan deltaResult, 1)
	go func() {
		val, err := watchStream.Recv()
		deltaCh <- deltaResult{val, err}
	}()
	select {
	case r := <-deltaCh:
		if r.err != nil {
			t.Fatalf("watch delta Recv: %v", r.err)
		}
		if got := unpackScalarInt(t, r.val); got != 1 {
			t.Fatalf("watch delta after inc = %d, want 1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch drain dead after owner-lane probe")
	}
}
