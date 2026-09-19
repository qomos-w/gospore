package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
)

func TestCellStatsCallable(t *testing.T) {
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

	// The root's OnStart closes `ready` before the cell transitions to
	// "started" (marking happens after OnStart returns). Poll the stats
	// callable until the cell reports started instead of asserting once —
	// a single-shot read races the transition.
	var stats CellStats
	deadline := time.Now().Add(2 * time.Second)
	for {
		stream := root.Invoke(context.Background(), cellStatsCallID, cellStatsReq{ActorPath: path})
		val, err := stream.Recv()
		stream.Close()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		stats = decodeCellStats(t, val)
		if stats.State == "started" || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if stats.ActorID != path {
		t.Errorf("ActorID = %q, want %q", stats.ActorID, path)
	}
	if stats.State != "started" {
		t.Errorf("State = %q, want started", stats.State)
	}
	if stats.OwnerQueue.Capacity != 256 {
		t.Errorf("OwnerQueue.Capacity = %d, want 256", stats.OwnerQueue.Capacity)
	}
	if stats.SystemQueue.Capacity != 64 {
		t.Errorf("SystemQueue.Capacity = %d, want 64", stats.SystemQueue.Capacity)
	}
	if stats.ReplyQueue.Capacity != 64 {
		t.Errorf("ReplyQueue.Capacity = %d, want 64", stats.ReplyQueue.Capacity)
	}
	// A freshly started root actor has no burst of work. PendingInvokes may
	// include the very call executing this stats request, so it is >= 0.
	if stats.PendingInvokes < 0 {
		t.Errorf("PendingInvokes = %d, want >= 0", stats.PendingInvokes)
	}
}

func TestCellStatsCallable_NotFound(t *testing.T) {
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
	stream := root.Invoke(context.Background(), cellStatsCallID, cellStatsReq{ActorPath: "nonexistent-actor-xxx"})
	defer stream.Close()

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for non-existent actor, got nil")
	}
}

func decodeCellStats(t *testing.T, val any) CellStats {
	t.Helper()
	var raw []byte
	switch v := val.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		// Already decoded into a map or struct.
		b, err := json.Marshal(val)
		if err != nil {
			t.Fatalf("re-marshal cell stats %T: %v", val, err)
		}
		raw = b
	}
	var stats CellStats
	if err := json.Unmarshal(raw, &stats); err != nil {
		t.Fatalf("unmarshal cell stats %q: %v", string(raw), err)
	}
	return stats
}

// TestCellStatsCallable_Lanes verifies the additive per-lane observability
// block: builtin lanes always appear with their capacities, and the pure
// lane carries timing entries for the stats call itself (which executes as
// a stateless handler). Consumers ignoring the field are unaffected — the
// JSON shape is purely additive.
func TestCellStatsCallable_Lanes(t *testing.T) {
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

	// The ring record for a stats call is written shortly after its reply
	// is delivered, so poll until a completed gospore.cell.stats execution
	// shows up in the pure lane (the callable is stateless and never
	// queues — waitNs stays 0, execNs must be positive).
	var stats CellStats
	foundStatsCall := false
	deadline := time.Now().Add(2 * time.Second)
	for !foundStatsCall && time.Now().Before(deadline) {
		stream := root.Invoke(context.Background(), cellStatsCallID, cellStatsReq{ActorPath: path})
		val, err := stream.Recv()
		stream.Close()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		stats = decodeCellStats(t, val)
		for _, r := range laneByName(t, stats, "pure").Timing.Recent {
			if r.CallID == cellStatsCallID && r.ExecNs > 0 {
				foundStatsCall = true
			}
		}
	}
	if !foundStatsCall {
		t.Fatal("pure lane recent ring never recorded a gospore.cell.stats execution")
	}

	if len(stats.Lanes) == 0 {
		t.Fatal("stats.Lanes empty, want builtin lanes")
	}
	owner := laneByName(t, stats, "owner")
	if owner.Capacity != 256 {
		t.Errorf("owner lane capacity = %d, want 256", owner.Capacity)
	}
	// Lane order must be stable (sorted by name).
	for i := 1; i < len(stats.Lanes); i++ {
		if stats.Lanes[i-1].Name >= stats.Lanes[i].Name {
			t.Fatalf("lanes not sorted: %q >= %q", stats.Lanes[i-1].Name, stats.Lanes[i].Name)
		}
	}
}

func laneByName(t *testing.T, stats CellStats, name string) LaneStats {
	t.Helper()
	for _, l := range stats.Lanes {
		if l.Name == name {
			return l
		}
	}
	t.Fatalf("lane %q missing from stats.Lanes", name)
	return LaneStats{}
}
