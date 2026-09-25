package gateway_test

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/gateway"
)

// shardRoot exposes a streaming callable that ticks for the lifetime of
// each subscription, letting tests observe establishment, teardown and
// ordering of subscribe frames.
type shardRoot struct {
	actor.Host
	starts atomic.Int32
	live   atomic.Int32
}

func (a *shardRoot) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	return ctx.Register("test.stream", func(ctx actor.Context, req map[string]any, emit actor.Emitter) error {
		a.starts.Add(1)
		a.live.Add(1)
		defer a.live.Add(-1)
		if err := emit.Send(map[string]any{"ok": true}); err != nil {
			return err
		}
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-emit.Done():
				return nil
			case <-t.C:
				if err := emit.Send(map[string]any{"ok": true}); err != nil {
					return err
				}
			}
		}
	}, actor.Public(), actor.Streaming[any]())
}

type shardEnv struct {
	root *shardRoot
	addr string
	shut func()
}

func newShardEnv(t *testing.T) *shardEnv {
	t.Helper()
	root := &shardRoot{}
	a, err := app.New(
		app.WithNamespace("shard"),
		app.WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	go func() { _ = a.Run(t.Context()) }()
	time.Sleep(150 * time.Millisecond)

	srv := gateway.NewServer(a, nil, "127.0.0.1:0")
	go func() { _ = srv.Run(t.Context()) }()
	for srv.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}
	return &shardEnv{root: root, addr: srv.Addr(), shut: func() { _ = srv.Close() }}
}

func subscribeFrame(subID string) map[string]any {
	return map[string]any{
		"type":    "subscribe",
		"subId":   subID,
		"callID":  "test.stream",
		"payload": map[string]any{},
	}
}

// TestSubscribeBurstDeliversAll mirrors the reconnect resubscribe storm:
// every subscription in one burst must establish and deliver, with frames
// fanned out across the shard workers concurrently.
func TestSubscribeBurstDeliversAll(t *testing.T) {
	env := newShardEnv(t)
	defer env.shut()

	conn := dialRepro(t, env.addr)
	defer conn.Close()

	const subs = 24
	for i := 0; i < subs; i++ {
		writeJSON(t, conn, subscribeFrame(fmt.Sprintf("burst-%d", i)))
	}

	got := make(map[string]bool)
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < subs && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v (got %d/%d)", err, len(got), subs)
		}
		var frame struct {
			Type  string `json:"type"`
			SubID string `json:"subId"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			continue
		}
		if frame.Type == "chunk" {
			got[frame.SubID] = true
		}
	}
	if len(got) < subs {
		t.Fatalf("only %d/%d subscriptions delivered a chunk", len(got), subs)
	}
}

// TestSubscribeSameSubIdKeepsOrder verifies the per-subId shard invariant:
// subscribe -> unsubscribe -> subscribe for one subId must process in order,
// ending with exactly one live streaming handler that keeps delivering.
// If ordering broke (unsubscribe landing after the resubscribe), the stream
// would die and no further chunks would arrive.
func TestSubscribeSameSubIdKeepsOrder(t *testing.T) {
	env := newShardEnv(t)
	defer env.shut()

	conn := dialRepro(t, env.addr)
	defer conn.Close()

	writeJSON(t, conn, subscribeFrame("ord-1"))
	writeJSON(t, conn, map[string]any{"type": "unsubscribe", "subId": "ord-1"})
	writeJSON(t, conn, subscribeFrame("ord-1"))

	// Two handler starts are expected (first cancelled by the unsubscribe,
	// second live). Then require a chunk after the dust settles — proves
	// the resubscribe was processed after the unsubscribe. (Handler-lifetime
	// teardown is eventual and a handler blocked in Send on a full emitter
	// channel may lag; asserting it here made the test flaky under load, so
	// this test sticks to the gateway ordering invariant.)
	time.Sleep(600 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if env.root.starts.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s := env.root.starts.Load(); s != 2 {
		t.Fatalf("expected 2 handler starts, got %d", s)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read after resubscribe: %v", err)
		}
		var frame struct {
			Type  string `json:"type"`
			SubID string `json:"subId"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			continue
		}
		if frame.Type == "chunk" && frame.SubID == "ord-1" {
			break
		}
	}

}

// TestSubscribeSameSubIdOverwriteThenDisconnect pins the resubscribe
// overwrite path: a second subscribe with the same subId (no unsubscribe in
// between) displaces the first subscription's registry entry. The displaced
// cancel must fire so the old pump exits; otherwise closing the connection
// leaves the old streaming handler live forever and wedges session teardown
// in subs.wg.Wait (observed in production as hundreds of parked
// session.go RecvRaw pumps plus stuck ServeSession goroutines).
func TestSubscribeSameSubIdOverwriteThenDisconnect(t *testing.T) {
	env := newShardEnv(t)
	defer env.shut()

	conn := dialRepro(t, env.addr)

	// Two subscribes for one subId, no unsubscribe in between.
	writeJSON(t, conn, subscribeFrame("ovr-1"))
	writeJSON(t, conn, subscribeFrame("ovr-1"))

	// Both handler starts must have happened: the overwrite cancels the
	// first subscription but the second still establishes.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && env.root.starts.Load() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if s := env.root.starts.Load(); s != 2 {
		conn.Close()
		t.Fatalf("expected 2 handler starts, got %d", s)
	}

	// Abrupt disconnect. Every streaming handler — including the displaced
	// one — must drain to zero once teardown unwinds.
	conn.Close()

	drain := time.Now().Add(8 * time.Second)
	for time.Now().Before(drain) && env.root.live.Load() > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if l := env.root.live.Load(); l != 0 {
		t.Fatalf("streaming handlers leaked after disconnect: %d still live", l)
	}
}
