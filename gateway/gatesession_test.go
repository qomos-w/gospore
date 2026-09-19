package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/gateway"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/cell"
)

// gateEnv drives a root actor whose "tick" events carry a large blob, so a
// client that stops reading blows past the per-connection write buffer
// budget (force-close) within seconds, while an active client keeps
// consuming the same stream.
type gateEnv struct {
	app    app.App
	addr   string
	rootID string
	shut   func()

	emitCtl chan struct{}
	emitWG  sync.WaitGroup
}

func (e *gateEnv) startEmit(period time.Duration, blobKB int) {
	e.emitWG.Add(1)
	go func() {
		defer e.emitWG.Done()
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		root := e.app.Self()
		blob := strings.Repeat("x", blobKB*1024)
		for {
			select {
			case <-e.emitCtl:
				return
			case <-ticker.C:
				c := root.Invoke(context.Background(), "test.emit", map[string]any{"blob": blob})
				if _, err := c.RecvRaw(); err != nil {
					fmt.Printf("[gate] test.emit err=%v\n", err)
				}
				_ = c.Close()
			}
		}
	}()
}

func (e *gateEnv) stop() {
	close(e.emitCtl)
	e.emitWG.Wait()
	e.shut()
}

func newGateEnv(t *testing.T, period time.Duration, blobKB int) *gateEnv {
	t.Helper()
	root := func() actor.Actor {
		return &gateRoot{}
	}
	a, err := app.New(
		app.WithNamespace("gatetest"),
		app.WithRootActor(root),
		// Keep ring replay bounded: full replay is blobKB*64 KiB, well
		// under the 32 MiB write-buffer budget, so only a client that
		// stops reading trips force-close.
		app.WithEventRingCapacity(64),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	time.Sleep(150 * time.Millisecond)

	srv := gateway.NewServer(a, nil, "127.0.0.1:0")
	srv.SetLogger(cell.NewDefaultLogger("gwgw", nil))
	ready := srv.ReadyChan()
	go func() { _ = srv.Run(context.Background()) }()
	<-ready
	env := &gateEnv{
		app:     a,
		addr:    srv.Addr(),
		rootID:  a.Self().ID().String(),
		shut:    func() { _ = srv.Close() },
		emitCtl: make(chan struct{}),
	}
	env.startEmit(period, blobKB)
	return env
}

type gateRoot struct {
	actor.Host
}

func (a *gateRoot) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := ctx.RegisterEventKind("tick", map[string]any{}, actor.Public()); err != nil {
		return err
	}
	if err := ctx.Register("test.emit", func(ctx actor.Context, req map[string]any) error {
		return ctx.EmitEvent("tick", req)
	}); err != nil {
		return err
	}
	if err := ctx.Register("echo", func(ctx actor.Context, req map[string]any) (map[string]any, error) {
		return req, nil
	}, actor.Public()); err != nil {
		return err
	}
	return nil
}

// gateDiff counts root children (gatesession cells are root children).
func gateDiff(t *testing.T, e *gateEnv, base int) int {
	t.Helper()
	return len(e.app.Tree().Children(e.app.Self())) - base
}

func gateWaitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func gateDial(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func gateWrite(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	if err := conn.WriteJSON(v); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// gateActive continuously reads tick chunks and probes echo latency.
type gateActive struct {
	conn       *websocket.Conn
	closeOnce  sync.Once
	chunks     atomic.Int64
	lastSeq    atomic.Int64
	maxGapMs   atomic.Int64
	lastChunk  atomic.Int64 // unix ms of last chunk
	maxEchoMs  atomic.Int64
	sumEchoMs  atomic.Int64
	echoCount  atomic.Int64
	replies    map[int64]chan string
	mu         sync.Mutex
	stop       chan struct{}
	done       chan struct{}
}

func newGateActive(t *testing.T, e *gateEnv) *gateActive {
	c := &gateActive{
		conn:   gateDial(t, e.addr),
		replies: make(map[int64]chan string),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	c.lastChunk.Store(time.Now().UnixMilli())
	gateWrite(t, c.conn, map[string]any{
		"type":    "subscribe",
		"subId":   "active-1",
		"callID":  "gospore.events.subscribe_instance",
		"payload": map[string]any{"actorId": e.rootID, "kind": "tick"},
	})
	go c.readLoop()
	return c
}

func (c *gateActive) readLoop() {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		c.conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var frame struct {
			Type    string          `json:"type"`
			ReqID   int64           `json:"reqId"`
			SubID   string          `json:"subId"`
			SeqNo   int64           `json:"seqNo"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			continue
		}
		switch frame.Type {
		case "chunk":
			now := time.Now().UnixMilli()
			gap := now - c.lastChunk.Load()
			for {
				old := c.maxGapMs.Load()
				if gap <= old || c.maxGapMs.CompareAndSwap(old, gap) {
					break
				}
			}
			c.lastChunk.Store(now)
			c.chunks.Add(1)
			c.lastSeq.Store(frame.SeqNo)
		case "reply", "error":
			c.mu.Lock()
			ch, ok := c.replies[frame.ReqID]
			delete(c.replies, frame.ReqID)
			c.mu.Unlock()
			if ok {
				ch <- frame.Type
			}
		}
	}
}

func (c *gateActive) echo(t *testing.T) time.Duration {
	t.Helper()
	reqID := time.Now().UnixNano()
	ch := make(chan string, 1)
	c.mu.Lock()
	c.replies[reqID] = ch
	c.mu.Unlock()
	start := time.Now()
	gateWrite(t, c.conn, map[string]any{
		"type":    "invoke",
		"reqId":   reqID,
		"callID":  "echo",
		"payload": map[string]any{"probe": reqID},
	})
	select {
	case typ := <-ch:
		d := time.Since(start)
		if typ == "error" {
			t.Fatalf("active echo returned error frame")
		}
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("active echo timed out after 10s")
		return 0
	}
}

func (c *gateActive) shutdown() {
	c.closeOnce.Do(func() {
		close(c.stop)
		_ = c.conn.Close()
	})
	<-c.done
}

// TestGateSessionSlowReaderIsolatedFromActiveClient is the topology repro:
// one ws session that stops reading entirely (its gatesession cell, reply
// pipeline and write buffer are its own), one active session on the same
// stream. The active session's invoke replies and subscription chunks must
// keep flowing; the stalled session is force-closed and recovers via
// reconnect + sinceSeqNo replay.
func TestGateSessionSlowReaderIsolatedFromActiveClient(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running topology test")
	}
	// 64 KiB ticks every 5ms ≈ 13 MB/s: the stalled reader crosses the
	// 32 MiB write-buffer budget in ~3s; the active reader keeps up.
	e := newGateEnv(t, 5*time.Millisecond, 64)
	defer e.stop()

	base := len(e.app.Tree().Children(e.app.Self()))

	active := newGateActive(t, e)
	defer active.shutdown()
	gateWaitFor(t, "first active chunk", 10*time.Second, func() bool { return active.chunks.Load() > 0 })
	gateWaitFor(t, "active gate spawned", 10*time.Second, func() bool { return gateDiff(t, e, base) >= 1 })

	// Stalled reader: subscribes (full ring replay lands immediately),
	// then never reads a single frame. Its TCP receive window closes; the
	// platform may reset the conn outright (observed on Windows loopback)
	// or the 32 MiB overflow budget force-closes it — either way the
	// session must unwind without touching the active one.
	stalled := gateDial(t, e.addr)
	if tc, ok := stalled.UnderlyingConn().(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(16 * 1024)
	}
	gateWrite(t, stalled, map[string]any{
		"type":    "subscribe",
		"subId":   "stalled-1",
		"callID":  "gospore.events.subscribe_instance",
		"payload": map[string]any{"actorId": e.rootID, "kind": "tick"},
	})

	// Probe the active session for the whole stall window. Every echo
	// must stay well under the 10s call timeout; in practice it is
	// milliseconds.
	probeStart := time.Now()
	for time.Since(probeStart) < 12*time.Second {
		d := active.echo(t)
		ms := d.Milliseconds()
		active.echoCount.Add(1)
		active.sumEchoMs.Add(ms)
		for {
			old := active.maxEchoMs.Load()
			if ms <= old || active.maxEchoMs.CompareAndSwap(old, ms) {
				break
			}
		}
		if d > 1*time.Second {
			t.Fatalf("active echo latency during stalled-reader window: %v", d)
		}
		time.Sleep(150 * time.Millisecond)
	}
	if active.chunks.Load() == 0 {
		t.Fatal("active client received no chunks during stalled-reader window")
	}
	if active.maxGapMs.Load() > 2000 {
		t.Fatalf("active chunk gap during stalled-reader window: %dms", active.maxGapMs.Load())
	}

	// The stalled session must be gone: its conn is dead (reset by the
	// platform or force-closed on overflow budget) and its gatesession
	// cell has left the tree.
	gateWaitFor(t, "stalled session unwound (gate destroyed)", 40*time.Second, func() bool {
		return gateDiff(t, e, base) == 1
	})

	// Reconnect + replay recovery: resubscribe from the last sequence
	// the active client observed; chunks must resume with higher seqNos.
	resume := gateDial(t, e.addr)
	defer resume.Close()
	since := active.lastSeq.Load()
	gateWrite(t, resume, map[string]any{
		"type":       "subscribe",
		"subId":      "resumed-1",
		"callID":     "gospore.events.subscribe_instance",
		"payload":    map[string]any{"actorId": e.rootID, "kind": "tick"},
		"sinceSeqNo": since,
	})
	resumed := int64(0)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && resumed < 10 {
		resume.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg, err := resume.ReadMessage()
		if err != nil {
			t.Fatalf("resumed read: %v", err)
		}
		var frame struct {
			Type  string `json:"type"`
			SeqNo int64  `json:"seqNo"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			continue
		}
		if frame.Type == "chunk" {
			if frame.SeqNo <= since {
				t.Fatalf("replayed chunk seqNo %d not greater than sinceSeqNo %d", frame.SeqNo, since)
			}
			resumed++
		}
	}
	if resumed < 10 {
		t.Fatalf("reconnect replay delivered only %d chunks", resumed)
	}

	avg := time.Duration(0)
	if n := active.echoCount.Load(); n > 0 {
		avg = time.Duration(active.sumEchoMs.Load()/n) * time.Millisecond
	}
	t.Logf("active session during stalled-reader window: echoes=%d avg=%v max=%v; chunks=%d maxGap=%dms",
		active.echoCount.Load(), avg, time.Duration(active.maxEchoMs.Load())*time.Millisecond,
		active.chunks.Load(), active.maxGapMs.Load())
}

// TestGateSessionTeardownUnwindsAndFlushes kills a subscribed connection
// abruptly and verifies the full unwind: the gatesession cell leaves the
// tree (which requires the workers to have exited — a worker hung in
// RecvRaw would block the destroy), its ref stops resolving, and no
// goroutines leak.
func TestGateSessionTeardownUnwindsAndFlushes(t *testing.T) {
	e := newGateEnv(t, 20*time.Millisecond, 1)
	defer e.stop()

	base := len(e.app.Tree().Children(e.app.Self()))
	goroutineBase := runtime.NumGoroutine()

	client := gateDial(t, e.addr)
	gateWrite(t, client, map[string]any{
		"type":    "subscribe",
		"subId":   "teardown-1",
		"callID":  "gospore.events.subscribe_instance",
		"payload": map[string]any{"actorId": e.rootID, "kind": "tick"},
	})
	client.SetReadDeadline(time.Now().Add(10 * time.Second))
	sawChunk := false
	for !sawChunk {
		_, msg, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("client read before teardown: %v", err)
		}
		var frame struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(msg, &frame) == nil && frame.Type == "chunk" {
			sawChunk = true
		}
	}

	gateWaitFor(t, "gate spawned", 10*time.Second, func() bool { return gateDiff(t, e, base) == 1 })
	children := e.app.Tree().Children(e.app.Self())
	var gateID id.ActorID
	found := false
	for _, r := range children {
		gateID = r.ID()
		found = true
	}
	if !found {
		t.Fatal("no gate child found")
	}

	// Abrupt TCP close (RST, no ws handshake).
	if tc, ok := client.UnderlyingConn().(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = client.Close()

	gateWaitFor(t, "gate destroyed after abrupt close", 20*time.Second, func() bool { return gateDiff(t, e, base) == 0 })

	// The destroyed gate must no longer resolve in the tree.
	if _, ok := e.app.Tree().LookupID(gateID); ok {
		t.Fatal("destroyed gate ref still resolves")
	}

	// Session goroutines (workers, shards, read loop, cell loops) must
	// have unwound.
	time.Sleep(2 * time.Second)
	gateWaitFor(t, "goroutines back to baseline", 15*time.Second, func() bool {
		return runtime.NumGoroutine() <= goroutineBase+3
	})

	// A fresh client works immediately after.
	fresh := gateDial(t, e.addr)
	defer fresh.Close()
	gateWrite(t, fresh, map[string]any{
		"type":    "invoke",
		"reqId":   int64(1),
		"callID":  "echo",
		"payload": map[string]any{"after": "teardown"},
	})
	fresh.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		_, msg, err := fresh.ReadMessage()
		if err != nil {
			t.Fatalf("fresh client read: %v", err)
		}
		var frame struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(msg, &frame) != nil {
			continue
		}
		if frame.Type == "reply" {
			break
		}
		if frame.Type == "error" {
			t.Fatal("fresh client echo errored after teardown")
		}
	}
}

// TestGateSessionConcurrentConnectsUniqueGates connects 8 sessions in
// parallel: every spawn must succeed with a unique child (a name collision
// would leave that session on the root-caller fallback), and all gates are
// destroyed again on disconnect.
func TestGateSessionConcurrentConnectsUniqueGates(t *testing.T) {
	e := newGateEnv(t, 50*time.Millisecond, 1)
	defer e.stop()

	base := len(e.app.Tree().Children(e.app.Self()))

	const n = 8
	conns := make([]*websocket.Conn, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	errCh := make(chan error, n)
	for i := range conns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+e.addr+"/ws", nil)
			if err != nil {
				errCh <- fmt.Errorf("dial %d: %w", i, err)
				return
			}
			conns[i] = conn
		}(i)
	}
	close(start)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	defer func() {
		for _, c := range conns {
			if c != nil {
				_ = c.Close()
			}
		}
	}()

	gateWaitFor(t, "8 gates spawned", 10*time.Second, func() bool { return gateDiff(t, e, base) == n })

	ids := make(map[string]bool)
	children := e.app.Tree().Children(e.app.Self())
	for _, r := range children {
		id := r.ID().String()
		if ids[id] {
			t.Fatal("duplicate gate actor id")
		}
		ids[id] = true
	}

	for _, c := range conns {
		_ = c.Close()
	}
	gateWaitFor(t, "all gates destroyed", 20*time.Second, func() bool { return gateDiff(t, e, base) == 0 })
}

// TestSpawnGatewaySessionNameTaken pins the sibling-name uniqueness the
// gateway relies on for its gatesession naming.
func TestSpawnGatewaySessionNameTaken(t *testing.T) {
	e := newGateEnv(t, 50*time.Millisecond, 1)
	defer e.stop()

	r1, err := e.app.SpawnGatewaySession("gatesession-dup")
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	if _, err := e.app.SpawnGatewaySession("gatesession-dup"); err == nil {
		t.Fatal("duplicate name accepted")
	}
	if err := e.app.DestroyGatewaySession(r1); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, ok := e.app.Tree().LookupID(r1.ID()); ok {
		t.Fatal("destroyed gate still resolves")
	}
}
