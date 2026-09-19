package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/gateway"
)

// reproRoot emits "tick" events on demand and answers "echo" invokes, mirroring
// an agent that streams step/turn events while answering chat submits.
type reproRoot struct {
	actor.Host
}

type emitReq struct {
	Token string `json:"token"`
}

func (a *reproRoot) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := ctx.RegisterEventKind("tick", map[string]any{}, actor.Public()); err != nil {
		return err
	}
	if err := ctx.Register("test.emit", func(ctx actor.Context, req emitReq) error {
		_ = req
		return ctx.EmitEvent("tick", map[string]any{"ts": time.Now().UnixMilli()})
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

type reproEnv struct {
	app     app.App
	addr    string
	rootID  string
	shut    func()
	emitCtl chan struct{}
	emitWG  sync.WaitGroup
}

func newReproEnv(t *testing.T) *reproEnv {
	t.Helper()
	a, err := app.New(
		app.WithNamespace("repro"),
		app.WithRootActor(func() actor.Actor { return &reproRoot{} }),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	time.Sleep(150 * time.Millisecond)

	srv := gateway.NewServer(a, nil, "127.0.0.1:0")
	go func() { _ = srv.Run(context.Background()) }()
	for srv.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}
	env := &reproEnv{
		app:     a,
		addr:    srv.Addr(),
		rootID:  a.Self().ID().String(),
		shut:    func() { _ = srv.Close() },
		emitCtl: make(chan struct{}),
	}
	env.startEmit(20 * time.Millisecond)
	return env
}

// startEmit drives test.emit on a ticker so "tick" events flow at hz.
func (e *reproEnv) startEmit(period time.Duration) {
	e.emitWG.Add(1)
	go func() {
		defer e.emitWG.Done()
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		root := e.app.Self()
		for {
			select {
			case <-e.emitCtl:
				return
			case <-ticker.C:
				c := root.Invoke(context.Background(), "test.emit", emitReq{Token: "t"})
				if _, err := c.RecvRaw(); err != nil {
					fmt.Printf("[repro] test.emit err=%v\n", err)
				}
				_ = c.Close()
			}
		}
	}()
}

func (e *reproEnv) stopEmit() {
	close(e.emitCtl)
	e.emitWG.Wait()
}

func (e *reproEnv) stop() {
	e.stopEmit()
	e.shut()
}

func dialRepro(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func writeJSON(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	if err := conn.WriteJSON(v); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// appEcho verifies the app server itself is live by invoking root in-process.
func (e *reproEnv) appEcho(t *testing.T) time.Duration {
	t.Helper()
	root := e.app.Self()
	start := time.Now()
	c := root.Invoke(context.Background(), "echo", map[string]any{"liveness": 1})
	defer c.Close()
	ch := make(chan error, 1)
	go func() {
		_, err := c.RecvRaw()
		ch <- err
	}()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("app-level echo failed: %v", err)
		}
		return time.Since(start)
	case <-time.After(30 * time.Second):
		t.Fatal("app-level echo timed out after 30s: web client wedged the app server")
		return 0
	}
}

// TestWebClientCannotWedgeAppServer is the hard invariant: no combination of
// client-side misbehavior — subscribe flood, invoke flood while never reading
// a single reply, and an abrupt disconnect — may stall the app server or any
// other connected client.
func TestWebClientCannotWedgeAppServer(t *testing.T) {
	env := newReproEnv(t)
	defer env.stop()

	healthy := newHealthyClient(t, env)
	defer healthy.shutdown(t)

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	waitFor("first healthy chunk", func() bool { return healthy.chunks.Load() > 0 })

	attacker := dialRepro(t, env.addr)
	for i := 0; i < 200; i++ {
		writeJSON(t, attacker, map[string]any{
			"type":    "subscribe",
			"subId":   fmt.Sprintf("atk-%d", i),
			"callID":  "gospore.events.subscribe_instance",
			"payload": map[string]any{"actorId": env.rootID, "kind": "tick"},
		})
	}
	for i := 0; i < 300; i++ {
		writeJSON(t, attacker, map[string]any{
			"type":    "invoke",
			"reqId":   int64(1000 + i),
			"callID":  "echo",
			"payload": map[string]any{"flood": i},
		})
	}
	// The attacker never reads a single frame.

	// While the attack saturates its own session, the app must stay live and
	// the bystander client must keep receiving events and fast echoes.
	before := healthy.chunks.Load()
	if d := healthy.echoLatency(t); d > 5*time.Second {
		t.Fatalf("healthy echo latency during attack: %v", d)
	}
	waitFor("healthy chunks during attack", func() bool { return healthy.chunks.Load() > before+5 })
	if d := env.appEcho(t); d > 5*time.Second {
		t.Fatalf("app-level echo latency during attack: %v", d)
	}

	// Abruptly kill the attacker (TCP RST, no close handshake).
	abruptClose(attacker)

	// After the disconnect the app and the bystander must still be healthy;
	// the attacker's session tears down within the write deadline window.
	time.Sleep(2 * time.Second)
	if d := env.appEcho(t); d > 5*time.Second {
		t.Fatalf("app-level echo latency after attack: %v", d)
	}
	if d := healthy.echoLatency(t); d > 5*time.Second {
		t.Fatalf("healthy echo latency after attack: %v", d)
	}
	waitFor("healthy chunks after attack", func() bool { return healthy.chunks.Load() > before+10 })

	// Long tail: the wedged attacker session must fully unwind (write
	// deadline closes the conn, slots and goroutines release). Keep probing
	// for a while to make sure nothing degrades.
	deadline := time.Now().Add(35 * time.Second)
	for time.Now().Before(deadline) {
		if d := env.appEcho(t); d > 5*time.Second {
			t.Fatalf("app-level echo degraded during attacker teardown: %v", d)
		}
		time.Sleep(2 * time.Second)
	}
	if healthy.chunks.Load() == 0 {
		t.Fatal("healthy client received no chunks at all")
	}
}

func abruptClose(conn *websocket.Conn) {
	if tc, ok := conn.UnderlyingConn().(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

// healthyClient counts "chunk" frames and answers echo probes via a single
// reader goroutine (gorilla conns allow exactly one concurrent reader).
type healthyClient struct {
	conn    *websocket.Conn
	stop    chan struct{}
	done    chan struct{}
	chunks  atomic.Int64
	mu      sync.Mutex
	closed  bool
	lastErr string
	replies map[int64]chan string
}

func newHealthyClient(t *testing.T, env *reproEnv) *healthyClient {
	c := &healthyClient{
		conn:    dialRepro(t, env.addr),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		replies: make(map[int64]chan string),
	}
	writeJSON(t, c.conn, map[string]any{
		"type":    "subscribe",
		"subId":   "healthy-1",
		"callID":  "gospore.events.subscribe_instance",
		"payload": map[string]any{"actorId": env.rootID, "kind": "tick"},
	})
	go c.readLoop()
	return c
}

func (c *healthyClient) readLoop() {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			c.mu.Lock()
			if !c.closed {
				c.lastErr = err.Error()
			}
			c.mu.Unlock()
			return
		}
		var frame struct {
			Type    string          `json:"type"`
			ReqID   int64           `json:"reqId"`
			SubID   string          `json:"subId"`
			Payload json.RawMessage `json:"payload"`
			Message string          `json:"message"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			continue
		}
		switch frame.Type {
		case "chunk":
			c.chunks.Add(1)
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

func (c *healthyClient) echoLatency(t *testing.T) time.Duration {
	t.Helper()
	reqID := time.Now().UnixNano()
	ch := make(chan string, 1)
	c.mu.Lock()
	c.replies[reqID] = ch
	c.mu.Unlock()
	start := time.Now()
	writeJSON(t, c.conn, map[string]any{
		"type":    "invoke",
		"reqId":   reqID,
		"callID":  "echo",
		"payload": map[string]any{"ping": 1},
	})
	select {
	case ft := <-ch:
		if ft == "error" {
			t.Fatal("healthy client echo returned error")
		}
		return time.Since(start)
	case <-time.After(30 * time.Second):
		t.Fatal("healthy client echo timed out after 30s")
		return 0
	}
}

func (c *healthyClient) shutdown(t *testing.T) {
	t.Helper()
	close(c.stop)
	c.mu.Lock()
	c.closed = true
	errMsg := c.lastErr
	c.mu.Unlock()
	_ = c.conn.Close()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		fmt.Println("[repro] healthy client read loop did not exit")
	}
	if errMsg != "" {
		t.Logf("healthy client connection ended with: %s", errMsg)
	}
}

// TestWebDisconnectDoesNotWedgeOthers: a web client that logs in, subscribes,
// interacts and then drops the connection must not stall event delivery or
// invokes for other clients.
func TestWebDisconnectDoesNotWedgeOthers(t *testing.T) {
	env := newReproEnv(t)
	defer env.stop()

	healthy := newHealthyClient(t, env)
	defer healthy.shutdown(t)

	victim := dialRepro(t, env.addr)
	writeJSON(t, victim, map[string]any{
		"type":    "subscribe",
		"subId":   "victim-1",
		"callID":  "gospore.events.subscribe_instance",
		"payload": map[string]any{"actorId": env.rootID, "kind": "tick"},
	})
	writeJSON(t, victim, map[string]any{
		"type":    "subscribe",
		"subId":   "victim-2",
		"callID":  "gospore.events.subscribe_instance",
		"payload": map[string]any{"actorId": env.rootID, "kind": "tick"},
	})
	writeJSON(t, victim, map[string]any{
		"type":    "invoke",
		"reqId":   1,
		"callID":  "echo",
		"payload": map[string]any{"hello": "world"},
	})
	victim.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 3; i++ {
		_, raw, err := victim.ReadMessage()
		if err != nil {
			t.Fatalf("victim read %d: %v", i, err)
		}
		fmt.Printf("[repro] victim frame %d: %q\n", i, string(raw[:min(len(raw), 120)]))
	}

	// Let events accumulate to both subscribers.
	time.Sleep(500 * time.Millisecond)

	base := healthy.echoLatency(t)
	t.Logf("baseline echo latency: %v chunks=%d", base, healthy.chunks.Load())

	abruptClose(victim)

	var worst time.Duration
	for i := 0; i < 40; i++ {
		d := healthy.echoLatency(t)
		if d > worst {
			worst = d
		}
		time.Sleep(100 * time.Millisecond)
	}
	chunks := healthy.chunks.Load()
	t.Logf("after victim disconnect: chunks=%d worstEcho=%v", chunks, worst)

	if worst > 3*time.Second {
		t.Fatalf("healthy client echo latency degraded to %v after victim disconnect", worst)
	}
	if chunks < 100 {
		t.Fatalf("healthy client only received %d chunks in ~4s of 50/s emission", chunks)
	}
}

// TestWebStalledReaderDoesNotWedgeOthers: same topology but the victim stays
// connected while never reading — the classic slow-client backpressure case.
func TestWebStalledReaderDoesNotWedgeOthers(t *testing.T) {
	env := newReproEnv(t)
	defer env.stop()

	healthy := newHealthyClient(t, env)
	defer healthy.shutdown(t)

	victim := dialRepro(t, env.addr)
	writeJSON(t, victim, map[string]any{
		"type":    "subscribe",
		"subId":   "victim-1",
		"callID":  "gospore.events.subscribe_instance",
		"payload": map[string]any{"actorId": env.rootID, "kind": "tick"},
	})
	// Deliberately never read from victim.

	var worst time.Duration
	for i := 0; i < 30; i++ {
		d := healthy.echoLatency(t)
		if d > worst {
			worst = d
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("with stalled victim: chunks=%d worstEcho=%v", healthy.chunks.Load(), worst)

	if worst > 3*time.Second {
		t.Fatalf("healthy client echo latency degraded to %v with a stalled reader attached", worst)
	}
	_ = victim.Close()
}
