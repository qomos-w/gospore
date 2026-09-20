package integration

// Cross-facet integration tests (P2-19): each test drives ≥2 structural
// faces of the system in one scenario — app wiring × gateway external
// surface (WS + HTTP unary) × transport × actor handlers — asserting
// system shapes no single package's unit tests can pin.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/gateway"
	"github.com/qomos-w/gospore/transport"
)

// ---- fixture actor ---------------------------------------------------------

type echoReq struct {
	V int `json:"v"`
}

type echoResp struct {
	V   int `json:"v"`
	Dbl int `json:"dbl"`
}

type echoActor struct {
	actor.Host
}

func (a *echoActor) OnStart(ctx actor.Context) error {
	return ctx.Register("it.echo", func(_ actor.Context, req echoReq) (echoResp, error) {
		return echoResp{V: req.V, Dbl: req.V * 2}, nil
	})
}

// ---- harness ---------------------------------------------------------------

// runAppWithGateway boots an App with the HTTP gateway installed through
// the app-level wiring (WithGatewayHTTP + WithGatewayWSTimeouts), waits
// for the listener, and returns its address.
func runAppWithGateway(t *testing.T, rootFn func() actor.Actor, wsTune gateway.WSTimeouts) (string, func()) {
	t.Helper()
	opts := []app.Option{
		app.WithNamespace("itest"),
		app.WithRootActor(rootFn),
		app.WithGatewayHTTP("", gateway.Nop()),
	}
	if wsTune != (gateway.WSTimeouts{}) {
		opts = append(opts, app.WithGatewayWSTimeouts(wsTune))
	}
	a, err := app.New(opts...)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	select {
	case <-a.GatewayReady():
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("gateway never became ready")
	}
	addr := a.GatewayServer().Addr()
	if addr == "" {
		cancel()
		t.Fatal("gateway address empty after ready")
	}
	return addr, func() {
		cancel()
		<-done
	}
}

func mustDialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ws, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("websocket dial %s: %v (status %d)", url, err, status)
	}
	return ws
}

func mustWriteWire(t *testing.T, ws *websocket.Conn, wire *gateway.WireFrame) {
	t.Helper()
	data, err := gateway.MarshalWireFrame(wire)
	if err != nil {
		t.Fatalf("MarshalWireFrame: %v", err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}

func mustReadWire(t *testing.T, ws *websocket.Conn) *gateway.WireFrame {
	t.Helper()
	mt, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("expected binary message, got type %d", mt)
	}
	wire, err := gateway.UnmarshalWireFrame(data)
	if err != nil {
		t.Fatalf("UnmarshalWireFrame: %v", err)
	}
	return wire
}

// ---- tests -----------------------------------------------------------------

// TestGateway_TypedUnary_WSAndHTTP_Agree pins that the same typed handler is
// reachable through two external faces — the WS binary protocol and the HTTP
// unary endpoint — with equivalent results. Faces crossed: app gateway
// wiring, WS frame codec, HTTP unary surface, actor handler, codec round-trip.
func TestGateway_TypedUnary_WSAndHTTP_Agree(t *testing.T) {
	addr, shutdown := runAppWithGateway(t, func() actor.Actor { return &echoActor{} }, gateway.WSTimeouts{})
	defer shutdown()

	// Face 1: WS binary invoke with a JSON-encoded payload.
	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()
	payload, _ := json.Marshal(echoReq{V: 21})
	mustWriteWire(t, ws, &gateway.WireFrame{
		Flags:   gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
		Type:    gateway.FrameTypeInvoke,
		CorID:   7,
		CallID:  "it.echo",
		Payload: payload,
	})
	reply := mustReadWire(t, ws)
	if reply.Type != gateway.FrameTypeReply {
		t.Fatalf("WS reply type = %v, want reply (msg=%q)", reply.Type, reply.ErrorMsg)
	}
	got := echoResp{}
	if reply.Flags.Encoding() == gateway.EncodingJSON {
		if err := json.Unmarshal(reply.Payload, &got); err != nil {
			t.Fatalf("decode JSON reply: %v (payload %q)", err, reply.Payload)
		}
	} else {
		// TBC binary payload: only structural sanity (typed decode needs the
		// server codec's schema).
		if !codec.IsTBCData(reply.Payload) {
			t.Fatalf("expected TBC payload, got %q", reply.Payload)
		}
		return
	}
	if got.V != 21 || got.Dbl != 42 {
		t.Fatalf("WS echo = %+v, want {21 42}", got)
	}

	// Face 2: HTTP unary endpoint.
	resp, err := http.Post("http://"+addr+"/api/it.echo", "application/json", strings.NewReader(`{"v":21}`))
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("http status = %d (body %s)", resp.StatusCode, body)
	}
	httpGot := echoResp{}
	if err := json.NewDecoder(resp.Body).Decode(&httpGot); err != nil {
		t.Fatalf("decode http reply: %v", err)
	}
	if httpGot != got {
		t.Fatalf("faces disagree: ws = %+v, http = %+v", got, httpGot)
	}
}

// TestGateway_CustomWSTimeouts_SilentClientDropped proves the app-level
// WSTimeouts plumbing reaches real connections with a deterministic,
// timer-driven scenario: a client that connects and then sends nothing is
// dropped by the tuned read-idle deadline (300ms) — impossible under the
// 65s default within the test's 3s window.
func TestGateway_CustomWSTimeouts_SilentClientDropped(t *testing.T) {
	addr, shutdown := runAppWithGateway(t,
		func() actor.Actor { return &echoActor{} },
		gateway.WSTimeouts{
			ReadIdle:         300 * time.Millisecond,
			PingInterval:     100 * time.Millisecond,
			WriteTimeout:     time.Second,
			StallForceClose:  5 * time.Second,
		},
	)
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	// Swallow server pings WITHOUT ponging. A normal gorilla client
	// auto-pongs, which refreshes the server's read deadline — correct
	// behavior for a silent-but-alive client. To exercise the idle drop
	// we must look genuinely dead: no frames at all.
	ws.SetPingHandler(func(string) error { return nil })

	// Connect, then stay silent. The server's read-idle deadline (300ms,
	// vs the 65s default) must tear the connection down.
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := ws.ReadMessage()
	if err == nil {
		t.Fatal("expected teardown of silent client, got a message")
	}
	if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
		t.Fatalf("client was not dropped within 3s — custom ReadIdle did not reach the connection: %v", err)
	}
}

// TestTrinity_TransportAndGatewayHitSameActor boots two Apps connected by the
// HTTP transport and drives one handler through both the inter-App transport
// face and the gateway's external HTTP face, asserting they resolve to the
// same actor instance (shared counter).
func TestTrinity_TransportAndGatewayHitSameActor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	remoteA, err := transport.NewHTTP("")
	if err != nil {
		t.Fatalf("NewHTTP A: %v", err)
	}
	defer remoteA.Close()
	remoteB, err := transport.NewHTTP("")
	if err != nil {
		t.Fatalf("NewHTTP B: %v", err)
	}
	defer remoteB.Close()

	router := transport.NewStaticRouter()
	router.Register(1, "http://"+remoteA.ListenAddr())
	router.Register(2, "http://"+remoteB.ListenAddr())
	remoteA.SetRouter(router)
	remoteB.SetRouter(router)

	serverApp, err := app.New(
		app.WithNamespace("trinity"),
		app.WithRuntimeSlot(2),
		app.WithRemoteTransport(remoteB),
		app.WithGatewayHTTP("", gateway.Nop()),
		app.WithRootActor(func() actor.Actor { return &counterActor{} }),
	)
	if err != nil {
		t.Fatalf("New server app: %v", err)
	}
	clientApp, err := app.New(
		app.WithNamespace("trinity"),
		app.WithRuntimeSlot(1),
		app.WithRemoteTransport(remoteA),
	)
	if err != nil {
		t.Fatalf("New client app: %v", err)
	}

	doneS := make(chan error, 1)
	doneC := make(chan error, 1)
	go func() { doneS <- serverApp.Run(ctx) }()
	go func() { doneC <- clientApp.Run(ctx) }()
	select {
	case <-serverApp.GatewayReady():
	case <-time.After(3 * time.Second):
		t.Fatal("server gateway never became ready")
	}

	gatewayAddr := serverApp.GatewayServer().Addr()

	// Face 1: inter-App transport invoke (client slot 1 → server slot 2).
	call := clientApp.RemoteRef(serverApp.Self().ID()).Invoke(ctx, "trinity.hit", []byte(`{}`))
	if call == nil {
		t.Fatal("Invoke returned nil call")
	}
	val, err := call.Recv()
	if err != nil {
		t.Fatalf("transport Recv: %v", err)
	}
	first := decodeHits(t, val)
	if first != 1 {
		t.Fatalf("transport face hits = %d, want 1", first)
	}

	// Face 2: external HTTP unary through the gateway.
	resp, err := http.Post("http://"+gatewayAddr+"/api/trinity.hit", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("http status = %d (body %s)", resp.StatusCode, body)
	}
	second := 0
	if err := json.NewDecoder(resp.Body).Decode(&struct{ Hits *int }{Hits: &second}); err != nil {
		t.Fatalf("decode http reply: %v", err)
	}
	if second != 2 {
		t.Fatalf("gateway face hits = %d, want 2 (same actor instance)", second)
	}

	cancel()
	<-doneS
	<-doneC
}

type counterActor struct {
	actor.Host
	hits atomic.Int64
}

func (a *counterActor) OnStart(ctx actor.Context) error {
	return ctx.Register("trinity.hit", func(_ actor.Context) (map[string]int64, error) {
		return map[string]int64{"hits": a.hits.Add(1)}, nil
	})
}

func decodeHits(t *testing.T, val any) int {
	t.Helper()
	switch v := val.(type) {
	case map[string]int64:
		return int(v["hits"])
	case []byte:
		var m map[string]int64
		if err := json.Unmarshal(v, &m); err != nil {
			t.Fatalf("unmarshal hits: %v", err)
		}
		return int(m["hits"])
	default:
		raw, _ := json.Marshal(v)
		var m map[string]int64
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal hits: %v", err)
		}
		return int(m["hits"])
	}
}
