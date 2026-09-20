package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/gateway"
)

// ---------------------------------------------------------------------------
// Streaming fixtures
// ---------------------------------------------------------------------------

type streamActor struct{ actor.Host }

func (a *streamActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("test.stream", func(_ actor.Context, req string, em actor.Emitter) error {
		count := 3
		if req != "" {
			if n, err := fmt.Sscanf(req, "%d", &count); err == nil && n == 1 {
				// use parsed count
			}
		}
		for i := 1; i <= count; i++ {
			// Slow down so unsubscribe cancellation has time to propagate.
			select {
			case <-em.Done():
				return nil
			default:
			}
			// Send JSON-object chunks so the codec produces valid JSON bytes
			// that can travel inside wsFrame.Payload (json.RawMessage).
			if err := em.Send(map[string]string{"item": fmt.Sprintf("chunk-%d", i)}); err != nil {
				return err
			}
			if count > 10 { // slow path for large-count streams (unsubscribe test)
				time.Sleep(50 * time.Millisecond)
			}
		}
		return nil
	})
	return nil
}

type streamErrorActor struct{ actor.Host }

func (a *streamErrorActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("test.stream_err", func(_ actor.Context, _ string, em actor.Emitter) error {
		_ = em.Send(map[string]string{"item": "chunk-1"})
		_ = em.Send(map[string]string{"item": "chunk-2"})
		return fmt.Errorf("stream failed")
	})
	return nil
}

// ---------------------------------------------------------------------------
// Binary-only reply fixtures
// ---------------------------------------------------------------------------

type unaryActor struct{ actor.Host }

type meResponse struct {
	ID string
}

func (a *unaryActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("test.me", func(_ actor.Context) (meResponse, error) {
		return meResponse{ID: "user-1"}, nil
	})
	return nil
}

// ---------------------------------------------------------------------------
// WebSocket subscribe tests
// ---------------------------------------------------------------------------

func TestGatewayWS_Subscribe_ChunksAndEnd(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &streamActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	// Send subscribe frame.
	subFrame := map[string]any{
		"type":    "subscribe",
		"subId":   "sub-1",
		"callID":  "test.stream",
		"payload": "3",
	}
	mustWriteJSON(t, ws, subFrame)

	// Expect three chunk frames then an end frame.
	var chunks []map[string]any
	for {
		frame := mustReadJSON(t, ws)
		ft, _ := frame["type"].(string)
		switch ft {
		case "chunk":
			payload, _ := frame["payload"].(map[string]any)
			chunks = append(chunks, payload)
		case "end":
			if subId, _ := frame["subId"].(string); subId != "sub-1" {
				t.Fatalf("end frame subId = %s, want sub-1", subId)
			}
			goto done
		case "error":
			msg, _ := frame["message"].(string)
			t.Fatalf("unexpected error frame: %s", msg)
		default:
			t.Fatalf("unexpected frame type: %s", ft)
		}
	}
done:
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3: %v", len(chunks), chunks)
	}
	for i, want := range []string{"chunk-1", "chunk-2", "chunk-3"} {
		item, _ := chunks[i]["item"].(string)
		if item != want {
			t.Fatalf("chunk[%d].item = %s, want %s", i, item, want)
		}
	}
}

func TestGatewayWS_Subscribe_TransID_IsMonotonic(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &streamActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	mustWriteJSON(t, ws, map[string]any{
		"type":    "subscribe",
		"subId":   "sub-transid",
		"callID":  "test.stream",
		"payload": "",
	})

	var lastTransID float64
	for {
		frame := mustReadJSON(t, ws)
		ft, _ := frame["type"].(string)
		if ft == "end" {
			break
		}
		if ft != "chunk" {
			t.Fatalf("expected chunk, got %s", ft)
		}
		transID, ok := frame["transId"].(float64)
		if !ok {
			t.Fatalf("chunk missing transId: %+v", frame)
		}
		if transID != 0 && lastTransID != 0 && transID != lastTransID+1 {
			t.Fatalf("transId not monotonic: got %v, want %v", transID, lastTransID+1)
		}
		lastTransID = transID
	}
	if lastTransID == 0 {
		t.Fatal("expected at least one chunk with transId")
	}
}

func TestGatewayWS_Subscribe_ErrorFrame(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &streamErrorActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	mustWriteJSON(t, ws, map[string]any{
		"type":    "subscribe",
		"subId":   "sub-err",
		"callID":  "test.stream_err",
		"payload": "",
	})

	var gotChunks int
	var gotError bool
	for {
		frame := mustReadJSON(t, ws)
		ft, _ := frame["type"].(string)
		switch ft {
		case "chunk":
			gotChunks++
		case "error":
			msg, _ := frame["message"].(string)
			if !strings.Contains(msg, "stream failed") {
				t.Fatalf("error message = %s, want 'stream failed'", msg)
			}
			gotError = true
			goto done
		default:
			t.Fatalf("unexpected frame type: %s", ft)
		}
	}
done:
	if gotChunks != 2 {
		t.Fatalf("got %d chunks, want 2", gotChunks)
	}
	if !gotError {
		t.Fatal("expected error frame, got none")
	}
}

func TestGatewayWS_Subscribe_Unsubscribe_CancelsStream(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &streamActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	mustWriteJSON(t, ws, map[string]any{
		"type":    "subscribe",
		"subId":   "sub-cancel",
		"callID":  "test.stream",
		"payload": "100",
	})

	// Wait for first chunk.
	frame := mustReadJSON(t, ws)
	if ft, _ := frame["type"].(string); ft != "chunk" {
		t.Fatalf("expected chunk, got %s", ft)
	}

	// Send unsubscribe.
	mustWriteJSON(t, ws, map[string]any{
		"type":  "unsubscribe",
		"subId": "sub-cancel",
	})

	// Drain any in-flight frames with a short timeout. Cancellation is
	// asynchronous — a few chunks may already be in the pipeline.
	var extra int
	for {
		ws.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
		extra++
		if extra > 50 {
			t.Fatalf("unsubscribe did not cancel stream: received >50 extra frames")
		}
	}

	// Total received must be far less than 100 (the requested count).
	total := 1 + extra
	if total >= 50 {
		t.Fatalf("unsubscribe ineffective: received %d chunks, expected <50", total)
	}
}

func TestGatewayWS_Subscribe_SinceSeqNo_Resumed(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &streamActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	// Subscribe with sinceSeqNo > 0 — the server should still serve the
	// stream (the actor side is stateless; seqNo is client-side bookkeeping).
	mustWriteJSON(t, ws, map[string]any{
		"type":       "subscribe",
		"subId":      "sub-resume",
		"callID":     "test.stream",
		"payload":    "2",
		"sinceSeqNo": 5,
	})

	// Server starts counting from 1 regardless of sinceSeqNo; the gateway
	// increments seqNo per chunk starting from sinceSeqNo.
	frame1 := mustReadJSON(t, ws)
	if frame1["type"] != "chunk" {
		t.Fatalf("expected chunk, got %v", frame1["type"])
	}
	seq1, _ := frame1["seqNo"].(float64)
	if seq1 != 6 {
		t.Fatalf("first chunk seqNo = %v, want 6", seq1)
	}

	frame2 := mustReadJSON(t, ws)
	if frame2["type"] != "chunk" {
		t.Fatalf("expected chunk, got %v", frame2["type"])
	}
	seq2, _ := frame2["seqNo"].(float64)
	if seq2 != 7 {
		t.Fatalf("second chunk seqNo = %v, want 7", seq2)
	}

	frame3 := mustReadJSON(t, ws)
	if frame3["type"] != "end" {
		t.Fatalf("expected end, got %v", frame3["type"])
	}
}

func TestGatewayWS_Subscribe_NotRegistered(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	mustWriteJSON(t, ws, map[string]any{
		"type":    "subscribe",
		"subId":   "sub-nope",
		"callID":  "test.notexist",
		"payload": nil,
	})

	frame := mustReadJSON(t, ws)
	if ft, _ := frame["type"].(string); ft != "error" {
		t.Fatalf("expected error frame, got %s", ft)
	}
	msg, _ := frame["message"].(string)
	if !strings.Contains(msg, "not registered") && !strings.Contains(msg, "service not found") {
		t.Fatalf("error message = %s, want 'not registered' or 'service not found'", msg)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newGatewayAppWithActor(t *testing.T, interceptor gateway.GatewayInterceptor, rootFn func() actor.Actor) (app.App, string, func()) {
	t.Helper()
	a, err := app.New(
		app.WithNamespace("test"),
		app.WithCodec(codec.NewJSON()),
		app.WithRootActor(rootFn),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)

	srv := gateway.NewServer(a, interceptor, "127.0.0.1:0")
	go func() { _ = srv.Run(ctx) }()

	for srv.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}

	shutdown := func() {
		cancel()
		<-done
	}
	return a, srv.Addr(), shutdown
}

func mustDialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ws, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		if resp != nil {
			t.Fatalf("websocket dial: %v (status %d)", err, resp.StatusCode)
		}
		t.Fatalf("websocket dial: %v", err)
	}
	return ws
}

func mustWriteJSON(t *testing.T, ws *websocket.Conn, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func mustReadJSON(t *testing.T, ws *websocket.Conn) map[string]any {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mt != websocket.TextMessage {
		t.Fatalf("expected text message, got %d", mt)
	}
	var frame map[string]any
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return frame
}

func mustReadWireFrame(t *testing.T, ws *websocket.Conn) *gateway.WireFrame {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("expected binary message, got %d", mt)
	}
	wire, err := gateway.UnmarshalWireFrame(data)
	if err != nil {
		t.Fatalf("unmarshal wire frame: %v", err)
	}
	return wire
}

// ---------------------------------------------------------------------------
// Binary-only mode tests
// ---------------------------------------------------------------------------

func newBinaryOnlyGatewayApp(t *testing.T, interceptor gateway.GatewayInterceptor) (app.App, string, func()) {
	t.Helper()
	return newBinaryOnlyGatewayAppWithActor(t, interceptor, func() actor.Actor { return &streamActor{} })
}

func newBinaryOnlyGatewayAppWithActor(t *testing.T, interceptor gateway.GatewayInterceptor, rootFn func() actor.Actor) (app.App, string, func()) {
	t.Helper()
	a, err := app.New(
		app.WithNamespace("test"),
		app.WithRootActor(rootFn),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)

	srv := gateway.NewServer(a, interceptor, "127.0.0.1:0")
	srv.WithBinaryOnly(true)
	go func() { _ = srv.Run(ctx) }()

	for srv.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}

	shutdown := func() {
		cancel()
		<-done
	}
	return a, srv.Addr(), shutdown
}

func mustWriteBinaryWire(t *testing.T, ws *websocket.Conn, wire *gateway.WireFrame) {
	t.Helper()
	data, err := gateway.MarshalWireFrame(wire)
	if err != nil {
		t.Fatalf("marshal wire frame: %v", err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write binary: %v", err)
	}
}

func TestGatewayWS_BinaryOnly_RejectsTextFrame(t *testing.T) {
	_, addr, shutdown := newBinaryOnlyGatewayApp(t, gateway.Nop())
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	mustWriteJSON(t, ws, map[string]any{
		"type":    "subscribe",
		"subId":   "sub-text",
		"callID":  "test.stream",
		"payload": "3",
	})

	frame := mustReadJSON(t, ws)
	if ft, _ := frame["type"].(string); ft != "error" {
		t.Fatalf("expected error frame, got %s", ft)
	}
	msg, _ := frame["message"].(string)
	if !strings.Contains(msg, "text frame not allowed") {
		t.Fatalf("error message = %s, want 'text frame not allowed'", msg)
	}
}

func TestGatewayWS_BinaryOnly_RejectsJSONEncodedPayload(t *testing.T) {
	_, addr, shutdown := newBinaryOnlyGatewayApp(t, gateway.Nop())
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	payload := []byte(`"3"`)
	wire := &gateway.WireFrame{
		Flags:    gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
		Type:     gateway.FrameTypeSubscribe,
		CorID:    1,
		Seq:      0,
		CallID:   "test.stream",
		SubID:    "sub-json-payload",
		Target:   "",
		ErrorMsg: "",
		Payload:  payload,
	}
	mustWriteBinaryWire(t, ws, wire)

	reply := mustReadWireFrame(t, ws)
	if reply.Type != gateway.FrameTypeError {
		t.Fatalf("expected error frame, got %v", reply.Type)
	}
	if !strings.Contains(reply.ErrorMsg, "binary-only: JSON-encoded payload not allowed") {
		t.Fatalf("error message = %s, want 'binary-only: JSON-encoded payload not allowed'", reply.ErrorMsg)
	}
}

func TestGatewayWS_BinaryOnly_UnaryNoPayloadReplyIsBinary(t *testing.T) {
	_, addr, shutdown := newBinaryOnlyGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &unaryActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	wire := &gateway.WireFrame{
		Flags:  gateway.MakeFlags(gateway.EncodingBinary, gateway.CompressionNone),
		Type:   gateway.FrameTypeInvoke,
		CorID:  7,
		CallID: "test.me",
	}
	mustWriteBinaryWire(t, ws, wire)

	reply := mustReadWireFrame(t, ws)
	if reply.Type != gateway.FrameTypeReply {
		t.Fatalf("expected reply frame, got %v", reply.Type)
	}
	if reply.Flags.Encoding() != gateway.EncodingBinary {
		t.Fatalf("expected binary-encoded reply, got %v", reply.Flags.Encoding())
	}
	if !codec.IsTBCData(reply.Payload) {
		t.Fatalf("expected TBC payload, got %s", string(reply.Payload))
	}
}
