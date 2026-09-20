package gateway

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// wsStallUpgrader upgrades and hands the raw server conn to a channel.
var wsStallUpgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// newStallTestServer upgrades one client conn and returns the server-side
// wsFrameConn plus the dialed client. The client deliberately never reads,
// so server writes block once the OS buffers fill (read buffer shrunk to
// 2 KiB), deterministically within a few hundred KiB.
func newStallTestServer(t *testing.T, tune wsConnTuning) (*wsFrameConn, *websocket.Conn) {
	t.Helper()
	connCh := make(chan *wsFrameConn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := wsStallUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fc := newWsFrameConnTuned(ws, nil, false, tune)
		connCh <- fc
		<-r.Context().Done()
		_ = fc.Close()
	}))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	client, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if tc, ok := client.UnderlyingConn().(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(2048)
	}
	fc := <-connCh
	t.Cleanup(func() {
		fc.signalClose()
		select {
		case <-fc.writeDone:
		case <-time.After(10 * time.Second):
			t.Logf("writeLoop did not exit after signalClose; leaking in test")
		}
	})
	return fc, client
}

// TestWsFrameConn_StallBuffersWithoutLoss verifies the core never-blocking
// contract against a stalled peer (never reads): Send keeps succeeding while
// frames spill from the bounded outCh into the byte-budgeted overflow queue,
// and once the peer resumes reading, every buffered frame flushes in order
// with zero loss. A short stall must NOT force-close the connection.
func TestWsFrameConn_StallBuffersWithoutLoss(t *testing.T) {
	fc, client := newStallTestServer(t, wsConnTuning{}) // default budget 32 MiB, horizon 10min

	orig := wsWriteTimeout
	wsWriteTimeout = 10 * time.Minute // keep the stalled write wedged, not deadline-failed
	defer func() { wsWriteTimeout = orig }()

	const n = 300
	frames := make([]*WireFrame, n)
	for i := range frames {
		frames[i] = &WireFrame{Type: FrameTypeChunk, CallID: fmt.Sprintf("k%d", i), Payload: make([]byte, 32*1024)}
	}

	// Send everything while the peer is stalled. No Send may fail or block.
	for i := 0; i < n; i++ {
		sendStart := time.Now()
		if err := fc.Send(frames[i]); err != nil {
			t.Fatalf("Send %d during stall: %v (overflow should buffer, not drop)", i, err)
		}
		if d := time.Since(sendStart); d > 500*time.Millisecond {
			t.Fatalf("Send %d took %v during stall; Send must never block", i, d)
		}
	}

	// Prove the overflow path actually engaged: frames beyond outCh and the
	// OS buffers are parked in the byte-budgeted queue.
	fc.ofMu.Lock()
	queued := fc.overflowBytes
	fc.ofMu.Unlock()
	if queued == 0 {
		t.Fatal("overflow queue empty; the stall never engaged the overflow path")
	}

	// The peer resumes. All buffered frames flush in FIFO order.
	ids := make([]string, 0, n)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			client.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, msg, err := client.ReadMessage()
			if err != nil {
				done <- fmt.Errorf("read %d after resume: %w", i, err)
				return
			}
			got, err := UnmarshalWireFrame(msg)
			if err != nil {
				done <- fmt.Errorf("unmarshal %d: %w", i, err)
				return
			}
			ids = append(ids, got.CallID)
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("timed out reading buffered frames after resume")
	}
	if len(ids) != n {
		t.Fatalf("expected %d frames delivered after resume, got %d (frames lost during stall)", n, len(ids))
	}
	for i, id := range ids {
		if want := fmt.Sprintf("k%d", i); id != want {
			t.Fatalf("frame %d: got CallID %q, want %q (ordering broken)", i, id, want)
		}
	}
}

// TestWsFrameConn_OverflowBudgetForceCloses verifies the safety valve: once
// the overflow queue exceeds its byte budget (peer stalled too long under
// heavy streaming), the transport force-closes so the client reconnects and
// resubscribes (event rings replay the gap via sinceSeqNo).
func TestWsFrameConn_OverflowBudgetForceCloses(t *testing.T) {
	fc, _ := newStallTestServer(t, wsConnTuning{
		overflowBudget:  64 * 1024,
		stallForceClose: time.Hour, // only the budget, not the horizon, may trigger
	})

	orig := wsWriteTimeout
	wsWriteTimeout = 10 * time.Minute // keep writeLoop wedged, not deadline-failed
	defer func() { wsWriteTimeout = orig }()

	frame := &WireFrame{Type: FrameTypeChunk, CallID: "k", Payload: make([]byte, 1024)}
	deadline := time.Now().Add(5 * time.Second)
	sent := 0
	for time.Now().Before(deadline) {
		if err := fc.Send(frame); err == io.EOF {
			// Force-close observed; a subsequent Send must stay EOF.
			if err := fc.Send(frame); err != io.EOF {
				t.Fatalf("Send after force-close returned %v, want io.EOF", err)
			}
			// Closing the raw conn unwedges writeLoop; it must exit.
			select {
			case <-fc.writeDone:
				return
			case <-time.After(5 * time.Second):
				t.Fatal("writeLoop did not exit after budget force-close")
			}
		}
		sent++
	}
	t.Fatalf("overflow budget never exceeded after %d sends; force-close did not trigger", sent)
}

// TestWsFrameConn_LongStallForceCloses ensures a write making zero progress
// for the full no-progress horizon (a dead peer, not a paused one) tears the
// connection down: Send starts failing with io.EOF and writeLoop exits. The
// transient-stall case (horizon never reached) is covered by the lossless
// buffering test above.
func TestWsFrameConn_LongStallForceCloses(t *testing.T) {
	fc, _ := newStallTestServer(t, wsConnTuning{
		stallForceClose: 200 * time.Millisecond,
		overflowBudget:  1 << 30, // only the horizon, not the budget, may trigger
	})

	orig := wsWriteTimeout
	wsWriteTimeout = 10 * time.Minute // the deadline must not fire; only the horizon
	defer func() { wsWriteTimeout = orig }()

	frame := &WireFrame{Type: FrameTypeChunk, CallID: "k", Payload: make([]byte, 4*1024)}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := fc.Send(frame); err == io.EOF {
			select {
			case <-fc.writeDone:
				return
			case <-time.After(5 * time.Second):
				t.Fatal("writeLoop did not exit after horizon force-close")
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Send never returned io.EOF after the stall horizon; session not force-closed")
}

// TestWsFrameConn_CloseBoundedAgainstWedgedWriteLoop ensures Close does not
// block forever when writeLoop is wedged inside a stalled WriteMessage. The
// gateway teardown calls conn.Close after a send failure; an unbounded wait
// would hold the session teardown (and its worker pool) hostage. Close must
// return within its drain budget, and the final raw close must unwedge
// writeLoop so it exits.
func TestWsFrameConn_CloseBoundedAgainstWedgedWriteLoop(t *testing.T) {
	fc, _ := newStallTestServer(t, wsConnTuning{
		closeDrainWait:  150 * time.Millisecond,
		stallForceClose: time.Hour, // the horizon must not rescue Close here
	})

	orig := wsWriteTimeout
	wsWriteTimeout = 10 * time.Minute // wedge writeLoop inside WriteMessage
	defer func() { wsWriteTimeout = orig }()

	frame := &WireFrame{Type: FrameTypeChunk, CallID: "k", Payload: make([]byte, 128*1024)}
	for i := 0; i < 64; i++ { // ~8 MiB: fills OS buffers + outCh, writeLoop wedges
		if err := fc.Send(frame); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond) // let writeLoop wedge inside WriteMessage

	done := make(chan struct{})
	go func() {
		fc.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked on a wedged writeLoop past its drain budget")
	}
	select {
	case <-fc.writeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("writeLoop did not exit after Close closed the raw conn")
	}
}

// TestWsFrameConn_DeadConnWriteTimeoutUnwedgesWriter verifies the write
// deadline's new role: it bounds the writeLoop goroutine, not the Send
// caller. Against a dead peer (TCP window closed), every Send returns fast
// with nil error while frames buffer; the stalled write inside writeLoop
// hits the deadline, the failure tears the connection down, and subsequent
// Sends report io.EOF.
func TestWsFrameConn_DeadConnWriteTimeoutUnwedgesWriter(t *testing.T) {
	fc, _ := newStallTestServer(t, wsConnTuning{}) // default horizon and budget

	orig := wsWriteTimeout
	wsWriteTimeout = 200 * time.Millisecond
	defer func() { wsWriteTimeout = orig }()

	big := &WireFrame{Type: FrameTypeChunk, CallID: "test.stream", Payload: make([]byte, 256*1024)}
	for i := 0; i < 64; i++ { // 16 MiB — far beyond any socket buffer, well under budget
		sendStart := time.Now()
		if err := fc.Send(big); err != nil {
			t.Fatalf("Send %d: %v (Send must not fail while budget remains)", i, err)
		}
		if d := time.Since(sendStart); d > 500*time.Millisecond {
			t.Fatalf("Send %d took %v; Send must never block", i, d)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := fc.Send(big); err == io.EOF {
			select {
			case <-fc.writeDone:
				return
			case <-time.After(5 * time.Second):
				t.Fatal("writeLoop did not exit after write deadline failure")
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Send never returned io.EOF after the write deadline; connection not torn down")
}

// TestWsFrameConn_SendSucceedsWithReader verifies normal delivery with a
// peer that keeps reading: frames arrive in CorID order end-to-end.
func TestWsFrameConn_SendSucceedsWithReader(t *testing.T) {
	fc, client := newStallTestServer(t, wsConnTuning{})

	orig := wsWriteTimeout
	wsWriteTimeout = 2 * time.Second
	defer func() { wsWriteTimeout = orig }()

	const n = 8
	got := make(chan uint64, n)
	go func() {
		for {
			client.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, msg, err := client.ReadMessage()
			if err != nil {
				return
			}
			wire, err := UnmarshalWireFrame(msg)
			if err != nil {
				return
			}
			got <- wire.CorID
		}
	}()

	for i := 0; i < n; i++ {
		sendStart := time.Now()
		if err := fc.Send(&WireFrame{Type: FrameTypeChunk, CorID: uint64(i)}); err != nil {
			t.Fatalf("Send %d with reading peer: %v", i, err)
		}
		if d := time.Since(sendStart); d > time.Second {
			t.Fatalf("Send %d took %v with a reading peer", i, d)
		}
	}
	for i := 0; i < n; i++ {
		select {
		case id := <-got:
			if id != uint64(i) {
				t.Fatalf("frame %d: got CorID %d (ordering broken)", i, id)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("frame %d not delivered to the reading peer", i)
		}
	}
}
