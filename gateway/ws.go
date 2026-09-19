package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/spore/identity"
	"github.com/qomos-w/spore/transport"
)

// wsFrame is the logical protocol frame exchanged between the session
// dispatch core and transport adapters. It mirrors the frontend
// WebSocketTransport wire protocol.
type wsFrame struct {
	Type       string          `json:"type"`
	ReqID      int64           `json:"reqId,omitempty"`
	CallID     string          `json:"callID,omitempty"`
	Target     string          `json:"target,omitempty"`
	From       string          `json:"from,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	SubID      string          `json:"subId,omitempty"`
	SinceSeqNo int64           `json:"sinceSeqNo,omitempty"`
	SeqNo      int64           `json:"seqNo,omitempty"`
	TransID    int64           `json:"transId,omitempty"`
	Message    string          `json:"message,omitempty"`
	TimeoutMs  int64           `json:"timeoutMs,omitempty"`
	// payloadIsBinary is set when the incoming wire frame carried a
	// binary-encoded (TBC) payload. It prevents the gateway from
	// JSON-unmarshalling the payload before passing it to the actor.
	payloadIsBinary bool
}

// wsUpgrader allows all origins (matching the HTTP CORS policy).
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// ---------------------------------------------------------------------------
// Frame type mapping
// ---------------------------------------------------------------------------

var wsTypeToFrameType = map[string]FrameType{
	"invoke":      FrameTypeInvoke,
	"reply":       FrameTypeReply,
	"error":       FrameTypeError,
	"chunk":       FrameTypeChunk,
	"end":         FrameTypeEnd,
	"subscribe":   FrameTypeSubscribe,
	"unsubscribe": FrameTypeUnsubscribe,
	"auth":        FrameTypeAuth,
	"auth_ok":     FrameTypeAuthOk,
	"ping":        FrameTypePing,
	"pong":        FrameTypePong,
}

var frameTypeToWsType = map[FrameType]string{
	FrameTypeInvoke:      "invoke",
	FrameTypeReply:       "reply",
	FrameTypeError:       "error",
	FrameTypeChunk:       "chunk",
	FrameTypeEnd:         "end",
	FrameTypeSubscribe:   "subscribe",
	FrameTypeUnsubscribe: "unsubscribe",
	FrameTypeAuth:        "auth",
	FrameTypeAuthOk:      "auth_ok",
	FrameTypePing:        "ping",
	FrameTypePong:        "pong",
}

func (s *Server) wireFrameToWsFrame(wire *WireFrame) (*wsFrame, error) {
	payload, err := Decompress(wire.Payload, wire.Flags.Compression())
	if err != nil {
		return nil, err
	}
	if s.binaryOnly && wire.Flags.Encoding() == EncodingJSON {
		return nil, errors.New("binary-only: JSON-encoded payload not allowed")
	}
	sinceSeqNo := int64(0)
	seqNo := int64(wire.Seq)
	if frameTypeToWsType[wire.Type] == "subscribe" {
		sinceSeqNo = int64(wire.Seq)
		seqNo = 0
	}
	return &wsFrame{
		Type:            frameTypeToWsType[wire.Type],
		ReqID:           int64(wire.CorID),
		CallID:          wire.CallID,
		Target:          wire.Target,
		From:            wire.From,
		Payload:         payload,
		SubID:           wire.SubID,
		SinceSeqNo:      sinceSeqNo,
		SeqNo:           seqNo,
		TransID:         int64(wire.TransID),
		Message:         wire.ErrorMsg,
		payloadIsBinary: wire.Flags.Encoding() == EncodingBinary,
	}, nil
}

var tbcMagic = []byte{0x54, 0x42, 0x43, 0x02} // "TBC\x02"

func isTBCData(data []byte) bool {
	return len(data) >= len(tbcMagic) && string(data[:len(tbcMagic)]) == string(tbcMagic)
}

func wsFrameToWireFrame(f *wsFrame) (*WireFrame, error) {
	ft, ok := wsTypeToFrameType[f.Type]
	if !ok {
		ft = FrameTypeError
	}
	payload := []byte(f.Payload)
	compressed, comp, err := Compress(payload)
	if err != nil {
		return nil, err
	}
	enc := EncodingJSON
	if isTBCData(payload) {
		enc = EncodingBinary
	}
	// For subscribe frames, carry SinceSeqNo in the Seq slot so the
	// binary protocol can resume subscriptions. For all other types,
	// Seq carries the chunk SeqNo.
	seq := f.SeqNo
	if f.Type == "subscribe" {
		seq = f.SinceSeqNo
	}
	return &WireFrame{
		Flags:    MakeFlags(enc, comp),
		Type:     ft,
		CorID:    uint64(f.ReqID),
		Seq:      uint32(seq),
		TransID:  uint64(f.TransID),
		CallID:   f.CallID,
		SubID:    f.SubID,
		Target:   f.Target,
		From:     f.From,
		ErrorMsg: f.Message,
		Payload:  compressed,
	}, nil
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

// encodeAuthOk builds a TBC-encoded AuthOk{Manifest: manifestJSON} payload.
func encodeAuthOk(manifestJSON []byte) json.RawMessage {
	desc, _ := schema.BuiltinDesc(schema.BuiltinAuthOk)
	_, _ = schema.BuiltinObject(schema.BuiltinAuthOk)
	val := map[string]any{"Manifest": string(manifestJSON)}
	id, _ := identity.NewCanonicalID(0, 0, 0, 0)
	bc := &transport.BinaryCodec{}
	view, err := bc.Encode(desc, id, val)
	if err != nil {
		return nil
	}
	return json.RawMessage(view.Data)
}

// decodeAuthReq decodes a TBC-encoded AuthReq payload and returns the token.
func decodeAuthReq(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	if !isTBCData(payload) {
		return string(payload)
	}
	desc, _ := schema.BuiltinDesc(schema.BuiltinAuthReq)
	bc := &transport.BinaryCodec{}
	view := transport.View{Kind: transport.ViewKindFull, Schema: desc, Data: payload}
	result, err := bc.Decode(view)
	if err != nil {
		return string(payload)
	}
	if m, ok := result.(map[string]any); ok {
		if t, ok := m["Token"].(string); ok {
			return t
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// WebSocket adapter — implements FrameConn over *websocket.Conn
// ---------------------------------------------------------------------------

// ErrBinaryOnlyTextFrame is returned by wsFrameConn.Recv when a text frame
// arrives on a binary-only connection. The session sends an error frame to
// the peer before closing.
var ErrBinaryOnlyTextFrame = errors.New("binary-only: text frame not allowed")

// ---------------------------------------------------------------------------
// Outbound never-blocking contract (aligned with the desktop Wails raw
// transport): Send only marshals and enqueues; a dedicated writeLoop owns the
// socket; a slow or suspended peer can stall only its own connection — never
// the gateway session, whose worker pool serializes on sendMu around Send,
// and never other clients.
// ---------------------------------------------------------------------------

const (
	// wsOutChCapacity bounds the fast-path queue consumed by writeLoop.
	wsOutChCapacity = 64

	// wsOverflowBudget bounds the overflow queue that absorbs frames while
	// the peer cannot accept writes (slow reader, suspended tab). Send never
	// blocks on a full outCh — it spills here instead — so a stalled client
	// stalls the stream without back-pressuring the gateway session and
	// without dropping a frame: buffered frames flush in order once the peer
	// resumes. Exceeding the budget force-closes the connection; the client
	// reconnects and resubscribes, and event rings replay the gap via
	// sinceSeqNo, so recovery is lossless for ring-backed streams.
	wsOverflowBudget = 32 << 20 // 32 MiB

	// wsStallForceClose is the no-progress horizon. A write that completes —
	// successfully or not — within the horizon is normal backpressure; zero
	// progress for the full horizon means the peer is dead rather than slow
	// (a hidden tab stalls reads for minutes without being wedged), and only
	// then is the connection force-closed.
	wsStallForceClose = 10 * time.Minute

	// wsCloseDrainTimeout bounds how long Close waits for writeLoop to flush
	// queued frames. A wedged writeLoop must not hold the session teardown
	// (sess.send → conn.Close) hostage, so past this budget the drain is
	// abandoned and the raw conn is closed to unwedge the writer.
	wsCloseDrainTimeout = 5 * time.Second
)

// wsFrameConn adapts a gorilla *websocket.Conn to the FrameConn interface.
// It handles WebSocket-specific concerns (ping/pong, read deadlines, binary
// and text frame encoding) while delegating all protocol logic to Session.
type wsFrameConn struct {
	conn       *websocket.Conn
	logger     actor.Logger
	writeMu    sync.Mutex // serializes WriteMessage/WriteControl between writeLoop and pingLoop
	textMode   atomic.Bool
	binaryOnly bool

	outCh chan wsOutbound
	// closed is closed exactly once by Close/signalClose; writeLoop and
	// enqueueOut treat it as the connection's death certificate.
	closed chan struct{}
	// writeDone is closed by writeLoop on exit. Close waits on it with a
	// bounded drain instead of a WaitGroup: a WaitGroup cannot time out,
	// and a writeLoop wedged in WriteMessage must not block Close forever.
	writeDone chan struct{}

	// ofMu guards the overflow queue. Ordering invariant: everything in
	// outCh predates everything in overflow. An enqueue spills to overflow
	// only when outCh is full, and stays on overflow until drained, so while
	// overflow is non-empty no new item can enter outCh. Consumers must
	// therefore drain outCh to empty before taking any overflow item.
	ofMu          sync.Mutex
	overflow      []wsOutbound
	overflowBytes int

	// Tuning knobs; zero values resolve to the package defaults.
	stallForceClose time.Duration
	closeDrainWait  time.Duration
	overflowBudget  int

	closeOnce sync.Once
}

// wsOutbound is one fully-marshaled outbound message. Marshaling in Send
// (under the session's sendMu) keeps writeLoop free of encoding work and
// preserves the TransID order established at enqueue time.
type wsOutbound struct {
	data []byte
	text bool
}

func newWsFrameConn(conn *websocket.Conn, logger actor.Logger, binaryOnly bool) *wsFrameConn {
	return newWsFrameConnTuned(conn, logger, binaryOnly, wsConnTuning{})
}

// wsConnTuning carries per-connection overrides; a zero field falls back to
// its package default, so tests need set only the knobs they exercise.
type wsConnTuning struct {
	stallForceClose time.Duration
	closeDrainWait  time.Duration
	overflowBudget  int
}

func newWsFrameConnTuned(conn *websocket.Conn, logger actor.Logger, binaryOnly bool, t wsConnTuning) *wsFrameConn {
	if t.stallForceClose <= 0 {
		t.stallForceClose = wsStallForceClose
	}
	if t.closeDrainWait <= 0 {
		t.closeDrainWait = wsCloseDrainTimeout
	}
	if t.overflowBudget <= 0 {
		t.overflowBudget = wsOverflowBudget
	}
	c := &wsFrameConn{
		conn:            conn,
		logger:          logger,
		binaryOnly:      binaryOnly,
		outCh:           make(chan wsOutbound, wsOutChCapacity),
		closed:          make(chan struct{}),
		writeDone:       make(chan struct{}),
		stallForceClose: t.stallForceClose,
		closeDrainWait:  t.closeDrainWait,
		overflowBudget:  t.overflowBudget,
	}
	go c.writeLoop()
	go c.pingLoop()
	return c
}

func (c *wsFrameConn) pingLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			c.writeMu.Lock()
			err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			c.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// Recv reads the next WebSocket message and returns it as a WireFrame.
func (c *wsFrameConn) Recv() (*WireFrame, error) {
	c.conn.SetReadDeadline(time.Now().Add(65 * time.Second))

	mt, msg, err := c.conn.ReadMessage()
	if err != nil {
		if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
			if c.logger != nil {
				c.logger.Warn("gateway: ws unexpected close", "err", err)
			}
		}
		return nil, err
	}

	if mt == websocket.BinaryMessage {
		wire, err := UnmarshalWireFrame(msg)
		if err != nil {
			return nil, err
		}
		return wire, nil
	}

	// Text frame.
	if c.binaryOnly {
		c.textMode.Store(true)
		return nil, ErrBinaryOnlyTextFrame
	}

	// Text frame — convert JSON wsFrame to WireFrame.
	var f wsFrame
	if err := json.Unmarshal(msg, &f); err != nil {
		return nil, errors.New("invalid frame")
	}
	c.textMode.Store(true)
	wire, err := wsFrameToWireFrame(&f)
	if err != nil {
		return nil, err
	}
	return wire, nil
}

// wsWriteTimeout bounds each outbound data write performed by writeLoop. The
// deadline constrains only the writer goroutine — Send never touches the
// socket — so a client whose TCP window is closed stalls its own writeLoop
// for at most this long before the write fails and the connection is torn
// down.
var wsWriteTimeout = 30 * time.Second

// Send marshals a WireFrame and enqueues it for writeLoop. It never blocks
// and never touches the socket: the fast path fills the bounded outCh, and
// once that is full frames spill into the byte-budgeted overflow queue. The
// write deadline therefore bounds writeLoop, not the caller — session.sendMu
// only ever covers marshal + enqueue, so no client behavior can park the
// session's worker pool on this connection.
func (c *wsFrameConn) Send(wire *WireFrame) error {
	if c.textMode.Load() {
		// Text mode: convert WireFrame back to wsFrame JSON. The wire
		// payload may be gzip-compressed (wsFrameToWireFrame compresses
		// large payloads); JSON frames carry it inline and uncompressed,
		// so decompress first — otherwise marshaling the gzip bytes as
		// *jsontext.Value fails and kills the session.
		payload, err := Decompress(wire.Payload, wire.Flags.Compression())
		if err != nil {
			return err
		}
		f := &wsFrame{
			Type:    frameTypeToWsType[wire.Type],
			ReqID:   int64(wire.CorID),
			CallID:  wire.CallID,
			Target:  wire.Target,
			From:    wire.From,
			Payload: payload,
			SubID:   wire.SubID,
			SeqNo:   int64(wire.Seq),
			TransID: int64(wire.TransID),
			Message: wire.ErrorMsg,
		}
		data, err := json.Marshal(f)
		if err != nil {
			return err
		}
		return c.enqueueOut(wsOutbound{data: data, text: true})
	}
	data, err := MarshalWireFrame(wire)
	if err != nil {
		return err
	}
	return c.enqueueOut(wsOutbound{data: data})
}

// enqueueOut pushes a marshaled frame toward writeLoop. It never blocks:
// while writeLoop is stalled (peer stopped reading, TCP window closed),
// frames spill from the bounded outCh into the byte-budgeted overflow queue
// and Send keeps succeeding, so the gateway session is never back-pressured
// and no frame is dropped — buffered frames flush in order once the peer
// resumes. Once the budget is exceeded the transport force-closes itself and
// io.EOF propagates like any send failure.
func (c *wsFrameConn) enqueueOut(item wsOutbound) error {
	c.ofMu.Lock()
	defer c.ofMu.Unlock()
	if c.isClosedLocked() {
		return io.EOF
	}
	// Fast path while no overflow exists. Sending under ofMu is safe: the
	// send is non-blocking, and writeLoop never takes ofMu to receive.
	if len(c.overflow) == 0 {
		select {
		case c.outCh <- item:
			return nil
		default:
		}
	}
	if c.overflowBytes+len(item.data) > c.overflowBudget {
		if c.logger != nil {
			c.logger.Warn("gateway: ws overflow budget exceeded; force-closing",
				"budgetBytes", c.overflowBudget, "queuedBytes", c.overflowBytes+len(item.data))
		}
		c.signalClose()
		return io.EOF
	}
	c.overflow = append(c.overflow, item)
	c.overflowBytes += len(item.data)
	return nil
}

// popOverflow removes the head of the overflow queue. Callers must only
// consume overflow items after outCh is drained to empty (ordering invariant,
// see ofMu).
func (c *wsFrameConn) popOverflow() (wsOutbound, bool) {
	c.ofMu.Lock()
	defer c.ofMu.Unlock()
	if len(c.overflow) == 0 {
		return wsOutbound{}, false
	}
	item := c.overflow[0]
	c.overflow[0] = wsOutbound{}
	c.overflow = c.overflow[1:]
	c.overflowBytes -= len(item.data)
	if len(c.overflow) == 0 {
		c.overflow = nil // release the slice backing array
	}
	return item, true
}

// isClosedLocked reports whether the connection is closed; the caller is
// expected to hold ofMu so the closed-check and the subsequent enqueue
// decision are atomic with respect to the drain path.
func (c *wsFrameConn) isClosedLocked() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// writeLoop is the single writer goroutine. Ordering: outCh contents
// strictly predate overflow contents (see ofMu), so outCh must be consumed
// to empty before taking any overflow item. A full outCh is always
// receivable and a non-empty overflow is always poppable, so neither fast
// path can stall. When closed fires, it drains both queues so no queued
// frame is lost before the conn goes away.
func (c *wsFrameConn) writeLoop() {
	defer close(c.writeDone)
	for {
		select {
		case item := <-c.outCh:
			if !c.writeItem(item) {
				return
			}
			continue
		default:
		}
		if item, ok := c.popOverflow(); ok {
			if !c.writeItem(item) {
				return
			}
			continue
		}
		select {
		case item := <-c.outCh:
			if !c.writeItem(item) {
				return
			}
		case <-c.closed:
			c.drainOut()
			return
		}
	}
}

// writeItem writes one frame under writeMu with the write deadline and
// reports whether the connection is still usable.
func (c *wsFrameConn) writeItem(item wsOutbound) bool {
	mt := websocket.BinaryMessage
	if item.text {
		mt = websocket.TextMessage
	}
	ok := c.writeGuard(func() error {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
		return c.conn.WriteMessage(mt, item.data)
	})
	if !ok {
		// The write failed (deadline exceeded on a dead peer, or the conn
		// was closed under us). The connection is unusable: tear it down so
		// the session read loop unblocks and upstream streams release.
		c.signalClose()
	}
	return ok
}

// writeGuard runs fn (one WriteMessage) and reports success. fn is bounded
// by the write deadline; the guard adds the no-progress horizon as a second,
// coarser bound: a write that returns — successfully or not — within the
// horizon is normal backpressure, but zero progress for the full horizon
// means the peer is dead rather than slow (a suspended tab stalls for
// minutes without being wedged), so the transport force-closes itself.
// teardown → reconnect → sinceSeqNo replay keeps the stream lossless.
func (c *wsFrameConn) writeGuard(fn func() error) bool {
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(c.stallForceClose)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			if c.logger != nil {
				c.logger.Warn("gateway: ws write made no progress for the stall horizon; force-closing",
					"horizon", c.stallForceClose)
			}
			c.signalClose()
		}
	}()
	err := fn()
	close(done)
	return err == nil
}

// drainOut flushes both queues in order after closed fires; it stops at the
// first write failure (the conn is gone) so it cannot linger.
func (c *wsFrameConn) drainOut() {
	for {
		select {
		case item := <-c.outCh:
			if !c.writeItem(item) {
				return
			}
		default:
			item, ok := c.popOverflow()
			if !ok {
				return
			}
			if !c.writeItem(item) {
				return
			}
		}
	}
}

// signalClose marks the connection closed and closes the raw conn without
// waiting for writeLoop to drain. Closing the raw conn unblocks the
// session's Recv (read loop exits, teardown releases subscriptions and
// worker slots) and unwedges any WriteMessage parked under writeMu. Callers
// are the force-close paths (overflow budget, stall horizon) and write
// failures — situations where the connection is already unusable and
// draining is pointless.
func (c *wsFrameConn) signalClose() {
	c.closeOnce.Do(func() { close(c.closed) })
	_ = c.conn.Close()
}

// Close terminates the connection, giving writeLoop a bounded window
// (closeDrainWait) to flush frames still queued in outCh/overflow, then
// closes the raw conn. The bounded wait is the point: an unbounded drain
// would hold the gateway teardown hostage behind a wedged writer, while the
// final raw close guarantees Recv unblocks and the writer unwedges even when
// the drain was abandoned.
func (c *wsFrameConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	select {
	case <-c.writeDone:
	case <-time.After(c.closeDrainWait):
		if c.logger != nil {
			c.logger.Warn("gateway: ws close drain abandoned; writeLoop stalled", "wait", c.closeDrainWait)
		}
	}
	_ = c.conn.Close()
	return nil
}

// ---------------------------------------------------------------------------
// WebSocket HTTP handler — thin adapter around ServeSession
// ---------------------------------------------------------------------------
// WebSocket HTTP handler — thin adapter around ServeSession
// ---------------------------------------------------------------------------

// wsUpgradeInFlight counts WebSocket upgrades in flight. When a mass
// force-close (e.g. an event storm exhausting many sessions' overflow
// budgets at once) makes every client reconnect simultaneously, the accept
// side would otherwise process the whole herd in one burst: auth handshakes,
// resubscribes and sinceSeqNo ring replays all stampeding together. When
// more than wsUpgradeJitterThreshold upgrades are in flight at once — the
// signature of such a herd — each is delayed by a random 0–2s so reconnects
// arrive staggered. Isolated reconnects and normal page loads pass through
// untouched; the client already backs off 0.5–1.5s on its own.
var wsUpgradeInFlight atomic.Int32

const (
	wsUpgradeJitterThreshold = 8
	wsUpgradeJitterMax       = 2 * time.Second
)

// to ServeSession for all protocol logic.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if wsUpgradeInFlight.Add(1) > wsUpgradeJitterThreshold {
		time.Sleep(time.Duration(rand.Int63n(int64(wsUpgradeJitterMax) + 1)))
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	wsUpgradeInFlight.Add(-1)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("gateway: ws upgrade", "err", err)
		}
		return
	}

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(65 * time.Second))
		return nil
	})

	customerID := r.Header.Get("X-Customer-ID")
	role := "anonymous"
	subject := ""

	// Optional connection-time auth via URL query.
	var urlAuth URLAuth
	if s.urlAuth != nil {
		if authRole, authSubject, err := s.urlAuth(r.Context(), r.URL.Query(), s.app); err == nil {
			role = authRole
			subject = authSubject
			if s.logger != nil {
				s.logger.Info("gateway: ws URL auth", "role", role, "subject", subject, "remote", r.RemoteAddr)
			}
		} else if r.URL.Query().Get("token") != "" {
			if s.logger != nil {
				s.logger.Warn("gateway: ws URL auth failed", "err", err, "remote", r.RemoteAddr)
			}
		}
		// When urlAuth is configured, enable frame-level auth handshake.
		urlAuth = s.urlAuth
	}

	fc := newWsFrameConn(conn, s.logger, s.binaryOnly)
	defer fc.Close()
	s.ServeSession(r.Context(), fc, SessionOptions{
		CustomerID: customerID,
		Role:       role,
		Subject:    subject,
		URLAuth:    urlAuth,
	})
}
