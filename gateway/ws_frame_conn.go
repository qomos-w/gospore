package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/qomos-w/gospore/actor"
)

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

	// wsPingInterval is how often pingLoop sends a WebSocket ping.
	wsPingInterval = 30 * time.Second

	// wsReadIdleTimeout bounds how long the server waits for the next peer
	// message (data frame or pong) before declaring the connection dead.
	// It must exceed wsPingInterval by a comfortable margin so a live but
	// quiet client (one that only answers pings) is not dropped.
	wsReadIdleTimeout = 65 * time.Second

	// wsPingWriteTimeout bounds each ping control write.
	wsPingWriteTimeout = 5 * time.Second
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
	stallForceClose  time.Duration
	closeDrainWait   time.Duration
	overflowBudget   int
	pingInterval     time.Duration
	readIdle         time.Duration
	writeTimeout     time.Duration
	pingWriteTimeout time.Duration

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

	// outChCapacity bounds the fast-path queue consumed by writeLoop.
	outChCapacity int
	// pingInterval is how often pingLoop sends a WebSocket ping.
	pingInterval time.Duration
	// readIdle bounds how long Recv waits for the next peer message
	// (also refreshed by pongs). Should exceed pingInterval by a
	// comfortable margin so a live-but-quiet client is not dropped.
	readIdle time.Duration
	// writeTimeout bounds each outbound data write.
	writeTimeout time.Duration
	// pingWriteTimeout bounds each ping control write.
	pingWriteTimeout time.Duration
}

// resolve fills zero tuning fields with the package defaults and returns
// the effective values used by the connection.
func (t wsConnTuning) resolve() wsConnTuning {
	if t.stallForceClose <= 0 {
		t.stallForceClose = wsStallForceClose
	}
	if t.closeDrainWait <= 0 {
		t.closeDrainWait = wsCloseDrainTimeout
	}
	if t.overflowBudget <= 0 {
		t.overflowBudget = wsOverflowBudget
	}
	if t.outChCapacity <= 0 {
		t.outChCapacity = wsOutChCapacity
	}
	if t.pingInterval <= 0 {
		t.pingInterval = wsPingInterval
	}
	if t.readIdle <= 0 {
		t.readIdle = wsReadIdleTimeout
	}
	if t.writeTimeout <= 0 {
		t.writeTimeout = wsWriteTimeout
	}
	if t.pingWriteTimeout <= 0 {
		t.pingWriteTimeout = wsPingWriteTimeout
	}
	return t
}

func newWsFrameConnTuned(conn *websocket.Conn, logger actor.Logger, binaryOnly bool, t wsConnTuning) *wsFrameConn {
	t = t.resolve()
	c := &wsFrameConn{
		conn:             conn,
		logger:           logger,
		binaryOnly:       binaryOnly,
		outCh:            make(chan wsOutbound, t.outChCapacity),
		closed:           make(chan struct{}),
		writeDone:        make(chan struct{}),
		stallForceClose:  t.stallForceClose,
		closeDrainWait:   t.closeDrainWait,
		overflowBudget:   t.overflowBudget,
		pingInterval:     t.pingInterval,
		readIdle:         t.readIdle,
		writeTimeout:     t.writeTimeout,
		pingWriteTimeout: t.pingWriteTimeout,
	}
	go c.writeLoop()
	go c.pingLoop()
	return c
}

func (c *wsFrameConn) pingLoop() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			c.writeMu.Lock()
			err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.pingWriteTimeout))
			c.writeMu.Unlock()
			if err != nil {
				// A ping control write failing within its deadline means
				// the peer cannot accept even control frames — the conn is
				// dead, not slow. Tear down instead of silently retiring
				// this loop and leaving the session to trip the (much
				// longer) read-idle deadline.
				if c.logger != nil {
					c.logger.Warn("gateway: ws ping write failed; force-closing", "err", err)
				}
				c.signalClose()
				return
			}
		}
	}
}

// Recv reads the next WebSocket message and returns it as a WireFrame.
func (c *wsFrameConn) Recv() (*WireFrame, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.readIdle))

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

// wsWriteTimeout is the default bound for each outbound data write performed
// by writeLoop (overridable per connection via wsConnTuning /
// gateway.Server WSTimeouts). The deadline constrains only the writer
// goroutine — Send never touches the socket — so a client whose TCP window
// is closed stalls its own writeLoop for at most this long before the write
// fails and the connection is torn down.
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
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
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
