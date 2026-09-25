package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/ref"
)

// FrameConn is a transport-agnostic bidirectional wire-frame stream.
// Each transport adapter (WebSocket, Wails IPC) implements this interface;
// ServeSession handles all protocol logic (auth handshake, invoke dispatch,
// subscription management) without knowing how frames reach the peer.
type FrameConn interface {
	// Recv blocks until the next inbound WireFrame arrives.
	// Returns io.EOF or another error when the connection is closed.
	Recv() (*WireFrame, error)

	// Send writes a WireFrame to the peer. Must be safe for concurrent use.
	Send(*WireFrame) error

	// Close terminates the connection and unblocks Recv.
	Close() error
}

// SessionOptions carries per-connection identity and configuration.
type SessionOptions struct {
	CustomerID string
	Role       string
	Subject    string
	// Token is an optional pre-validated JWT. When set and URLAuth is
	// configured, ServeSession resolves the identity immediately instead
	// of starting with an anonymous auth-timeout window.
	Token string
	// If non-nil, the session performs the auth handshake: it waits for
	// an inbound "auth" frame, resolves the token via this function, and
	// sends back an auth_ok frame before processing invoke/subscribe.
	// When nil, the session runs with the identity in Role/Subject.
	URLAuth URLAuth
}

// session is the transport-agnostic dispatch core. It owns the worker
// pool, subscription registry, auth state machine, and TransID counter.
type session struct {
	server *Server
	conn   FrameConn
	opts   SessionOptions

	// gateRef is the connection's dedicated gatesession caller cell.
	// Every invoke/subscribe the session issues resolves through it, so
	// reply slots (and slow-consumer backpressure) live in this cell's
	// pending table instead of the root pipeline shared by all sessions.
	// Written once in run before any worker starts; nil means spawn
	// failed and the legacy root-caller path is used.
	gateRef ref.Ref

	transID   atomic.Int64
	useBinary bool

	role    string
	subject string

	// sendMu serializes TransID assignment + conn.Send so that frames
	// leave in the same order their TransIDs were assigned. Without this,
	// concurrent workers (32 invoke goroutines + subscribe goroutine) can
	// assign TransID 1 then 2 but send 2 before 1, causing the client's
	// transId-gap detector to close the connection.
	sendMu sync.Mutex
}

func (sess *session) send(f wsFrame) {
	sess.sendMu.Lock()
	defer sess.sendMu.Unlock()
	f.TransID = sess.transID.Add(1)
	wire, err := wsFrameToWireFrame(&f)
	if err != nil {
		if sess.server.logger != nil {
			sess.server.logger.Error("gateway: session send marshal", "err", err)
		}
		return
	}
	if err := sess.conn.Send(wire); err != nil {
		if sess.server.logger != nil {
			sess.server.logger.Warn("gateway: session send failed; closing connection", "err", err)
		}
		// A failed write (e.g. write deadline exceeded on a stalled peer)
		// leaves the session wedged: Recv still blocks and every queued
		// frame would fail too. Close the conn so the read loop unblocks
		// and the session tears down — invoke streams cancel, subscribe
		// goroutines exit, upstream streams are released.
		_ = sess.conn.Close()
	}
}

// ServeSession runs the transport-agnostic dispatch loop on conn.
// It blocks until conn closes or ctx is cancelled. The caller is
// responsible for closing conn after ServeSession returns.
func (s *Server) ServeSession(ctx context.Context, conn FrameConn, opts SessionOptions) {
	if opts.URLAuth == nil {
		opts.URLAuth = s.urlAuth
	}

	// If a pre-validated token is provided, resolve identity immediately
	// so the session skips the anonymous auth-timeout window.
	if opts.Token != "" && opts.URLAuth != nil {
		q := url.Values{}
		q.Set("token", opts.Token)
		if role, subject, err := opts.URLAuth(ctx, q, s.app); err == nil {
			opts.Role = role
			opts.Subject = subject
		} else if s.logger != nil {
			s.logger.Warn("gateway: session token resolve failed", "err", err)
		}
	}

	sess := &session{
		server:  s,
		conn:    conn,
		opts:    opts,
		role:    opts.Role,
		subject: opts.Subject,
	}
	sess.run(ctx)
}

func (sess *session) run(ctx context.Context) {
	s := sess.server
	conn := sess.conn

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.logger != nil {
		s.logger.Info("gateway: session started", "role", sess.role, "subject", sess.subject)
	}
	defer func() {
		if s.logger != nil {
			s.logger.Info("gateway: session ended", "role", sess.role, "subject", sess.subject)
		}
	}()

	// Dedicated gatesession caller cell for this connection. Spawned on
	// the accept goroutine (µs-ms; never inside an actor lane) before
	// any worker or the read loop exists, so every invoke/subscribe the
	// session issues is already caller-isolated when the first frame
	// arrives. The destroy defer is registered before the subscription
	// cleanup defer, so LIFO tears subscriptions down first and the
	// pending-table flush never races a live worker.
	gate, gateDone := s.sessionGate("ws")
	if gate != nil {
		sess.gateRef = gate
		defer gateDone()
	}

	// Auth timeout: if auth is configured but identity is still anonymous,
	// enforce a random 5-15s window for the client to authenticate.
	var authCancel context.CancelFunc
	if sess.opts.URLAuth != nil && sess.role == "anonymous" {
		var authCtx context.Context
		authCtx, authCancel = context.WithTimeout(ctx, time.Duration(5+rand.Intn(11))*time.Second)
		go func() {
			<-authCtx.Done()
			if errors.Is(authCtx.Err(), context.DeadlineExceeded) {
				if s.logger != nil {
					s.logger.Warn("gateway: session auth timeout")
				}
				sess.send(wsFrame{Type: "error", Message: "auth timeout"})
				conn.Close()
			}
		}()
	}

	// Subscription registry.
	subs := &subscriptionMap{m: make(map[string]subEntry)}
	defer func() {
		subs.Lock()
		for _, e := range subs.m {
			e.cancel()
		}
		subs.Unlock()
		subs.wg.Wait()
	}()

	// Worker pool.
	type workerMsg struct {
		frame wsFrame
	}
	invokeCh := make(chan workerMsg, 64)
	var workers sync.WaitGroup

	const invokePoolSize = 128
	for i := 0; i < invokePoolSize; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for msg := range invokeCh {
				sess.handleInvoke(ctx, msg.frame, subs)
			}
		}()
	}

	// Subscribe handling is sharded by subId: frames for the same subId
	// stay on one shard so cancel-then-resubscribe ordering is preserved,
	// while the reconnect resubscribe storm (dozens of distinct subIds,
	// each a synchronous beginSubscription call establishment) re-establishes
	// in parallel instead of serially behind one worker.
	const subscribeShardCount = 8
	subscribeShards := make([]chan workerMsg, subscribeShardCount)
	for i := range subscribeShards {
		subscribeShards[i] = make(chan workerMsg, 64)
		workers.Add(1)
		go func(ch chan workerMsg) {
			defer workers.Done()
			for msg := range ch {
				switch msg.frame.Type {
				case "subscribe":
					sess.handleSubscribe(ctx, msg.frame, subs)
				case "unsubscribe":
					subs.Lock()
					if e, ok := subs.m[msg.frame.SubID]; ok {
						e.cancel()
						delete(subs.m, msg.frame.SubID)
					}
					subs.Unlock()
				}
			}
		}(subscribeShards[i])
	}

	// Read loop.
	go func() {
		defer func() {
			close(invokeCh)
			for _, ch := range subscribeShards {
				close(ch)
			}
		}()
		for {
			wire, err := conn.Recv()
			if err != nil {
				if errors.Is(err, ErrBinaryOnlyTextFrame) {
					sess.send(wsFrame{Type: "error", Message: err.Error()})
				} else if !errors.Is(err, io.EOF) && s.logger != nil {
					s.logger.Error("gateway: session recv", "err", err)
				}
				return
			}

			sess.useBinary = true

			framePtr, err := s.wireFrameToWsFrame(wire)
			if err != nil {
				sess.send(wsFrame{Type: "error", Message: err.Error()})
				continue
			}
			frame := *framePtr

			switch frame.Type {
			case "auth":
				sess.handleAuthFrame(ctx, frame, authCancel)
			case "invoke":
				invokeCh <- workerMsg{frame: frame}
			case "subscribe", "unsubscribe":
				subscribeShards[subscribeShardIndex(frame.SubID)%uint32(len(subscribeShards))] <- workerMsg{frame: frame}
			case "ping":
				sess.send(wsFrame{Type: "pong"})
			}
		}
	}()

	workers.Wait()
}

func (sess *session) handleAuthFrame(ctx context.Context, f wsFrame, authCancel context.CancelFunc) {
	s := sess.server
	if sess.opts.URLAuth == nil {
		sess.send(wsFrame{Type: "error", Message: "auth not supported"})
		return
	}
	payload := f.Payload
	if s.binaryOnly && !f.payloadIsBinary {
		sess.send(wsFrame{Type: "error", Message: "binary-only: auth payload must be TBC-encoded"})
		return
	}
	token := decodeAuthReq(payload)
	q := url.Values{}
	if token != "" {
		q.Set("token", token)
	}
	if s.logger != nil {
		s.logger.Info("gateway: session auth frame", "tokenPresent", token != "", "currentRole", sess.role)
	}
	authRole, authSubject, err := sess.opts.URLAuth(ctx, q, s.app)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("gateway: session auth failed", "err", err)
		}
		sess.send(wsFrame{Type: "error", Message: "auth failed: " + err.Error()})
		return
	}
	// Only apply the auth frame result when it provides a real identity.
	// If the auth frame failed to extract a token (empty subject), keep
	// the identity resolved during token pre-parsing in ServeSession —
	// otherwise we silently downgrade a pre-authenticated session to
	// anonymous.
	if authSubject != "" {
		sess.role = authRole
		sess.subject = authSubject
	}
	if s.logger != nil {
		s.logger.Info("gateway: session auth ok", "role", sess.role, "subject", sess.subject)
	}
	if authCancel != nil {
		authCancel()
	}

	manifestJSON := s.app.ManifestJSON()
	if gm, err := s.app.ExportGosporeManifest(); err == nil {
		if data, err := json.Marshal(gm); err == nil {
			manifestJSON = data
		}
	}
	var authOkPayload json.RawMessage
	if s.binaryOnly || sess.useBinary {
		authOkPayload = encodeAuthOk(manifestJSON)
	} else {
		authOkPayload, _ = json.Marshal(map[string]string{"Manifest": string(manifestJSON)})
	}
	sess.send(wsFrame{Type: "auth_ok", Payload: authOkPayload, payloadIsBinary: sess.useBinary || s.binaryOnly})
}

func (sess *session) handleInvoke(ctx context.Context, f wsFrame, subs *subscriptionMap) {
	s := sess.server
	var payload any
	if len(f.Payload) > 0 {
		if f.payloadIsBinary {
			payload = []byte(f.Payload)
		} else if s.binaryOnly {
			sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: "binary-only: invoke payload must be TBC-encoded"})
			return
		} else if err := json.Unmarshal(f.Payload, &payload); err != nil {
			sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: "invalid payload"})
			return
		}
	}

	req := &GatewayRequest{
		CallID:     f.CallID,
		CustomerID: sess.opts.CustomerID,
		Role:       sess.role,
		Service:    serviceFromCallID(f.CallID),
		Target:     f.Target,
		From:       f.From,
		Payload:    payload,
		Headers:    map[string]string{},
	}

	if err := s.interceptor.Before(ctx, req); err != nil {
		sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: err.Error()})
		return
	}

	raw, err := s.invokeActor(ctx, sess.gateRef, f.CallID, f.Target, f.From, payload, sess.opts.CustomerID, sess.role, sess.subject, f.TimeoutMs)

	if err != nil && errors.Is(err, io.EOF) {
		err = nil
	}

	resp := &GatewayResponse{Duration: 0}
	if err != nil {
		resp.Error = err
		sess.send(wsFrame{Type: "error", ReqID: f.ReqID, CallID: f.CallID, Message: err.Error()})
	} else {
		sess.send(wsFrame{Type: "reply", ReqID: f.ReqID, CallID: f.CallID, Payload: raw})
	}

	go func() {
		defer func() { recover() }()
		s.interceptor.After(ctx, req, resp)
	}()
}

// subscribeShardIndex maps a subId to a fixed shard with FNV-1a so every
// frame for one subscription is processed by the same serial worker.
func subscribeShardIndex(subID string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(subID); i++ {
		h ^= uint32(subID[i])
		h *= 16777619
	}
	return h
}

// subscriptionMap is the shared subscription registry type.
type subscriptionMap struct {
	sync.Mutex
	m   map[string]subEntry
	seq uint64
	wg  sync.WaitGroup
}

// subEntry ties one registry slot to the identity of its latest
// registration. Resubscribing the same SubID cancels the displaced entry
// and installs a new one; the displaced pump's exit must not delete the
// newer registration (delete-by-key would strand it without a cancel,
// wedging session teardown in wg.Wait forever).
type subEntry struct {
	cancel context.CancelFunc
	id     uint64
}

func (sess *session) handleSubscribe(ctx context.Context, f wsFrame, subs *subscriptionMap) {
	s := sess.server
	if s.logger != nil {
		s.logger.Info("gateway: session subscribe", "callID", f.CallID, "target", f.Target, "subId", f.SubID)
	}
	var payload any
	if len(f.Payload) > 0 {
		if f.payloadIsBinary {
			payload = []byte(f.Payload)
		} else if s.binaryOnly {
			sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: "binary-only: subscribe payload must be TBC-encoded"})
			return
		} else if err := json.Unmarshal(f.Payload, &payload); err != nil {
			sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: "invalid payload"})
			return
		}
	}

	req := &GatewayRequest{
		CallID:     f.CallID,
		CustomerID: sess.opts.CustomerID,
		Role:       sess.role,
		Service:    serviceFromCallID(f.CallID),
		Target:     f.Target,
		From:       f.From,
		Payload:    payload,
		Headers:    map[string]string{},
	}

	if err := s.interceptor.Before(ctx, req); err != nil {
		sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: err.Error()})
		return
	}

	subCtx, cancel := context.WithCancel(ctx)
	subs.Lock()
	// Resubscribe with the same SubID displaces the previous subscription's
	// cancel from the registry. Fire the displaced cancel now: nothing else
	// ever will (the session teardown loop only walks entries still in the
	// map, and the request ctx cancels only after ServeSession returns —
	// which itself waits on this very wg). Leaving it unfired wedges the old
	// pump in RecvRaw forever and deadlocks session teardown in wg.Wait.
	if prev, ok := subs.m[f.SubID]; ok {
		prev.cancel()
	}
	subs.seq++
	entry := subEntry{cancel: cancel, id: subs.seq}
	subs.m[f.SubID] = entry
	subs.Unlock()

	call, err := s.beginSubscription(subCtx, sess.gateRef, f.CallID, f.Target, f.From, payload, sess.opts.CustomerID, sess.role, sess.subject)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("gateway: session subscribe failed", "callID", f.CallID, "target", f.Target, "err", err)
		}
		cancel()
		subs.Lock()
		if cur, ok := subs.m[f.SubID]; ok && cur.id == entry.id {
			delete(subs.m, f.SubID)
		}
		subs.Unlock()
		sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: err.Error()})
		return
	}

	subs.wg.Add(1)
	go func() {
		if s.logger != nil {
			s.logger.Info("gateway: session subscribe goroutine START", "callID", f.CallID, "subId", f.SubID)
		}
		defer call.Close()
		defer func() {
			if s.logger != nil {
				s.logger.Info("gateway: session subscribe goroutine EXIT", "callID", f.CallID, "subId", f.SubID)
			}
			subs.Lock()
			// Delete only when this slot still belongs to this
			// registration: a resubscribe has since displaced us, and
			// removing the newer entry would strand it without a cancel.
			if cur, ok := subs.m[f.SubID]; ok && cur.id == entry.id {
				delete(subs.m, f.SubID)
			}
			subs.Unlock()
			cancel()
			subs.wg.Done()
		}()

		go func() {
			<-subCtx.Done()
			call.Cancel()
		}()

		start := time.Now()
		resp := &GatewayResponse{}
		seqNo := f.SinceSeqNo

		for {
			raw, err := call.RecvRaw()
			if err == io.EOF {
				if s.logger != nil {
					s.logger.Info("gateway: session subscribe RecvRaw EOF", "callID", f.CallID, "subId", f.SubID)
				}
				resp.Duration = time.Since(start)
				sess.send(wsFrame{Type: "end", SubID: f.SubID})
				break
			}
			if err != nil {
				if s.logger != nil {
					s.logger.Error("gateway: session subscribe RecvRaw ERROR", "callID", f.CallID, "subId", f.SubID, "err", err)
				}
				resp.Error = err
				resp.Duration = time.Since(start)
				sess.send(wsFrame{Type: "error", ReqID: f.ReqID, Message: err.Error()})
				break
			}
			if s.logger != nil {
				s.logger.Debug("gateway: session subscribe RecvRaw CHUNK", "callID", f.CallID, "subId", f.SubID, "seqNo", seqNo+1, "rawLen", len(raw))
			}
			seqNo++
			sess.send(wsFrame{Type: "chunk", SubID: f.SubID, SeqNo: seqNo, Payload: raw})
		}

		go func() {
			defer func() { recover() }()
			s.interceptor.After(ctx, req, resp)
		}()
	}()
}
