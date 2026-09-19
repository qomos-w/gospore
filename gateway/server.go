package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	pathpkg "path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/spore/identity"
)

var errServiceNotFound = errors.New("service not found")
var errInvokeFailed = errors.New("invoke failed")

// GatewayInvokeTimeout caps how long a unary WebSocket invoke blocks waiting
// for a reply. Without it, a dropped reply frame (e.g. replyQ full under load)
// causes the worker to block forever, eventually starving the invoke pool and
// timing out ALL subsequent invokes at the client (120s).
// Var (not const) so tests can shrink the window.
var GatewayInvokeTimeout = 60 * time.Second

// LogEntry is one structured log record.
type LogEntry struct {
	Timestamp string         `json:"ts"`
	Level     string         `json:"level"`
	Caller    string         `json:"caller,omitempty"`
	Message   string         `json:"msg"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// LogQuery carries filter parameters for the /logs endpoint.
type LogQuery struct {
	Level  string // exact level match (case-insensitive)
	Caller string // substring match on caller
	Limit  int    // max entries to return (0 = default 200)
}

// LogProvider returns structured log entries for the /logs endpoint.
type LogProvider interface {
	QueryLogs(opts LogQuery) []LogEntry
}

// URLAuth extracts caller identity (role + subject) from URL query parameters.
// Called once per WebSocket connection. Returning empty role means anonymous.
type URLAuth func(ctx context.Context, q url.Values, app AppHost) (role string, subject string, err error)

// Server is the HTTP gateway that sits between external callers and the
// actor system. It is not itself an actor; it runs in its own goroutine
// and uses the App's public surfaces (LookupService, Self, Codec).
type Server struct {
	app         AppHost
	interceptor GatewayInterceptor
	urlAuth     URLAuth
	httpServer  *http.Server
	listener    net.Listener
	listenerMu  sync.Mutex
	staticFS    fs.FS
	ready       chan struct{}
	logProvider LogProvider
	extraRoutes func(mux *http.ServeMux)
	logger      actor.Logger
	binaryOnly  bool

	// extraAddrs holds additional listen addresses beyond the primary
	// httpServer.Addr. When set, Run creates a listener for each and
	// serves the same handler on all of them. This enables dual binding
	// (e.g. 127.0.0.1 + a specific LAN IP) without exposing 0.0.0.0.
	extraAddrs     []string
	extraListeners []net.Listener
	extraServers   []*http.Server

	// gateSeq uniquifies gatesession child names across every connection
	// this Server accepts (ws sessions, SSE, per-request unary HTTP).
	gateSeq atomic.Uint64
}

// NewServer creates a Gateway HTTP server bound to addr.
// If addr is empty, an ephemeral port on 127.0.0.1 is used.
// A nil interceptor is replaced with Nop().
func NewServer(app AppHost, interceptor GatewayInterceptor, addr string) *Server {
	if interceptor == nil {
		interceptor = Nop()
	}
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	return &Server{
		app:         app,
		interceptor: interceptor,
		httpServer:  &http.Server{Addr: addr},
	}
}

func (s *Server) SetStaticFS(fsys fs.FS) {
	s.staticFS = fsys
}

// SetLogProvider installs a structured log provider for the /logs endpoint.
func (s *Server) SetLogProvider(p LogProvider) {
	s.logProvider = p
}

// SetLogger injects a structured logger for gateway diagnostics.
func (s *Server) SetLogger(logger actor.Logger) {
	s.logger = logger
}

// SetExtraRoutes registers a callback that adds custom routes to the
// gateway's ServeMux. Called once during Run, after standard routes are
// registered but before the server starts listening. This allows downstream
// consumers to extend the gateway without modifying the core server.
func (s *Server) SetExtraRoutes(fn func(mux *http.ServeMux)) {
	s.extraRoutes = fn
}

// SetExtraAddrs configures additional listen addresses. Each address gets
// its own listener in Run, sharing the same handler as the primary listener.
// This allows dual binding without 0.0.0.0 (e.g. loopback + LAN IP).
func (s *Server) SetExtraAddrs(addrs []string) {
	s.extraAddrs = addrs
}

// WithURLAuth installs a per-connection authentication hook. Called once
// when a WebSocket connection is established; the returned role and subject
// are propagated on every message frame via gospore caller headers.
func (s *Server) WithURLAuth(fn URLAuth) {
	s.urlAuth = fn
}

// WithBinaryOnly rejects text frames and JSON-encoded payloads on the
// WebSocket connection. When enabled, all incoming messages must be binary
// wire frames with TBC-encoded struct payloads.
func (s *Server) WithBinaryOnly(v bool) {
	s.binaryOnly = v
}

// ReadyChan returns a channel that closes when the server is accepting
// connections. Call before Run.
func (s *Server) ReadyChan() <-chan struct{} {
	if s.ready == nil {
		s.ready = make(chan struct{})
	}
	return s.ready
}

// Run starts the HTTP server and blocks until ctx is cancelled or an
// unrecoverable error occurs. The server shares the App's lifecycle:
// cancelling ctx triggers graceful shutdown.
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/logs", s.handleLogs)
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/api/", s.handleCall)
	mux.HandleFunc("/schema", s.handleSchema)
	if handler := s.staticHandler(); handler != nil {
		mux.Handle("/", handler)
	}
	if s.extraRoutes != nil {
		s.extraRoutes(mux)
	}
	s.httpServer.Handler = withCORS(mux)

	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("gateway: listen: %w", err)
	}
	s.listenerMu.Lock()
	s.listener = ln
	s.listenerMu.Unlock()

	// Signal readiness — the kernel is accepting connections on the socket.
	if s.ready != nil {
		close(s.ready)
	}

	// Preserve actual bound address.
	s.httpServer.Addr = ln.Addr().String()
	if s.logger != nil {
		s.logger.Info("gateway: listening", "addr", s.httpServer.Addr)
	}

	// Start extra listeners (dual binding) — each shares the same handler.
	// A failed extra listener is logged as a warning but does NOT abort the
	// primary listener; the loopback gateway (used by frp and the desktop
	// frontend) must stay up.
	for _, addr := range s.extraAddrs {
		exLn, err := net.Listen("tcp", addr)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("gateway: extra listen failed", "addr", addr, "error", err)
			}
			continue
		}
		s.extraListeners = append(s.extraListeners, exLn)
		if s.logger != nil {
			s.logger.Info("gateway: listening", "addr", exLn.Addr().String())
		}
		extraSrv := &http.Server{Handler: s.httpServer.Handler}
		go func(srv *http.Server, l net.Listener) {
			_ = srv.Serve(l)
		}(extraSrv, exLn)
		s.extraServers = append(s.extraServers, extraSrv)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.httpServer.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
		for _, es := range s.extraServers {
			_ = es.Shutdown(shutdownCtx)
		}
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// Close performs an immediate shutdown of the HTTP server.
func (s *Server) Close() error {
	var firstErr error
	if s.httpServer != nil {
		firstErr = s.httpServer.Close()
	}
	for _, es := range s.extraServers {
		_ = es.Close()
	}
	return firstErr
}

// Addr returns the actual listen address. Empty until Run is called.
func (s *Server) Addr() string {
	s.listenerMu.Lock()
	ln := s.listener
	s.listenerMu.Unlock()
	if ln == nil {
		return ""
	}
	return ln.Addr().String()
}

func (s *Server) staticHandler() http.Handler {
	if s.staticFS == nil {
		return nil
	}
	distFS, err := fs.Sub(s.staticFS, "dist")
	if err != nil {
		return nil
	}
	indexHTML, err := fs.ReadFile(distFS, "index.html")
	if err != nil {
		return nil
	}
	fileServer := http.FileServerFS(distFS)
	opt := newStaticOptimizer(distFS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			fileServer.ServeHTTP(w, r)
			return
		}
		cleanPath := pathpkg.Clean(strings.TrimPrefix(r.URL.Path, "/"))
		if cleanPath == "." {
			cleanPath = "index.html"
		}
		if info, err := fs.Stat(distFS, cleanPath); err == nil && !info.IsDir() {
			opt.serveFile(w, r, cleanPath, fileServer)
			return
		}
		opt.serveBytes(w, r, "index.html", indexHTML)
	})
}

func (s *Server) handleCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	// 1. Extract callID from path: /api/{callID}
	callID := strings.TrimPrefix(r.URL.Path, "/api/")
	if callID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing call ID"})
		return
	}

	// 2. Read and parse body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "bad body"})
		return
	}
	var payload any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
			return
		}
	}

	// 3. Extract identity headers.
	customerID := r.Header.Get("X-Customer-ID")
	role := r.Header.Get("X-Role")
	subject := r.Header.Get("X-Subject")
	target := r.URL.Query().Get("target")
	from := r.URL.Query().Get("from")

	req := &GatewayRequest{
		CallID:     callID,
		CustomerID: customerID,
		Role:       role,
		Service:    serviceFromCallID(callID),
		Target:     target,
		From:       from,
		Payload:    payload,
		Headers: map[string]string{
			"Content-Type": r.Header.Get("Content-Type"),
		},
	}

	// 4. Interceptor Before (sync).
	if err := s.interceptor.Before(r.Context(), req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if errors.Is(err, ErrGatewayDenied) {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// 5. Resolve target actor.
	targetRef, ok := s.resolveTarget(req.Service, callID, req.Target, req.From)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found"})
		return
	}

	// 6. Invoke actor through a per-request gatesession caller cell.
	start := time.Now()
	hdrs := map[string]string{"gospore.caller_role": role}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	gate, gateDone := s.sessionGate("http")
	defer gateDone()
	call := s.invokeAs(gate, targetRef, r.Context(), callID, payload, hdrs)
	if call == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invoke failed"})
		return
	}
	defer call.Close()

	raw, err := call.RecvRaw()
	duration := time.Since(start)

	resp := &GatewayResponse{
		Body:     raw,
		Duration: duration,
	}
	if err != nil && !errors.Is(err, io.EOF) {
		resp.Error = err
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	} else {
		w.Header().Set("Content-Type", "application/json")
		if raw == nil {
			w.WriteHeader(http.StatusNoContent)
		} else {
			_, _ = w.Write(raw)
		}
	}

	// 7. Interceptor After (async, fire-and-forget).
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				// Swallow interceptor panics to avoid crashing the gateway.
			}
		}()
		s.interceptor.After(r.Context(), req, resp)
	}()
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	// 1. Parse body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "bad body"})
		return
	}
	var reqBody struct {
		CallID    string `json:"callID"`
		Target    string `json:"target,omitempty"`
		From      string `json:"from,omitempty"`
		Payload   any    `json:"payload"`
		TimeoutMs int64  `json:"timeoutMs,omitempty"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &reqBody); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
			return
		}
	}
	if reqBody.CallID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing callID"})
		return
	}

	// 2. Build gateway request.
	customerID := r.Header.Get("X-Customer-ID")
	role := r.Header.Get("X-Role")
	subject := r.Header.Get("X-Subject")
	service := serviceFromCallID(reqBody.CallID)
	from := r.URL.Query().Get("from")
	if reqBody.From != "" {
		from = reqBody.From
	}
	req := &GatewayRequest{
		CallID:     reqBody.CallID,
		CustomerID: customerID,
		Role:       role,
		Service:    service,
		Target:     reqBody.Target,
		From:       from,
		Payload:    reqBody.Payload,
		Headers: map[string]string{
			"Content-Type": r.Header.Get("Content-Type"),
		},
	}

	// 3. Interceptor Before.
	if err := s.interceptor.Before(r.Context(), req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if errors.Is(err, ErrGatewayDenied) {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// 4. Resolve target.
	targetRef, ok := s.resolveTarget(service, reqBody.CallID, reqBody.Target, from)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found"})
		return
	}

	// 5. Invoke.
	start := time.Now()
	hdrs := map[string]string{"gospore.caller_role": role}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	invokeCtx := r.Context()
	timeoutCancel := func() {}
	if reqBody.TimeoutMs > 0 {
		invokeCtx, timeoutCancel = context.WithTimeout(r.Context(), time.Duration(reqBody.TimeoutMs)*time.Millisecond)
	}
	defer timeoutCancel()
	gate, gateDone := s.sessionGate("http")
	defer gateDone()
	call := s.invokeAs(gate, targetRef, invokeCtx, reqBody.CallID, reqBody.Payload, hdrs)
	if call == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invoke failed"})
		return
	}
	defer call.Close()

	raw, err := call.RecvRaw()
	duration := time.Since(start)

	resp := &GatewayResponse{
		Body:     raw,
		Duration: duration,
	}
	if err != nil && !errors.Is(err, io.EOF) {
		resp.Error = err
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	} else {
		w.Header().Set("Content-Type", "application/json")
		if raw == nil {
			w.WriteHeader(http.StatusNoContent)
		} else {
			_, _ = w.Write(raw)
		}
	}

	// 6. Interceptor After.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
			}
		}()
		s.interceptor.After(r.Context(), req, resp)
	}()
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	// 1. Parse query params.
	callID := r.URL.Query().Get("callID")
	if callID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing callID"})
		return
	}
	var payload any
	if payloadStr := r.URL.Query().Get("payload"); payloadStr != "" {
		if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid payload json"})
			return
		}
	}

	// 2. Build gateway request.
	customerID := r.Header.Get("X-Customer-ID")
	role := r.Header.Get("X-Role")
	subject := r.Header.Get("X-Subject")
	service := serviceFromCallID(callID)
	target := r.URL.Query().Get("target")
	from := r.URL.Query().Get("from")
	req := &GatewayRequest{
		CallID:     callID,
		CustomerID: customerID,
		Role:       role,
		Service:    service,
		Target:     target,
		From:       from,
		Payload:    payload,
		Headers: map[string]string{
			"Content-Type": r.Header.Get("Content-Type"),
		},
	}

	// 3. Interceptor Before.
	if err := s.interceptor.Before(r.Context(), req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if errors.Is(err, ErrGatewayDenied) {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// 4. Resolve target.
	targetRef, ok := s.resolveTarget(service, callID, target, from)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found"})
		return
	}

	// 5. Set up SSE.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "streaming unsupported"})
		return
	}

	// 6. Invoke and stream through a per-connection gatesession caller
	// cell, destroyed when the SSE request ends.
	hdrs := map[string]string{"gospore.caller_role": role}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	gate, gateDone := s.sessionGate("sse")
	defer gateDone()
	call := s.invokeAs(gate, targetRef, r.Context(), callID, payload, hdrs)
	if call == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invoke failed"})
		return
	}
	defer call.Close()

	// Cancel the in-flight call when the HTTP client disconnects
	// so RecvRaw() unblocks and the handler can exit.
	go func() {
		<-r.Context().Done()
		call.Cancel()
	}()

	start := time.Now()
	resp := &GatewayResponse{}

	for {
		raw, err := call.RecvRaw()
		if err == io.EOF {
			resp.Duration = time.Since(start)
			fmt.Fprintf(w, "data: %s\n\n", `{"done":true}`)
			flusher.Flush()
			break
		}
		if err != nil {
			resp.Error = err
			resp.Duration = time.Since(start)
			fmt.Fprintf(w, "data: %s\n\n", fmt.Sprintf(`{"error":"%s"}`, err.Error()))
			flusher.Flush()
			break
		}
		fmt.Fprintf(w, "data: %s\n\n", raw)
		flusher.Flush()
	}

	// 7. Interceptor After.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
			}
		}()
		s.interceptor.After(r.Context(), req, resp)
	}()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if s.logProvider == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "log provider not configured"})
		return
	}

	q := r.URL.Query()
	limit := 200
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
		limit = n
	}
	opts := LogQuery{
		Level:  q.Get("level"),
		Caller: q.Get("caller"),
		Limit:  limit,
	}

	entries := s.logProvider.QueryLogs(opts)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	data, err := s.app.Schemas().Marshal()
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

// sessionGate spawns a transient gatesession caller cell for one gateway
// connection or request. Invoke sites route through app.InvokeAsCaller with
// the returned ref so reply slots land in the connection's own pending
// table; DestroyGatewaySession (via the returned cleanup) flushes that
// table with terminal errors, unblocking in-flight calls.
//
// A nil ref (spawn failed, e.g. app teardown already in progress) means
// callers fall back to legacy root-caller semantics — availability over
// isolation on the shutdown path.
func (s *Server) sessionGate(scope string) (ref.Ref, func()) {
	if s.app == nil {
		return nil, func() {}
	}
	name := fmt.Sprintf("gatesession-%s-%d-%04x", scope, s.gateSeq.Add(1), rand.Uint32()&0xffff)
	r, err := s.app.SpawnGatewaySession(name)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("gateway: gatesession spawn failed; falling back to root caller", "err", err)
		}
		return nil, func() {}
	}
	return r, func() {
		if err := s.app.DestroyGatewaySession(r); err != nil && s.logger != nil {
			s.logger.Warn("gateway: gatesession destroy failed", "err", err)
		}
	}
}

// invokeAs resolves the caller switch: gatesession ref when the gateway
// managed to spawn one, legacy bare-target invoke otherwise.
func (s *Server) invokeAs(caller, targetRef ref.Ref, ctx context.Context, callID string, payload any, hdrs map[string]string) *invoke.Call {
	if caller != nil {
		return s.app.InvokeAsCaller(caller, targetRef, callID, payload, hdrs)
	}
	return targetRef.Invoke(ctx, callID, payload, hdrs)
}

// invokeActor resolves the target actor for callID, invokes it, and returns
// the raw response body. Shared by HTTP and WebSocket handlers.
//
// If target is non-empty it is treated as a canonical ActorID hex string and
// addresses the actor with that ID directly; the service fallback is used
// only when target is empty. If from is non-empty, scoped service lookup is
// attempted from that actor before falling back to the global service.
func (s *Server) invokeActor(ctx context.Context, caller ref.Ref, callID, target, from string, payload any, customerID, role, subject string, timeoutMs int64) ([]byte, error) {
	service := serviceFromCallID(callID)
	targetRef, ok := s.resolveTarget(service, callID, target, from)
	if !ok {
		return nil, errServiceNotFound
	}
	hdrs := map[string]string{
		"gospore.caller_role": role,
	}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	if s.binaryOnly {
		hdrs["gospore.reply_encoding"] = "binary"
	}
	timeout := GatewayInvokeTimeout
	if timeoutMs > 0 {
		timeout = time.Duration(timeoutMs) * time.Millisecond
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, timeout)
	defer timeoutCancel()

	call := s.invokeAs(caller, targetRef, timeoutCtx, callID, payload, hdrs)
	if call == nil {
		return nil, errInvokeFailed
	}
	defer call.Close()

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-timeoutCtx.Done():
			call.Cancel()
		case <-done:
		}
	}()

	raw, err := call.RecvRaw()
	if errors.Is(err, invoke.ErrCallCancelled) {
		return nil, fmt.Errorf("invoke %s: gateway timeout after %s", callID, timeout)
	}
	return raw, err
}

// beginSubscription resolves the target and starts an actor invocation for
// streaming. The caller reads chunks from the returned Call.
// Shared by HTTP SSE and WebSocket handlers. caller is the connection's
// gatesession ref (nil falls back to root-caller semantics).
func (s *Server) beginSubscription(ctx context.Context, caller ref.Ref, callID, target, from string, payload any, customerID, role, subject string) (*invoke.Call, error) {
	service := serviceFromCallID(callID)
	targetRef, ok := s.resolveTarget(service, callID, target, from)
	if !ok {
		return nil, errServiceNotFound
	}
	hdrs := map[string]string{
		"gospore.caller_role": role,
	}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	if s.binaryOnly {
		hdrs["gospore.reply_encoding"] = "binary"
	}
	call := s.invokeAs(caller, targetRef, ctx, callID, payload, hdrs)
	if call == nil {
		return nil, errInvokeFailed
	}
	return call, nil
}

// resolveTarget finds the actor Ref for a service call.
//
// If target is non-empty it is parsed as a canonical ActorID hex string and
// looked up via the App's actor tree. A non-empty but unresolvable target
// fails the lookup outright (no fallback to service) so misrouting surfaces
// immediately rather than being masked.
//
// When target is empty and from is a valid actor ID, the tree is asked for
// the nearest scoped service visible to that caller. This lets external
// callers reach subtree-scoped services such as "project" by naming a
// descendant actor in whose ancestor chain the service is exposed.
//
// If neither target nor from resolves, the original behaviour is preserved:
// first try LookupService(service); then fall back to the App's root actor
// (Self).
func (s *Server) resolveTarget(service, callID, target, from string) (ref.Ref, bool) {
	if target != "" {
		cid, err := identity.ParseCanonicalID(target)
		if err != nil {
			return nil, false
		}
		if r, ok := s.app.Tree().LookupID(id.From(cid)); ok {
			return r, true
		}
		return nil, false
	}
	if from != "" && service != "" {
		cid, err := identity.ParseCanonicalID(from)
		if err == nil {
			if callerRef, ok := s.app.Tree().LookupID(id.From(cid)); ok {
				if r, ok := s.app.Tree().LookupScopedService(callerRef, service); ok {
					return r, true
				}
			}
		}
	}
	if service != "" {
		if r, ok := s.app.LookupService(service); ok {
			return r, true
		}

	}
	if self := s.app.Self(); self != nil {
		return self, true
	}
	return nil, false
}

// withCORS wraps a handler to allow cross-origin requests from any origin.
// This lets desktop webviews (wails://, file://, etc.) reach the gateway.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && origin != "null" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		} else {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Customer-ID, X-Role, X-Subject")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// serviceFromCallID extracts the leading segment before the first dot.
// For "quota.check" it returns "quota"; for "ping" it returns "ping".
func serviceFromCallID(callID string) string {
	if i := strings.Index(callID, "."); i > 0 {
		return callID[:i]
	}
	return callID
}
