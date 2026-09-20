package gateway

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	pathpkg "path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/actor"
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

	wsTimeouts WSTimeouts
}

// WSTimeouts exposes the WebSocket connection tuning knobs at the Server
// level. The zero value selects the package defaults for every field, so
// callers set only what they need. Values are applied per accepted
// connection at upgrade time.
type WSTimeouts struct {
	// PingInterval is how often the server pings the peer.
	// Default 30s.
	PingInterval time.Duration
	// ReadIdle bounds how long the server waits for any peer message
	// (data frame or pong) before dropping the connection. Default 65s;
	// must comfortably exceed PingInterval.
	ReadIdle time.Duration
	// WriteTimeout bounds each outbound data write. Default 30s.
	WriteTimeout time.Duration
	// PingWriteTimeout bounds each ping control write. Default 5s.
	PingWriteTimeout time.Duration
	// StallForceClose is the no-progress horizon after which a wedged
	// writer is force-closed. Default 10m.
	StallForceClose time.Duration
	// CloseDrainTimeout bounds how long Close waits for queued frames to
	// flush before force-closing the raw conn. Default 5s.
	CloseDrainTimeout time.Duration
	// OverflowBudgetBytes bounds the overflow queue for frames buffered
	// while the peer cannot accept writes. Default 32 MiB.
	OverflowBudgetBytes int
	// OutChCapacity bounds the fast-path outbound queue. Default 64.
	OutChCapacity int
}

// SetWSTimeouts installs WebSocket connection tuning. Zero fields fall
// back to package defaults. Must be called before Run; later changes
// affect only connections accepted afterwards.
func (s *Server) SetWSTimeouts(t WSTimeouts) {
	s.wsTimeouts = t
}

func (t WSTimeouts) tuning() wsConnTuning {
	return wsConnTuning{
		stallForceClose:  t.StallForceClose,
		closeDrainWait:   t.CloseDrainTimeout,
		overflowBudget:   t.OverflowBudgetBytes,
		outChCapacity:    t.OutChCapacity,
		pingInterval:     t.PingInterval,
		readIdle:         t.ReadIdle,
		writeTimeout:     t.WriteTimeout,
		pingWriteTimeout: t.PingWriteTimeout,
	}
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
		ready:       make(chan struct{}),
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
// connections. Safe to call from any goroutine, concurrently with Run;
// the channel is created eagerly in NewServer so there is no lazy-init
// race between a reader here and Run's close.
func (s *Server) ReadyChan() <-chan struct{} {
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
	close(s.ready)

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
