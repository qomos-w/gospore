package transport

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/qomos-w/gospore/message"
)

// HTTP is a Transport over HTTP.
//
// In point-to-point mode it POSTs outbound frames to a fixed target URL
// set via SetTargetURL. In routed mode it uses a Router to resolve the
// destination address from the Frame's target ActorID runtime slot.
//
// Inbound frames are accepted via an HTTP handler on an auto-allocated
// listen address. Use ListenAddr() to discover the local endpoint.
//
// This is a minimal Phase-2 reference implementation; production
// deployments may add TLS, connection pooling, circuit breakers,
// and header-based metadata.
type HTTP struct {
	targetURL string
	router    Router
	client    *http.Client
	inbound   chan message.Frame
	server    *http.Server
	listener  net.Listener

	mu     sync.Mutex
	closed bool
}

// DefaultInboundCapacity is the inbound frame buffer used by NewHTTP
// and NewMemoryRemotePair when no explicit capacity is given.
const DefaultInboundCapacity = 64

// NewHTTP creates an HTTP transport that listens on an ephemeral
// local port with the DefaultInboundCapacity (64) inbound buffer.
// targetURL is the peer endpoint (e.g. "http://127.0.0.1:8081");
// it may be empty if the peer address is not yet known — call
// SetTargetURL before the first Send.
func NewHTTP(targetURL string) (*HTTP, error) {
	return NewHTTPWithBuffer(targetURL, DefaultInboundCapacity)
}

// NewHTTPWithBuffer creates an HTTP transport with an explicit inbound
// frame buffer capacity. The buffer bounds backpressure: once full,
// inbound POSTs are rejected with 503 until the consumer drains
// Receive(). Capacity <= 0 falls back to DefaultInboundCapacity.
func NewHTTPWithBuffer(targetURL string, inboundCapacity int) (*HTTP, error) {
	if inboundCapacity <= 0 {
		inboundCapacity = DefaultInboundCapacity
	}
	h := &HTTP{
		targetURL: targetURL,
		client:    &http.Client{},
		inbound:   make(chan message.Frame, inboundCapacity),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/frame", h.handleFrame)
	h.server = &http.Server{Handler: mux}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("transport/http: listen: %w", err)
	}
	h.listener = ln

	go func() {
		_ = h.server.Serve(ln)
	}()

	return h, nil
}

// ListenAddr returns the local address the transport is listening on.
func (h *HTTP) ListenAddr() string {
	if h.listener == nil {
		return ""
	}
	return h.listener.Addr().String()
}

// SetTargetURL updates the peer endpoint. Safe to call between Send
// invocations; not safe during an in-flight Send.
func (h *HTTP) SetTargetURL(url string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.targetURL = url
}

// SetRouter installs a Router for dynamic address resolution. When set,
// Send resolves the target address from frame.To.RuntimeSlot() instead
// of using the static targetURL. The Router takes precedence over
// SetTargetURL.
func (h *HTTP) SetRouter(r Router) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.router = r
}

func (h *HTTP) handleFrame(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	frame, err := message.Unmarshal(data)
	if err != nil {
		http.Error(w, "bad frame", http.StatusBadRequest)
		return
	}

	select {
	case h.inbound <- frame:
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "buffer full", http.StatusServiceUnavailable)
	}
}

// Send delivers one Frame to the peer via HTTP POST.
func (h *HTTP) Send(frame message.Frame) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return fmt.Errorf("transport: HTTP closed")
	}

	url := h.targetURL + "/frame"
	if h.router != nil {
		if addr := h.router.Route(frame.To.RuntimeSlot()); addr != "" {
			url = addr + "/frame"
		}
	}
	h.mu.Unlock()

	if url == "/frame" {
		return fmt.Errorf("transport: HTTP target URL not set")
	}

	data, err := message.Marshal(frame)
	if err != nil {
		return fmt.Errorf("transport: marshal: %w", err)
	}

	resp, err := h.client.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("transport: post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transport: post: %s", resp.Status)
	}
	return nil
}

// Receive returns the channel of inbound frames.
func (h *HTTP) Receive() <-chan message.Frame {
	return h.inbound
}

// Close shuts down the HTTP server and closes the inbound channel.
func (h *HTTP) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()

	if h.server != nil {
		_ = h.server.Close()
	}
	close(h.inbound)
	return nil
}

var _ Transport = (*HTTP)(nil)
