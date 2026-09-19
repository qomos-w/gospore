package transport

import "context"

// Server is the Phase 2 cross-process actor-aware transport server face.
//
// It accepts external network connections (the concrete protocol — TCP,
// WebSocket, QUIC, etc. — is provided by spore or a third-party library),
// wraps each connection as a ConnProxy, and routes inbound Frames to the
// local Cell layer via Transport.
//
// Server also enforces actor-level concerns that raw network libraries do
// not handle: identity authentication, visibility checks on inbound Calls,
// and connection lifecycle alignment with the host App.
//
// Phase 1 ships only the interface shape; Phase 2 provides the concrete
// implementation. App and Cell layers may reference Server without a
// concrete backend because Serve blocks until shutdown.
type Server interface {
	// Serve starts accepting connections and blocks until the context is
	// cancelled or Close is called. Inbound Frames are delivered to the
	// local actor tree via the provided Transport.
	Serve(ctx context.Context, tr Transport) error

	// Close shuts down the server and all active connections.
	// Safe to call multiple times.
	Close() error
}
