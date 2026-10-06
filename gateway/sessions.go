package gateway

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// SessionInfo is a point-in-time snapshot of one live gateway session (one
// WebSocket client connection). Role and Subject reflect the latest auth
// state: a session that starts anonymous and authenticates via an auth
// frame is updated in place.
type SessionInfo struct {
	// Kind is the transport scope the session was accepted under; the
	// WebSocket entry point reports "ws".
	Kind string
	Role string
	// Subject identifies the caller (JWT user id, glass session id, ...).
	Subject   string
	StartedAt time.Time
}

// sessionRegistry tracks live ServeSession connections so the host can
// answer "who is connected right now". Entries are registered when the
// dispatch loop starts and removed when it returns; identity upgrades via
// the auth frame rewrite the entry in place. Zero value is ready to use.
type sessionRegistry struct {
	mu      sync.Mutex
	entries map[*sessionEntry]SessionInfo
}

// sessionEntry is the registry's identity token for one session. It must
// be non-zero-sized: Go returns the same address (runtime.zerobase) for
// every zero-byte allocation, which would collapse all sessions onto one
// map key.
type sessionEntry struct{ id int64 }

var sessionEntrySeq atomic.Int64

func (r *sessionRegistry) add(info SessionInfo) *sessionEntry {
	e := &sessionEntry{id: sessionEntrySeq.Add(1)}
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[*sessionEntry]SessionInfo)
	}
	r.entries[e] = info
	r.mu.Unlock()
	return e
}

func (r *sessionRegistry) remove(e *sessionEntry) {
	if e == nil {
		return
	}
	r.mu.Lock()
	delete(r.entries, e)
	r.mu.Unlock()
}

func (r *sessionRegistry) updateIdentity(e *sessionEntry, role, subject string) {
	if e == nil {
		return
	}
	r.mu.Lock()
	if info, ok := r.entries[e]; ok {
		info.Role = role
		info.Subject = subject
		r.entries[e] = info
	}
	r.mu.Unlock()
}

func (r *sessionRegistry) snapshot() []SessionInfo {
	r.mu.Lock()
	out := make([]SessionInfo, 0, len(r.entries))
	for _, info := range r.entries {
		out = append(out, info)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out
}

// ActiveSessions returns a snapshot of the live gateway sessions, oldest
// first. Empty when no client is connected.
func (s *Server) ActiveSessions() []SessionInfo {
	return s.sessions.snapshot()
}
