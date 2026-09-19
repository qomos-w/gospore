package transport

import (
	"fmt"
	"sync"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// Local is the in-process Transport. It maintains a registry of
// ActorID → recv function and delivers Frames directly via Recv,
// bypassing the network entirely.
type Local struct {
	mu     sync.RWMutex
	recvs  map[id.ActorID]func(mailbox.Envelope) error
	closed bool
}

// NewLocal constructs the in-process Transport.
//
// On Send it looks up Frame.To in the registry and writes the envelope
// directly to that Cell's Recv().
func NewLocal() *Local {
	return &Local{
		recvs: make(map[id.ActorID]func(mailbox.Envelope) error),
	}
}

// Register adds a recv function under the given ActorID. Must be called
// before any Send targeting this ID. Safe to call from any goroutine.
func (t *Local) Register(target id.ActorID, recv func(mailbox.Envelope) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recvs[target] = recv
}

// Unregister removes the recv function for the given ActorID. No-op if
// the ID is not registered. Safe to call from any goroutine.
func (t *Local) Unregister(target id.ActorID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.recvs, target)
}

// Lookup returns the recv function registered for target, or (nil, false)
// if none. Used by the App deliver closure to route cross-actor
// envelopes directly.
func (t *Local) Lookup(target id.ActorID) (func(mailbox.Envelope) error, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return nil, false
	}
	recv, ok := t.recvs[target]
	return recv, ok
}

// Send delivers frame to the target Cell's Recv. Returns an error
// if the target ActorID is not registered or the transport is closed.
func (t *Local) Send(frame message.Frame) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return fmt.Errorf("gospore/transport: transport is closed")
	}

	recv, ok := t.recvs[frame.To]
	if !ok {
		return fmt.Errorf("gospore/transport: target %s not registered", frame.To.Canonical())
	}

	return recv(mailbox.Envelope{Frame: frame})
}

// Receive returns nil for Local transport — local frames are delivered
// directly to target cells via Send, never through a Receive channel.
func (t *Local) Receive() <-chan message.Frame {
	return nil
}

// Close shuts the transport down. Clears the registry. Safe to call
// multiple times.
func (t *Local) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	t.recvs = make(map[id.ActorID]func(mailbox.Envelope) error)
	return nil
}

// Compile-time check.
var _ Transport = (*Local)(nil)
