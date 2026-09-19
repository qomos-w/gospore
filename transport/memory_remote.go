package transport

import (
	"errors"
	"sync"

	"github.com/qomos-w/gospore/message"
)

// ErrRemoteClosed is returned by MemoryRemote.Send after Close.
var ErrRemoteClosed = errors.New("transport: remote closed")

// MemoryRemote is a test-double cross-process Transport that uses
// in-memory channels instead of real network connections. Two Apps
// in the same process can communicate through a MemoryRemote pair,
// making it possible to test cross-App scenarios without a real
// network stack.
//
// MemoryRemote satisfies Transport, so it can be injected into any
// layer that expects a Transport (Cell, Invoke, etc.).
//
// Create a pair with NewMemoryRemotePair().
type MemoryRemote struct {
	mu       sync.Mutex
	outbound chan<- message.Frame
	inbound  <-chan message.Frame
	closed   bool
}

// NewMemoryRemotePair creates two MemoryRemotes that are wired to
// each other: sending on A delivers to B's Receive channel, and
// vice versa.
func NewMemoryRemotePair() (a, b *MemoryRemote) {
	aToB := make(chan message.Frame, 64)
	bToA := make(chan message.Frame, 64)

	a = &MemoryRemote{
		outbound: aToB,
		inbound:  bToA,
	}
	b = &MemoryRemote{
		outbound: bToA,
		inbound:  aToB,
	}
	return a, b
}

// Send delivers the frame to the paired remote's Receive channel.
func (r *MemoryRemote) Send(frame message.Frame) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRemoteClosed
	}
	r.mu.Unlock()

	select {
	case r.outbound <- frame:
		return nil
	default:
		return errors.New("transport: remote outbound buffer full")
	}
}

// Receive returns the inbound channel. The caller must drain the
// channel before calling Close to avoid goroutine leaks.
func (r *MemoryRemote) Receive() <-chan message.Frame {
	return r.inbound
}

// Close shuts down the remote. It does NOT close the underlying
// channels (the pair shares them); it only marks this side as closed
// so subsequent Sends return ErrRemoteClosed.
func (r *MemoryRemote) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

var _ Transport = (*MemoryRemote)(nil)
