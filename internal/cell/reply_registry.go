package cell

import (
	"sync"

	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/message"
)

// replyRegistry manages reply frame routing and streaming cancellation
// callbacks for a single Cell. It unifies the two formerly separate
// concerns — routing Reply/Error/End frames back to the caller's
// PendingTable, and tracking active Emitter cancel functions — so that
// routeToWaitingStream and cancelStream hit the same registry.
//
// The registry holds the PendingTable directly: the former
// routeReply func field was wired to exactly pt.Deliver at every
// construction site (App root, App child, tests), making the closure a
// pure indirection.
type replyRegistry struct {
	pending *invoke.PendingTable

	mu       sync.Mutex
	emitters map[uint64]func() // CorID -> cancel func
}

func newReplyRegistry(pt *invoke.PendingTable) *replyRegistry {
	return &replyRegistry{
		pending:  pt,
		emitters: make(map[uint64]func()),
	}
}

// deliver routes a Reply/Error/End frame to the caller's waiting stream.
// Returns true when a pending table was present (does not indicate
// whether the frame was actually consumed).
func (r *replyRegistry) deliver(frame message.Frame) bool {
	if r.pending == nil {
		return false
	}
	return r.pending.Deliver(frame)
}

// registerCancel records a cancel function for an in-flight streaming call.
func (r *replyRegistry) registerCancel(corID uint64, cancel func()) {
	r.mu.Lock()
	r.emitters[corID] = cancel
	r.mu.Unlock()
}

// unregisterCancel removes the cancel function for a completed call.
func (r *replyRegistry) unregisterCancel(corID uint64) {
	r.mu.Lock()
	delete(r.emitters, corID)
	r.mu.Unlock()
}

// cancel invokes and removes the cancel function for corID, if any.
// Returns true when a cancel function was found and invoked.
func (r *replyRegistry) cancel(corID uint64) bool {
	r.mu.Lock()
	cancel, ok := r.emitters[corID]
	delete(r.emitters, corID)
	r.mu.Unlock()
	if ok && cancel != nil {
		cancel()
	}
	return ok
}

func (r *replyRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.emitters)
}

// clear cancels every registered emitter (used on idle/destroy).
func (r *replyRegistry) clear() {
	r.mu.Lock()
	cancels := make([]func(), 0, len(r.emitters))
	for corID, cancel := range r.emitters {
		cancels = append(cancels, cancel)
		delete(r.emitters, corID)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		if cancel != nil {
			cancel()
		}
	}
}
