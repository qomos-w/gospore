package cell

import (
	"sync"

	"github.com/qomos-w/gospore/message"
)

// replyRegistry manages reply frame routing and streaming cancellation
// callbacks for a single Cell. It unifies the two formerly separate
// concerns — routing Reply/Error/End frames back to the caller's
// PendingTable, and tracking active Emitter cancel functions — so that
// routeToWaitingStream and cancelStream hit the same registry.
type replyRegistry struct {
	routeReply func(message.Frame) bool

	mu       sync.Mutex
	emitters map[uint64]func() // CorID -> cancel func
}

func newReplyRegistry(routeReply func(message.Frame) bool) *replyRegistry {
	return &replyRegistry{
		routeReply: routeReply,
		emitters:   make(map[uint64]func()),
	}
}

// deliver routes a Reply/Error/End frame to the caller's waiting stream.
// Returns true when a routeReply hook was present (does not indicate
// whether the frame was actually consumed).
func (r *replyRegistry) deliver(frame message.Frame) bool {
	if r.routeReply == nil {
		return false
	}
	return r.routeReply(frame)
}

// registerCancel records a cancel function for an in-flight streaming call.
func (r *replyRegistry) registerCancel(corID uint64, cancel func()) {
	r.mu.Lock()
	r.emitters[corID] = cancel
	r.mu.Unlock()
}

// unregisterCancel removes the cancel function for a CorID. Idempotent.
func (r *replyRegistry) unregisterCancel(corID uint64) {
	r.mu.Lock()
	delete(r.emitters, corID)
	r.mu.Unlock()
}

// cancel looks up the cancel function for a CorID, removes it, and invokes
// it. Returns true if a cancel function was found and invoked.
func (r *replyRegistry) cancel(corID uint64) bool {
	r.mu.Lock()
	fn, ok := r.emitters[corID]
	delete(r.emitters, corID)
	r.mu.Unlock()
	if ok {
		fn()
	}
	return ok
}

// len returns the number of registered cancel callbacks.
func (r *replyRegistry) len() int {
	r.mu.Lock()
	n := len(r.emitters)
	r.mu.Unlock()
	return n
}

// clear removes all cancel callbacks.
func (r *replyRegistry) clear() {
	r.mu.Lock()
	r.emitters = make(map[uint64]func())
	r.mu.Unlock()
}
