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

	mu        sync.Mutex
	emitters  map[uint64]func() // CorID -> cancel func
	earlyCxl  map[uint64]struct{}
	earlyFifo []uint64
}

// earlyCancelMemoCap bounds the memo of KindCancel frames that arrived
// before their invocation started. Such frames travel the reply lane while
// the invocation envelope may still be queued on its dispatch lane, so no
// ordering guarantee exists; the memo closes the race. CorIDs are never
// reused, so stale entries are pure memory — the FIFO cap evicts.
const earlyCancelMemoCap = 4096

func newReplyRegistry(pt *invoke.PendingTable) *replyRegistry {
	return &replyRegistry{
		pending:  pt,
		emitters: make(map[uint64]func()),
		earlyCxl: make(map[uint64]struct{}),
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
// If a KindCancel for this CorID already arrived (cancel-before-start
// race), the recorded function fires immediately so the handler observes
// emit.Done() the moment it starts instead of running forever for a
// consumer that already cancelled.
func (r *replyRegistry) registerCancel(corID uint64, cancel func()) {
	r.mu.Lock()
	r.emitters[corID] = cancel
	_, early := r.earlyCxl[corID]
	delete(r.earlyCxl, corID)
	r.mu.Unlock()
	if early && cancel != nil {
		cancel()
	}
}

// unregisterCancel removes the cancel function for a completed call.
func (r *replyRegistry) unregisterCancel(corID uint64) {
	r.mu.Lock()
	delete(r.emitters, corID)
	r.mu.Unlock()
}

// cancel invokes and removes the cancel function for corID, if any.
// When no emitter is registered yet, the CorID is memoized so a late
// registerCancel still observes the cancellation. Returns true when a
// cancel function was found and invoked.
func (r *replyRegistry) cancel(corID uint64) bool {
	r.mu.Lock()
	cancel, ok := r.emitters[corID]
	delete(r.emitters, corID)
	if !ok {
		if _, seen := r.earlyCxl[corID]; !seen {
			r.earlyCxl[corID] = struct{}{}
			r.earlyFifo = append(r.earlyFifo, corID)
			if len(r.earlyFifo) > earlyCancelMemoCap {
				drop := r.earlyFifo[0]
				r.earlyFifo = r.earlyFifo[1:]
				delete(r.earlyCxl, drop)
			}
		}
	}
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
	clear(r.earlyCxl)
	r.earlyFifo = r.earlyFifo[:0]
	r.mu.Unlock()
	for _, cancel := range cancels {
		if cancel != nil {
			cancel()
		}
	}
}
