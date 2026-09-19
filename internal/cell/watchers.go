package cell

import (
	"sync"

	"github.com/qomos-w/gospore/ref"
)

// watcherSet tracks the actors watching this Cell for lifecycle events
// (Terminated / Started / Restarted etc.). Watch adds, Unwatch removes,
// and the Terminated notification path walks the snapshot. All methods are
// goroutine-safe.
type watcherSet struct {
	mu  sync.RWMutex
	set map[ref.Ref]struct{}
}

func newWatcherSet() *watcherSet {
	return &watcherSet{set: make(map[ref.Ref]struct{})}
}

func (w *watcherSet) Add(r ref.Ref) {
	w.mu.Lock()
	w.set[r] = struct{}{}
	w.mu.Unlock()
}

func (w *watcherSet) Remove(r ref.Ref) {
	w.mu.Lock()
	delete(w.set, r)
	w.mu.Unlock()
}

func (w *watcherSet) Has(r ref.Ref) bool {
	w.mu.RLock()
	_, ok := w.set[r]
	w.mu.RUnlock()
	return ok
}

func (w *watcherSet) Len() int {
	w.mu.RLock()
	n := len(w.set)
	w.mu.RUnlock()
	return n
}

// Snapshot returns a fresh slice of all watcher refs.
func (w *watcherSet) Snapshot() []ref.Ref {
	w.mu.RLock()
	out := make([]ref.Ref, 0, len(w.set))
	for r := range w.set {
		out = append(out, r)
	}
	w.mu.RUnlock()
	return out
}
