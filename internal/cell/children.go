package cell

import (
	"sync"

	"github.com/qomos-w/gospore/ref"
)

// childSet tracks the live children of one Cell. Spawn adds, Stop removes,
// and cascade-Stop walks the snapshot. All methods are goroutine-safe: the
// Cell goroutine mutates, but external queries (Tree operators) may read
// concurrently.
type childSet struct {
	mu   sync.RWMutex
	set  map[ref.Ref]struct{}
}

func newChildSet() *childSet {
	return &childSet{set: make(map[ref.Ref]struct{})}
}

func (c *childSet) Add(r ref.Ref) {
	c.mu.Lock()
	c.set[r] = struct{}{}
	c.mu.Unlock()
}

func (c *childSet) Remove(r ref.Ref) {
	c.mu.Lock()
	delete(c.set, r)
	c.mu.Unlock()
}

func (c *childSet) Has(r ref.Ref) bool {
	c.mu.RLock()
	_, ok := c.set[r]
	c.mu.RUnlock()
	return ok
}

func (c *childSet) Len() int {
	c.mu.RLock()
	n := len(c.set)
	c.mu.RUnlock()
	return n
}

// Snapshot returns a fresh slice containing all live child refs. The caller
// can iterate without holding the lock; mutations after Snapshot returns do
// not affect the returned slice.
func (c *childSet) Snapshot() []ref.Ref {
	c.mu.RLock()
	out := make([]ref.Ref, 0, len(c.set))
	for r := range c.set {
		out = append(out, r)
	}
	c.mu.RUnlock()
	return out
}
