package transport

import "sync"

// StaticRouter is a simple in-memory Router backed by a map.
// It is suitable for single-process tests, static deployments,
// and as the reference implementation for Phase 2 routing tables.
//
// In production Phase 2 deployments StaticRouter is replaced by
// dynamic implementations (gossip DHT, K8s DNS, consul, etc.)
// that watch discovery.Provider and rebuild the route table
// automatically.
type StaticRouter struct {
	mu      sync.RWMutex
	routes  map[uint16]string
}

// NewStaticRouter creates an empty StaticRouter.
func NewStaticRouter() *StaticRouter {
	return &StaticRouter{
		routes: make(map[uint16]string),
	}
}

// Register maps runtime slot to network address. Re-registering
// the same slot overwrites the prior entry.
func (r *StaticRouter) Register(slot uint16, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes[slot] = addr
}

// Unregister removes a slot from the route table.
func (r *StaticRouter) Unregister(slot uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.routes, slot)
}

// Route returns the network address for the given runtime slot.
// An empty string means the slot is local or unknown.
func (r *StaticRouter) Route(slot uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.routes[slot]
}

var _ Router = (*StaticRouter)(nil)
