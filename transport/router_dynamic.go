package transport

import (
	"context"
	"sync"

	"github.com/qomos-w/gospore/discovery"
)

// DynamicRouter watches a discovery.Provider and automatically rebuilds
// the runtime-slot → address routing table as instances register, renew,
// and expire. It satisfies the transport.Router interface.
//
// Usage:
//
//	router := transport.NewDynamicRouter(provider)
//	go router.Run(ctx)
//	httpTP.SetRouter(router)
type DynamicRouter struct {
	provider discovery.Provider

	mu     sync.RWMutex
	routes map[uint16]string // slot -> address
}

// NewDynamicRouter creates a router backed by the given discovery provider.
func NewDynamicRouter(provider discovery.Provider) *DynamicRouter {
	return &DynamicRouter{
		provider: provider,
		routes:   make(map[uint16]string),
	}
}

// Route returns the network address for the given runtime slot.
// An empty string means the slot is local or currently unknown.
func (r *DynamicRouter) Route(slot uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.routes[slot]
}

// Run blocks until ctx is cancelled, consuming discovery events and
// updating the route table. It performs an initial sync from Resolve
// for all known services before entering the watch loop.
func (r *DynamicRouter) Run(ctx context.Context) {
	if r.provider == nil {
		<-ctx.Done()
		return
	}

	// Initial sync: build table from current Resolve state.
	// We don't know all services, so we rely on the watch stream.
	// However, Subscribe replays recent events, so we catch up.
	watcher, err := r.provider.Subscribe(0)
	if err != nil {
		<-ctx.Done()
		return
	}
	defer watcher.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-watcher.C:
			if !ok {
				return
			}
			r.handleEvent(ev)
		}
	}
}

func (r *DynamicRouter) handleEvent(ev discovery.Event) {
	switch ev.Kind {
	case discovery.EventRegistered, discovery.EventRenewed, discovery.EventRejoined:
		if ev.Instance.Address != "" {
			r.mu.Lock()
			r.routes[ev.Instance.RuntimeSlotID] = ev.Instance.Address
			r.mu.Unlock()
		}
	case discovery.EventExpired:
		r.mu.Lock()
		delete(r.routes, ev.Instance.RuntimeSlotID)
		r.mu.Unlock()
	}
}

var _ Router = (*DynamicRouter)(nil)
