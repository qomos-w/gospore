package transport

// Router maps runtime slots to network addresses for cross-process
// Frame delivery. It is an external concern (not part of Transport) so
// that Transport only ferries bytes between addresses already resolved.
//
// When a Frame's To field targets an ActorID belonging to a different
// runtime slot, gospore queries Router for the destination address,
// then hands the Frame to the cross-process Transport implementation.
//
// Concrete implementations may use static config (deployment manifest),
// gossip (distributed hash table), or a central registry (consul/etcd).
//
// Router is a Phase 2 extension point; Phase 1 ships only the
// interface shape.
type Router interface {
	// Route returns the network address for the App that owns the
	// given runtime slot. An empty string means the slot is local
	// or currently unknown.
	Route(slot uint16) string
}
