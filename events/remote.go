package events

import "github.com/qomos-w/gospore/id"

// RemoteSink is the out-of-process delivery face of the events system.
// When a Record is emitted for an actor that has remote subscribers,
// the events store forwards it through the configured RemoteSink.
//
// Phase 1 deployments leave RemoteSink nil; only in-process
// subscriptions (actor.Subscription[Record]) are active.
// Phase 2 transport implementations wire a RemoteSink that serialises
// Records and pushes them over the network to out-of-process observers.
//
// Push is invoked OUTSIDE the events store lock and may be called
// concurrently from different Push callers, potentially out of order.
// Implementations must be safe for concurrent use and non-blocking;
// silently drop on backpressure. Sinks that need per-actor ordering
// must buffer internally.
type RemoteSink interface {
	// Push delivers rec to remote subscribers of actorID.
	Push(actorID id.ActorID, rec Record)
}
