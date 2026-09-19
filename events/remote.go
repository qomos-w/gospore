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
type RemoteSink interface {
	// Push delivers rec to remote subscribers of actorID.
	// Implementations must be non-blocking; silently drop on backpressure.
	Push(actorID id.ActorID, rec Record)
}
