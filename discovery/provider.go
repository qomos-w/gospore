// Package discovery is the cross-App service-instance SPI.
//
// A Provider lets an App register its own instances under a service
// name, renew their leases, deregister on graceful shutdown, resolve
// the live instance set for a service, and subscribe to a stream of
// lifecycle events.
//
// Discovery is an App capability — not a central node. The default
// implementation is in-memory (sufficient for single-process Phase 1
// and as the reference implementation behind exp10 / exp20). Phase 2
// deployments inject distributed implementations (gossip / K8s DNS /
// consul) via app.WithDiscoveryProvider without changing the App
// surface.
//
// See ARCHITECTURE.md §4.15 for the three-layer resolution model
// (Service → Group → Instance), the lease lifecycle contract, and
// the Watch stream replay semantics.
package discovery

import (
	"time"

	"github.com/qomos-w/gospore/id"
)

// Provider is the SPI an App talks to for cross-App instance
// membership.
type Provider interface {
	// Register adds instance to the live set with the given TTL.
	// The instance is removed automatically once LeaseUntil elapses
	// without a Renew call.
	Register(instance Instance, ttl time.Duration) error

	// Renew extends the lease for the instance bound to namespace,
	// emitting a renewed Event.
	Renew(namespace string, ttl time.Duration) error

	// Deregister explicitly removes the instance bound to namespace.
	// Used during graceful shutdown — does not wait for TTL expiry.
	Deregister(namespace string) error

	// Resolve returns the current live instance set for service.
	Resolve(service string) []Instance

	// Subscribe returns a Watcher whose channel receives lifecycle
	// Events. If since > 0 the Watcher first replays events with
	// SeqNo > since from the event ring (best-effort, default ring
	// 256), then transitions to live delivery. Slow consumers are
	// dropped non-blockingly; reconnect with the last seen SeqNo
	// to recover via replay.
	Subscribe(since uint64) (Watcher, error)
}

// Instance is one registered service-instance record.
//
// (Service, InstanceGroupID, RuntimeSlotID, Namespace) are the
// three-layer addressing model: Service is the role, InstanceGroupID
// is the instance class (e.g. canary vs primary), and (RuntimeSlotID,
// Namespace) uniquely identifies the running process.
type Instance struct {
	// Namespace is the registering App's namespace. Identity key —
	// re-registering the same namespace replaces the prior entry.
	Namespace string
	// Service is the service name this instance exposes. Matches
	// service.Registry's `^[a-z][a-z0-9_-]*$`.
	Service string
	// InstanceGroupID classifies instances of the same service into
	// groups (e.g. `auth-primary`, `auth-canary`).
	InstanceGroupID string
	// RuntimeSlotID matches the App's runtime slot. Together with
	// Namespace it uniquely addresses the running instance.
	RuntimeSlotID uint16
	// Address is the network endpoint of this instance, e.g.
	// "http://192.168.1.10:8080". Used by cross-process Transport
	// to route frames to the correct physical process.
	Address string
	// ActorID is the actor that exposed this service. Used by remote
	// peers to construct a ref.Ref for cross-App invocation.
	ActorID id.ActorID
	// LeaseUntil is the wall-clock expiry. Renew advances it.
	LeaseUntil time.Time
}

// RefInfo returns the addressing information needed to construct a
// cross-App Ref for this instance. Phase 2 transport uses Namespace
// (App identity) and RuntimeSlotID (process identity) to route Frames
// to the correct remote App.
func (i Instance) RefInfo() (namespace string, slot uint16) {
	return i.Namespace, i.RuntimeSlotID
}

// Watcher is the consumer side of a Subscribe call. C delivers
// Events in SeqNo order. Close the Watcher to unsubscribe when done;
// the Provider then closes C and reclaims resources.
type Watcher struct {
	C    <-chan Event
	stop func()
}

// Close unsubscribes from the provider and closes the event channel.
// Safe to call multiple times.
func (w Watcher) Close() {
	if w.stop != nil {
		w.stop()
	}
}

// Event is one lifecycle record emitted by the Provider. SeqNo is
// monotonic across all events the Provider has emitted; subscribers
// pass SeqNo back via Subscribe(since) to resume.
type Event struct {
	SeqNo    uint64
	Kind     EventKind
	Service  string
	Instance Instance
}

// EventKind discriminates the four lifecycle events a Watcher can
// observe.
type EventKind string

const (
	// EventRegistered is emitted when an instance first joins the
	// live set.
	EventRegistered EventKind = "registered"
	// EventRenewed is emitted on Renew, before LeaseUntil expiry.
	EventRenewed EventKind = "renewed"
	// EventExpired is emitted when LeaseUntil has elapsed without
	// Renew or Deregister; the instance leaves Resolve's live set.
	EventExpired EventKind = "expired"
	// EventRejoined is emitted when a previously-Expired instance
	// re-registers under the same namespace, preserving identity
	// continuity (distinct from a fresh registered).
	EventRejoined EventKind = "rejoined"
)
