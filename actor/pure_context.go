package actor

import (
	"context"
	"time"

	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/resource"
	"github.com/qomos-w/gospore/schema"
)

// PureContext is the context handed to stateless handlers (those whose
// first parameter is PureContext). Stateless handlers run on forked
// goroutines with no mailbox serialization, so PureContext deliberately
// omits only the Registrar family — operations that mutate the cell-local
// handler table, component map, event/timer metadata, or loop lanes. Those
// structures are populated during OnInit/OnStart and frozen thereafter;
// mutating them from a stateless handler would be a semantic error.
//
// All tree operators (Spawn, Stop, Destroy, Watch, Unwatch) ARE exposed
// because they are fully thread-safe: they delegate to tree.Spawn (tree
// mutex), tree.Stop (RLock + message delivery), tree.Destroy (Lock +
// message delivery), or deliver() (transport lookup), none of which touch
// mutable cell-local state. Similarly, Planner returns an immutable
// reference whose methods hold no mutable state of their own.
//
// Every method exposed here is either:
//   - a read of an immutable cell field (Self, Parent, Root, Lifecycle), or
//   - a read guarded by the owner's internal lock (Children via childSet,
//     LookupID/LookupService via tree/service RWMutex), or
//   - an atomic operation (NewID via atomic counter), or
//   - a side-effect whose implementation is internally synchronized
//     (EmitEvent, After, Spawn, Stop, Destroy, Watch, Unwatch, Planner).
type PureContext interface {
	CallID() string
	Caller() ref.Ref
	Identity() id.Identity
	Logger() Logger
	Done() <-chan struct{}

	// Self returns this actor's own Ref. Immutable after cell init.
	Self() ref.Ref
	// Parent returns the parent actor's Ref. Immutable after cell init.
	Parent() ref.Ref
	// Children returns a snapshot of live child refs. Thread-safe snapshot
	// copy; safe to iterate without holding locks.
	Children() []ref.Ref
	// Root returns the App root actor's Ref. Immutable after cell init.
	Root() ref.Ref

	// NewID allocates a globally-unique 128-bit canonical ID from the
	// App-global generator. Atomic; concurrency-safe.
	NewID() id.ActorID

	Namespace() string
	Codec() codec.Codec
	Schemas() schema.Reader
	Resources() resource.Registry
	Clock() Clock
	HasEventSubscribers(kind string) bool
	EventSubscriberCount(kind string) int

	// EmitEvent publishes an event to the App-scoped bus. Thread-safe;
	// safe to call from concurrent stateless handlers.
	EmitEvent(kind string, payload any) error

	// LookupService resolves a service name to a Ref. Read-only; thread-safe.
	LookupService(name string) (ref.Ref, bool)
	// LookupID resolves an ActorID to a Ref. Read-only; thread-safe.
	LookupID(aid id.ActorID) (ref.Ref, bool)

	// Spawn creates a new child under Self. Thread-safe: the tree's internal
	// mutex serializes the node insertion and childSet.Add, and the spawn
	// function reference is immutable after cell construction. Safe to call
	// from concurrent stateless handlers.
	Spawn(props Props, name string) (ref.Ref, error)

	// Stop idles target without removing it from the tree. Thread-safe:
	// tree.Stop takes an RLock then delivers an Idle message via the
	// transport. Safe to call from concurrent stateless handlers.
	Stop(target ref.Ref) error

	// Destroy removes target and its subtree from the tree. Thread-safe:
	// tree.Destroy takes the tree Lock, removes nodes, then delivers
	// Destroy messages and waits for each cell to finish (bounded by the
	// configured terminate timeout). Safe to call from concurrent stateless
	// handlers; the blocking wait occupies only the calling goroutine.
	Destroy(target ref.Ref) error

	// Watch subscribes to target's lifecycle / invocation events.
	// Thread-safe: delivers a Watch message via the transport.
	Watch(target ref.Ref, kinds ...WatchKind) error

	// Unwatch removes any active watch on target. Thread-safe: delivers an
	// Unwatch message via the transport.
	Unwatch(target ref.Ref)

	// Planner returns this actor's Planner, or nil if the actor was not
	// spawned with Planner capability. The returned Planner is immutable and
	// its methods (Plan/Call/Stream) are thread-safe — they delegate to
	// tree.Spawn, the atomic corID generator, and cross-actor Invoke.
	Planner() Planner

	// After schedules a delayed self-invoke of callID. Returns immediately;
	// the delivery runs on an internal goroutine.
	After(delay time.Duration, callID string, payload any) error

	// Lifecycle returns a context.Context cancelled when this actor stops.
	// Read of an immutable field; useful as a cancellation signal for
	// non-blocking background work.
	Lifecycle() context.Context
}
