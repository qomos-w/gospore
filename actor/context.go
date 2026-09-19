package actor

import (
	"context"
	"fmt"
	"time"

	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/plan"
	"github.com/qomos-w/gospore/promise"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/resource"
	"github.com/qomos-w/gospore/schema"
)

// IdentityContext exposes identity, call metadata, and lifecycle signals.
// Most fields (Caller, CallID, Identity) are only meaningful inside a
// handler invocation; outside one (e.g. in OnStart) they return zero values.
type IdentityContext interface {
	Self() ref.Ref
	Parent() ref.Ref
	Children() []ref.Ref
	Caller() ref.Ref
	CallID() string
	Identity() id.Identity
	Done() <-chan struct{}
	Logger() Logger

	// NewID allocates a fresh 128-bit canonical ID from the App-global
	// id.Canonical generator. Use it for any runtime-issued identifier that
	// must be globally unique within the App process (request IDs, frame
	// IDs, event IDs, step IDs, ...). Concurrency-safe; never returns the
	// zero value. The ID is returned as id.ActorID purely as the canonical
	// 128-bit wrapper — the type name is structural, not semantic; callers
	// outside Spawn freely use it for non-actor identifiers.
	NewID() id.ActorID
}

// Registrar exposes the registration surface.
// Only valid inside OnStart; calls after OnStart returns are undefined.
type Registrar interface {
	// Register binds a Call ID to a Go handler. The handler must be a
	// function whose first parameter is Context (stateful) or PureContext
	// (stateless); the second parameter is the request type; an optional
	// third Emitter parameter marks the handler as streaming.
	Register(callID string, handler any, opts ...RegisterOption) error

	// RegisterScript binds a spore script source to a Call ID.
	// mode is explicit since script source cannot encode (stateful vs
	// stateless) via parameter types.
	RegisterScript(callID string, source string, mode HandlerMode, opts ...RegisterOption) error

	// AttachComponent attaches a dynamic component to this actor.
	// Must be called inside OnStart.
	AttachComponent(name string, schemaID uint64, initial any, opts ...RegisterOption) error

	// RegisterDomain declares a named domain on this actor. The domain
	// name is used for namespace tracking and manifest export even
	// without exposure. The returned DomainHandle may be used to
	// optionally expose the domain as an App-level service (Expose)
	// or a descendant-scoped service (ExposeChildren). Only valid
	// inside OnStart.
	RegisterDomain(name string) *DomainHandle

	// RegisterEventKind declares an event type that this actor may emit.
	// kind is the wire identifier (e.g. "turn") and example is a zero-value
	// of the struct type used for the event payload. Only valid inside OnStart.
	// Optional RegisterOption values (e.g. Public, AdminOnly) control the
	// compile-time export visibility of the event type in manifest/codegen.
	RegisterEventKind(kind string, example any, opts ...RegisterOption) error

	// SubscribeEventKind subscribes this actor to events of the given kind
	// emitted by any cell in the App, regardless of identity or service.
	// The handler runs on a dedicated drain goroutine bounded by the actor's
	// lifecycle; slow handlers never block the emitter (drop-oldest
	// backpressure with per-subscription drop counters). Only valid inside
	// OnStart; returns a cancel func that unsubscribes.
	SubscribeEventKind(kind string, handler func(EventEnvelope)) (func(), error)
	// RegisterLoop declares a named execution loop that the runtime may route
	// stateless or stateful work onto. Only valid inside OnStart.
	RegisterLoop(name string, mode HandlerMode) error

	// RegisterTimer declares routing metadata for delayed self-calls issued via
	// After. Only valid inside OnStart.
	RegisterTimer(callID string, opts ...RegisterOption) error
}

// TreeOperator manages the actor's child subtree and remote-actor watches.
type TreeOperator interface {
	// Spawn creates a new child under Self. Parent is always Self.
	Spawn(props Props, name string) (ref.Ref, error)
	// Stop idles target without removing it from the tree. Target must
	// be Self or one of Self's descendants. OnStop is called; the actor
	// remains in the tree and can still respond to callables.
	Stop(target ref.Ref) error
	// Destroy removes target and its subtree from the tree. Target must
	// be Self or one of Self's descendants. OnStop (if not yet called)
	// and OnDestroy are called; services unregistered; watchers notified.
	Destroy(target ref.Ref) error
	// Watch subscribes to target's lifecycle / invocation events. Cross-subtree
	// watches are permitted. kinds defaults to WatchTerminated.
	Watch(target ref.Ref, kinds ...WatchKind) error
	// Unwatch removes any active watch on target.
	Unwatch(target ref.Ref)
}

// Locator addresses actors anywhere in the App.
type Locator interface {
	LookupID(id id.ActorID) (ref.Ref, bool)
	LookupService(name string) (ref.Ref, bool)
}

// AppContext exposes App-wide read-only infrastructure plus the timer.
type AppContext interface {
	Namespace() string
	Codec() codec.Codec
	Schemas() schema.Reader
	Resources() resource.Registry
	Root() ref.Ref
	Clock() Clock
	// After schedules a delayed self-Invoke of callID with payload.
	After(delay time.Duration, callID string, payload any) error
}

// EventEmitter publishes events declared via RegisterEventKind. Valid
// during OnStart and any handler invocation; calls on a stopped Context
// return DiagEventAfterStop.
//
// Distinct from Emitter (streaming chunk sender), which is delivered as
// a handler parameter for streaming callables. EventEmitter sits on the
// Context itself and broadcasts to the App-scoped EventBus.
type EventEmitter interface {
	// EmitEvent publishes payload under kind. kind must have been declared
	// via Registrar.RegisterEventKind during OnStart, and the runtime type
	// of payload must equal the example type passed at registration.
	// Returns DiagEventKindUnknown / DiagEventPayloadMismatch on validation
	// failure, DiagEventAfterStop on stopped contexts, DiagEventNoBus when
	// the runtime has no EventBus wired (configuration error).
	EmitEvent(kind string, payload any) error

	// HasEventSubscribers reports whether there are any live observers for
	// this actor's event kind. It is intended only as a cost gate for
	// expensive observer-oriented work and must not be used to suppress
	// core domain state changes.
	HasEventSubscribers(kind string) bool

	// EventSubscriberCount reports the current observer count for this actor's
	// event kind. It is intended for observability and adaptive sampling; do
	// not rely on it for correctness-critical control flow.
	EventSubscriberCount(kind string) int
}

// Planner is the orchestration capability for creating Plan Nodes and
// performing one-shot Call/Stream invocations. Not all actors have a
// Planner — ctx.Planner() returns nil for actors spawned without the
// Planner capability (see Props.WithPlanner).
type Planner interface {
	// Plan creates a long-lived Plan Node child under Self that wraps a
	// single Invoke. Default state is Pending; the caller must explicitly
	// Start it and is responsible for stopping the node when done.
	Plan(target ref.Ref, callID string, payload any, opts ...plan.Option) (plan.Node, error)

	// Call performs a one-shot unary invocation and returns a Promise
	// that resolves with the result value. Non-blocking: the caller
	// can attach Then/Catch handlers or call Await() to block.
	Call(ctx context.Context, target ref.Ref, callID string, payload any) *promise.Promise[any]

	// Stream performs a streaming invocation with chunk-level callback.
	// Each chunk is delivered to onChunk from a background goroutine.
	// The returned Promise resolves with nil on io.EOF, or rejects on
	// error. If onChunk returns non-nil, the stream is closed and the
	// promise resolves with nil (consumer-requested stop, not an error).
	Stream(ctx context.Context, target ref.Ref, callID string, payload any, onChunk func(any) error) *promise.Promise[any]
}

// EventEnvelope is the payload delivered to SubscribeEventKind handlers.
// It carries the emitting actor's canonical ID, exposed service names,
// the event kind, and the raw Go payload value.
type EventEnvelope struct {
	ActorId  string
	Services []string
	Kind     string
	Payload  any
}

// Context is the full handler-facing context, composed from the six
// capability subinterfaces. User code targets Context directly; the
// subinterfaces exist for documentation and capability scoping.
type Context interface {
	IdentityContext
	Registrar
	TreeOperator
	Locator
	AppContext
	EventEmitter

	// Planner returns this actor's Planner, or nil if the actor was not
	// spawned with Planner capability (see Props.WithPlanner).
	Planner() Planner

	// Lifecycle returns a context.Context scoped to this actor's lifetime.
	// It is cancelled after the actor's OnStop returns (or after a Restart
	// tears down the old instance). Use it as the parent for internal
	// goroutines and cross-actor calls instead of context.Background().
	Lifecycle() context.Context
}

// ComponentHost is the gospore-internal extension implemented by the
// concrete Context value. It exposes the dynamic-component table to the
// generic GetComponent / MustGetComponent / HasComponent helpers in this
// package without widening the public Context surface.
//
// User code does not implement or assert against this interface; it
// exists so the helpers can read components attached via
// Registrar.AttachComponent across package boundaries.
type ComponentHost interface {
	// Component returns the value bound to name and a found flag.
	// The returned `any` is the raw value as supplied to AttachComponent;
	// the helpers below perform the type assertion against T.
	Component(name string) (any, bool)
}

// GetComponent retrieves a dynamic component (one attached via
// AttachComponent) by name, returning the zero value and false on
// miss or type mismatch. Static components — fields tagged with
// `gospore:"component"` on the actor struct — are accessed as
// regular fields and do NOT go through this API.
func GetComponent[T any](ctx Context, name string) (T, bool) {
	var zero T
	if ctx == nil {
		return zero, false
	}
	host, ok := ctx.(ComponentHost)
	if !ok {
		return zero, false
	}
	raw, ok := host.Component(name)
	if !ok {
		return zero, false
	}
	typed, ok := raw.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}

// MustGetComponent is like GetComponent but panics on miss or type mismatch.
func MustGetComponent[T any](ctx Context, name string) T {
	if ctx == nil {
		panic(fmt.Errorf("%s: nil context", DiagComponentUnknown))
	}
	host, ok := ctx.(ComponentHost)
	if !ok {
		panic(fmt.Errorf("%s: context does not host components", DiagComponentUnknown))
	}
	raw, ok := host.Component(name)
	if !ok {
		panic(fmt.Errorf("%s: component %q not attached", DiagComponentUnknown, name))
	}
	typed, ok := raw.(T)
	if !ok {
		var zero T
		panic(fmt.Errorf("%s: component %q stored as %T, requested as %T",
			DiagComponentTypeMismatch, name, raw, zero))
	}
	return typed
}

// HasComponent reports whether a dynamic component with name is attached.
func HasComponent(ctx Context, name string) bool {
	if ctx == nil {
		return false
	}
	host, ok := ctx.(ComponentHost)
	if !ok {
		return false
	}
	_, ok = host.Component(name)
	return ok
}
