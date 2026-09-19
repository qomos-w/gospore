// Package actor defines the actor model surface: Actor / Context / Watcher,
// handler modes and visibility, register options, watch kinds, the lifecycle
// and invocation event types, Emitter / Subscription, Base, Props, and CallID.
//
// Cross-package dependency direction:
//
//	actor → ref → id, path, schema, codec, invoke
//	actor → plan
package actor

import "github.com/qomos-w/gospore/ref"

// Actor is the minimal contract every actor implements.
// OnStart is called once before any handler invocation; the only place
// where Register / RegisterScript / AttachComponent / Expose are valid.
// OnStop is called once after the last handler has returned, in reverse
// order of any embedded Actor chain.
type Actor interface {
	Type() string
	OnStart(ctx Context) error
	OnStop(ctx Context) error
}

// Initializable is an optional lifecycle hook called before OnStart.
// OnInit runs during the init phase — it should only load state, assign
// fields, and inject dependencies. Registering callables, exposing
// services, starting goroutines, or making cross-actor calls belongs in
// OnStart. The base struct provides a no-op OnInit so all actors satisfy
// this interface without explicit implementation.
type Initializable interface {
	OnInit(ctx Context) error
}

// Destroyable is an optional lifecycle hook called when an actor is
// removed from the tree (ctx.Destroy / App shutdown). It runs after
// OnStop, in reverse order of any embedded Actor chain. Use it to
// release resources that must not outlive the actor's tree presence
// (e.g. cascading Destroy of child actors). The base struct provides
// a no-op OnDestroy so all actors satisfy this interface without
// explicit implementation.
type Destroyable interface {
	OnDestroy(ctx Context) error
}

// Watcher is the optional callback for actors interested in the legacy
// "target terminated" notification. Prefer Context.Watch with explicit
// WatchKind for new code; Watcher is kept for the simple, common case.
type Watcher interface {
	OnTerminated(ctx Context, who ref.Ref)
}
