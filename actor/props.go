package actor

import "github.com/qomos-w/gospore/id"

// Props bundles every configuration value needed to spawn one actor.
// Construct via PropsFromFunc; Props is a value type.
type Props struct {
	factory         func() Actor
	asyncStart      bool
	eventVisibility Visibility
	role            string     // default caller role propagated on outbound calls
	planEnabled     bool       // enables Planner capability for this actor
	actorID         id.ActorID // pre-assigned ID; zero means auto-generate
}

// PropsFromFunc constructs Props from an Actor factory.
// The factory is invoked once per spawn (and once per supervisor restart),
// so it must produce a fresh Actor instance each call.
func PropsFromFunc(factory func() Actor) Props {
	return Props{factory: factory, eventVisibility: VisibilityInternal}
}

// Factory returns the actor factory captured by PropsFromFunc.
// Used by gospore-internal cell wiring; user code rarely needs this.
func (p Props) Factory() func() Actor {
	return p.factory
}

// WithAsyncStart returns a copy of Props that opts out of Spawn waiting
// for OnStart completion after the child has been tree-registered.
//
// Spawn still triggers Start immediately once batch init is complete; this
// flag only changes whether the caller blocks for the child's readiness.
func (p Props) WithAsyncStart() Props {
	p.asyncStart = true
	return p
}

// AsyncStart reports whether this Props requests async OnStart.
func (p Props) AsyncStart() bool {
	return p.asyncStart
}

// WithEventVisibility returns a copy of Props with the given visibility
// applied to lifecycle events emitted by this actor (WatchTerminated,
// WatchStarted, WatchRestarted, WatchChildSpawned, WatchChildTerminated).
// Defaults to VisibilityInternal.
func (p Props) WithEventVisibility(v Visibility) Props {
	p.eventVisibility = v
	return p
}

// EventVisibility returns the visibility configured for this actor's
// lifecycle events. Defaults to VisibilityInternal.
func (p Props) EventVisibility() Visibility {
	return p.eventVisibility
}

// WithRole returns a copy of Props with the given default caller role.
// The role is forwarded as gospore.caller_role on outbound calls when
// the caller does not supply an explicit role header.
func (p Props) WithRole(role string) Props {
	p.role = role
	return p
}

// Role returns the default caller role configured for this actor.
func (p Props) Role() string {
	return p.role
}

// WithPlanner returns a copy of Props that grants the Planner capability
// to the spawned actor. Without this, ctx.Planner() returns nil.
func (p Props) WithPlanner() Props {
	p.planEnabled = true
	return p
}

// PlanEnabled reports whether this Props grants the Planner capability.
func (p Props) PlanEnabled() bool {
	return p.planEnabled
}

// WithID returns a copy of Props that pre-assigns the given ActorID.
// A zero-value ID means auto-generate (default behaviour).
func (p Props) WithID(id id.ActorID) Props {
	p.actorID = id
	return p
}

// ID returns the pre-assigned ActorID. Zero-value means auto-generate.
func (p Props) ID() id.ActorID {
	return p.actorID
}
