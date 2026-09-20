package cell

import (
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/ref"
)

// Host is the capability bundle a Cell draws app-level services from.
// It is intentionally opaque: the Cell resolves the optional capability
// groups below via type assertion at construction time, so a host
// implementation supplies exactly the groups it supports and every
// missing group falls back to the documented default behavior
// (see host_fallbacks_test.go for the pinned contract).
type Host = any

// ServiceHost exposes the global and scoped service directory. The
// owner/caller arguments carry the acting cell's identity — this is
// what allows a single shared host instance to serve every cell
// without per-cell closures.
type ServiceHost interface {
	// LookupService resolves a global service name (local registry,
	// then discovery fallback). Name-only: no caller identity is
	// needed for global visibility.
	LookupService(name string) (ref.Ref, bool)
	// ExposeService registers a global service on owner's behalf.
	ExposeService(owner ref.Ref, name string) error
	// UnexposeService unregisters a global service on owner's behalf.
	UnexposeService(owner ref.Ref, name string)
	// ExposeScoped registers a service visible only to owner's
	// descendants.
	ExposeScoped(owner ref.Ref, name string) error
	// UnexposeScoped unregisters one of owner's scoped services.
	UnexposeScoped(owner ref.Ref, name string)
	// UnexposeAllScoped unregisters every scoped service of owner.
	UnexposeAllScoped(owner ref.Ref)
	// LookupScopedService resolves a scoped service by the caller's
	// position in the actor tree.
	LookupScopedService(caller ref.Ref, name string) (ref.Ref, bool)
}

// TreeHost provides actor-tree management.
type TreeHost interface {
	Spawn(parent ref.Ref, props actor.Props, name string) (ref.Ref, error)
	Stop(target ref.Ref) error
	Destroy(target ref.Ref) error
	LookupID(aid id.ActorID) (ref.Ref, bool)
}

// InvokeAsHost issues a call with an explicit caller identity (the
// planner's transport). It is deliberately separate from TreeHost:
// the no-host fallback (target.Invoke, sender becomes the target
// itself) changes observable semantics and must stay optional.
type InvokeAsHost interface {
	InvokeAs(caller ref.Ref, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call
}

// DeliveryHost carries cross-actor message delivery and root
// escalation. RootEscalated is only reached by parent-less cells —
// children always escalate to their parent via Deliver.
type DeliveryHost interface {
	Deliver(target ref.Ref, env mailbox.Envelope) error
	RootEscalated(reason error)
}
