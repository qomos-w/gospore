package app

import (
	"context"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/ref"
)

// cellHost adapts the App to the cell.Host capability groups. A single
// shared instance serves every cell: per-cell identity travels as the
// owner/caller arguments instead of per-cell closures. The App methods
// it forwards to are safe for concurrent use.
type cellHost struct {
	a *appImpl
}

func (a *appImpl) cellHost() cell.Host {
	return cellHost{a: a}
}

func (h cellHost) LookupService(name string) (ref.Ref, bool) {
	return h.a.LookupService(name)
}

func (h cellHost) ExposeService(owner ref.Ref, name string) error {
	return h.a.exposeService(owner, name)
}

func (h cellHost) UnexposeService(owner ref.Ref, name string) {
	h.a.unexposeService(owner, name)
}

func (h cellHost) ExposeScoped(owner ref.Ref, name string) error {
	return h.a.exposeServiceToChildren(owner, name)
}

func (h cellHost) UnexposeScoped(owner ref.Ref, name string) {
	h.a.unexposeServiceToChildren(owner, name)
}

func (h cellHost) UnexposeAllScoped(owner ref.Ref) {
	h.a.unexposeAllScopedServices(owner)
}

func (h cellHost) LookupScopedService(caller ref.Ref, name string) (ref.Ref, bool) {
	return h.a.lookupScopedService(caller, name)
}

func (h cellHost) Spawn(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
	return h.a.tree.Spawn(parent, props, name)
}

func (h cellHost) Stop(target ref.Ref) error {
	return h.a.tree.Stop(target)
}

func (h cellHost) Destroy(target ref.Ref) error {
	return h.a.tree.Destroy(target)
}

func (h cellHost) LookupID(aid id.ActorID) (ref.Ref, bool) {
	return h.a.tree.LookupID(aid)
}

func (h cellHost) InvokeAs(caller ref.Ref, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call {
	return h.a.invokeAs(caller, target, callID, payload, headers...)
}

func (h cellHost) Deliver(target ref.Ref, env mailbox.Envelope) error {
	return h.a.deliver(target, env)
}

// RootEscalated cancels the App run context. Only parent-less cells
// (the root) reach this path — children escalate to their parent.
func (h cellHost) RootEscalated(reason error) {
	if fn, ok := h.a.cancel.Load().(context.CancelFunc); ok && fn != nil {
		fn()
	}
}
