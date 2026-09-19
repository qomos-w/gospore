package app

// Service exposure (global and scoped) and gateway-session actors.
import (
	"fmt"

	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/ref"
)

// App is the process-level entry point and root actor of one
// gospore service. App embeds actor.Actor: it has its own OnStart /
// OnStop chain, runs handlers in the `app.*` reserved namespace, and
func (a *appImpl) LookupService(name string) (ref.Ref, bool) {
	if r, ok := a.svcReg.Lookup(name); ok {
		return r, true
	}
	if a.cfg.discoveryProvider != nil {
		instances := a.cfg.discoveryProvider.Resolve(name)
		for _, inst := range instances {
			if inst.RuntimeSlotID == a.cfg.runtimeSlot {
				// Local instance — re-check registry in case sync is behind.
				if r, ok := a.svcReg.Lookup(name); ok {
					return r, true
				}
				continue
			}
			// Remote instance — construct a transparent RemoteRef.
			if !inst.ActorID.IsZero() && a.remoteTP != nil {
				return a.RemoteRef(inst.ActorID), true
			}
		}
	}
	return nil, false
}

func (a *appImpl) exposeService(owner ref.Ref, name string) error {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	if err := a.svcReg.Register(name, owner); err != nil {
		return err
	}
	if err := a.tree.RegisterGlobalService(owner, name, owner); err != nil {
		a.svcReg.Unregister(name)
		return err
	}
	return nil
}

func (a *appImpl) unexposeService(owner ref.Ref, name string) {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	a.svcReg.Unregister(name)
	a.tree.UnregisterGlobalService(owner, name)
}

func (a *appImpl) exposeServiceToChildren(owner ref.Ref, name string) error {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	return a.tree.RegisterScopedService(owner, name, owner)
}

func (a *appImpl) unexposeServiceToChildren(owner ref.Ref, name string) {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	a.tree.UnregisterScopedService(owner, name)
}

func (a *appImpl) unexposeAllScopedServices(owner ref.Ref) {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	a.tree.UnregisterAllScopedServices(owner)
}

func (a *appImpl) lookupScopedService(caller ref.Ref, name string) (ref.Ref, bool) {
	return a.tree.LookupScopedService(caller, name)
}
func (a *appImpl) deliver(target ref.Ref, env mailbox.Envelope) error {
	recv, ok := a.tp.Lookup(target.ID())
	if ok {
		return recv(env)
	}
	if a.remoteTP != nil {
		if sys, ok := env.Payload.(mailbox.SystemMsg); ok {
			body, err := encodeSystemMsg(sys)
			if err != nil {
				return err
			}
			return a.remoteTP.Send(message.Frame{
				To:   target.ID(),
				Kind: message.KindSystem,
				Body: body,
			})
		}
		frame := env.Frame
		if frame.To.IsZero() {
			frame.To = target.ID()
		}
		return a.remoteTP.Send(frame)
	}
	return fmt.Errorf("gospore/app: deliver target %s not registered", target.ID().Canonical())
}
func (a *appImpl) SpawnGatewaySession(name string) (ref.Ref, error) {
	return a.tree.Spawn(a.rootRef, actor.PropsFromFunc(func() actor.Actor { return &actor.Host{} }), name)
}

// terminate path delivers Destroy to the cell, whose handleDestroy
// flushes the pending table (terminal errors to every in-flight call)
// before the registry entry is dropped.
func (a *appImpl) DestroyGatewaySession(r ref.Ref) error {
	return a.tree.Destroy(r)
}

// allocate is the Tree Allocator closure. Creates a Cell for a new actor.
// reserveSpawn allocates the identity for a new child without
// constructing it. Wired as the tree Allocator's Reserve phase: it runs
// under the tree write lock and must not block.
