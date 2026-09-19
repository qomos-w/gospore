// Package tree implements the actor tree the App holds.
//
// Tree is the SOLE owner of the (ActorID → cell) and (Path → cell) indices.
// Context.LookupID and the transport's wire-ID resolution both terminate
// in Tree.LookupID. There is exactly one Tree per App.
package tree

import (
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/ref"
)

// Tree manages the actor hierarchy.
type Tree interface {
	// Root returns the App's root actor reference.
	Root() ref.Ref

	// Spawn creates a new actor under parent with the given props and
	// name. Constraints:
	//   - The name must be unique among parent's children.
	//   - parent may not be self or any descendant of the new actor
	//     (no cycles).
	Spawn(parent ref.Ref, props actor.Props, name string) (ref.Ref, error)

	// Stop idles target without removing it from the tree. The cell
	// transitions to idle (OnStop called, background work halted) but
	// remains in the tree and can still respond to callables. Does
	// not cascade to children.
	Stop(target ref.Ref) error

	// Destroy removes target and its entire subtree from the tree
	// LIFO: descendants first, then target. Equivalent to the
	// pre-idle-split Stop semantics. Triggers OnDestroy on each
	// cell, unregisters services, notifies watchers, and closes
	// mailboxes.
	Destroy(target ref.Ref) error

	// Children returns parent's direct children.
	Children(parent ref.Ref) []ref.Ref

	// Parent returns child's parent. The root has (nil, false).
	Parent(child ref.Ref) (ref.Ref, bool)

	// LookupID resolves an ActorID to its current Ref. Authoritative
	// for transport-arrived frames whose To field carries an ActorID.
	LookupID(id id.ActorID) (ref.Ref, bool)

	// Walk performs a depth-first traversal. visit returning false
	// halts traversal early.
	Walk(visit func(ref.Ref) bool)

	// OnChange registers a callback invoked after Spawn or Destroy
	// completes successfully. Callbacks must not call Spawn, Destroy,
	// or any Tree mutation method. Multiple callbacks may be registered;
	// they fire in registration order.
	OnChange(callback func())

	// RegisterScopedService registers a scoped service visible only to
	// descendants of owner. name must be a valid service name. Returns an
	// error if name is invalid or conflicts with an existing service in
	// the owner's subtree.
	RegisterScopedService(owner ref.Ref, name string, target ref.Ref) error

	// UnregisterScopedService removes a scoped service registration.
	UnregisterScopedService(owner ref.Ref, name string)

	// UnregisterAllScopedServices removes all scoped service
	// registrations for owner.
	UnregisterAllScopedServices(owner ref.Ref)

	// LookupScopedService resolves name to the nearest scoped service
	// visible from caller by walking up the parent chain. Returns
	// (nil, false) if no scoped service is found.
	LookupScopedService(caller ref.Ref, name string) (ref.Ref, bool)

	// RegisterGlobalService validates that a global service registration
	// does not conflict with scoped services in owner's subtree. The
	// actual global registry update is performed by the caller.
	RegisterGlobalService(owner ref.Ref, name string, target ref.Ref) error

	// UnregisterGlobalService removes the global service conflict
	// tracking entry for owner.
	UnregisterGlobalService(owner ref.Ref, name string)
}
