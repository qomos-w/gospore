package actor

import (
	"fmt"

	"github.com/qomos-w/gospore/internal/collections"
)

// ReloadDiff describes the result of comparing oldDef against newDef
// for an `_gospore_.script_reload` request. Returned by
// ClassifyReloadDiff when the change set is compatible with the §4.20
// line 2604-2615 reload contract; consumers (the Cell-tier reload
// executor) iterate the slices to drive the actual swap path.
//
// Slice ordering follows newDef's declaration order — important for
// the Cell so that swap effects appear in a predictable sequence in
// downstream events / projection updates.
type ReloadDiff struct {
	// AddedHandlers carries the HandlerDecls present in newDef but not
	// in oldDef (by CallID). Cell registers each as a fresh invoker.
	AddedHandlers []HandlerDecl

	// ChangedHandlers carries the HandlerDecls present in both
	// Definitions. Mode is guaranteed equal — mode-mismatch is rejected
	// by the classifier as incompatible. Source and Compiled equality
	// is NOT inspected by the leaf (bytecode comparison is opaque); the
	// Cell may dedupe no-op swaps by comparing Source itself, but this
	// is an optimization, not a correctness concern.
	ChangedHandlers []HandlerDecl

	// AddedComponents carries the ComponentDecls present in newDef but
	// not in oldDef (by Name). Cell creates fresh spore binding
	// entries and initializes each from its Initial. Existing
	// components retain state per §4.20.
	AddedComponents []ComponentDecl

	// MetaChanged is true when the shallow contents of oldDef.Meta and
	// newDef.Meta differ (treating nil and empty as equal). Cell
	// refreshes the projection-exposed metadata on true.
	MetaChanged bool
}

// ClassifyReloadDiff compares oldDef against newDef per the §4.20 line
// 2604-2615 compatibility table.
//
// Allowed (returns ReloadDiff, nil error):
//   - Add a new script handler (CallID not in oldDef)
//   - Modify an existing handler's Source or Compiled (mode unchanged)
//   - Add a new dynamic component (Name not in oldDef)
//   - Modify Meta entries (free-form, no actor-behavior tie-in)
//   - First-time content load (oldDef nil, newDef non-nil)
//   - Trivial no-op (both nil)
//
// Rejected (returns ReloadDiff{}, DiagIncompatibleReload-tagged error):
//   - Remove the entire data layer (oldDef non-nil, newDef nil) —
//     callers must use _gospore_.script_replace with DropState=true.
//   - Change Definition Namespace (identity change).
//   - Delete a script handler (CallID in oldDef but not newDef).
//   - Change a handler's Mode (Stateful ↔ Stateless).
//   - Delete a dynamic component (Name in oldDef but not newDef).
//   - Change a dynamic component's SchemaID.
//
// Out of scope at the leaf (left to the Cell-tier reload executor):
//   - Handler I/O schema change detection — needs spore reflection.
//   - Go-handler-immutable check (DiagGoHandlerImmutable) — needs the
//     Cell-side callable registry to know which CallIDs are Go-bound.
//   - Bytecode equality between two non-nil Compiled artifacts — opaque
//     `any` comparison cannot be done safely without a fingerprint.
//
// The data-layer classifier owns the pure structural rules; the Cell
// owns runtime-tier checks. M07-precedent split.
func ClassifyReloadDiff(oldDef, newDef *Definition) (ReloadDiff, error) {
	if newDef == nil {
		if oldDef == nil {
			return ReloadDiff{}, nil
		}
		return ReloadDiff{}, fmt.Errorf(
			"%s: newDef is nil; %s cannot remove the entire data layer (use %s with DropState=true)",
			DiagIncompatibleReload, CallIDReload, CallIDReplace)
	}

	// newDef is non-nil from here. oldDef may be nil ("first content
	// load" — every handler / component / Meta entry is purely added).
	var (
		oldNamespace  string
		oldHandlers   map[string]HandlerDecl
		oldComponents map[string]ComponentDecl
		oldMeta       map[string]string
	)
	if oldDef != nil {
		oldNamespace = oldDef.Namespace
		oldHandlers = make(map[string]HandlerDecl, len(oldDef.Handlers))
		for _, h := range oldDef.Handlers {
			oldHandlers[h.CallID] = h
		}
		oldComponents = make(map[string]ComponentDecl, len(oldDef.Components))
		for _, c := range oldDef.Components {
			oldComponents[c.Name] = c
		}
		oldMeta = oldDef.Meta
	}

	if oldDef != nil && oldNamespace != newDef.Namespace {
		return ReloadDiff{}, fmt.Errorf(
			"%s: Namespace change %q → %q is not allowed in %s",
			DiagIncompatibleReload, oldNamespace, newDef.Namespace, CallIDReload)
	}

	var diff ReloadDiff

	newComponentNames := collections.NewSet[string](len(newDef.Components))
	for _, c := range newDef.Components {
		newComponentNames.Add(c.Name)
		if old, exists := oldComponents[c.Name]; exists {
			if old.SchemaID != c.SchemaID {
				return ReloadDiff{}, fmt.Errorf(
					"%s: component %q SchemaID change %d → %d is not allowed in %s",
					DiagIncompatibleReload, c.Name, old.SchemaID, c.SchemaID, CallIDReload)
			}
		} else {
			diff.AddedComponents = append(diff.AddedComponents, c)
		}
	}
	for name := range oldComponents {
		if !newComponentNames.Has(name) {
			return ReloadDiff{}, fmt.Errorf(
				"%s: component %q was removed; deletion requires %s with DropState=true",
				DiagIncompatibleReload, name, CallIDReplace)
		}
	}

	newHandlerCallIDs := collections.NewSet[string](len(newDef.Handlers))
	for _, h := range newDef.Handlers {
		newHandlerCallIDs.Add(h.CallID)
		if old, exists := oldHandlers[h.CallID]; exists {
			if old.Mode != h.Mode {
				return ReloadDiff{}, fmt.Errorf(
					"%s: handler %q Mode change %s → %s is not allowed in %s",
					DiagIncompatibleReload, h.CallID, old.Mode.String(), h.Mode.String(), CallIDReload)
			}
			diff.ChangedHandlers = append(diff.ChangedHandlers, h)
		} else {
			diff.AddedHandlers = append(diff.AddedHandlers, h)
		}
	}
	for callID := range oldHandlers {
		if !newHandlerCallIDs.Has(callID) {
			return ReloadDiff{}, fmt.Errorf(
				"%s: handler %q was removed; deletion requires %s with DropState=true",
				DiagIncompatibleReload, callID, CallIDReplace)
		}
	}

	diff.MetaChanged = !metaEqual(oldMeta, newDef.Meta)

	return diff, nil
}

// metaEqual returns true when two Meta maps carry the same key/value
// pairs (treating nil and empty as equal).
func metaEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
