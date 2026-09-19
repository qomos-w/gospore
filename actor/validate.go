package actor

import (
	"fmt"

	"github.com/qomos-w/gospore/internal/collections"
)

// ValidateDefinition runs the pure-data half of §4.20 OnStart step 1.
// nil is legal — the empty configuration is a first-class state per
// §4.20 line 1933, so a nil Definition validates without error.
//
// The check covers every invariant that does NOT need app.Schemas() or
// spore reflection (those layers stay Cell-bound, M07-precedent split
// — same shape used by projection.ScanComponents in cycle 8):
//
//   - Namespace: actor.ValidNamespace + not literal "app". (The reserved
//     literal "_gospore_" is unreachable through ValidNamespace because
//     it has a leading underscore, so no separate gate is needed for
//     that name.)
//   - ComponentDecl: Name non-empty + Name unique within the slice +
//     SchemaID != 0. (Schema presence is a Cell-tier check —
//     DiagUnregisteredSchema needs app.Schemas().)
//   - HandlerDecl: CallID via actor.ValidCallID + first segment ==
//     Namespace via actor.MatchesAppNamespace + Mode is one of
//     actor.ModeStateful / ModeStateless + at least one of Source or
//     Compiled is set + CallID unique within the slice.
//
// All errors are framed against DiagDefinitionInvalid; callers map the
// returned error onto the gospore.actor.definition_invalid
// diagnostic envelope.
func ValidateDefinition(def *Definition) error {
	if def == nil {
		return nil
	}
	if !ValidNamespace(def.Namespace) {
		return fmt.Errorf("%s: namespace %q is not a valid identifier (^[a-z][a-z0-9_]*$)",
			DiagDefinitionInvalid, def.Namespace)
	}
	if def.Namespace == ReservedNamespaceApp {
		return fmt.Errorf("%s: namespace %q is reserved by gospore",
			DiagDefinitionInvalid, def.Namespace)
	}

	componentNames := collections.NewSet[string](len(def.Components))
	for i, c := range def.Components {
		if c.Name == "" {
			return fmt.Errorf("%s: component[%d] has empty Name",
				DiagDefinitionInvalid, i)
		}
		if !componentNames.Add(c.Name) {
			return fmt.Errorf("%s: component[%d] Name %q is duplicated within Definition",
				DiagDefinitionInvalid, i, c.Name)
		}
		if c.SchemaID == 0 {
			return fmt.Errorf("%s: component[%d] %q has SchemaID 0; must be non-zero",
				DiagDefinitionInvalid, i, c.Name)
		}
	}

	handlerCallIDs := collections.NewSet[string](len(def.Handlers))
	for i, h := range def.Handlers {
		if !ValidCallID(h.CallID) {
			return fmt.Errorf("%s: handler[%d] CallID %q is not a valid identifier (%s)",
				DiagDefinitionInvalid, i, h.CallID, DiagCallableInvalidID)
		}
		if !MatchesAppNamespace(h.CallID, def.Namespace) {
			return fmt.Errorf("%s: handler[%d] CallID %q first segment is not %q (%s)",
				DiagDefinitionInvalid, i, h.CallID, def.Namespace, DiagCallableNamespaceMismatch)
		}
		if h.Mode != ModeStateful && h.Mode != ModeStateless {
			return fmt.Errorf("%s: handler[%d] %q Mode %d is not one of Stateful (%d) / Stateless (%d)",
				DiagDefinitionInvalid, i, h.CallID, int(h.Mode),
				int(ModeStateful), int(ModeStateless))
		}
		if h.Source == "" && h.Compiled == nil {
			return fmt.Errorf("%s: handler[%d] %q has neither Source nor Compiled set",
				DiagDefinitionInvalid, i, h.CallID)
		}
		if !handlerCallIDs.Add(h.CallID) {
			return fmt.Errorf("%s: handler[%d] CallID %q is duplicated within Definition",
				DiagDefinitionInvalid, i, h.CallID)
		}
	}
	return nil
}
