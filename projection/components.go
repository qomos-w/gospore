package projection

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/qomos-w/gospore/actor"
)

// ComponentSlot describes one `gospore:"component"`-tagged field detected
// by ScanComponents. The Cell layer uses these slots as the per-actor
// projection input list:
//
//   - Name is the Go field name; surfaced as Snapshot.Fields key and as
//     the projection wire identifier.
//   - Ptr is &actor.Field — addressable so SnapshotOf / ApplyTo can read
//     and overwrite the field in place via spore binding.
//   - Type is reflect.TypeOf(actor.Field) (the field type, not the
//     pointer type). Cell uses this to look the field up in app.Schemas()
//     and to apply DiagUnstableComponentType detection (e.g. native maps).
//
// ComponentSlot intentionally does not carry a TypeDesc / schema handle —
// schema resolution lives in Cell where app.Schemas() is reachable.
// ScanComponents is pure reflection over the struct definition.
type ComponentSlot struct {
	Name       string
	Ptr        any
	Type       reflect.Type
	Visibility actor.Visibility
}

// ScanComponents walks a pointer-to-struct actor target and returns one
// ComponentSlot per `gospore:"component"`-tagged field, in declaration
// order. Errors are framed against DiagTagInvalid; callers map the
// returned error onto the gospore.projection.tag_invalid diagnostic
// envelope.
//
// Validation rules (§4.18 / line 1991-1995):
//
//  1. target must be a non-nil pointer to a struct. nil / non-pointer /
//     pointer-to-non-struct → DiagTagInvalid.
//  2. Tagged fields must be exported. An unexported tagged field is
//     unaddressable via reflect even if the struct is addressable, so
//     ApplyTo cannot write back. → DiagTagInvalid.
//  3. Tag value's first comma-separated token must be "component"; all
//     other tags are silently ignored (a struct may carry json / yaml
//     tags alongside gospore tags without confusing the scanner).
//  4. Tags may have an optional second comma-separated token that is a
//     visibility level: "public", "admin", "diagnostic", or "internal".
//     The grammar is "component" or "component,<visibility>". Three or
//     more tokens → DiagTagInvalid. An invalid visibility token →
//     DiagTagInvalid.
//
// Field-name uniqueness ("同 actor 内 tag 字段名唯一", §4.18) is
// guaranteed by Go itself within a single struct, so the scanner does
// not re-check it. If the scanner is later extended to recurse into
// embedded structs, that path will need to add a duplicate-name guard.
//
// What ScanComponents deliberately does NOT do (lives in Cell):
//
//   - Schema registration check (DiagUnregisteredSchema needs
//     app.Schemas()).
//   - Type-stability check (DiagUnstableComponentType needs spore).
//   - BindStruct call (needs app + schema handles).
//   - Embedded struct recursion. The pseudocode in §4.18 does not
//     recurse, and embedded actor.Host / actor.Host / actor.Definition are
//     un-tagged so the top-level scan suffices.
func ScanComponents(target any) ([]ComponentSlot, error) {
	if target == nil {
		return nil, fmt.Errorf("%s: actor is nil; expected pointer to struct", DiagTagInvalid)
	}
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("%s: actor is %s; expected pointer to struct",
			DiagTagInvalid, rv.Kind())
	}
	if rv.IsNil() {
		return nil, fmt.Errorf("%s: actor is a nil pointer; expected pointer to struct", DiagTagInvalid)
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s: actor points to %s; expected pointer to struct",
			DiagTagInvalid, rv.Kind())
	}
	rt := rv.Type()

	var slots []ComponentSlot

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag, ok := f.Tag.Lookup("gospore")
		if !ok {
			continue
		}
		parts := strings.Split(tag, ",")
		head := strings.TrimSpace(parts[0])
		if head != "component" {
			// Not a component tag — gospore reserves this tag namespace
			// for future markers, so silently skip values we don't
			// recognise. (A typo'd "component" → "componnet" lands here;
			// we cannot distinguish it from a deliberately different
			// marker without a registry.)
			continue
		}

		// From here we are definitely committed to "this field is a
		// component" — any further error is a structural failure.

		if !f.IsExported() {
			return nil, fmt.Errorf("%s: field %q is unexported; component fields must be exported",
				DiagTagInvalid, f.Name)
		}

		vis := actor.VisibilityInternal
		if len(parts) > 2 {
			return nil, fmt.Errorf("%s: field %q tag has %d comma-separated tokens; expected 1 or 2 (component[,visibility])",
				DiagTagInvalid, f.Name, len(parts))
		}
		if len(parts) == 2 {
			token := strings.TrimSpace(parts[1])
			parsed, ok := actor.ParseVisibility(token)
			if !ok {
				return nil, fmt.Errorf("%s: field %q tag has invalid visibility %q; expected public, admin, diagnostic, or internal",
					DiagTagInvalid, f.Name, token)
			}
			vis = parsed
		}

		fv := rv.Field(i)

		slots = append(slots, ComponentSlot{
			Name:       f.Name,
			Ptr:        fv.Addr().Interface(),
			Type:       f.Type,
			Visibility: vis,
		})
	}
	return slots, nil
}
