package schema

import "errors"

// Diagnostic codes raised on the App-scoped wire-type registry surface.
// Surfaced via Error frames and structured logs; callers compare against
// these constants rather than matching message strings or sentinel
// errors directly when they need to discriminate at the wire layer.
//
// Each constant mirrors a row from ARCHITECTURE.md §7.5 with the
// gospore.schema.* prefix. The four codes that the schema package itself
// emits at Register / Import time also have a paired sentinel in
// errors.go (ErrSchemaIDZero / ErrSchemaIDTaken / ErrSchemaNameTaken /
// ErrSchemaImportConflict); MapErrToDiag below is the canonical
// translation. The remaining two codes (Unknown, UnregisteredForCallable)
// are emitted by lookup-side consumers (Cell / handler reflection /
// scriptbridge) when Resolve returns false at wire / OnStart time, and
// have no schema-package sentinel — the constants live here because the
// wire identity sits under the gospore.schema.* namespace, not because
// the schema package itself raises them.
const (
	// DiagIDZero indicates Register or Unmarshal saw id == 0 (the
	// reserved zero value). Maps to ErrSchemaIDZero at the Go API
	// surface. Validation row.
	DiagIDZero = "gospore.schema.id_zero"

	// DiagIDTaken indicates Register / Unmarshal saw an id already
	// bound within the owner namespace. Maps to ErrSchemaIDTaken at
	// the Go API surface. Validation row.
	DiagIDTaken = "gospore.schema.id_taken"

	// DiagNameTaken indicates Register / Unmarshal saw a name already
	// bound within the owner namespace. Maps to ErrSchemaNameTaken at
	// the Go API surface. Validation row.
	DiagNameTaken = "gospore.schema.name_taken"

	// DiagUnknown indicates a wire frame's (SchemaNS, SchemaID) pair
	// is not visible in the local Set (the foreign namespace was
	// never Imported, or Imported but the id was not present). The
	// schema package itself returns (Entry, false) from Resolve;
	// Cell / transport layer the diagnostic on top when the lookup
	// is required to succeed (e.g., decoding an inbound Call frame).
	// Runtime row.
	DiagUnknown = "gospore.schema.unknown"

	// DiagUnregisteredForCallable indicates a handler input / output
	// (or streaming chunk) type lacks a SchemaSet entry. Enforced at
	// handler reflection / OnStart time in app.Schemas() — the
	// schema package itself does not perform this check. Validation
	// row. ARCHITECTURE.md §4.6.
	DiagUnregisteredForCallable = "gospore.schema.unregistered_for_callable"

	// DiagImportConflict indicates Import saw a foreign namespace
	// re-imported with a structurally divergent (id, desc) under the
	// same key, the same-namespace self-import case, or a nil
	// foreign argument. Maps to ErrSchemaImportConflict at the Go
	// API surface. Validation row.
	DiagImportConflict = "gospore.schema.import_conflict"
)

// MapErrToDiag returns the §7.5 wire-format diagnostic code that
// corresponds to err, or "" when err is nil or has no schema-tier
// wire code. Callers in Cell / transport / scriptbridge use this to
// translate an internal sentinel into the wire envelope without
// duplicating the switch on every call site.
//
// Coverage:
//   - ErrSchemaIDZero          → DiagIDZero
//   - ErrSchemaIDTaken         → DiagIDTaken
//   - ErrSchemaNameTaken       → DiagNameTaken
//   - ErrSchemaImportConflict  → DiagImportConflict
//
// ErrSchemaInvalidNamespace and ErrSchemaReadOnly intentionally return
// "" — neither has a code under the gospore.schema.* namespace in §7.5
// (invalid namespace surfaces under gospore.app.invalid_namespace at
// the App layer; read-only is a Go-side guard that never reaches the
// wire — a Set returned from Unmarshal is local-only). Unknown /
// UnregisteredForCallable also have no schema-package sentinel — they
// are emitted by the consumer side after Resolve returns false, so a
// Go error never carries them inside this package; consumers that need
// those codes use the constants directly.
//
// Uses errors.Is so the helper composes with wrapped sentinels
// (fmt.Errorf("...: %w", ErrSchemaIDZero) still maps to DiagIDZero).
func MapErrToDiag(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrSchemaIDZero):
		return DiagIDZero
	case errors.Is(err, ErrSchemaIDTaken):
		return DiagIDTaken
	case errors.Is(err, ErrSchemaNameTaken):
		return DiagNameTaken
	case errors.Is(err, ErrSchemaImportConflict):
		return DiagImportConflict
	}
	return ""
}
