package schema

import "errors"

// Sentinel errors returned by Set.Register and Set.Import.
var (
	// ErrSchemaIDZero is returned when Register is called with id == 0.
	ErrSchemaIDZero = errors.New("gospore/schema: id 0 is reserved")

	// ErrSchemaIDTaken is returned when Register reuses an id within the
	// owner namespace.
	ErrSchemaIDTaken = errors.New("gospore/schema: id already taken in namespace")

	// ErrSchemaNameTaken is returned when Register reuses a name within
	// the owner namespace.
	ErrSchemaNameTaken = errors.New("gospore/schema: name already taken in namespace")

	// ErrSchemaImportConflict is returned when Import sees the same
	// namespace re-imported with a structurally divergent (id, desc)
	// for the same key.
	ErrSchemaImportConflict = errors.New("gospore/schema: import conflict")

	// ErrSchemaInvalidNamespace is returned for namespaces that fail the
	// `^[a-z][a-z0-9_]*$` rule or use a reserved prefix.
	ErrSchemaInvalidNamespace = errors.New("gospore/schema: invalid namespace")

	// ErrSchemaReadOnly is returned by Register / Import / Marshal calls
	// against a Set returned from Unmarshal — those carry only foreign
	// snapshots and may not be mutated.
	ErrSchemaReadOnly = errors.New("gospore/schema: read-only set")
)
