package schema

import (
	"errors"

	spore "github.com/qomos-w/spore/schema"
)

// BuildManifest produces a manifest JSON describing every namespace's
// declared callables and types from a slice of NamespaceDecl.
//
// Thin wrapper over spore.BuildManifest; gospore re-exposes it here so
// the codegen pipeline can stay package-local.
//
// Note: spore.BuildManifest only reflects unary methods
// (func(R, Req) (Final, error)). Streaming callables must be appended
// separately via BuildManifestWithStreaming.
func BuildManifest(decls []spore.NamespaceDecl) (spore.Manifest, error) {
	return spore.BuildManifest(decls)
}

// MethodsOf returns the receiver's methods as a slice of opaque method
// values, suitable for feeding into spore reflection (CallableDesc /
// DescribeGoFunction). Thin wrapper over spore.MethodsOf.
func MethodsOf(receiver any) []any {
	return spore.MethodsOf(receiver)
}

// StreamingDef describes one streaming callable for manifest generation.
// BuildManifest (spore reflection) only supports unary methods, so
// streaming callables must be declared explicitly and appended via
// BuildManifestWithStreaming.
type StreamingDef struct {
	// Namespace is the wire-form namespace (e.g. "auth").
	Namespace string
	// CallableName is the snake_case callable name (e.g. "tail_logins").
	CallableName string
	// ReqType is the request struct value (used for reflection).
	ReqType any
	// ChunkType is the chunk struct value (used for reflection).
	ChunkType any
	// FinalType is the final struct value (used for reflection).
	FinalType any
	// Visibility controls whether the streaming callable and its schema
	// types are exported to frontend codegen. One of: "internal",
	// "diagnostic", "admin", "public". Defaults to "internal".
	Visibility string
	// Description is a human-readable description emitted into the manifest.
	Description string
}

// BuildManifestWithStreaming combines spore's reflection-driven
// BuildManifest for unary methods with explicit streaming definitions.
// It first calls BuildManifest on decls, then appends streaming schemas
// and callables with auto-allocated contiguous schema IDs.
//
// Usage:
//
//	base, err := schema.BuildManifestWithStreaming(
//	    []spore.NamespaceDecl{{Namespace: "auth", Methods: schema.MethodOf(actor)}},
//	    []schema.StreamingDef{{
//	        Namespace: "auth", CallableName: "tail_logins",
//	        ReqType: TailLoginsReq{}, ChunkType: LoginEvent{}, FinalType: TailLoginsFinal{},
//	    }},
//	)
func BuildManifestWithStreaming(decls []spore.NamespaceDecl, streaming []StreamingDef) (spore.Manifest, error) {
	m, err := spore.BuildManifest(decls)
	if err != nil {
		return spore.Manifest{}, err
	}

	nextID := nextSchemaID(m)
	for _, def := range streaming {
		schemas, callable := def.toManifest(nextID)
		m.Schemas = append(m.Schemas, schemas...)
		m.Callables = append(m.Callables, callable)
		nextID += uint64(len(schemas))
	}
	return m, nil
}

func nextSchemaID(m spore.Manifest) uint64 {
	var max uint64
	for _, s := range m.Schemas {
		if s.SchemaID > max {
			max = s.SchemaID
		}
	}
	next := max + 1
	if next < BuiltinUserStart {
		return BuiltinUserStart
	}
	return next
}

func (d StreamingDef) toManifest(startID uint64) (schemas []spore.ManifestSchema, callable spore.ManifestCallable) {
	reqName := typeName(d.ReqType)
	chunkName := typeName(d.ChunkType)
	finalName := typeName(d.FinalType)

	vis := d.Visibility
	if vis == "" {
		vis = "internal"
	}
	schemas = []spore.ManifestSchema{
		{Namespace: d.Namespace, SchemaID: startID, Name: reqName, Visibility: vis, Object: mustDescribe(d.ReqType)},
		{Namespace: d.Namespace, SchemaID: startID + 1, Name: chunkName, Visibility: vis, Object: mustDescribe(d.ChunkType)},
		{Namespace: d.Namespace, SchemaID: startID + 2, Name: finalName, Visibility: vis, Object: mustDescribe(d.FinalType)},
	}

	callable = spore.ManifestCallable{
		Namespace:     d.Namespace,
		Name:          d.CallableName,
		Visibility:    vis,
		Description:   d.Description,
		Mode:          string(spore.CallableModeStreaming),
		ReqSchemaID:   startID,
		ChunkSchemaID: startID + 1,
		FinalSchemaID: startID + 2,
		Req:           StructRef(reqName),
		Chunk:         ptr(StructRef(chunkName)),
		Final:         StructRef(finalName),
	}
	return schemas, callable
}

func typeName(v any) string {
	d, err := spore.DescribeGoStruct(v)
	if err != nil {
		return ""
	}
	return d.Name
}

func mustDescribe(v any) spore.ObjectDesc {
	d, err := spore.DescribeGoStruct(v)
	if err != nil {
		return spore.ObjectDesc{}
	}
	return d
}

// StructRef returns a TypeDesc reference to a named struct type. It
// is used by manifest import and by callers (e.g. app manifest
// export) that reference types previously registered by name.
func StructRef(name string) spore.TypeDesc {
	return spore.TypeDesc{
		Kind:      spore.TypeKindStruct,
		Name:      name,
		ClassName: name,
	}
}

// ImportFromManifest imports every schema from a spore manifest into dst.
// Each ManifestSchema is converted to a schema.Entry with a TypeDesc
// reference derived from the ObjectDesc kind and name. Schema IDs and
// namespace are taken directly from the manifest.
//
// Schemas with duplicate names (same struct referenced from multiple callable
// namespaces) are skipped — the first registration wins. This is safe because
// duplicate names always represent the same Go type.
func ImportFromManifest(dst Set, manifest spore.Manifest) error {
	for _, ms := range manifest.Schemas {
		desc := spore.TypeDesc{
			Kind:      ms.Object.Kind,
			Name:      ms.Object.Name,
			ClassName: ms.Object.Name,
			ClassID:   ms.SchemaID,
		}
		err := dst.Register(ms.SchemaID, ms.Name, desc, ms.Object)
		if err != nil {
			if errors.Is(err, ErrSchemaNameTaken) {
				continue
			}
			return err
		}
	}
	return nil
}

func ptr[T any](v T) *T {
	return &v
}
