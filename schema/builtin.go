package schema

import (
	spore "github.com/qomos-w/spore/schema"
)

// Builtin schema IDs 0-127 are universal: they identify wire-format scalars
// and scalar-only containers without consulting any namespace registry.
// Any namespace may freely reference them; user-defined schemas live in
// [BuiltinReserveEnd+1, 65535] within their namespace.
//
// Scalar names follow spore.DescribeReflectType conventions:
//   - "any" denotes interface{}; handlers using it trigger a warning.
//   - "bytes" denotes []byte (not []uint8) and is a scalar, not a container.
//   - Container IDs are reserved for scalar-element containers only;
//     struct-element containers ([]Foo, map[K]Foo) must be wrapped in a
//     named struct at the top level of any callable signature.
const (
	BuiltinVoid uint64 = 0
	BuiltinAny  uint64 = 1

	BuiltinBool   uint64 = 2
	BuiltinInt    uint64 = 6  // int32
	BuiltinLong   uint64 = 7  // int64 — Go int's wire target
	BuiltinUInt   uint64 = 8  // uint64
	BuiltinULong  uint64 = 9  // uint64
	BuiltinFloat  uint64 = 10 // float32
	BuiltinDouble uint64 = 11 // float64
	BuiltinString uint64 = 12
	BuiltinBytes  uint64 = 13 // []byte

	BuiltinArrayAny    uint64 = 16
	BuiltinArrayBool   uint64 = 17
	BuiltinArrayInt    uint64 = 18
	BuiltinArrayLong   uint64 = 19
	BuiltinArrayDouble uint64 = 20
	BuiltinArrayString uint64 = 21

	BuiltinMapStringAny    uint64 = 32
	BuiltinMapStringBool   uint64 = 33
	BuiltinMapStringLong   uint64 = 34
	BuiltinMapStringDouble uint64 = 35
	BuiltinMapStringString uint64 = 36

	// System protocol structs (64-79 reserved for gateway control frames).
	// Struct field names in BuiltinDesc/BuiltinObject follow the wire
	// contract (the handler structs' json tags): auth frames use their
	// declared PascalCase names, event/projection frames use lowerFirst.
	BuiltinAuthReq                   uint64 = 64 // { Token: string }
	BuiltinAuthOk                    uint64 = 65 // { Manifest: string }
	BuiltinEventSubscribeServiceReq  uint64 = 66 // { serviceName: string, kind: string }
	BuiltinEventSubscribeInstanceReq uint64 = 67 // { actorId: string, kind: string }
	BuiltinProjectionGetReq          uint64 = 68 // { actorPath: string, component: string, schemaId: uint64 }
	BuiltinProjectionWatchReq        uint64 = 69 // { actorPath: string, component: string, schemaId: uint64 }

	// BuiltinReserveEnd is the last reserved slot. User schemas start at
	// BuiltinReserveEnd+1 (128) within their namespace.
	BuiltinReserveEnd uint64 = 127
)

// BuiltinUserStart is the first sid available for user schemas in any namespace.
const BuiltinUserStart uint64 = BuiltinReserveEnd + 1

// IsBuiltin reports whether id falls in the universal builtin region.
func IsBuiltin(id uint64) bool {
	return id <= BuiltinReserveEnd
}

// BuiltinDesc returns the spore TypeDesc bound to a builtin id.
// Returns (zero, false) when id is not a registered builtin slot.
func BuiltinDesc(id uint64) (spore.TypeDesc, bool) {
	desc, ok := builtinByID[id]
	return desc, ok
}

// BuiltinObject returns the ObjectDesc for a system protocol struct.
// Returns (zero, false) when id is not a system protocol struct.
func BuiltinObject(id uint64) (spore.ObjectDesc, bool) {
	o, ok := builtinObjectMap[id]
	return o, ok
}

// BuiltinIDFor returns the builtin id that represents desc on the wire,
// or 0/false when desc does not map to a builtin slot.
//
// Matching rules:
//   - TypeKindVoid → BuiltinVoid (id 0)
//   - TypeKindScalar with a known name → the matching builtin scalar id
//   - TypeKindArray whose Element is a known scalar → the matching builtin
//     scalar-array id
//   - TypeKindMap whose Key and Value are both known scalars → the matching
//     builtin scalar-map id (only string-keyed combinations covered today)
//   - TypeKindStruct whose ClassName matches a system protocol struct →
//     the matching builtin system struct id
//   - Anything else (class, struct-container, nested container,
//     unknown scalar) → (0, false)
func BuiltinIDFor(desc spore.TypeDesc) (uint64, bool) {
	switch desc.Kind {
	case spore.TypeKindVoid:
		return BuiltinVoid, true
	case spore.TypeKindScalar:
		id, ok := builtinScalarName[desc.Name]
		return id, ok
	case spore.TypeKindArray:
		if desc.Element == nil || desc.Element.Kind != spore.TypeKindScalar {
			return 0, false
		}
		id, ok := builtinArrayElem[desc.Element.Name]
		return id, ok
	case spore.TypeKindMap:
		if desc.Key == nil || desc.Value == nil {
			return 0, false
		}
		if desc.Key.Kind != spore.TypeKindScalar || desc.Value.Kind != spore.TypeKindScalar {
			return 0, false
		}
		id, ok := builtinMapKey[mapKey{key: desc.Key.Name, value: desc.Value.Name}]
		return id, ok
	case spore.TypeKindStruct:
		id, ok := builtinStructName[desc.ClassName]
		return id, ok
	}
	return 0, false
}

// IsBuiltinAny reports whether desc represents the "any" scalar. Handlers
// whose top-level types resolve to any get a warning at registration time.
func IsBuiltinAny(desc spore.TypeDesc) bool {
	return desc.Kind == spore.TypeKindScalar && desc.Name == "any"
}

type mapKey struct{ key, value string }

var (
	builtinScalarName = map[string]uint64{
		"any":    BuiltinAny,
		"bool":   BuiltinBool,
		"int":    BuiltinInt,
		"long":   BuiltinLong,
		"uint":   BuiltinUInt,
		"ulong":  BuiltinULong,
		"float":  BuiltinFloat,
		"double": BuiltinDouble,
		"string": BuiltinString,
		"bytes":  BuiltinBytes,
	}

	builtinArrayElem = map[string]uint64{
		"any":    BuiltinArrayAny,
		"bool":   BuiltinArrayBool,
		"int":    BuiltinArrayInt,
		"long":   BuiltinArrayLong,
		"double": BuiltinArrayDouble,
		"string": BuiltinArrayString,
	}

	builtinMapKey = map[mapKey]uint64{
		{key: "string", value: "any"}:    BuiltinMapStringAny,
		{key: "string", value: "bool"}:   BuiltinMapStringBool,
		{key: "string", value: "long"}:   BuiltinMapStringLong,
		{key: "string", value: "double"}: BuiltinMapStringDouble,
		{key: "string", value: "string"}: BuiltinMapStringString,
	}

	builtinStructName = map[string]uint64{
		"AuthReq":                   BuiltinAuthReq,
		"AuthOk":                    BuiltinAuthOk,
		"eventSubscribeServiceReq":  BuiltinEventSubscribeServiceReq,
		"eventSubscribeInstanceReq": BuiltinEventSubscribeInstanceReq,
		"projectionGetReq":          BuiltinProjectionGetReq,
		"projectionWatchReq":        BuiltinProjectionWatchReq,
	}

	builtinByID = func() map[uint64]spore.TypeDesc {
		out := map[uint64]spore.TypeDesc{
			BuiltinVoid: {Kind: spore.TypeKindVoid, Name: "void"},
		}
		for name, id := range builtinScalarName {
			out[id] = spore.TypeDesc{Kind: spore.TypeKindScalar, Name: name}
		}
		for name, id := range builtinArrayElem {
			elem := spore.TypeDesc{Kind: spore.TypeKindScalar, Name: name}
			out[id] = spore.TypeDesc{Kind: spore.TypeKindArray, Name: "array", Element: &elem}
		}
		for mk, id := range builtinMapKey {
			k := spore.TypeDesc{Kind: spore.TypeKindScalar, Name: mk.key}
			v := spore.TypeDesc{Kind: spore.TypeKindScalar, Name: mk.value}
			out[id] = spore.TypeDesc{Kind: spore.TypeKindMap, Name: "map", Key: &k, Value: &v}
		}
		// System protocol structs.
		out[BuiltinAuthReq] = spore.TypeDesc{
			Kind: spore.TypeKindStruct, Name: "AuthReq", ClassName: "AuthReq", ClassID: BuiltinAuthReq,
		}
		out[BuiltinAuthOk] = spore.TypeDesc{
			Kind: spore.TypeKindStruct, Name: "AuthOk", ClassName: "AuthOk", ClassID: BuiltinAuthOk,
		}
		out[BuiltinEventSubscribeServiceReq] = spore.TypeDesc{
			Kind: spore.TypeKindStruct, Name: "eventSubscribeServiceReq", ClassName: "eventSubscribeServiceReq", ClassID: BuiltinEventSubscribeServiceReq,
		}
		out[BuiltinEventSubscribeInstanceReq] = spore.TypeDesc{
			Kind: spore.TypeKindStruct, Name: "eventSubscribeInstanceReq", ClassName: "eventSubscribeInstanceReq", ClassID: BuiltinEventSubscribeInstanceReq,
		}
		out[BuiltinProjectionGetReq] = spore.TypeDesc{
			Kind: spore.TypeKindStruct, Name: "projectionGetReq", ClassName: "projectionGetReq", ClassID: BuiltinProjectionGetReq,
		}
		out[BuiltinProjectionWatchReq] = spore.TypeDesc{
			Kind: spore.TypeKindStruct, Name: "projectionWatchReq", ClassName: "projectionWatchReq", ClassID: BuiltinProjectionWatchReq,
		}
		return out
	}()

	// BuiltinObject returns the ObjectDesc for a system protocol struct.
	builtinObjectMap = map[uint64]spore.ObjectDesc{
		BuiltinAuthReq: {
			Kind: spore.TypeKindStruct,
			Name: "AuthReq",
			Fields: []spore.FieldDesc{
				{Name: "Token", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
			},
		},
		BuiltinAuthOk: {
			Kind: spore.TypeKindStruct,
			Name: "AuthOk",
			Fields: []spore.FieldDesc{
				{Name: "Manifest", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
			},
		},
		BuiltinEventSubscribeServiceReq: {
			Kind: spore.TypeKindStruct,
			Name: "eventSubscribeServiceReq",
			Fields: []spore.FieldDesc{
				{Name: "serviceName", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
				{Name: "kind", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
			},
		},
		BuiltinEventSubscribeInstanceReq: {
			Kind: spore.TypeKindStruct,
			Name: "eventSubscribeInstanceReq",
			Fields: []spore.FieldDesc{
				{Name: "actorId", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
				{Name: "kind", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
			},
		},
		BuiltinProjectionGetReq: {
			Kind: spore.TypeKindStruct,
			Name: "projectionGetReq",
			Fields: []spore.FieldDesc{
				{Name: "actorPath", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
				{Name: "component", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
				{Name: "schemaId", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "uint64"}},
			},
		},
		BuiltinProjectionWatchReq: {
			Kind: spore.TypeKindStruct,
			Name: "projectionWatchReq",
			Fields: []spore.FieldDesc{
				{Name: "actorPath", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
				{Name: "component", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}},
				{Name: "schemaId", Type: spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "uint64"}},
			},
		},
	}
)
