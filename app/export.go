package app

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/gateway"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/internal/collections"
	"github.com/qomos-w/gospore/schema"
	spore "github.com/qomos-w/spore/schema"
)

// GatewayReady returns a channel that closes when the HTTP gateway is
// accepting connections. Returns nil if no gateway is configured.
func (a *appImpl) GatewayReady() <-chan struct{} {
	if a.gatewaySrv == nil {
		return nil
	}
	return a.gatewaySrv.ReadyChan()
}

// GatewayServer returns the HTTP gateway's *gateway.Server,
// or nil if no gateway is configured.
func (a *appImpl) GatewayServer() *gateway.Server {
	return a.gatewaySrv
}

// ExportGosporeManifest produces a manifest describing every callable,
// projection component, and event kind registered across the App's actor
// tree. The manifest is suitable for feeding into gospore-gen-ts.

// WaitForAllCellsStart blocks until every cell in the actor tree has either
// completed OnStart or reached a terminal (stopped) state. This is useful for
// tools like sporecode-gen-manifest that need a complete view of all registered
// callables and cannot rely solely on tree node-count stability.
func (a *appImpl) WaitForAllCellsStart(timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		a.cellMu.RLock()
		unsettled := make([]string, 0, 4)
		for _, c := range a.cells {
			if !c.IsStarted() && !c.IsStopped() {
				unsettled = append(unsettled, fmt.Sprintf("%s(%s)", c.Self().ID().Canonical(), c.Actor().Type()))
			}
		}
		a.cellMu.RUnlock()
		if len(unsettled) == 0 {
			return nil
		}
		select {
		case <-deadline:
			return fmt.Errorf("gospore/app: timeout waiting for all cells to start: unsettled cells: %v", unsettled)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (a *appImpl) ExportGosporeManifest() (schema.GosporeManifest, error) {
	a.cellMu.RLock()
	cells := make(map[id.ActorID]*cell.Cell, len(a.cells))
	for k, v := range a.cells {
		cells[k] = v
	}
	a.cellMu.RUnlock()
	return buildGosporeManifest(cells, a.schemas)
}

// typeKey is the map key for deduplicating struct types in manifest export.
type typeKey struct {
	ns   string
	name string
}

// buildGosporeManifest constructs a GosporeManifest from a snapshot of cells.
func buildGosporeManifest(cells map[id.ActorID]*cell.Cell, runtimeSchemas schema.Set) (schema.GosporeManifest, error) {
	var gm schema.GosporeManifest

	// Sort cells by actor type for deterministic output.
	var sorted []*cell.Cell
	for _, c := range cells {
		sorted = append(sorted, c)
	}
	slices.SortStableFunc(sorted, func(a, b *cell.Cell) int {
		return cmp.Compare(a.Actor().Type(), b.Actor().Type())
	})

	// Schema IDs come from the runtime schema.Set so manifest export and
	// on-the-wire Reply frames share one numbering source.
	structTypes := map[typeKey]reflect.Type{}   // (ns,name) → reflect.Type
	structIDs := map[typeKey]uint64{}           // (ns,name) → runtime schema ID
	structVis := map[typeKey]actor.Visibility{} // (ns,name) → max visibility

	// maxVis returns the greater of a and b (Public > Admin > Diagnostic > Internal).
	maxVis := func(a, b actor.Visibility) actor.Visibility {
		if a > b {
			return a
		}
		return b
	}

	var assignStructID func(ns string, rt reflect.Type, vis actor.Visibility) uint64

	assignStructID = func(ns string, rt reflect.Type, vis actor.Visibility) uint64 {
		if rt == nil {
			return 0
		}
		for rt.Kind() == reflect.Pointer || rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
			rt = rt.Elem()
		}
		if rt.Kind() != reflect.Struct {
			return 0
		}
		if isTimeType(rt) {
			return 0
		}
		key := typeKey{ns: ns, name: rt.Name()}
		prevVis, seen := structVis[key]
		newVis := maxVis(prevVis, vis)
		structVis[key] = newVis
		// On re-entry with no visibility upgrade there is nothing to propagate;
		// avoid infinite recursion on cyclic struct graphs.
		if seen && newVis == prevVis {
			return structIDs[key]
		}
		if _, ok := structIDs[key]; !ok {
			// Prefer a pre-registered schema ID (e.g. from a static fragment) so
			// manifest export and on-the-wire IDs stay stable. The registry keys
			// schemas by local name, so we match by rt.Name() regardless of the
			// callable namespace.
			if runtimeSchemas != nil {
				if entry, ok := runtimeSchemas.LookupByName(rt.Name()); ok {
					structIDs[key] = entry.ID
					structTypes[key] = rt
				}
			}
			if _, ok := structIDs[key]; !ok {
				desc, err := spore.DescribeReflectType(rt)
				if err != nil {
					return 0
				}
				if id, ok := schema.BuiltinIDFor(desc); ok {
					// Universal builtins (e.g. gospore system protocol structs) keep
					// the same ID regardless of callable namespace.
					structIDs[key] = id
				} else if runtimeSchemas != nil && ns == runtimeSchemas.Namespace() {
					id, err := runtimeSchemas.RegisterAuto(rt.Name(), desc)
					if err != nil {
						panic(fmt.Errorf("manifest register schema %s.%s: %w", ns, rt.Name(), err))
					}
					structIDs[key] = id
				} else {
					id := nextFreeStructID(structIDs)
					structIDs[key] = id
				}
				structTypes[key] = rt
			}
		}
		for i := 0; i < rt.NumField(); i++ {
			field := rt.Field(i)
			if !field.IsExported() {
				continue
			}
			_ = assignFieldStructIDs(ns, field.Type, newVis, assignStructID)
		}
		return structIDs[key]
	}

	// elementType unwraps pointer/array/slice to the underlying type.
	elementType := func(rt reflect.Type) reflect.Type {
		if rt == nil {
			return nil
		}
		for rt.Kind() == reflect.Pointer || rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array {
			rt = rt.Elem()
		}
		return rt
	}

	// First pass: walk cells to discover all struct types used by
	// callables, projections, and events; simultaneously compute max visibility.
	for _, c := range sorted {
		ns := c.Actor().Type()

		// Callables.
		if ht := c.Handlers(); ht != nil {
			callIDs := ht.CallIDs()
			slices.Sort(callIDs)
			for _, callID := range callIDs {
				inv, ok := ht.Lookup(callID)
				if !ok || inv.Fn == nil {
					continue // skip script handlers
				}
				callableNS := actor.CallID(callID).Namespace()
				if callableNS == "" {
					callableNS = ns
				}
				vis := inv.Visibility
				reqType, finalType, chunkType, _ := callableTypes(inv.Fn, inv.Desc.Mode == spore.CallableModeStreaming)
				// If Streaming[T]() declared a concrete chunk type, prefer it over
				// the any extracted from the untyped actor.Emitter signature.
				if inv.ChunkType != nil {
					chunkType = inv.ChunkType
				}
				_ = assignStructID(callableNS, reqType, vis)
				_ = assignStructID(callableNS, finalType, vis)
				_ = assignStructID(callableNS, chunkType, vis)
			}
		}

		// Projections.
		slots := c.ComponentSlots()
		for _, slot := range slots {
			_ = assignStructID(ns, slot.Type, slot.Visibility)
		}


		// Events.
		for _, entry := range c.EventMetaEntries() {
			_ = assignStructID(ns, entry.Type, entry.Visibility)
		}
	}

	// Emit ManifestSchema entries (sorted for determinism).
	var schemaKeys []typeKey
	for k := range structTypes {
		schemaKeys = append(schemaKeys, k)
	}
	slices.SortFunc(schemaKeys, func(a, b typeKey) int {
		if c := cmp.Compare(a.ns, b.ns); c != 0 {
			return c
		}
		return cmp.Compare(a.name, b.name)
	})
	for _, key := range schemaKeys {
		rt := structTypes[key]
		obj, err := spore.DescribeGoStruct(reflect.New(rt).Elem().Interface())
		if err != nil {
			return gm, fmt.Errorf("describe struct %s.%s: %w", key.ns, key.name, err)
		}
		vis := structVis[key]
		gm.Schemas = append(gm.Schemas, spore.ManifestSchema{
			Namespace:  key.ns,
			SchemaID:   structIDs[key],
			Name:       key.name,
			Visibility: vis.String(),
			Object:     obj,
		})
	}

	// Second pass: emit ManifestCallable, ProjectionDecl, EventDecl.
	for _, c := range sorted {
		ns := c.Actor().Type()
		actorPath := ns // stable logical identifier, not ephemeral runtime ULID

		// Callables.
		if ht := c.Handlers(); ht != nil {
			callIDs := ht.CallIDs()
			slices.Sort(callIDs)
			for _, callID := range callIDs {
				inv, ok := ht.Lookup(callID)
				if !ok || inv.Fn == nil {
					continue // skip script handlers
				}
				callableNS := actor.CallID(callID).Namespace()
				wireNS := callableNS
				if callableNS == "" {
					// Single-segment (actor-local) callable: struct schemas still
					// group under the actor type, but the wire callID carries no
					// namespace prefix (it IS the callID).
					callableNS = ns
				}
				callableName := actor.CallID(callID).Name()
				reqType, finalType, chunkType, isStreaming := callableTypes(inv.Fn, inv.Desc.Mode == spore.CallableModeStreaming)
				// If Streaming[T]() declared a concrete chunk type, prefer it over
				// the any extracted from the untyped actor.Emitter signature.
				if inv.ChunkType != nil {
					chunkType = inv.ChunkType
				}

			var mc spore.ManifestCallable
			mc.Namespace = wireNS
			mc.Name = callableName
			mc.ActorType = ns
			mc.Visibility = inv.Visibility.String()
			mc.Description = inv.Description
			mc.FinalDesc = inv.FinalDesc
			mc.ChunkDesc = inv.ChunkDesc
			mc.Effect = inv.Effect
			mc.Service = inv.ServiceName
			mc.ToolName = inv.ToolName

				if isStreaming {
					mc.Mode = string(spore.CallableModeStreaming)
					if reqType != nil {
						mc.Req, mc.ReqSchemaID = typeDescAndID(callableNS, reqType, structIDs)
					} else {
						mc.ReqSchemaID = schema.BuiltinVoid
						mc.Req = spore.TypeDesc{Kind: spore.TypeKindVoid, Name: "void"}
					}
					if chunkType != nil {
						td, sid := typeDescAndID(callableNS, chunkType, structIDs)
						mc.Chunk = &td
						if sid == 0 && td.Kind == spore.TypeKindScalar && td.Name == "any" {
							mc.ChunkSchemaID = schema.BuiltinAny
						} else {
							mc.ChunkSchemaID = sid
						}
					} else {
						mc.ChunkSchemaID = schema.BuiltinAny
						mc.Chunk = &spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "any"}
					}
					mc.FinalSchemaID = schema.BuiltinVoid
					mc.Final = spore.TypeDesc{Kind: spore.TypeKindVoid, Name: "void"}
				} else {
					mc.Mode = string(spore.CallableModeUnary)
					if reqType != nil {
						mc.Req, mc.ReqSchemaID = typeDescAndID(callableNS, reqType, structIDs)
					} else {
						mc.ReqSchemaID = schema.BuiltinVoid
						mc.Req = spore.TypeDesc{Kind: spore.TypeKindVoid, Name: "void"}
					}
					if finalType != nil {
						mc.Final, mc.FinalSchemaID = typeDescAndID(callableNS, finalType, structIDs)
					} else {
						mc.FinalSchemaID = schema.BuiltinVoid
						mc.Final = spore.TypeDesc{Kind: spore.TypeKindVoid, Name: "void"}
					}
				}
				gm.Callables = append(gm.Callables, mc)
			}
		}

		// Projections.
		for _, slot := range c.ComponentSlots() {
			td, schemaID := typeDescAndID(ns, slot.Type, structIDs)
			schemaName := td.Name
			if schemaName == "" {
				schemaName = td.ClassName
			}
			gm.Projections = append(gm.Projections, schema.ProjectionDecl{
				Namespace:  ns,
				ActorPath:  actorPath,
				Component:  slot.Name,
				SchemaID:   schemaID,
				SchemaName: schemaName,
				Type:       td,
				Mode:       "full",
				Visibility: slot.Visibility.String(),
			})
		}

		svc := c.Services()
		firstService := ""
		if len(svc) > 0 {
			firstService = svc[0]
		}

		// Events.
		eventMeta := c.EventMetaEntries()
		eventKinds := collections.SortedKeys(eventMeta)
		for _, kind := range eventKinds {
			entry := eventMeta[kind]
			et := elementType(entry.Type)
			key := typeKey{ns: ns, name: et.Name()}
			gm.Events = append(gm.Events, schema.EventDecl{
				Namespace:  ns,
				ActorPath:  actorPath,
				Kind:       kind,
				SchemaID:   structIDs[key],
				ServiceName: firstService,
				Visibility:  entry.Visibility.String(),
				Loop:        entry.Loop,
			})
		}
	}

	// Third pass: enrich request schema field descriptions from registration options.
	// Match by request type name because the Invoker's RequestSchemaID is
	// allocated during Register and may differ from the export pass IDs.
	schemaByName := make(map[string]int, len(gm.Schemas))
	for i, s := range gm.Schemas {
		schemaByName[s.Name] = i
	}
	for _, c := range sorted {
		ht := c.Handlers()
		if ht == nil {
			continue
		}
		descCallIDs := ht.CallIDs()
		slices.Sort(descCallIDs)
		for _, callID := range descCallIDs {
			inv, ok := ht.Lookup(callID)
			if !ok || inv.ParamDescs == nil || inv.Fn == nil {
				continue
			}
			reqType, _, _, _ := callableTypes(inv.Fn, inv.Desc.Mode == spore.CallableModeStreaming)
			if reqType == nil {
				continue
			}
			for reqType.Kind() == reflect.Pointer {
				reqType = reqType.Elem()
			}
			if reqType.Kind() != reflect.Struct {
				continue
			}
			idx, ok := schemaByName[reqType.Name()]
			if !ok {
				continue
			}
			for j := range gm.Schemas[idx].Object.Fields {
				if d, ok := inv.ParamDescs[gm.Schemas[idx].Object.Fields[j].Name]; ok {
					gm.Schemas[idx].Object.Fields[j].Description = d
				}
			}
		}
	}

	return gm, nil
}

// nextFreeStructID returns the smallest ID >= BuiltinUserStart that is not
// already assigned in structIDs. This keeps auto-allocated IDs from colliding
// with pre-registered schema IDs.
func nextFreeStructID(structIDs map[typeKey]uint64) uint64 {
	id := schema.BuiltinUserStart
	for {
		taken := false
		for _, existing := range structIDs {
			if existing == id {
				taken = true
				break
			}
		}
		if !taken {
			return id
		}
		id++
		if id == 0 {
			panic("manifest export: schema id overflow")
		}
	}
}

func assignFieldStructIDs(ns string, rt reflect.Type, vis actor.Visibility, assignStructID func(string, reflect.Type, actor.Visibility) uint64) uint64 {
	if rt == nil {
		return 0
	}
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	switch rt.Kind() {
	case reflect.Struct:
		return assignStructID(ns, rt, vis)
	case reflect.Slice, reflect.Array:
		return assignFieldStructIDs(ns, rt.Elem(), vis, assignStructID)
	case reflect.Map:
		_ = assignFieldStructIDs(ns, rt.Key(), vis, assignStructID)
		return assignFieldStructIDs(ns, rt.Elem(), vis, assignStructID)
	default:
		return 0
	}
}

// typeDescAndID returns the TypeDesc and schema ID for a Go type.
// Struct types are looked up in the schema registry; everything else is
// described via spore.DescribeReflectType with schema ID 0.
func typeDescAndID(ns string, rt reflect.Type, structIDs map[typeKey]uint64) (spore.TypeDesc, uint64) {
	if rt == nil {
		return spore.TypeDesc{}, 0
	}
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() == reflect.Struct {
		if isTimeType(rt) {
			return spore.TypeDesc{Kind: spore.TypeKindScalar, Name: "string"}, 0
		}
		key := typeKey{ns: ns, name: rt.Name()}
		return schema.StructRef(rt.Name()), structIDs[key]
	}
	td, err := spore.DescribeReflectType(rt)
	if err != nil {
		return spore.TypeDesc{}, 0
	}
	return td, 0
}

// callableTypes extracts the request, final, and chunk reflect.Types from a
// gospore handler function. The first parameter (Context/PureContext) is
// skipped. For streaming handlers the emitter parameter is also skipped and
// its chunk type is extracted from the Send method signature.
func callableTypes(fn any, isStreaming bool) (reqType reflect.Type, finalType reflect.Type, chunkType reflect.Type, _ bool) {
	if fn == nil {
		return nil, nil, nil, isStreaming
	}
	typ := reflect.TypeOf(fn)
	if typ.Kind() != reflect.Func {
		return nil, nil, nil, isStreaming
	}

	// Skip first parameter (Context/PureContext).
	for i := 1; i < typ.NumIn(); i++ {
		in := typ.In(i)
		if isEmitterType(in) {
			if send := emitterSendMethod(in); send != nil && send.Type.NumIn() == 1 {
				chunkType = send.Type.In(0)
			}
			continue
		}
		if reqType == nil {
			reqType = in
		}
	}

	// Examine returns: last error is stripped; at most one non-error remains.
	for i := 0; i < typ.NumOut(); i++ {
		out := typ.Out(i)
		if out.Implements(errorType) {
			continue
		}
		if finalType == nil {
			finalType = out
		}
	}

	return reqType, finalType, chunkType, isStreaming
}

var errorType = reflect.TypeOf((*error)(nil)).Elem()
var emitterType = reflect.TypeOf((*actor.Emitter)(nil)).Elem()

// isEmitterType reports whether t is actor.Emitter or actor.EmitterT[R].
// Uses duck-typing (Send + Done methods) to survive Go version differences
// in reflect.Type.Name() formatting for instantiated generic interfaces.
func isEmitterType(t reflect.Type) bool {
	if t == emitterType {
		return true
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Interface {
		return false
	}
	hasSend, hasDone := false, false
	for i := 0; i < t.NumMethod(); i++ {
		switch t.Method(i).Name {
		case "Send":
			hasSend = true
		case "Done":
			hasDone = true
		}
	}
	return hasSend && hasDone
}

// emitterSendMethod returns the "Send" method on an emitter interface type.
func emitterSendMethod(t reflect.Type) *reflect.Method {
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		if m.Name == "Send" {
			return &m
		}
	}
	return nil
}

// isTimeType reports whether rt is time.Time (or *time.Time).
// time.Time serializes to an ISO-8601 string on the wire, so it is
// treated as a scalar rather than a struct in manifest export.
func isTimeType(rt reflect.Type) bool {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	return rt.Kind() == reflect.Struct && rt.PkgPath() == "time" && rt.Name() == "Time"
}

func (a *appImpl) ActorType(aid id.ActorID) string {
	a.cellMu.RLock()
	c, ok := a.cells[aid]
	a.cellMu.RUnlock()
	if !ok {
		return ""
	}
	return c.Actor().Type()
}
