// Package handler implements the per-cell callable Table that maps
// callIDs to invokers and their schema descriptions.
//
// The Table sits between Cell (which dispatches Call frames) and
// spore reflection (which produces the schema.CallableDesc + signed
// invoker closure). Cell looks up an invoker by callID and runs it
// against the InvokeEnv built from the inbound Frame.
//
// This file holds the data-shape and the read path (Lookup). The
// reflective Register pipeline lives behind a panic stub until M09
// items 3-6 land — see ARCHITECTURE.md §4.12 for the exact steps.
package handler

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/schema"
	tschema "github.com/qomos-w/spore/schema"
)

type SchemaRegistrar interface {
	RegisterAuto(name string, desc tschema.TypeDesc) (uint64, error)
}

// Table is the per-actor callable index. Cell creates one Table per
// cell instance; Register populates it (M09 items 3-6) and dispatch
// reads it via Lookup. The two backing maps are kept separate because
// CallableDesc is also surfaced via the manifest builder for cross-app
// schema discovery — keeping it independent from the invoker closure
// avoids dragging the runtime into manifest emission.
//
// Table is safe for concurrent Lookup/Desc/CallIDs with Register/
// RegisterScript/Put. The RWMutex overhead is negligible on the hot
// path because uncontended RLock is a few nanoseconds.
type Table struct {
	mu        sync.RWMutex
	invokers  map[string]*Invoker
	descs     map[string]tschema.CallableDesc
	schemaReg SchemaRegistrar
	root      bool
	strict    bool
}

// NewTable returns an empty Table ready to receive Register calls.
// The two backing maps are eagerly allocated so Lookup is allocation-
// free even on a fresh table.
func NewTable() *Table {
	return newTable(false, nil)
}

func NewTableWithSchema(reg SchemaRegistrar) *Table {
	return newTable(false, reg)
}

// NewStrictTable returns a Table that rejects callable signatures not
// following the single-struct wire rule: after the injected Context,
// a callable must accept exactly zero or one struct parameter (plus an
// optional Emitter for streaming), and any non-error return must be
// exactly one struct.
func NewStrictTable() *Table {
	return newTable(true, nil)
}

// NewStrictTableWithSchema returns a strict Table backed by a schema
// registrar.
func NewStrictTableWithSchema(reg SchemaRegistrar) *Table {
	return newTable(true, reg)
}

// NewRootTable returns a Table for the root actor (App). Root tables
// can register callables in the "app" reserved namespace; non-root
// tables cannot.
func NewRootTable() *Table {
	return newRootTable(false, nil)
}

func NewRootTableWithSchema(reg SchemaRegistrar) *Table {
	return newRootTable(false, reg)
}

// NewStrictRootTable returns a strict root Table.
func NewStrictRootTable() *Table {
	return newRootTable(true, nil)
}

// NewStrictRootTableWithSchema returns a strict root Table backed by a
// schema registrar.
func NewStrictRootTableWithSchema(reg SchemaRegistrar) *Table {
	return newRootTable(true, reg)
}

func newTable(strict bool, reg SchemaRegistrar) *Table {
	return &Table{
		invokers:  map[string]*Invoker{},
		descs:     map[string]tschema.CallableDesc{},
		schemaReg: reg,
		strict:    strict,
	}
}

func newRootTable(strict bool, reg SchemaRegistrar) *Table {
	return &Table{
		invokers:  map[string]*Invoker{},
		descs:     map[string]tschema.CallableDesc{},
		schemaReg: reg,
		root:      true,
		strict:    strict,
	}
}

// Lookup returns the invoker registered under callID and a found bool.
// The returned *Invoker is the live entry — Cell must not mutate it.
// Returns (nil, false) when callID is unknown.
func (t *Table) Lookup(callID string) (*Invoker, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.RLock()
	inv, ok := t.invokers[callID]
	t.mu.RUnlock()
	return inv, ok
}

// Desc returns the schema.CallableDesc registered under callID and a
// found bool. Surfaced separately from Lookup because the manifest
// builder consumes descs without needing the invoker closure.
func (t *Table) Desc(callID string) (tschema.CallableDesc, bool) {
	if t == nil {
		return tschema.CallableDesc{}, false
	}
	t.mu.RLock()
	d, ok := t.descs[callID]
	t.mu.RUnlock()
	return d, ok
}

// Visibility returns the visibility registered under callID. Returns
// (VisibilityInternal, false) when callID is unknown.
func (t *Table) Visibility(callID string) (actor.Visibility, bool) {
	if t == nil {
		return actor.VisibilityInternal, false
	}
	t.mu.RLock()
	inv, ok := t.invokers[callID]
	t.mu.RUnlock()
	if !ok {
		return actor.VisibilityInternal, false
	}
	return inv.Visibility, true
}

// CallIDs returns the set of registered callIDs as a snapshot. The
// order is unspecified; callers that need a deterministic listing
// should sort. Useful for manifest emission and diagnostics.
func (t *Table) CallIDs() []string {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	out := make([]string, 0, len(t.invokers))
	for k := range t.invokers {
		out = append(out, k)
	}
	t.mu.RUnlock()
	return out
}

// CrossAppCallable describes one callable registered with CrossApp=true.
type CrossAppCallable struct {
	CallID          string
	Desc            tschema.CallableDesc
	RequestSchemaID uint64
	ParamSchemaIDs  []uint64
	ReturnSchemaID  uint64
	ReturnSchemaIDs []uint64
	ChunkSchemaID   uint64
}

// CrossAppCallables returns a snapshot of every callable marked for
// cross-app exposure. The order is unspecified.
func (t *Table) CrossAppCallables() []CrossAppCallable {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]CrossAppCallable, 0)
	for callID, inv := range t.invokers {
		if !inv.CrossApp {
			continue
		}
		out = append(out, CrossAppCallable{
			CallID:          callID,
			Desc:            inv.Desc,
			RequestSchemaID: inv.RequestSchemaID,
			ParamSchemaIDs:  inv.ParamSchemaIDs,
			ReturnSchemaID:  inv.ReturnSchemaID,
			ReturnSchemaIDs: inv.ReturnSchemaIDs,
			ChunkSchemaID:   inv.ChunkSchemaID,
		})
	}
	return out
}

// Register binds callID to a handler implementation following the
// ARCHITECTURE.md §4.12 pipeline:
//
//  1. Validate callID format (actor.ValidCallID) and namespace
//     (actor.IsReservedNamespace).
//  2. Classify handler mode (ClassifyHandlerMode).
//  3. Describe the handler's wire-visible shape (DescribeHandler).
//  4. Consistency check against existing entry, if any (DescConsistent,
//     ModeConsistent).
//  5. Build the Invoker.Run closure via reflect.
//  6. Store the Invoker and CallableDesc.
//
// Register binds callID to a handler implementation following the
// ARCHITECTURE.md §4.12 pipeline. Thread-safe via write lock.
// The resulting invoker carries an empty ServiceName; the cell layer
// uses RegisterWithService to inject the domain-derived route service.
func (t *Table) Register(callID string, fn any, opts ...actor.RegisterOption) error {
	return t.register(callID, fn, "", opts...)
}

// RegisterWithService is Register with an explicit route service name.
// This is the cell-facing entry point: ServiceName derives solely from
// RegisterDomain domains (actor.ServiceNameFor), so gospore no longer
// accepts a service name as a RegisterOption.
func (t *Table) RegisterWithService(callID string, fn any, service string, opts ...actor.RegisterOption) error {
	return t.register(callID, fn, service, opts...)
}

func (t *Table) register(callID string, fn any, service string, opts ...actor.RegisterOption) error {
	if err := t.validateCallID(callID); err != nil {
		return err
	}

	mode, err := ClassifyHandlerMode(fn)
	if err != nil {
		return err
	}
	if explicitMode, ok := actor.ResolveMode(opts...); ok {
		if err := ModeConsistent(mode, explicitMode); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableModeMismatch, err)
		}
		mode = explicitMode
	}

	desc, err := DescribeHandler(callID, fn)
	if err != nil {
		return err
	}
	if t.strict {
		if err := validateStrictSignature(callID, desc); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableSignatureMismatch, err)
		}
	}
	chunkTy, _ := actor.ResolveStreamingChunkType(opts...)
	if err := validateTopLevelCallableTypes(callID, fn, desc, chunkTy); err != nil {
		return err
	}
	vis := actor.ResolveVisibility(opts...)
	crossApp := actor.ResolveCrossApp(opts...)
	loop := actor.ResolveLoopOrDefault(mode, opts...)
	regDesc := actor.ResolveDescription(opts...)
	paramDescs := actor.ResolveParamDescs(opts...)
	finalDesc := actor.ResolveFinalDesc(opts...)
	chunkDesc := actor.ResolveChunkDesc(opts...)
	effect := actor.ResolveEffect(opts...)
	serviceName := service
	toolName := actor.ResolveToolName(opts...)
	if toolName != "" {
		if err := actor.ValidateToolName(toolName); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableToolNameInvalid, err)
		}
	}
	requestSchemaID, returnSchemaID, chunkSchemaID, paramSchemaIDs, returnSchemaIDs, err := t.allocateSchemaIDs(callID, fn, desc, chunkTy)
	if err != nil {
		return err
	}

	// Build the chunk TypeDesc for streaming emitters so binary codec can
	// encode chunks with the correct schema rather than falling back to "any".
	var chunkTypeDesc tschema.TypeDesc
	if chunkTy != nil {
		chunkTypeDesc, _ = tschema.DescribeReflectType(chunkTy)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Re-registration consistency: if callID already exists, the incoming
	// descriptor, mode, and visibility must match exactly. Idempotent
	// re-register with identical shape is a no-op.
	if existing, ok := t.invokers[callID]; ok {
		if err := DescConsistent(existing.Desc, desc); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableSignatureMismatch, err)
		}
		if err := ModeConsistent(existing.Mode, mode); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableModeMismatch, err)
		}
		if err := VisibilityConsistent(existing.Visibility, vis); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableVisibilityMismatch, err)
		}
		if err := LoopConsistent(existing.Loop, loop); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableLoopMismatch, err)
		}
		return nil
	}

	run := buildRunClosure(fn, mode, desc, returnSchemaID, chunkSchemaID, chunkTypeDesc)
	t.invokers[callID] = &Invoker{
		Mode:            mode,
		Loop:            loop,
		CallID:          callID,
		Desc:            desc,
		Visibility:      vis,
		Description:     regDesc,
		ParamDescs:      paramDescs,
		FinalDesc:       finalDesc,
		ChunkDesc:       chunkDesc,
		Effect:          effect,
		ServiceName:     serviceName,
		ToolName:        toolName,
		RequestSchemaID: requestSchemaID,
		ParamSchemaIDs:  paramSchemaIDs,
		ReturnSchemaID:  returnSchemaID,
		ReturnSchemaIDs: returnSchemaIDs,
		ChunkSchemaID:   chunkSchemaID,
		ChunkType:       chunkTy,
		ChunkTypeDesc:   chunkTypeDesc,
		CrossApp:        crossApp,
		Fn:              fn,
		Run:             run,
	}
	t.descs[callID] = desc

	return nil
}

// RegisterScript binds callID to a script-backed handler. Scripts do not
// carry a Go function, so the spore-reflective steps (ClassifyHandlerMode,
// DescribeHandler, buildRunClosure) are skipped. The shared validation
// from Register — reserved-namespace gate + callID format — still runs;
// re-registration is mode-consistent only because script signatures are
// opaque to gospore. The resulting Invoker carries Script != nil and an
// empty Desc; Cell.dispatchCall routes it to the script runtime instead
// of the direct Run closure.
//
// Thread-safe via write lock.
func (t *Table) RegisterScript(callID string, source string, mode actor.HandlerMode, opts ...actor.RegisterOption) error {
	return t.registerScript(callID, source, mode, "", opts...)
}

// RegisterScriptWithService is RegisterScript with an explicit route
// service name, mirroring RegisterWithService for script-backed handlers.
func (t *Table) RegisterScriptWithService(callID string, source string, mode actor.HandlerMode, service string, opts ...actor.RegisterOption) error {
	return t.registerScript(callID, source, mode, service, opts...)
}

func (t *Table) registerScript(callID string, source string, mode actor.HandlerMode, service string, opts ...actor.RegisterOption) error {
	if err := t.validateCallID(callID); err != nil {
		return err
	}

	vis := actor.ResolveVisibility(opts...)
	loop := actor.ResolveLoopOrDefault(mode, opts...)
	serviceName := service

	t.mu.Lock()
	defer t.mu.Unlock()

	if existing, ok := t.invokers[callID]; ok {
		if err := ModeConsistent(existing.Mode, mode); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableModeMismatch, err)
		}
		if err := VisibilityConsistent(existing.Visibility, vis); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableVisibilityMismatch, err)
		}
		if err := LoopConsistent(existing.Loop, loop); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagCallableLoopMismatch, err)
		}
		// Re-register with identical mode and visibility swaps the script
		// body in place rather than allocating a fresh Invoker so any
		// external pointer keeps observing the live entry.
		existing.Script = &ScriptHandle{Source: source}
		if existing.ServiceName == "" && serviceName != "" {
			existing.ServiceName = serviceName
		}
		return nil
	}

	t.invokers[callID] = &Invoker{
		Mode:        mode,
		Loop:        loop,
		CallID:      callID,
		Visibility:  vis,
		ServiceName: serviceName,
		Script:      &ScriptHandle{Source: source},
	}
	return nil
}

// StampServiceForDomain sets ServiceName to domain for every registered
// invoker whose callID first segment equals domain AND whose ServiceName
// is currently empty. Idempotent: already-stamped invokers are skipped.
// Returns the count stamped.
func (t *Table) StampServiceForDomain(domain string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for callID, inv := range t.invokers {
		if inv.ServiceName != "" {
			continue
		}
		seg := callID
		if i := strings.IndexByte(callID, '.'); i > 0 {
			seg = callID[:i]
		} else {
			continue
		}
		if seg == domain {
			inv.ServiceName = domain
			n++
		}
	}
	return n
}

func (t *Table) allocateSchemaIDs(callID string, fn any, desc tschema.CallableDesc, chunkTy reflect.Type) (requestSchemaID, returnSchemaID, chunkSchemaID uint64, paramSchemaIDs, returnSchemaIDs []uint64, err error) {
	if t == nil || t.schemaReg == nil {
		return 0, 0, 0, nil, nil, nil
	}
	callableNS := actor.CallID(callID).Namespace()
	allocateTopLevelSchemaID := func(desc tschema.TypeDesc) (uint64, error) {
		if id, ok := schema.BuiltinIDFor(desc); ok {
			return id, nil
		}
		name := schemaNameForDesc(desc, callableNS)
		if name == "" {
			return 0, nil
		}
		return t.schemaReg.RegisterAuto(name, desc)
	}
	if len(desc.Parameters) > 0 {
		id, err := allocateTopLevelSchemaID(desc.Parameters[0].Type)
		if err != nil {
			return 0, 0, 0, nil, nil, err
		}
		requestSchemaID = id
	}
	paramSchemaIDs = make([]uint64, 0, len(desc.Parameters))
	for _, param := range desc.Parameters {
		id, err := allocateTopLevelSchemaID(param.Type)
		if err != nil {
			return 0, 0, 0, nil, nil, err
		}
		paramSchemaIDs = append(paramSchemaIDs, id)
	}
	if len(desc.Returns) > 0 {
		id, err := allocateTopLevelSchemaID(desc.Returns[0])
		if err != nil {
			return 0, 0, 0, nil, nil, err
		}
		returnSchemaID = id
	}
	returnSchemaIDs = make([]uint64, 0, len(desc.Returns))
	for _, ret := range desc.Returns {
		id, err := allocateTopLevelSchemaID(ret)
		if err != nil {
			return 0, 0, 0, nil, nil, err
		}
		returnSchemaIDs = append(returnSchemaIDs, id)
	}
	chunkDesc, ok := streamingChunkDesc(fn)
	if !ok {
		return requestSchemaID, returnSchemaID, 0, paramSchemaIDs, returnSchemaIDs, nil
	}
	// If signature-based detection returned "any" (untyped Emitter) but
	// Streaming[T]() declared a concrete type, use the concrete type.
	if chunkDesc.Kind == tschema.TypeKindScalar && chunkDesc.Name == "any" && chunkTy != nil {
		chunkDesc, err = tschema.DescribeReflectType(chunkTy)
		if err != nil {
			return 0, 0, 0, nil, nil, err
		}
	}
	chunkSchemaID, err = allocateTopLevelSchemaID(chunkDesc)
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}
	return requestSchemaID, returnSchemaID, chunkSchemaID, paramSchemaIDs, returnSchemaIDs, nil
}

func streamingChunkDesc(fn any) (tschema.TypeDesc, bool) {
	if fn == nil {
		return tschema.TypeDesc{}, false
	}
	typ := reflect.TypeOf(fn)
	if typ.Kind() != reflect.Func {
		return tschema.TypeDesc{}, false
	}
	for i := 1; i < typ.NumIn(); i++ {
		in := typ.In(i)
		if !isEmitterType(in) {
			continue
		}
		send := emitterSendMethod(in)
		if send == nil || send.Type.NumIn() != 1 {
			return tschema.TypeDesc{}, false
		}
		desc, err := tschema.DescribeReflectType(send.Type.In(0))
		if err != nil {
			return tschema.TypeDesc{}, false
		}
		return desc, true
	}
	return tschema.TypeDesc{}, false
}

func schemaNameForDesc(desc tschema.TypeDesc, fallback string) string {
	switch desc.Kind {
	case tschema.TypeKindStruct:
		if desc.ClassName != "" {
			return desc.ClassName
		}
		if desc.Name != "" && desc.Name != "struct" {
			return desc.Name
		}
		return fallback
	default:
		return ""
	}
}

func validateTopLevelCallableTypes(callID string, fn any, desc tschema.CallableDesc, chunkTy reflect.Type) error {
	for _, param := range desc.Parameters {
		if err := validateTopLevelType(param.Type); err != nil {
			return fmt.Errorf("%s: %s param %q: %w", actor.DiagCallableSignatureMismatch, callID, param.Name, err)
		}
		warnTopLevelAny(callID, "param "+param.Name, param.Type)
	}
	for i, ret := range desc.Returns {
		if err := validateTopLevelType(ret); err != nil {
			return fmt.Errorf("%s: %s return %d: %w", actor.DiagCallableSignatureMismatch, callID, i, err)
		}
		warnTopLevelAny(callID, fmt.Sprintf("return %d", i), ret)
	}
	if chunk, ok := streamingChunkDesc(fn); ok {
		// If signature-based detection returned "any" but Streaming[T]() declared a
		// concrete type, validate the concrete type instead.
		if chunk.Kind == tschema.TypeKindScalar && chunk.Name == "any" && chunkTy != nil {
			var err error
			chunk, err = tschema.DescribeReflectType(chunkTy)
			if err != nil {
				return fmt.Errorf("%s: %s stream chunk: %w", actor.DiagCallableSignatureMismatch, callID, err)
			}
		}
		if err := validateTopLevelType(chunk); err != nil {
			return fmt.Errorf("%s: %s stream chunk: %w", actor.DiagCallableSignatureMismatch, callID, err)
		}
		warnTopLevelAny(callID, "stream chunk", chunk)
	}
	return nil
}

func validateTopLevelType(desc tschema.TypeDesc) error {
	switch desc.Kind {
	case tschema.TypeKindVoid, tschema.TypeKindScalar, tschema.TypeKindStruct:
		return nil
	case tschema.TypeKindArray:
		if desc.Element == nil {
			return fmt.Errorf("array missing element type")
		}
		if desc.Element.Kind != tschema.TypeKindScalar {
			return fmt.Errorf("top-level arrays must contain scalar elements")
		}
		return nil
	case tschema.TypeKindMap:
		if desc.Key == nil || desc.Value == nil {
			return fmt.Errorf("map missing key or value type")
		}
		if desc.Key.Kind != tschema.TypeKindScalar || desc.Value.Kind != tschema.TypeKindScalar {
			return fmt.Errorf("top-level maps must use scalar keys and scalar values")
		}
		return nil
	default:
		return fmt.Errorf("unsupported top-level type kind %q", desc.Kind)
	}
}

func warnTopLevelAny(callID, position string, desc tschema.TypeDesc) {
	if desc.Kind == tschema.TypeKindScalar && desc.Name == "any" {
		log.Printf("gospore: warning: callable %s uses top-level any in %s", callID, position)
	}
}

// validateStrictSignature enforces the sporecode single-struct wire rule:
// after the injected Context, a callable must accept exactly zero or one
// struct parameter (plus an optional Emitter for streaming). The return
// side is intentionally not restricted here: gospore already limits
// non-error returns to one value, and scalar returns are unambiguous on
// the wire. The goal of this rule is to eliminate multi-parameter
// ambiguity, not to force boilerplate wrappers on every getter.
//
// All struct types (params and returns) must be named: anonymous structs
// have no stable ClassName so the manifest exporter cannot register them
// in the schema set, producing orphan schema IDs that break binary codec
// lookup at the wire layer.
func validateStrictSignature(callID string, desc tschema.CallableDesc) error {
	// Streaming descriptors have a Streaming marker and no Returns.
	if desc.Mode == tschema.CallableModeStreaming {
		if len(desc.Parameters) != 1 {
			return fmt.Errorf("callable %s: streaming handler must accept exactly one struct parameter (got %d)", callID, len(desc.Parameters))
		}
		if desc.Parameters[0].Type.Kind != tschema.TypeKindStruct {
			return fmt.Errorf("callable %s: streaming request parameter must be a struct, got %s", callID, desc.Parameters[0].Type.Kind)
		}
		if err := requireNamedStruct(callID, "streaming request parameter", desc.Parameters[0].Type); err != nil {
			return err
		}
		return nil
	}

	switch len(desc.Parameters) {
	case 0:
		// No request parameter is allowed for tell-style or getter callables.
	case 1:
		if desc.Parameters[0].Type.Kind != tschema.TypeKindStruct {
			return fmt.Errorf("callable %s: request parameter must be a struct, got %s", callID, desc.Parameters[0].Type.Kind)
		}
		if err := requireNamedStruct(callID, "request parameter", desc.Parameters[0].Type); err != nil {
			return err
		}
	default:
		return fmt.Errorf("callable %s: expected exactly one struct parameter, got %d", callID, len(desc.Parameters))
	}
	for i, ret := range desc.Returns {
		if err := requireNamedStruct(callID, fmt.Sprintf("return %d", i), ret); err != nil {
			return err
		}
	}
	return nil
}

// requireNamedStruct rejects struct TypeDescs whose ClassName is empty.
// Non-struct types pass through unchanged.
func requireNamedStruct(callID, position string, desc tschema.TypeDesc) error {
	if desc.Kind != tschema.TypeKindStruct {
		return nil
	}
	if desc.ClassName == "" {
		return fmt.Errorf("%s: callable %s: %s is an anonymous struct; declare a named struct type so the schema ID is stable",
			actor.DiagCallableAnonymousStruct, callID, position)
	}
	return nil
}

// validateCallID applies the reserved-namespace gate and the callID
// format check shared by Register and RegisterScript. The gate fires
// before format validation per IsReservedNamespace's documented order.
func (t *Table) validateCallID(callID string) error {
	reservedPrefix, isReserved := actor.IsReservedNamespace(callID)
	if isReserved {
		// Root tables may register in the "app" namespace.
		// All tables may register in the "gospore" namespace (system protocol handlers).
		if !(t.root && reservedPrefix == actor.ReservedNamespaceApp) && reservedPrefix != actor.ReservedNamespaceGospore {
			return fmt.Errorf("%s: %q uses reserved namespace %q",
				actor.DiagCallableReservedNamespace, callID, reservedPrefix)
		}
	}
	// Skip format validation for gospore system protocol callIDs.
	if !isReserved || reservedPrefix != actor.ReservedNamespaceGospore {
		if !actor.ValidCallID(callID) {
			return fmt.Errorf("%s: %q", actor.DiagCallableInvalidID, callID)
		}
	}
	return nil
}

// Put inserts a pre-built Invoker under callID. Used internally by Register
// and available for test wiring when the reflective pipeline is not needed.
func (t *Table) Put(callID string, inv *Invoker) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.invokers[callID] = inv
	if inv != nil {
		t.descs[callID] = inv.Desc
	}
	t.mu.Unlock()
}
