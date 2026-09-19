package cell

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	gschema "github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/plan"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/spore/identity"
	"github.com/qomos-w/spore/schema"
	"github.com/qomos-w/spore/script"
)

// nativeCellContext extracts the Cell fields needed by scriptRuntimeOwner
// to bind native capabilities. Implemented by Cell.
type nativeCellContext interface {
	nativeSelf() ref.Ref
	nativeParent() ref.Ref
	nativeNamespace() string
	nativeDone() <-chan struct{}
	nativeClock() actor.Clock
	nativeSpawnFn() func(ref.Ref, actor.Props, string) (ref.Ref, error)
	nativeStopFn() func(ref.Ref) error
	nativeDeliverFn() func(ref.Ref, mailbox.Envelope) error
	nativeLookupIDFn() func(id.ActorID) (ref.Ref, bool)
	nativeLookupServiceFn() func(string) (ref.Ref, bool)
	nativeChildrenFn() func() []ref.Ref
	nativeRoot() ref.Ref
	nativeInvokeTable() *invoke.PendingTable
	nativeCorIDGen() *id.CorIDGenerator
	nativePolicyStore() actor.PolicyStore
	nativeProjectionsFn() *projection.StoreImpl
	nativeHandlersFn() *handler.Table
}

// StreamChunk is the universal bridge contract for consuming an invoke stream
// from Spore scripts. Data carries the payload (any type), Ok signals "more
// data", and Err carries an error message when the stream fails.
type StreamChunk struct {
	Data any    `json:"data"`
	Ok   bool   `json:"ok"`
	Err  string `json:"err"`
}

// ActorRef is the host-backed interface for a remote actor reference.
// Scripts obtain an ActorRef via ctx.lookup_id(id) or ctx.lookup_service(name) and call methods on it.
type ActorRef interface {
	Invoke(callID string, payload any) any
	InvokeStream(callID string, payload any) Stream
}

// Stream is the host-backed interface for consuming a streaming call.
type Stream interface {
	Recv() StreamChunk
	Close() bool
}

// ScriptContext is the host-backed interface exposed to Spore scripts.
// Each handler invocation receives a ScriptContext as its first argument.
type ScriptContext interface {
	Self() string
	Parent() string
	Namespace() string
	Children() []string
	Now() string
	LookupID(id string) ActorRef
	LookupService(name string) ActorRef
	Spawn(name string, mode int) string
	Stop(id string) bool
	Watch(id string) bool
	Unwatch(id string) bool
	After(ms int, callID string, payload any) bool
	Root() string
	Done() bool
	CheckPolicy(role string, scope string) actor.PolicyCheckResult
	PolicyVersion() uint64
	Plan(target string, callID string, payload any, timeoutMs int) string
	Call(target string, callID string, payload any, timeoutMs int) any
	Stream(target string, callID string, payload any, timeoutMs int) Stream
}

// scriptContextAdapter implements ScriptContext by delegating to the owner.
type scriptContextAdapter struct {
	owner *scriptRuntimeOwner
}

// actorRefAdapter implements ActorRef for a specific target actor.
type actorRefAdapter struct {
	owner     *scriptRuntimeOwner
	targetRef ref.Ref
}

// streamAdapter implements Stream for a specific invoke.Stream.
type streamAdapter struct {
	stream invoke.Stream
	owner  *scriptRuntimeOwner
}

// scriptRuntimeOwner is the cell-tier wrapper around a Spore script.Runtime.
// Each Cell gets its own independent Runtime (per-Cell isolation). Native
// actor capabilities are bound as a single host-backed interface object in the
// "gospore" namespace, exposed to scripts as ScriptContext.
type scriptRuntimeOwner struct {
	spore       *script.Runtime
	codec         codec.Codec
	sporeYields map[string]int // callID -> yield count
	adapter       *scriptContextAdapter
	// Cell context for native capability bindings
	self           ref.Ref
	parent         ref.Ref
	namespace      string
	done           <-chan struct{}
	clock          actor.Clock
	spawn          func(ref.Ref, actor.Props, string) (ref.Ref, error)
	stopFn         func(ref.Ref) error
	deliver        func(ref.Ref, mailbox.Envelope) error
	lookupID       func(id.ActorID) (ref.Ref, bool)
	lookupService  func(string) (ref.Ref, bool)
	children       func() []ref.Ref
	root           ref.Ref
	invokeTable    *invoke.PendingTable
	corIDGen       *id.CorIDGenerator
	policyStore    actor.PolicyStore
	projections    *projection.StoreImpl
	handlers       *handler.Table
	componentSlots []projection.ComponentSlot
}

func newScriptRuntimeOwner(c codec.Codec, cx nativeCellContext) *scriptRuntimeOwner {
	o := &scriptRuntimeOwner{
		codec: c,
	}
	if cx != nil {
		o.self = cx.nativeSelf()
		o.parent = cx.nativeParent()
		o.namespace = cx.nativeNamespace()
		o.done = cx.nativeDone()
		o.clock = cx.nativeClock()
		o.spawn = cx.nativeSpawnFn()
		o.stopFn = cx.nativeStopFn()
		o.deliver = cx.nativeDeliverFn()
		o.lookupID = cx.nativeLookupIDFn()
		o.lookupService = cx.nativeLookupServiceFn()
		o.children = cx.nativeChildrenFn()
		o.root = cx.nativeRoot()
		o.invokeTable = cx.nativeInvokeTable()
		o.corIDGen = cx.nativeCorIDGen()
		o.policyStore = cx.nativePolicyStore()
		o.projections = cx.nativeProjectionsFn()
		o.handlers = cx.nativeHandlersFn()
	}
	return o
}

// ensureRuntime lazily creates a Spore Runtime for this Cell.
// Called before loadSporeModule and invoke.
func (o *scriptRuntimeOwner) ensureRuntime() error {
	if o.spore != nil {
		return nil
	}
	rt, err := script.NewRuntime()
	if err != nil {
		return fmt.Errorf("script runtime init: %w", err)
	}
	o.spore = rt
	o.sporeYields = map[string]int{}
	return nil
}

// setSporeRuntime injects a pre-created Runtime (test-only hook).
// When non-nil, ensureRuntime is a no-op.
func (o *scriptRuntimeOwner) setSporeRuntime(rt *script.Runtime) {
	o.spore = rt
	if o.sporeYields == nil {
		o.sporeYields = map[string]int{}
	}
}

// bindActorComponents registers each `gospore:"component"`-tagged struct
// field with the Spore runtime so script code can import the type.
// Non-struct slots (e.g. scalar component fields) are silently skipped —
// they participate in projection via SnapshotOfSlots but do not need a
// Spore struct binding.
func (o *scriptRuntimeOwner) bindActorComponents(slots []projection.ComponentSlot) error {
	if o == nil || len(slots) == 0 {
		return nil
	}
	o.componentSlots = slots
	if err := o.ensureRuntime(); err != nil {
		return err
	}
	return o.bindComponentSlotsLocked()
}

func (o *scriptRuntimeOwner) bindComponentSlotsLocked() error {
	if o.spore == nil || len(o.componentSlots) == 0 {
		return nil
	}
	ns := o.namespace
	if ns == "" {
		ns = "actor"
	}
	for _, slot := range o.componentSlots {
		ft := slot.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if ft.Kind() != reflect.Struct {
			continue // BindStruct only accepts struct types
		}
		zero := reflect.New(ft).Elem().Interface()
		if err := o.spore.BindStruct(ns, slot.Name, zero); err != nil {
			return fmt.Errorf("bind component %s: %w", slot.Name, err)
		}
	}
	return nil
}

func (o *scriptRuntimeOwner) loadSporeModule(def *actor.Definition) error {
	if o == nil || def == nil {
		return nil
	}
	// Create a fresh Runtime on each reload so native capability bindings
	// reflect the current Cell context. Spore Reset() preserves old
	// pending bindings and re-commits them, sealing the binding surface.
	rt, err := script.NewRuntime()
	if err != nil {
		return fmt.Errorf("script runtime init: %w", err)
	}
	o.spore = rt
	o.sporeYields = map[string]int{}
	var parts []string
	for _, h := range def.Handlers {
		parts = append(parts, h.Source)
		o.sporeYields[h.CallID] = strings.Count(h.Source, "yield ")
	}
	src := strings.Join(parts, "\n")
	if err := o.bindNativeCapabilities(); err != nil {
		return fmt.Errorf("script native capabilities: %w", err)
	}
	if err := o.bindComponentSlotsLocked(); err != nil {
		return fmt.Errorf("script component bindings: %w", err)
	}
	if err := o.spore.LoadSource("gospore_cell", src); err != nil {
		return fmt.Errorf("script load: %w", err)
	}
	o.adapter = &scriptContextAdapter{owner: o}
	return nil
}

func (o *scriptRuntimeOwner) bindNativeCapabilities() error {
	if o.spore == nil {
		return nil
	}
	if err := o.spore.BindStruct("gospore", "StreamChunk", StreamChunk{}); err != nil {
		return fmt.Errorf("bind StreamChunk: %w", err)
	}
	if err := o.spore.BindStruct("gospore", "PolicyCheckResult", actor.PolicyCheckResult{}); err != nil {
		return fmt.Errorf("bind PolicyCheckResult: %w", err)
	}
	actorRefDesc := schema.InterfaceDesc{
		Methods: []schema.MethodDesc{
			{
				Name: "invoke",
				Parameters: []schema.ParameterDesc{
					{Name: "call_id", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "payload", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "any"}},
			},
			{
				Name: "invoke_stream",
				Parameters: []schema.ParameterDesc{
					{Name: "call_id", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "payload", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindClass, ClassName: "Stream"}},
			},
		},
	}
	if err := o.spore.BindInterfaceObject("gospore", "ActorRef", actorRefDesc, &actorRefAdapter{}); err != nil {
		return fmt.Errorf("bind ActorRef: %w", err)
	}
	streamDesc := schema.InterfaceDesc{
		Methods: []schema.MethodDesc{
			{
				Name:    "recv",
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindStruct, Name: "StreamChunk"}},
			},
			{
				Name:    "close",
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "bool"}},
			},
		},
	}
	if err := o.spore.BindInterfaceObject("gospore", "Stream", streamDesc, &streamAdapter{}); err != nil {
		return fmt.Errorf("bind Stream: %w", err)
	}
	scriptCtxDesc := schema.InterfaceDesc{
		Methods: []schema.MethodDesc{
			{Name: "self", Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}}},
			{Name: "parent", Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}}},
			{Name: "namespace", Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}}},
			{Name: "children", Returns: []schema.TypeDesc{{Kind: schema.TypeKindArray, Element: &schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}}},
			{Name: "now", Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}}},
			{
				Name:       "lookup_id",
				Parameters: []schema.ParameterDesc{{Name: "id", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}},
				Returns:    []schema.TypeDesc{{Kind: schema.TypeKindClass, ClassName: "ActorRef"}},
			},
			{
				Name:       "lookup_service",
				Parameters: []schema.ParameterDesc{{Name: "name", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}},
				Returns:    []schema.TypeDesc{{Kind: schema.TypeKindClass, ClassName: "ActorRef"}},
			},
			{
				Name:       "spawn",
				Parameters: []schema.ParameterDesc{{Name: "name", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}, {Name: "mode", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "int"}}},
				Returns:    []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}},
			},
			{
				Name:       "stop",
				Parameters: []schema.ParameterDesc{{Name: "path", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}},
				Returns:    []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "bool"}},
			},
			{
				Name:       "watch",
				Parameters: []schema.ParameterDesc{{Name: "path", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}},
				Returns:    []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "bool"}},
			},
			{
				Name:       "unwatch",
				Parameters: []schema.ParameterDesc{{Name: "path", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}},
				Returns:    []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "bool"}},
			},
			{
				Name: "after",
				Parameters: []schema.ParameterDesc{
					{Name: "ms", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "int"}},
					{Name: "call_id", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "payload", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "bool"}},
			},
			{Name: "root", Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}}},
			{Name: "done", Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "bool"}}},
			{
				Name: "check_policy",
				Parameters: []schema.ParameterDesc{
					{Name: "role", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "scope", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindStruct, Name: "PolicyCheckResult"}},
			},
			{Name: "policy_version", Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "int"}}},
			{
				Name: "plan",
				Parameters: []schema.ParameterDesc{
					{Name: "target", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "call_id", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "payload", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}},
					{Name: "timeout_ms", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "int"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}},
			},
			{
				Name: "call",
				Parameters: []schema.ParameterDesc{
					{Name: "target", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "call_id", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "payload", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}},
					{Name: "timeout_ms", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "int"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "any"}},
			},
			{
				Name: "stream",
				Parameters: []schema.ParameterDesc{
					{Name: "target", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "call_id", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "payload", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}},
					{Name: "timeout_ms", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "int"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindClass, ClassName: "Stream"}},
			},
		},
	}
	if o.adapter == nil {
		o.adapter = &scriptContextAdapter{owner: o}
	} else {
		o.adapter.owner = o
	}
	if err := o.spore.BindInterfaceObject("gospore", "Context", scriptCtxDesc, o.adapter); err != nil {
		return fmt.Errorf("bind Context: %w", err)
	}
	return nil
}

func (o *scriptRuntimeOwner) invoke(inv *handler.Invoker, env handler.InvokeEnv) error {
	if inv == nil || inv.Script == nil {
		return fmt.Errorf("script invoke: nil script invoker")
	}
	if err := o.ensureRuntime(); err != nil {
		return err
	}
	if o.spore == nil {
		return fmt.Errorf("script runtime missing")
	}
	callable := sporeCallableName(inv.CallID)
	var payload any
	if env.Payload != nil {
		switch p := env.Payload.(type) {
		case []byte:
			if len(p) > 0 {
				payload = p
			}
		default:
			payload = env.Payload
		}
	} else if len(env.Frame.Body) > 0 {
		payload = env.Frame.Body
	}
	callArgs := []any{o.adapter}
	if payload != nil {
		callArgs = append(callArgs, payload)
	}
	yields := o.sporeYields[inv.CallID]
	if yields == 0 {
		result, err := o.spore.Call(callable, callArgs...)
		if err != nil {
			return err
		}
		if err := result.Unwrap(); err != nil {
			return err
		}
		if !result.Void() {
			o.replyValue(env.Reply, inv.CallID, result.Value, inv.ReturnSchemaID)
		}
		env.Reply(message.Frame{Kind: message.KindEnd, CorID: env.Frame.CorID})
		return nil
	}
	for i := 0; i < yields; i++ {
		next, err := o.spore.CallNext(callable, callArgs...)
		if err != nil {
			return err
		}
		if err := next.Unwrap(); err != nil {
			return err
		}
		o.replyValue(env.Reply, inv.CallID, next.Value, inv.ChunkSchemaID)
	}
	final, err := o.spore.CallFinal(callable, callArgs...)
	if err != nil {
		return err
	}
	if err := final.Unwrap(); err != nil {
		return err
	}
	if !final.Void() {
		o.replyValue(env.Reply, inv.CallID, final.Value, inv.ReturnSchemaID)
	}
	env.Reply(message.Frame{Kind: message.KindEnd, CorID: env.Frame.CorID})
	return nil
}

// ---------------------------------------------------------------------------
// ScriptContext implementation
// ---------------------------------------------------------------------------

func (a *scriptContextAdapter) Self() string {
	if a.owner.self == nil {
		return ""
	}
	return a.owner.self.ID().String()
}

func (a *scriptContextAdapter) Parent() string {
	if a.owner.parent == nil {
		return ""
	}
	return a.owner.parent.ID().String()
}

func (a *scriptContextAdapter) Namespace() string {
	return a.owner.namespace
}

func (a *scriptContextAdapter) Children() []string {
	if a.owner.children == nil {
		return nil
	}
	refs := a.owner.children()
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.ID().String()
	}
	return out
}

func (a *scriptContextAdapter) Now() string {
	if a.owner.clock == nil {
		return time.Now().Format(time.RFC3339Nano)
	}
	return a.owner.clock.Now().Format(time.RFC3339Nano)
}


func (a *scriptContextAdapter) LookupID(idStr string) ActorRef {
	if a.owner.lookupID == nil {
		return nil
	}
	cid, err := identity.ParseCanonicalID(idStr)
	if err != nil {
		return nil
	}
	r, ok := a.owner.lookupID(id.From(cid))
	if !ok {
		return nil
	}
	adapter := &actorRefAdapter{owner: a.owner, targetRef: r}
	if a.owner.spore != nil {
		_ = a.owner.spore.RegisterHostInterfaceInstance("gospore", "ActorRef", adapter)
	}
	return adapter
}

func (a *scriptContextAdapter) LookupService(name string) ActorRef {
	if a.owner.lookupService == nil {
		return nil
	}
	r, ok := a.owner.lookupService(name)
	if !ok {
		return nil
	}
	adapter := &actorRefAdapter{owner: a.owner, targetRef: r}
	if a.owner.spore != nil {
		_ = a.owner.spore.RegisterHostInterfaceInstance("gospore", "ActorRef", adapter)
	}
	return adapter
}

func (a *scriptContextAdapter) Spawn(name string, _ int) string {
	if a.owner.spawn == nil {
		return ""
	}
	props := actor.PropsFromFunc(func() actor.Actor { return &actor.Host{} })
	r, err := a.owner.spawn(a.owner.self, props, name)
	if err != nil {
		return ""
	}
	return r.ID().String()
}

func (a *scriptContextAdapter) Stop(idStr string) bool {
	if a.owner.lookupID == nil || a.owner.stopFn == nil {
		return false
	}
	cid, err := identity.ParseCanonicalID(idStr)
	if err != nil {
		return false
	}
	r, ok := a.owner.lookupID(id.From(cid))
	if !ok {
		return false
	}
	return a.owner.stopFn(r) == nil
}

func (a *scriptContextAdapter) Watch(idStr string) bool {
	if a.owner.lookupID == nil || a.owner.deliver == nil {
		return false
	}
	cid, err := identity.ParseCanonicalID(idStr)
	if err != nil {
		return false
	}
	r, ok := a.owner.lookupID(id.From(cid))
	if !ok {
		return false
	}
	return a.owner.deliver(r, mailbox.Envelope{Payload: mailbox.Watch{Watcher: a.owner.self}}) == nil
}

func (a *scriptContextAdapter) Unwatch(idStr string) bool {
	if a.owner.lookupID == nil || a.owner.deliver == nil {
		return false
	}
	cid, err := identity.ParseCanonicalID(idStr)
	if err != nil {
		return false
	}
	r, ok := a.owner.lookupID(id.From(cid))
	if !ok {
		return false
	}
	return a.owner.deliver(r, mailbox.Envelope{Payload: mailbox.Unwatch{Watcher: a.owner.self}}) == nil
}

func (a *scriptContextAdapter) After(ms int, callID string, payload any) bool {
	if a.owner.deliver == nil || a.owner.clock == nil {
		return false
	}
	d := time.Duration(ms) * time.Millisecond
	go func() {
		select {
		case <-a.owner.clock.After(d):
		case <-a.owner.done:
			return
		}
		body, mode, encoding, schemaID, err := a.owner.encodePayload(callID, payload)
		if err != nil {
			return
		}
		_ = a.owner.deliver(a.owner.self, mailbox.Envelope{
			Frame: message.Frame{
				From:        a.owner.self.ID(),
				To:          a.owner.self.ID(),
				Kind:        message.KindCall,
				CallID:      callID,
				SchemaID:    schemaID,
				PayloadMode: mode,
				Encoding:    encoding,
				Body:        body,
			},
			Sender: a.owner.self,
		})
	}()
	return true
}

func (a *scriptContextAdapter) Plan(target string, callID string, payload any, timeoutMs int) string {
	node := a.spawnPlanNode(target, callID, payload, timeoutMs)
	if node == nil {
		return ""
	}
	return node.Ref().ID().String()
}

func (a *scriptContextAdapter) Call(target string, callID string, payload any, timeoutMs int) any {
	node := a.spawnPlanNode(target, callID, payload, timeoutMs)
	if node == nil {
		return nil
	}
	result, err := node.Result()
	if a.owner.stopFn != nil {
		_ = a.owner.stopFn(node.Ref()) // auto-destroy
	}
	if err != nil {
		return nil
	}
	if b, ok := result.([]byte); ok {
		var m any
		if json.Unmarshal(b, &m) == nil {
			return m
		}
		return string(b)
	}
	return result
}

func (a *scriptContextAdapter) Stream(target string, callID string, payload any, timeoutMs int) Stream {
	node := a.spawnPlanNode(target, callID, payload, timeoutMs)
	if node == nil {
		return nil
	}
	sa := &planNodeStreamAdapter{node: node, owner: a.owner}
	if a.owner.spore != nil {
		_ = a.owner.spore.RegisterHostInterfaceInstance("gospore", "Stream", sa)
	}
	return sa
}

// spawnPlanNode is the shared helper for Plan, Call, and Stream.
func (a *scriptContextAdapter) spawnPlanNode(target, callID string, payload any, timeoutMs int) *planNodeActor {
	if a.owner.spawn == nil || a.owner.lookupID == nil {
		return nil
	}
	cid, err := identity.ParseCanonicalID(target)
	if err != nil {
		return nil
	}
	r, ok := a.owner.lookupID(id.From(cid))
	if !ok {
		return nil
	}
	timeout := time.Duration(timeoutMs) * time.Millisecond
	cfg := planNodeConfig{
		target:      r,
		callID:      callID,
		payload:     payload,
		autoStart:   true,
		timeout:     timeout,
		projections: a.owner.projections,
	}
	planActor := newPlanNodeActor(cfg)
	childRef, err := a.owner.spawn(a.owner.self, planActor.Props(), "")
	if err != nil {
		return nil
	}
	planActor.setSelfRef(childRef)
	return planActor
}
func (a *scriptContextAdapter) Root() string {
	if a.owner.root == nil {
		return ""
	}
	return a.owner.root.ID().String()
}

func (a *scriptContextAdapter) Done() bool {
	if a.owner.done == nil {
		return false
	}
	select {
	case <-a.owner.done:
		return true
	default:
		return false
	}
}

func (a *scriptContextAdapter) CheckPolicy(roleStr, scope string) actor.PolicyCheckResult {
	if a.owner.policyStore == nil {
		return actor.PolicyCheckResult{Allow: false, Found: false}
	}
	allow, found := a.owner.policyStore.Evaluate(id.Role(roleStr), scope)
	return actor.PolicyCheckResult{Allow: allow, Found: found}
}

func (a *scriptContextAdapter) PolicyVersion() uint64 {
	if a.owner.policyStore == nil {
		return 0
	}
	return a.owner.policyStore.Version()
}

// ---------------------------------------------------------------------------
// ActorRef implementation
// ---------------------------------------------------------------------------

func (a *actorRefAdapter) Invoke(callID string, payload any) any {
	if a.owner == nil || a.owner.invokeTable == nil || a.owner.corIDGen == nil || a.owner.deliver == nil || a.targetRef == nil {
		return nil
	}
	body, mode, encoding, schemaID, err := a.owner.encodePayload(callID, payload)
	if err != nil {
		return nil
	}
	stream, err := invoke.Invoke(
		a.owner.self.ID(),
		a.targetRef.ID(),
		callID,
		invoke.CallModeUnary,
		body,
		func(frame message.Frame) error {
			frame.SchemaID = schemaID
			frame.PayloadMode = mode
			frame.Encoding = encoding
			return a.makeSend()(frame)
		},
		a.owner.corIDGen,
		a.owner.invokeTable,
	)
	if err != nil {
		return nil
	}
	defer stream.Close()
	val, err := stream.Recv()
	if err != nil {
		return nil
	}
	if b, ok := val.([]byte); ok {
		if a.owner != nil && a.owner.codec != nil {
			return string(b)
		}
		var decoded any
		if json.Unmarshal(b, &decoded) == nil {
			return decoded
		}
		return string(b)
	}
	return val
}

func (a *actorRefAdapter) InvokeStream(callID string, payload any) Stream {
	if a.owner == nil || a.owner.invokeTable == nil || a.owner.corIDGen == nil || a.owner.deliver == nil || a.targetRef == nil {
		return nil
	}
	body, mode, encoding, schemaID, err := a.owner.encodePayload(callID, payload)
	if err != nil {
		return nil
	}
	stream, err := invoke.Invoke(
		a.owner.self.ID(),
		a.targetRef.ID(),
		callID,
		invoke.CallModeStream,
		body,
		func(frame message.Frame) error {
			frame.SchemaID = schemaID
			frame.PayloadMode = mode
			frame.Encoding = encoding
			return a.makeSend()(frame)
		},
		a.owner.corIDGen,
		a.owner.invokeTable,
	)
	if err != nil {
		return nil
	}
	sa := &streamAdapter{stream: stream, owner: a.owner}
	if a.owner.spore != nil {
		_ = a.owner.spore.RegisterHostInterfaceInstance("gospore", "Stream", sa)
	}
	return sa
}

func (a *actorRefAdapter) makeSend() invoke.SendFunc {
	return func(frame message.Frame) error {
		return a.owner.deliver(a.targetRef, mailbox.Envelope{Frame: frame, Sender: a.owner.self})
	}
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

func (s *streamAdapter) Recv() StreamChunk {
	if s.stream == nil {
		return StreamChunk{Err: "not found", Ok: true}
	}
	val, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			return StreamChunk{Ok: false}
		}
		return StreamChunk{Err: err.Error(), Ok: true}
	}
	if b, ok := val.([]byte); ok {
		if s.owner != nil && s.owner.codec != nil {
			return StreamChunk{Data: string(b), Ok: true}
		}
		var decoded any
		if json.Unmarshal(b, &decoded) == nil {
			return StreamChunk{Data: decoded, Ok: true}
		}
		return StreamChunk{Data: string(b), Ok: true}
	}
	return StreamChunk{Data: val, Ok: true}
}

func (s *streamAdapter) Close() bool {
	if s.stream == nil {
		return false
	}
	return s.stream.Close() == nil
}

// planNodeStreamAdapter implements Stream backed by a supervised plan.Node.
// The underlying node is auto-destroyed when the stream is consumed or closed.
type planNodeStreamAdapter struct {
	node  plan.Node
	owner *scriptRuntimeOwner
	once  sync.Once
}

func (s *planNodeStreamAdapter) Recv() StreamChunk {
	val, err := s.node.Recv()
	if err != nil {
		if err == io.EOF {
			s.destroy()
			return StreamChunk{Ok: false}
		}
		return StreamChunk{Err: err.Error(), Ok: true}
	}
	if b, ok := val.([]byte); ok {
		if s.owner != nil && s.owner.codec != nil {
			return StreamChunk{Data: string(b), Ok: true}
		}
		var decoded any
		if json.Unmarshal(b, &decoded) == nil {
			return StreamChunk{Data: decoded, Ok: true}
		}
		return StreamChunk{Data: string(b), Ok: true}
	}
	return StreamChunk{Data: val, Ok: true}
}

func (s *planNodeStreamAdapter) Close() bool {
	s.destroy()
	return s.node.Stop() == nil
}

func (s *planNodeStreamAdapter) destroy() {
	s.once.Do(func() {
		if s.owner != nil && s.owner.stopFn != nil {
			_ = s.owner.stopFn(s.node.Ref())
		}
	})
}

// ---------------------------------------------------------------------------
// Codec helpers
// ---------------------------------------------------------------------------

// encodePayload converts a script-side any payload into wire bytes for the
// given callID. It uses the App's codec when available, falling back to
// json.Marshal only when the codec is nil. []byte and string pass through
// unchanged so raw payloads avoid an extra encode/decode round-trip.
func (o *scriptRuntimeOwner) encodePayload(callID string, payload any) ([]byte, message.PayloadMode, message.Encoding, uint64, error) {
	if payload == nil {
		return nil, message.PayloadModeValue, message.EncodingNone, 0, nil
	}
	if b, ok := payload.([]byte); ok {
		return b, message.PayloadModeValue, message.EncodingNone, gschema.BuiltinBytes, nil
	}
	if s, ok := payload.(string); ok {
		return []byte(s), message.PayloadModeValue, message.EncodingNone, gschema.BuiltinString, nil
	}
	if o.codec == nil {
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		return body, message.PayloadModeRaw, message.EncodingJSON, 0, nil
	}
	var desc schema.TypeDesc
	var schemaID uint64
	if o.handlers != nil {
		if inv, ok := o.handlers.Lookup(callID); ok {
			schemaID = inv.RequestSchemaID
		}
		if d, ok := o.handlers.Desc(callID); ok && len(d.Parameters) > 0 {
			desc = d.Parameters[0].Type
		}
	}
	if desc.Kind == "" {
		desc = schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}
	}
	body, err := o.codec.Encode(desc, payload)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	return body, message.PayloadModeRaw, o.codec.Encoding(), schemaID, nil
}

// ---------------------------------------------------------------------------
// replyValue / sporeCallableName
// ---------------------------------------------------------------------------

func (o *scriptRuntimeOwner) replyValue(reply func(message.Frame), callID string, value any, schemaID uint64) {
	if value == nil {
		return
	}
	frame := message.Frame{Kind: message.KindReply, SchemaID: schemaID}
	if err, ok := value.(error); ok {
		frame.Body = []byte(err.Error())
		frame.PayloadMode = message.PayloadModeValue
		frame.Encoding = message.EncodingNone
		reply(frame)
		return
	}
	if b, ok := value.([]byte); ok {
		frame.Body = b
		frame.PayloadMode = message.PayloadModeValue
		frame.Encoding = message.EncodingNone
		reply(frame)
		return
	}
	if s, ok := value.(string); ok && o.codec == nil {
		// No codec: keep the legacy raw Value-mode shape (decode side returns
		// Value-mode bodies as []byte).
		frame.Body = []byte(s)
		frame.PayloadMode = message.PayloadModeValue
		frame.Encoding = message.EncodingNone
		reply(frame)
		return
	}
	if o.codec != nil {
		var desc schema.TypeDesc
		if o.handlers != nil {
			if d, ok := o.handlers.Desc(callID); ok && len(d.Returns) > 0 {
				desc = d.Returns[0]
			}
		}
		if desc.Kind == "" {
			if _, isStr := value.(string); isStr {
				// Strings encode against the builtin string schema so callers
				// decode a string via the frame's SchemaID (BuiltinString).
				desc = schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}
			} else {
				desc = schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "any"}
			}
		}
		encoded, err := o.codec.Encode(desc, value)
		if err != nil {
			frame.Body = []byte(fmt.Sprintf("gospore.script.codec_encode: %s", err.Error()))
			frame.PayloadMode = message.PayloadModeValue
			frame.Encoding = message.EncodingNone
		} else {
			frame.Body = encoded
			frame.PayloadMode = message.PayloadModeRaw
			frame.Encoding = o.codec.Encoding()
		}
		reply(frame)
		return
	}
	b, err := json.Marshal(value)
	if err != nil {
		frame.Body = []byte(fmt.Sprintf("gospore.script.json_encode: %s", err.Error()))
		frame.PayloadMode = message.PayloadModeValue
		frame.Encoding = message.EncodingNone
	} else {
		frame.Body = b
		frame.PayloadMode = message.PayloadModeRaw
		frame.Encoding = message.EncodingJSON
	}
	reply(frame)
}

func sporeCallableName(callID string) string {
	idx := strings.LastIndex(callID, ".")
	if idx < 0 || idx == len(callID)-1 {
		return callID
	}
	return callID[idx+1:]
}
