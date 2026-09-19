package cell

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/discovery"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/plan"
	"github.com/qomos-w/gospore/promise"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/resource"
	"github.com/qomos-w/gospore/schema"
)

// serviceDiscoveryTTL is the lease duration for services registered with
// the cross-App discovery provider. One minute is long enough to avoid
// churn during normal operation and short enough that a crashed App
// disappears from the resolve view quickly.
const serviceDiscoveryTTL = time.Minute

type startContext struct {
	cell       *Cell
	callID     string
	caller     ref.Ref
	components map[string]any
	logger     actor.Logger
	planner    *planner
}

func newStartContext(c *Cell) *startContext {
	prefix := ""
	if c.self != nil {
		prefix = c.self.ID().String()
	}
	logger := c.logger
	if logger == nil {
		logger = newDefaultLogger(prefix, c.logHook)
	}
	sc := &startContext{cell: c, components: map[string]any{}, logger: logger}
	if c.props.PlanEnabled() {
		sc.planner = &planner{cell: c}
	}
	return sc
}

type callContext struct {
	*startContext
	identity id.Identity
}

func newCallContext(c *Cell, env handler.InvokeEnv) *callContext {
	base := newStartContext(c)
	base.callID = env.Frame.CallID
	base.caller = env.Caller
	return &callContext{startContext: base, identity: env.Identity}
}

func (c *startContext) Self() ref.Ref {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.self
}

func (c *startContext) Parent() ref.Ref {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.parent
}

func (c *startContext) Children() []ref.Ref {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.Children()
}

func (c *startContext) Caller() ref.Ref       { return c.caller }
func (c *startContext) CallID() string         { return c.callID }
func (c *startContext) Identity() id.Identity  { return id.Identity{} }
func (c *callContext) Identity() id.Identity   { return c.identity }

// NewID returns a fresh 128-bit canonical ID from the App-global generator.
// Concurrency-safe. Panics if the Cell was constructed without an IDGen
// (test fixtures that exercise contexts outside an App must supply one).
func (c *startContext) NewID() id.ActorID {
	if c == nil || c.cell == nil || c.cell.idGen == nil {
		panic("gospore/cell: NewID called without IDGen wired (App always wires; test fixture must supply id.NewCanonical)")
	}
	return c.cell.idGen.Next()
}

func (c *startContext) Done() <-chan struct{} {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.Done()
}

func (c *startContext) Logger() actor.Logger {
	if c == nil {
		return newDefaultLogger("", nil)
	}
	return c.logger
}

// deriveServiceName returns the route service name for callID derived
// from the cell's declared domains: when the callID's first segment
// matches a declared domain, that domain is the service name. Flat or
// unmatched callIDs derive to "" (agent-local).
func deriveServiceName(c *Cell, callID string) string {
	return actor.ServiceNameFor(c.Domains(), callID)
}

func (c *startContext) Register(callID string, fn any, opts ...actor.RegisterOption) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("register %q: nil context", callID)
	}
	if c.cell.handlers == nil {
		c.cell.handlers = c.cell.newHandlerTable()
	}
	service := deriveServiceName(c.cell, callID)
	if err := c.cell.handlers.RegisterWithService(callID, fn, service, opts...); err != nil {
		return err
	}
	inv, _ := c.cell.handlers.Lookup(callID)
	if inv != nil {
		if err := c.validateLoop(inv.Loop, inv.Mode); err != nil {
			return fmt.Errorf("register %q: %w", callID, err)
		}
	}
	return nil
}

func (c *startContext) RegisterScript(callID string, source string, mode actor.HandlerMode, opts ...actor.RegisterOption) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("register script %q: nil context", callID)
	}
	if c.cell.handlers == nil {
		c.cell.handlers = handler.NewTable()
	}
	service := deriveServiceName(c.cell, callID)
	if err := c.cell.handlers.RegisterScriptWithService(callID, source, mode, service, opts...); err != nil {
		return err
	}
	inv, _ := c.cell.handlers.Lookup(callID)
	if inv != nil {
		if err := c.validateLoop(inv.Loop, inv.Mode); err != nil {
			return fmt.Errorf("register script %q: %w", callID, err)
		}
	}
	return nil
}

func (c *startContext) RegisterLoop(name string, mode actor.HandlerMode) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("register loop %q: nil context", name)
	}
	if name == "" {
		return fmt.Errorf("register loop: empty name")
	}
	switch name {
	case actor.DefaultLoopOwner, actor.DefaultLoopPure:
		return nil
	case actor.DefaultLoopReply:
		return fmt.Errorf("register loop %q: reply loop is reserved", name)
	}
	c.cell.loopsMu.Lock()
	defer c.cell.loopsMu.Unlock()
	if _, exists := c.cell.loops[name]; !exists {
		c.cell.loops[name] = &loopLane{mode: mode}
	}
	return nil
}

func (c *startContext) RegisterTimer(callID string, opts ...actor.RegisterOption) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("register timer %q: nil context", callID)
	}
	if callID == "" {
		return fmt.Errorf("register timer: empty callID")
	}
	mode := actor.ModeStateful
	if explicitMode, ok := actor.ResolveMode(opts...); ok {
		mode = explicitMode
	}
	loop := actor.ResolveLoopOrDefault(mode, opts...)
	if err := c.validateLoop(loop, mode); err != nil {
		return fmt.Errorf("register timer %q: %w", callID, err)
	}
	c.cell.timerMetaMu.Lock()
	c.cell.timerMeta[callID] = timerMetaEntry{Mode: mode, Loop: loop}
	c.cell.timerMetaMu.Unlock()
	return nil
}

func (c *startContext) validateLoop(loop string, mode actor.HandlerMode) error {
	switch loop {
	case "", actor.DefaultLoopOwner:
		return nil
	case actor.DefaultLoopPure:
		if mode != actor.ModeStateless {
			return fmt.Errorf("pure loop requires stateless mode")
		}
		return nil
	case actor.DefaultLoopReply:
		return fmt.Errorf("reply loop is reserved")
	default:
		c.cell.loopsMu.Lock()
		lane, ok := c.cell.loops[loop]
		c.cell.loopsMu.Unlock()
		if !ok {
			return fmt.Errorf("loop %q is not registered", loop)
		}
		// A stateless loop can only accept stateless handlers.
		// A stateful loop can accept any handler mode.
		if lane != nil && lane.mode == actor.ModeStateless && mode != actor.ModeStateless {
			return fmt.Errorf("loop %q mode mismatch: handler is %s, loop is stateless", loop, mode)
		}
		return nil
	}
}

func (c *startContext) RegisterEventKind(kind string, example any, opts ...actor.RegisterOption) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("register event kind %q: nil context", kind)
	}
	if kind == "" {
		return fmt.Errorf("%s: event kind must not be empty", actor.DiagEventKindInvalid)
	}
	if example == nil {
		return fmt.Errorf("register event kind %q: example value must not be nil", kind)
	}
	mode := actor.ModeStateful
	if explicitMode, ok := actor.ResolveMode(opts...); ok {
		mode = explicitMode
	}
	loop := actor.ResolveLoopOrDefault(mode, opts...)
	if err := c.validateLoop(loop, mode); err != nil {
		return fmt.Errorf("register event kind %q: %w", kind, err)
	}
	vis := actor.ResolveVisibility(opts...)
	c.cell.eventMetaMu.Lock()
	if _, exists := c.cell.eventMeta[kind]; exists {
		c.cell.eventMetaMu.Unlock()
		return fmt.Errorf("%s: event kind %q already registered", actor.DiagEventKindDuplicate, kind)
	}
	c.cell.eventMeta[kind] = eventMetaEntry{
		Type:       reflect.TypeOf(example),
		Visibility: vis,
		Mode:       mode,
		Loop:       loop,
	}
	c.cell.eventMetaMu.Unlock()
	return nil
}

// SubscribeEventKind subscribes this actor to events of the given kind
// emitted by any cell. The handler runs on a dedicated goroutine that
// drains the subscription channel; it exits when the subscription is
// cancelled or the cell's lifecycle context is done. Returns a cancel
// func that unsubscribes. Only valid inside OnStart.
func (c *startContext) SubscribeEventKind(kind string, handler func(actor.EventEnvelope)) (func(), error) {
	if c == nil || c.cell == nil {
		return nil, fmt.Errorf("subscribe event kind %q: nil context", kind)
	}
	if kind == "" {
		return nil, fmt.Errorf("%s: event kind must not be empty", actor.DiagEventKindInvalid)
	}
	if handler == nil {
		return nil, fmt.Errorf("subscribe event kind %q: handler must not be nil", kind)
	}
	if c.cell.eventBus == nil {
		return nil, fmt.Errorf("%s: no event bus wired into cell", actor.DiagEventNoBus)
	}
	var ident id.Identity
	sub, cancelSub := c.cell.eventBus.SubscribeByKind(kind, ident)

	ctx := c.cell.lifecycleCtx
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		for {
			select {
			case <-sub.Done:
				return
			case <-ctx.Done():
				cancelSub()
				return
			case env, ok := <-sub.Ch:
				if !ok {
					return
				}
				handler(actor.EventEnvelope{
					ActorId:  env.ActorId,
					Services: env.Services,
					Kind:     env.Kind,
					Payload:  env.Payload,
				})
			}
		}
	}()
	return cancelSub, nil
}

// EmitEvent publishes payload under kind to the App-scoped event bus.
// Validates that kind has been registered on this actor and that the
// runtime payload type matches the example registered. Forward-only:
// if there are no current subscribers, the event is dropped silently.
func (c *startContext) EmitEvent(kind string, payload any) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("emit event %q: nil context", kind)
	}
	entry, ok := c.cell.eventMeta[kind]
	if !ok {
		return fmt.Errorf("%s: event kind %q not registered on actor %s",
			actor.DiagEventKindUnknown, kind, c.cell.namespace)
	}
	if payload == nil {
		return fmt.Errorf("%s: event %q payload must not be nil", actor.DiagEventPayloadMismatch, kind)
	}
	if got := reflect.TypeOf(payload); got != entry.Type {
		return fmt.Errorf("%s: event %q registered as %s, emit got %s",
			actor.DiagEventPayloadMismatch, kind, entry.Type, got)
	}
	if c.cell.eventBus == nil {
		return fmt.Errorf("%s: no event bus wired into cell", actor.DiagEventNoBus)
	}
	key := c.cell.eventRoutingKey()
	if c.cell.logger != nil {
		actorID := ""
		if c.cell.self != nil {
			actorID = c.cell.self.ID().String()
		}
		c.cell.logger.Debug("eventbus: EmitEvent", "routingKey", key, "kind", kind, "actorId", actorID)
	}
	c.cell.eventBus.Publish(key, kind, c.cell.services, payload)
	return nil
}

func (c *startContext) HasEventSubscribers(kind string) bool {
	return c.EventSubscriberCount(kind) > 0
}

func (c *startContext) EventSubscriberCount(kind string) int {
	if c == nil || c.cell == nil || c.cell.eventBus == nil || kind == "" {
		return 0
	}
	actorId := c.cell.eventRoutingKey()
	count := c.cell.eventBus.SubscriberCountByInstance(actorId, kind)
	for _, svc := range c.cell.services {
		count += c.cell.eventBus.SubscriberCountByService(svc, kind)
	}
	return count
}

func (c *callContext) Register(callID string, fn any, opts ...actor.RegisterOption) error {
	return c.startContext.Register(callID, fn, opts...)
}

func (c *callContext) RegisterScript(callID string, source string, mode actor.HandlerMode, opts ...actor.RegisterOption) error {
	return c.startContext.RegisterScript(callID, source, mode, opts...)
}

func (c *callContext) RegisterEventKind(kind string, _ any, _ ...actor.RegisterOption) error {
	return fmt.Errorf("%s: RegisterEventKind %q must be called from OnStart, not a request handler", actor.DiagEventRegisterAfterStart, kind)
}

func (c *callContext) RegisterLoop(name string, _ actor.HandlerMode) error {
	return fmt.Errorf("%s: RegisterLoop %q must be called from OnStart, not a request handler", actor.DiagEventRegisterAfterStart, name)
}

func (c *callContext) RegisterTimer(callID string, _ ...actor.RegisterOption) error {
	return fmt.Errorf("%s: RegisterTimer %q must be called from OnStart, not a request handler", actor.DiagEventRegisterAfterStart, callID)
}

func (c *callContext) AttachComponent(name string, schemaID uint64, initial any, opts ...actor.RegisterOption) error {
	return c.startContext.AttachComponent(name, schemaID, initial, opts...)
}

func (c *callContext) RegisterDomain(name string) *actor.DomainHandle {
	return c.startContext.RegisterDomain(name)
}

func (c *callContext) applyScriptReload(req actor.ReloadReq) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("%s: nil context", actor.DiagSwapFailedRolledBack)
	}
	if req.Definition == nil {
		return fmt.Errorf("%s: %s requires a non-nil Definition", actor.DiagIncompatibleReload, actor.CallIDReload)
	}
	c.cell.pureWG.Wait()
	if owner := c.cell.scriptRuntime; owner != nil {
		if err := owner.loadSporeModule(req.Definition); err != nil {
			return fmt.Errorf("%s: %w", actor.DiagSwapFailedRolledBack, err)
		}
	}
	if c.cell.handlers == nil {
		c.cell.handlers = c.cell.newHandlerTable()
	}
	for _, decl := range req.Definition.Handlers {
		inv := &handler.Invoker{Mode: decl.Mode, CallID: decl.CallID, Script: &handler.ScriptHandle{Source: decl.Source}}
		if existing, ok := c.cell.handlers.Lookup(decl.CallID); ok && existing != nil {
			existing.Script = inv.Script
		} else {
			c.cell.handlers.Put(decl.CallID, inv)
		}
	}
	if host, ok := c.cell.actor.(*actor.Host); ok {
		host.SetDefinition(req.Definition)
	}
	return nil
}

func (c *callContext) applyScriptReplace(req actor.ReplaceReq) error {
	if c == nil || c.cell == nil {
		return fmt.Errorf("%s: nil context", actor.DiagSwapFailedRolledBack)
	}
	if err := actor.ValidateReplaceReq(&req); err != nil {
		return err
	}
	if req.Definition == nil || c.cell.handlers == nil {
		return nil
	}
	c.cell.pureWG.Wait()
	c.cell.scriptRuntime = newScriptRuntimeOwner(c.cell.codec, c.cell)
	if err := c.cell.scriptRuntime.loadSporeModule(req.Definition); err != nil {
		return fmt.Errorf("%s: %w", actor.DiagSwapFailedRolledBack, err)
	}
	for _, decl := range req.Definition.Handlers {
		if existing, ok := c.cell.handlers.Lookup(decl.CallID); ok && existing != nil {
			existing.Script = &handler.ScriptHandle{Source: decl.Source}
			continue
		}
		inv := &handler.Invoker{Mode: decl.Mode, CallID: decl.CallID, Script: &handler.ScriptHandle{Source: decl.Source}}
		c.cell.handlers.Put(decl.CallID, inv)
	}
	if host, ok := c.cell.actor.(*actor.Host); ok {
		host.SetDefinition(req.Definition)
	}
	return nil
}

func (c *startContext) AttachComponent(name string, schemaID uint64, initial any, _ ...actor.RegisterOption) error {
	if c == nil {
		return fmt.Errorf("attach component %q: nil context", name)
	}
	c.components[name] = struct {
		SchemaID uint64
		Initial  any
	}{SchemaID: schemaID, Initial: initial}
	return nil
}

func (c *startContext) RegisterDomain(name string) *actor.DomainHandle {
	if c == nil || c.cell == nil {
		return actor.NewDomainHandle(name, nil, nil)
	}
	c.cell.AddDomain(name)
	// Backfill: any already-registered invoker whose callID first segment
	// matches the new domain and whose ServiceName is still empty gets
	// stamped with this domain name. This handles the "Register before
	// RegisterDomain" ordering (callables registered at startup, domain
	// declared afterwards).
	if c.cell.handlers != nil {
		c.cell.handlers.StampServiceForDomain(name)
	}
	exposeFn := func(svcName string) error {
		var err error
		if c.cell.exposeService != nil {
			err = c.cell.exposeService(svcName)
		} else if c.cell.svcReg != nil {
			err = c.cell.svcReg.Register(svcName, c.cell.self)
		}
		if err != nil {
			return err
		}
		if c.cell.discoveryProvider != nil && c.cell.namespace != "" {
			inst := discovery.Instance{
				Namespace:       c.cell.namespace,
				Service:         svcName,
				RuntimeSlotID:   c.cell.self.ID().RuntimeSlot(),
				InstanceGroupID: c.cell.namespace + "-default",
				Address:         c.cell.discoveryAddress,
				ActorID:         c.cell.self.ID(),
			}
			_ = c.cell.discoveryProvider.Register(inst, serviceDiscoveryTTL)
		}
		c.cell.AddService(svcName)
		return nil
	}
	exposeToChildrenFn := func(svcName string) error {
		if c.cell.exposeServiceToChildren == nil {
			return fmt.Errorf("exposeToChildren %q: not available", svcName)
		}
		if err := c.cell.exposeServiceToChildren(svcName); err != nil {
			return err
		}
		c.cell.AddChildService(svcName)
		return nil
	}
	return actor.NewDomainHandle(name, exposeFn, exposeToChildrenFn)
}

func (c *startContext) LookupID(aid id.ActorID) (ref.Ref, bool) {
	if c == nil || c.cell == nil || c.cell.lookupID == nil {
		return nil, false
	}
	return c.cell.lookupID(aid)
}
func (c *startContext) LookupService(name string) (ref.Ref, bool) {
	if c == nil || c.cell == nil {
		return nil, false
	}
	if c.cell.lookupScopedService != nil {
		if r, ok := c.cell.lookupScopedService(name); ok {
			return r, true
		}
	}
	if c.cell.lookupService != nil {
		return c.cell.lookupService(name)
	}
	if c.cell.svcReg != nil {
		return c.cell.svcReg.Lookup(name)
	}
	return nil, false
}
func (c *startContext) Namespace() string {
	if c == nil || c.cell == nil {
		return ""
	}
	return c.cell.namespace
}
func (c *startContext) Codec() codec.Codec {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.codec
}
func (c *startContext) Schemas() schema.Reader {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.schemas
}
func (c *startContext) Root() ref.Ref {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.rootRef
}
func (c *startContext) Resources() resource.Registry {
	if c == nil || c.cell == nil {
		return nil
	}
	return c.cell.resReg
}
func (c *startContext) Clock() actor.Clock {
	if c == nil || c.cell == nil || c.cell.clock == nil {
		return staticClock{}
	}
	return c.cell.clock
}
func (c *startContext) After(delay time.Duration, callID string, payload any) error {
	if c == nil || c.cell == nil || c.cell.deliver == nil {
		return fmt.Errorf("after: not available")
	}
	clock := c.Clock()
	go func() {
		select {
		case <-clock.After(delay):
		case <-c.cell.done:
			return
		}
		var body []byte
		if b, ok := payload.([]byte); ok {
			body = b
		}
		loop := actor.DefaultLoopOwner
		c.cell.timerMetaMu.RLock()
		if meta, ok := c.cell.timerMeta[callID]; ok && meta.Loop != "" {
			loop = meta.Loop
		}
		c.cell.timerMetaMu.RUnlock()
		if loop == actor.DefaultLoopOwner && c.cell.handlers != nil {
			if inv, ok := c.cell.handlers.Lookup(callID); ok && inv != nil && inv.Loop != "" {
				loop = inv.Loop
			}
		}
		frame := message.Frame{
			From:   c.cell.self.ID(),
			To:     c.cell.self.ID(),
			Kind:   message.KindCall,
			CallID: callID,
			Body:   body,
		}
		if frame.Headers == nil {
			frame.Headers = map[string]string{}
		}
		frame.Headers["gospore.loop"] = loop
		_ = c.cell.deliver(c.cell.self, mailbox.Envelope{
			Frame:  frame,
			Payload: payload,
			Sender: c.cell.self,
		})
	}()
	return nil
}
func (c *startContext) Spawn(props actor.Props, name string) (ref.Ref, error) {
	if c == nil || c.cell == nil || c.cell.spawn == nil {
		return nil, fmt.Errorf("spawn: not available")
	}
	return c.cell.spawn(c.cell.self, props, name)
}
func (c *startContext) Stop(target ref.Ref) error {
	if c == nil || c.cell == nil || c.cell.stop == nil {
		return nil
	}
	return c.cell.stop(target)
}
func (c *startContext) Destroy(target ref.Ref) error {
	if c == nil || c.cell == nil || c.cell.destroy == nil {
		return nil
	}
	return c.cell.destroy(target)
}
func (c *startContext) Watch(target ref.Ref, kinds ...actor.WatchKind) error {
	if c == nil || c.cell == nil || c.cell.deliver == nil {
		return fmt.Errorf("watch: not available")
	}
	if target == nil {
		return fmt.Errorf("watch: nil target")
	}
	return c.cell.deliver(target, mailbox.Envelope{
		Payload: mailbox.Watch{Watcher: c.cell.self, Kinds: kinds},
	})
}
func (c *startContext) Unwatch(target ref.Ref) {
	if c == nil || c.cell == nil || c.cell.deliver == nil || target == nil {
		return
	}
	_ = c.cell.deliver(target, mailbox.Envelope{
		Payload: mailbox.Unwatch{Watcher: c.cell.self},
	})
}
func (c *startContext) Planner() actor.Planner {
	// Return a true nil interface (not a typed-nil *planner) when the actor
	// lacks the Planner capability. A typed-nil return defeats every
	// `planner == nil` / `planner != nil` guard in caller code, so the nil
	// receiver's own Plan/Call/Stream guards become the only line of defence
	// and surface as opaque "plan: spawn not available" errors.
	if c == nil || c.planner == nil {
		return nil
	}
	return c.planner
}

func (c *startContext) Lifecycle() context.Context {
	if c == nil || c.cell == nil || c.cell.lifecycleCtx == nil {
		return context.Background()
	}
	return c.cell.lifecycleCtx
}

func (c *startContext) Component(name string) (any, bool) {
	if c == nil {
		return nil, false
	}
	v, ok := c.components[name]
	return v, ok
}

type staticClock struct{}

func (staticClock) Now() time.Time { return time.Time{} }

// After delegates to time.After so ctx.After remains usable when no
// Clock has been injected (e.g. unit tests that wire a Cell directly
// without going through app.New). Real callers go through the App's
// configured Clock and never reach this fallback.
func (staticClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// stopContext is the read-only Context delivered to actor.OnStop. It
// reuses startContext for read access (Self, Parent, Children, Logger,
// Codec, Schemas, Resources, Clock, Lookup*, Component) but rejects
// every mutation surface: Register / RegisterScript / AttachComponent
// / Expose / Spawn / Stop / Watch / Unwatch / After / Plan all return
// DiagContextStopped (Unwatch is a silent no-op since its signature
// has no return).
//
// The actor is past the point where new state should be accepted —
// handlers have drained, watchers will be notified imminently — so
// allowing fresh registrations or child spawns would race the very
// teardown that just invoked OnStop.
type stopContext struct {
	*startContext
}

func newStopContext(c *Cell) *stopContext {
	return &stopContext{startContext: newStartContext(c)}
}

func stopErr(op string) error {
	return fmt.Errorf("%s: %s on stopped context", actor.DiagContextStopped, op)
}

func (c *stopContext) Register(callID string, _ any, _ ...actor.RegisterOption) error {
	return stopErr("Register " + callID)
}

func (c *stopContext) RegisterScript(callID string, _ string, _ actor.HandlerMode, _ ...actor.RegisterOption) error {
	return stopErr("RegisterScript " + callID)
}

func (c *stopContext) RegisterLoop(name string, _ actor.HandlerMode) error {
	return stopErr("RegisterLoop " + name)
}

func (c *stopContext) RegisterTimer(callID string, _ ...actor.RegisterOption) error {
	return stopErr("RegisterTimer " + callID)
}

func (c *stopContext) EmitEvent(kind string, _ any) error {
	return fmt.Errorf("%s: EmitEvent %s on stopped context", actor.DiagEventAfterStop, kind)
}

func (c *stopContext) AttachComponent(name string, _ uint64, _ any, _ ...actor.RegisterOption) error {
	return stopErr("AttachComponent " + name)
}

func (c *stopContext) RegisterDomain(name string) *actor.DomainHandle {
	_ = stopErr("RegisterDomain " + name)
	return actor.NewDomainHandle(name, nil, nil)
}

func (c *stopContext) Spawn(_ actor.Props, name string) (ref.Ref, error) {
	return nil, stopErr("Spawn " + name)
}

func (c *stopContext) Stop(_ ref.Ref) error {
	return stopErr("Stop")
}

func (c *stopContext) Destroy(_ ref.Ref) error {
	return stopErr("Destroy")
}

func (c *stopContext) Watch(_ ref.Ref, _ ...actor.WatchKind) error {
	return stopErr("Watch")
}

// Unwatch on the stopped context is a no-op: the signature has no
// return value, and the actor's watchers are about to be torn down
// anyway.
func (c *stopContext) Unwatch(_ ref.Ref) {}

func (c *stopContext) After(_ time.Duration, callID string, _ any) error {
	return stopErr("After " + callID)
}

type stoppedPlanner struct{}

func (stoppedPlanner) Plan(_ ref.Ref, _ string, _ any, _ ...plan.Option) (plan.Node, error) {
	return nil, stopErr("Plan")
}
func (stoppedPlanner) Call(_ context.Context, _ ref.Ref, _ string, _ any) *promise.Promise[any] {
	return promise.Reject[any](stopErr("Call"))
}
func (stoppedPlanner) Stream(_ context.Context, _ ref.Ref, _ string, _ any, _ func(any) error) *promise.Promise[any] {
	return promise.Reject[any](stopErr("Stream"))
}

func (c *stopContext) Planner() actor.Planner {
	return stoppedPlanner{}
}

// initContext is the Context delivered to actor.OnInit. It reuses
// startContext for read access but rejects mutation surfaces that belong
// in OnStart: Register / RegisterScript / Expose / After / Watch / Plan.
// Spawn is allowed — root actors need to build the child tree during init.
type initContext struct {
	*startContext
}

func newInitContext(c *Cell) *initContext {
	return &initContext{startContext: newStartContext(c)}
}

func initErr(op string) error {
	return fmt.Errorf("%s: %s on init context", actor.DiagContextStopped, op)
}

func (c *initContext) Register(callID string, _ any, _ ...actor.RegisterOption) error {
	return initErr("Register " + callID)
}

func (c *initContext) RegisterScript(callID string, _ string, _ actor.HandlerMode, _ ...actor.RegisterOption) error {
	return initErr("RegisterScript " + callID)
}

func (c *initContext) RegisterLoop(name string, _ actor.HandlerMode) error {
	return initErr("RegisterLoop " + name)
}

func (c *initContext) RegisterTimer(callID string, _ ...actor.RegisterOption) error {
	return initErr("RegisterTimer " + callID)
}

func (c *initContext) EmitEvent(kind string, _ any) error {
	return initErr("EmitEvent " + kind)
}

func (c *initContext) AttachComponent(name string, _ uint64, _ any, _ ...actor.RegisterOption) error {
	return initErr("AttachComponent " + name)
}

func (c *initContext) RegisterDomain(name string) *actor.DomainHandle {
	_ = initErr("RegisterDomain " + name)
	return actor.NewDomainHandle(name, nil, nil)
}

func (c *initContext) After(_ time.Duration, callID string, _ any) error {
	return initErr("After " + callID)
}

func (c *initContext) Watch(_ ref.Ref, _ ...actor.WatchKind) error {
	return initErr("Watch")
}

func (c *initContext) Unwatch(_ ref.Ref) {}

func (c *initContext) Planner() actor.Planner {
	return nil
}

// NewStartContextForTest creates a startContext for unit tests.
func NewStartContextForTest(c *Cell) actor.Context {
	return newStartContext(c)
}

// NewCallContextForTest creates a callContext for unit tests so the
// negative path of handler-time API restrictions (e.g. RegisterEventKind
// returning DiagEventRegisterAfterStart) can be exercised without going
// through the cell run loop.
func NewCallContextForTest(c *Cell) actor.Context {
	return &callContext{startContext: newStartContext(c)}
}

// planner implements actor.Planner for actors spawned with
// Props.WithPlanner(). Plan creates long-lived nodes; Call and Stream
// are one-shots that auto-destroy the underlying node.
type planner struct {
	cell *Cell
}

func (p *planner) Plan(target ref.Ref, callID string, payload any, opts ...plan.Option) (plan.Node, error) {
	if p == nil || p.cell == nil || p.cell.spawn == nil {
		return nil, fmt.Errorf("plan: spawn not available")
	}
	if target == nil {
		return nil, fmt.Errorf("plan: nil target")
	}

	cfg := plan.NewConfig(opts...)

	nodeCfg := planNodeConfig{
		target:        target,
		callID:        callID,
		payload:       payload,
		autoStart:     cfg.AutoStart,
		timeout:       cfg.Timeout,
		keepAfterDone: cfg.KeepAfterDone,
		name:          cfg.Name,
		projections:   p.cell.projections,
	}

	name := nodeCfg.name
	if name == "" {
		name = "_plan_" + strconv.FormatUint(uint64(p.cell.corIDGen.Next()), 10)
	}

	actor := newPlanNodeActor(nodeCfg)
	ref, err := p.cell.spawn(p.cell.self, actor.Props(), name)
	if err != nil {
		return nil, err
	}

	actor.setSelfRef(ref)
	return actor, nil
}

// invokeOn routes a one-shot call through the cell's PlannerInvoke closure
// when available, so the message sender is the planner's owning actor rather
// than the target itself. Falls back to the legacy target.Invoke path for
// cells constructed without the closure (e.g. narrow test fixtures).
func (p *planner) invokeOn(target ref.Ref, ctx context.Context, callID string, payload any) *invoke.Call {
	if p != nil && p.cell != nil && p.cell.plannerInvoke != nil {
		return p.cell.plannerInvoke(target, callID, payload)
	}
	return target.Invoke(ctx, callID, payload)
}

func (p *planner) Call(ctx context.Context, target ref.Ref, callID string, payload any) *promise.Promise[any] {
	return promise.Async(func(resolve func(any), reject func(any)) {
		call := p.invokeOn(target, ctx, callID, payload)
		// Always release the pending slot. Final drains the stream to
		// terminal but does NOT close it; without Close the PendingTable
		// slot and the buffered channel leak for every planner.Call site.
		defer call.Close()
		result, err := call.Final(ctx)
		if err != nil && !errors.Is(err, io.EOF) {
			reject(err)
			return
		}
		// io.EOF means the callable completed without producing a reply value
		// (e.g., an error-only handler that returned nil). Treat as success.
		resolve(result)
	})
}

func (p *planner) Stream(ctx context.Context, target ref.Ref, callID string, payload any, onChunk func(any) error) *promise.Promise[any] {
	return promise.Async(func(resolve func(any), reject func(any)) {
		call := p.invokeOn(target, ctx, callID, payload)
		// Release the pending slot on every exit path (EOF, error, or
		// consumer stop). Without this, every planner.Stream leaks a
		// PendingTable slot and its 256-buffered channel.
		defer call.Close()

		// Translate ctx cancellation into call cancellation so
		// Recv unblocks when the caller gives up.
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				call.Cancel()
			case <-stop:
			}
		}()

		for {
			chunk, err := call.Recv()
			if errors.Is(err, io.EOF) {
				resolve(nil)
				return
			}
			if err != nil {
				reject(err)
				return
			}
			if err := onChunk(chunk); err != nil {
				resolve(nil) // consumer signalled stop, not an error
				return
			}
		}
	})
}

var _ actor.Context = (*startContext)(nil)
var _ actor.Context = (*callContext)(nil)
var _ actor.Context = (*stopContext)(nil)
var _ actor.Context = (*initContext)(nil)
var _ actor.ComponentHost = (*startContext)(nil)
var _ actor.ComponentHost = (*callContext)(nil)
var _ actor.ComponentHost = (*stopContext)(nil)
var _ actor.ComponentHost = (*initContext)(nil)
var _ actor.Planner = (*planner)(nil)
var _ actor.PureContext = (*callContext)(nil)
