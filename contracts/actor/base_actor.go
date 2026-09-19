package actor

import (
	"github.com/qomos-w/gospore/contracts"
	"github.com/qomos-w/gospore/model"
)

// CallableHandler is the user-provided implementation for one named callable.
type CallableHandler func(ctx contracts.Expose, req interface{})

// BaseActor is the foundation actor base that users embed to obtain automatic
// Receiver, FuncCaller, ProtocolProvider, and CapabilityProvider behaviour.
// The user only needs to RegisterCallable; the runtime routes Invoke requests
// to the matching handler automatically.
type BaseActor struct {
	schema         model.CapabilitySchema
	handlers       map[string]CallableHandler
	defaultHandler func(ctx contracts.Expose, msg interface{})
}

// RegisterCallable registers one named callable and its handler.
// The mode is recorded in the actor's schema for protocol discovery.
func (a *BaseActor) RegisterCallable(name string, mode model.InvocationMode, handler CallableHandler) {
	if a.handlers == nil {
		a.handlers = make(map[string]CallableHandler)
	}
	if handler == nil {
		delete(a.handlers, name)
		a.removeCallableFromSchema(name)
		return
	}
	a.handlers[name] = handler
	a.upsertCallableSchema(name, mode)
}

// RegisterSchema replaces the entire capability schema.
func (a *BaseActor) RegisterSchema(schema model.CapabilitySchema) {
	a.schema = schema
}

// SetDefaultHandler sets the fallback handler for messages that are not
// CapabilityInvokeRequest (e.g. internal notifications or raw messages).
func (a *BaseActor) SetDefaultHandler(handler func(ctx contracts.Expose, msg interface{})) {
	a.defaultHandler = handler
}

// Receive implements Receiver. It auto-routes CapabilityInvokeRequest to the
// registered callable; everything else goes to the default handler.
func (a *BaseActor) Receive(ctx contracts.Expose, msg interface{}) {
	if routed, ok := msg.(model.CapabilityInvokeRequest); ok {
		a.InvokeCallable(ctx, routed.Route.CallableName, routed.Body)
		return
	}
	if a.defaultHandler != nil {
		a.defaultHandler(ctx, msg)
	}
}

// Invoke implements FuncCaller. It expects req to be a CapabilityInvokeRequest
// and routes to the matching callable. If req is not routed and there is exactly
// one callable registered, it is used as the default.
func (a *BaseActor) Invoke(ctx contracts.Expose, req interface{}) {
	if routed, ok := req.(model.CapabilityInvokeRequest); ok {
		a.InvokeCallable(ctx, routed.Route.CallableName, routed.Body)
		return
	}
	callables := a.schema.Callables
	if len(callables) == 1 {
		a.InvokeCallable(ctx, callables[0].Name, req)
		return
	}
	ctx.Error(model.ErrCapabilityRouteRequired)
}

// InvokeCallable dispatches to the handler registered for callable.
func (a *BaseActor) InvokeCallable(ctx contracts.Expose, callable string, req interface{}) {
	handler, ok := a.handlers[callable]
	if !ok {
		ctx.Error(model.ErrCapabilityCallableNotFound)
		return
	}
	handler(ctx, req)
}

// Protocols implements ProtocolProvider.
func (a *BaseActor) Protocols() []model.CallableProtocol {
	if len(a.schema.Callables) == 0 {
		return nil
	}
	out := make([]model.CallableProtocol, len(a.schema.Callables))
	copy(out, a.schema.Callables)
	return out
}

// CapabilitySchema implements CapabilityProvider.
func (a *BaseActor) CapabilitySchema() *model.CapabilitySchema {
	schema := a.schema
	if len(a.schema.Callables) > 0 {
		schema.Callables = make([]model.CallableProtocol, len(a.schema.Callables))
		copy(schema.Callables, a.schema.Callables)
	}
	if len(a.schema.Metadata) > 0 {
		schema.Metadata = make(map[string]string, len(a.schema.Metadata))
		for k, v := range a.schema.Metadata {
			schema.Metadata[k] = v
		}
	}
	return &schema
}

func (a *BaseActor) upsertCallableSchema(name string, mode model.InvocationMode) {
	for i := range a.schema.Callables {
		if a.schema.Callables[i].Name == name {
			a.schema.Callables[i].Mode = mode
			return
		}
	}
	a.schema.Callables = append(a.schema.Callables, model.CallableProtocol{
		Name: name,
		Mode: mode,
	})
}

func (a *BaseActor) removeCallableFromSchema(name string) {
	filtered := a.schema.Callables[:0]
	for _, c := range a.schema.Callables {
		if c.Name != name {
			filtered = append(filtered, c)
		}
	}
	a.schema.Callables = filtered
}

// BaseCapabilityActor is the legacy capability-oriented base. It delegates to
// BaseActor for all behaviour while keeping the old RegisterCallable signature
// (mode is hard-coded to ModeInvoke for compatibility).
type BaseCapabilityActor struct {
	BaseActor
}

// NewBaseCapabilityActor creates a new BaseCapabilityActor with the given schema.
func NewBaseCapabilityActor(schema model.CapabilitySchema) *BaseCapabilityActor {
	a := &BaseCapabilityActor{}
	a.BaseActor.RegisterSchema(schema)
	return a
}

// RegisterCallable registers one callable with ModeInvoke.
func (a *BaseCapabilityActor) RegisterCallable(callable string, handler CallableHandler) {
	a.BaseActor.RegisterCallable(callable, model.ModeInvoke, handler)
}
