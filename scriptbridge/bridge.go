// Package scriptbridge is the gospore → spore capability adapter:
// it wraps gospore's runtime surfaces (projection, events, app
// topology, invoke) as spore binding.RegisteredCapability values so
// in-actor scripts can call gospore.<namespace>.<method>(...) the same
// way TS / JS clients call them through gospore-ts.
//
// ScriptBridge is a *metadata* layer, not a proxy layer — it describes
// gospore capabilities to spore but does not dispatch the calls.
// Real invocation still flows through Ref.Invoke → Cell → handler.
//
// Five capability namespaces (see ARCHITECTURE.md §4.19 for the full
// method table):
//
//	gospore.projection — get / field (unary), watch (streaming)
//	gospore.events     — recent (unary), subscribe_service / subscribe_instance (streaming)
//	gospore.app        — lookup_path / lookup_service (unary), tree (streaming)
//	gospore.policy     — check (unary), version (unary)
//	gospore.invoke     — dynamic; one method per registered callID
package scriptbridge

import (
	"slices"

	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/spore/binding"
	"github.com/qomos-w/spore/schema"
)

// Bridge holds a fixed set of spore capabilities derived from an
// App. Build per App; share across script runtimes belonging to that
// App. The dynamic gospore.invoke namespace is rebuilt on each Build
// (i.e. after any callID registration / reload).
type Bridge struct {
	caps []binding.RegisteredCapability
}

// Build constructs the four-namespace Bridge for a. The returned
// Bridge is read-only after construction. Call again after any
// callID registration to refresh the dynamic gospore.invoke
// namespace.
func Build(a app.App) *Bridge {
	caps := []binding.RegisteredCapability{
		newCapability("gospore.projection", []StaticMethod{
			{ID: StaticProjectionGet, Mode: ExecutionUnary},
			{ID: StaticProjectionField, Mode: ExecutionUnary},
			{ID: StaticProjectionWatch, Mode: ExecutionStreaming},
		}),
		newCapability("gospore.events", []StaticMethod{
			{ID: StaticEventsSubscribeService, Mode: ExecutionStreaming},
			{ID: StaticEventsSubscribeInstance, Mode: ExecutionStreaming},
			{ID: StaticEventsRecent, Mode: ExecutionUnary},
		}),
		newCapability("gospore.app", []StaticMethod{
			{ID: StaticAppLookupPath, Mode: ExecutionUnary},
			{ID: StaticAppLookupService, Mode: ExecutionUnary},
			{ID: StaticAppTree, Mode: ExecutionStreaming},
		}),
		newPolicyCapability(a),
		newInvokeCapability(a),
	}
	return &Bridge{caps: caps}
}

// Caps returns every RegisteredCapability the Bridge owns, suitable
// for spore ScriptBinding.Mount. Callers must not mutate the
// returned slice.
func (b *Bridge) Caps() []binding.RegisteredCapability {
	if b == nil || len(b.caps) == 0 {
		return nil
	}
	out := make([]binding.RegisteredCapability, len(b.caps))
	for i, cap := range b.caps {
		out[i] = cloneCapability(cap)
	}
	return out
}

func newCapability(name string, methods []StaticMethod) binding.RegisteredCapability {
	cap := binding.RegisteredCapability{
		Desc: binding.CapabilityDesc{
			Name:      name,
			Kind:      "native",
			Callables: make([]schema.CallableDesc, 0, len(methods)),
			Metadata:  map[string]string{},
		},
		Callables: map[string]binding.CapabilityCallable{},
		Values:    map[string]any{},
	}
	for _, method := range methods {
		cap.Desc.Callables = append(cap.Desc.Callables, schema.CallableDesc{
			Name: methodName(method.ID),
		})
	}
	return cap
}

func newInvokeCapability(a app.App) binding.RegisteredCapability {
	cap := newCapability("gospore.invoke", nil)
	host, ok := a.(Host)
	if !ok {
		return cap
	}
	tbl := host.HandlerTable()
	callIDs := tbl.CallIDs()
	slices.Sort(callIDs)
	for _, callID := range callIDs {
		desc, ok := tbl.Desc(callID)
		if !ok {
			continue
		}
		cap.Desc.Callables = append(cap.Desc.Callables, schema.CloneCallableDesc(desc))
	}
	return cap
}

func newPolicyCapability(a app.App) binding.RegisteredCapability {
	return newCapability("gospore.policy", []StaticMethod{
		{ID: StaticPolicyCheck, Mode: ExecutionUnary},
		{ID: StaticPolicyVersion, Mode: ExecutionUnary},
	})
}

func methodName(id string) string {
	switch id {
	case StaticProjectionGet:
		return "get"
	case StaticProjectionField:
		return "field"
	case StaticProjectionWatch:
		return "watch"
	case StaticEventsSubscribeService:
		return "subscribe_service"
	case StaticEventsSubscribeInstance:
		return "subscribe_instance"
	case StaticEventsRecent:
		return "recent"
	case StaticAppLookupPath:
		return "lookup_path"
	case StaticAppLookupService:
		return "lookup_service"
	case StaticAppTree:
		return "tree"
	case StaticPolicyCheck:
		return "check"
	case StaticPolicyVersion:
		return "version"
	default:
		return ""
	}
}

func cloneCapability(cap binding.RegisteredCapability) binding.RegisteredCapability {
	out := binding.RegisteredCapability{
		Desc: binding.CapabilityDesc{
			Name:      cap.Desc.Name,
			Kind:      cap.Desc.Kind,
			Callables: make([]schema.CallableDesc, len(cap.Desc.Callables)),
			Objects:   append([]schema.ObjectDesc(nil), cap.Desc.Objects...),
			Values:    append([]binding.CapabilityValueDesc(nil), cap.Desc.Values...),
			Pipelines: append([]binding.PipelineDesc(nil), cap.Desc.Pipelines...),
		},
		Callables: map[string]binding.CapabilityCallable{},
		Values:    map[string]any{},
	}
	for i, callable := range cap.Desc.Callables {
		out.Desc.Callables[i] = schema.CloneCallableDesc(callable)
	}
	if len(cap.Desc.Metadata) != 0 {
		out.Desc.Metadata = make(map[string]string, len(cap.Desc.Metadata))
		for k, v := range cap.Desc.Metadata {
			out.Desc.Metadata[k] = v
		}
	}
	if len(cap.Desc.TypeAliases) != 0 {
		out.Desc.TypeAliases = make(map[string]schema.TypeDesc, len(cap.Desc.TypeAliases))
		for k, v := range cap.Desc.TypeAliases {
			out.Desc.TypeAliases[k] = v
		}
	}
	for k, v := range cap.Callables {
		out.Callables[k] = v
	}
	for k, v := range cap.Values {
		out.Values[k] = v
	}
	return out
}
