package gateway

import (
	"context"
	"fmt"
	"sync"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/spore/schema"
	"github.com/qomos-w/spore/script"
)

// ScriptInterceptor is a GatewayInterceptor that delegates Before/After
// logic to a Spore script. The script must export two functions:
//
//	export fun before(ctx: Context, req: any): string
//	export fun after(ctx: Context, req: any, resp: any): string
//
// The script receives a simplified gospore.Context (no Spawn/Stop/Watch)
// and can call actors via ctx.lookup_service(...).invoke(...).
//
// State management (quota, rate limit counters) should live in Go actors;
// the interceptor script is purely for policy orchestration.
type ScriptInterceptor struct {
	app    AppHost
	rt     *script.Runtime
	mu     sync.Mutex
	ctx    *gatewayScriptContext
	source string
}

// NewScriptInterceptor creates a ScriptInterceptor from source text.
// The runtime is created once and guarded by a mutex so concurrent
// requests do not race on VM state.
func NewScriptInterceptor(app AppHost, source string) (*ScriptInterceptor, error) {
	rt, err := script.NewRuntime()
	if err != nil {
		return nil, fmt.Errorf("gateway script interceptor: create runtime: %w", err)
	}

	gctx := &gatewayScriptContext{app: app, rt: rt}
	if err := bindGatewayScriptCapabilities(rt, gctx); err != nil {
		return nil, fmt.Errorf("gateway script interceptor: bind capabilities: %w", err)
	}

	if err := rt.LoadSource("interceptor", source); err != nil {
		return nil, fmt.Errorf("gateway script interceptor: load source: %w", err)
	}

	return &ScriptInterceptor{
		app:    app,
		rt:     rt,
		ctx:    gctx,
		source: source,
	}, nil
}

// Before runs the script's before function under the runtime lock.
// A non-empty string return aborts the gateway request with ErrGatewayDenied.
// Script runtime or execution errors are returned as-is (surfaced as 500).
func (s *ScriptInterceptor) Before(ctx context.Context, req *GatewayRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.rt.Call("before", s.ctx, gatewayRequestToMap(req))
	if err != nil {
		return fmt.Errorf("gateway script interceptor: before runtime error: %w", err)
	}
	if result.Error != nil {
		return fmt.Errorf("gateway script interceptor: before script error: %v", result.Error)
	}
	if result.Ok() && result.Value != nil {
		if msg, ok := result.Value.(string); ok {
			if msg != "" {
				return fmt.Errorf("%w: %s", ErrGatewayDenied, msg)
			}
			return nil // empty string means allow
		}
		return fmt.Errorf("gateway script interceptor: before returned non-string %T", result.Value)
	}
	return nil
}

// After runs the script's after function under the runtime lock.
// Errors are swallowed; After must not block the HTTP response path.
func (s *ScriptInterceptor) After(ctx context.Context, req *GatewayRequest, resp *GatewayResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, _ = s.rt.Call("after", s.ctx, gatewayRequestToMap(req), gatewayResponseToMap(resp))
}

// Reload hot-swaps the interceptor script without recreating the runtime.
func (s *ScriptInterceptor) Reload(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear cached refs — actors may have been restarted with new IDs.
	s.ctx.mu.Lock()
	s.ctx.refCache = nil
	s.ctx.mu.Unlock()

	if err := s.rt.Reset(); err != nil {
		return fmt.Errorf("gateway script interceptor: reset runtime: %w", err)
	}
	if err := bindGatewayScriptCapabilities(s.rt, s.ctx); err != nil {
		return fmt.Errorf("gateway script interceptor: rebind capabilities: %w", err)
	}
	if err := s.rt.LoadSource("interceptor", source); err != nil {
		return fmt.Errorf("gateway script interceptor: load source: %w", err)
	}
	s.source = source
	return nil
}

// bindGatewayScriptCapabilities registers the minimal gospore capability
// surface that interceptor scripts need.
func bindGatewayScriptCapabilities(rt *script.Runtime, gctx *gatewayScriptContext) error {
	// StreamChunk — used by stream adapters.
	if err := rt.BindStruct("gospore", "StreamChunk", cell.StreamChunk{}); err != nil {
		return fmt.Errorf("bind StreamChunk: %w", err)
	}

	// PolicyCheckResult — used by check_policy.
	if err := rt.BindStruct("gospore", "PolicyCheckResult", actor.PolicyCheckResult{}); err != nil {
		return fmt.Errorf("bind PolicyCheckResult: %w", err)
	}

	// ActorRef interface.
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
	if err := rt.BindInterfaceObject("gospore", "ActorRef", actorRefDesc, &gatewayActorRefAdapter{}); err != nil {
		return fmt.Errorf("bind ActorRef: %w", err)
	}

	// Stream interface.
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
	if err := rt.BindInterfaceObject("gospore", "Stream", streamDesc, &gatewayStreamAdapter{}); err != nil {
		return fmt.Errorf("bind Stream: %w", err)
	}

	// ScriptContext interface (gateway variant — no Spawn/Stop/Watch/After/Plan).
	scriptCtxDesc := schema.InterfaceDesc{
		Methods: []schema.MethodDesc{
			{
				Name:    "self",
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}},
			},
			{
				Name:    "namespace",
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "string"}},
			},
			{
				Name:       "lookup",
				Parameters: []schema.ParameterDesc{{Name: "path", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}}},
				Returns:    []schema.TypeDesc{{Kind: schema.TypeKindClass, ClassName: "ActorRef"}},
			},
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
				Name: "check_policy",
				Parameters: []schema.ParameterDesc{
					{Name: "role", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
					{Name: "scope", Type: schema.TypeDesc{Kind: schema.TypeKindScalar, Name: "string"}},
				},
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindStruct, Name: "PolicyCheckResult"}},
			},
			{
				Name:    "policy_version",
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindScalar, Name: "int"}},
			},
		},
	}
	if err := rt.BindInterfaceObject("gospore", "Context", scriptCtxDesc, gctx); err != nil {
		return fmt.Errorf("bind Context: %w", err)
	}

	return nil
}

// gatewayRequestToMap converts a GatewayRequest into a map[string]any that
// Spore scripts can consume as a map value.
func gatewayRequestToMap(req *GatewayRequest) map[string]any {
	if req == nil {
		return nil
	}
	return map[string]any{
		"customer_id": req.CustomerID,
		"role":        req.Role,
		"service":     req.Service,
		"call_id":     req.CallID,
		"payload":     req.Payload,
		"headers":     req.Headers,
	}
}

// gatewayResponseToMap converts a GatewayResponse into a map[string]any.
func gatewayResponseToMap(resp *GatewayResponse) map[string]any {
	if resp == nil {
		return nil
	}
	m := map[string]any{
		"body":      resp.Body,
		"duration":  int(resp.Duration.Milliseconds()),
		"error_msg": "",
	}
	if resp.Error != nil {
		m["error_msg"] = resp.Error.Error()
	}
	return m
}
