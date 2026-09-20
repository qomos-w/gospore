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
// Concurrency model: a single spore Runtime is not safe for concurrent
// Call, so the interceptor maintains a pool of independently-loaded
// runtimes (default interceptorPoolSize). Concurrent requests execute on
// distinct VMs — a slow actor invoke inside one script no longer
// serializes every other request behind a global lock.
//
// State: each pooled VM keeps its own globals, so script-level state is
// per-slot, not global. Quota, rate-limit counters, and any other state
// shared across requests must live in Go actors; the interceptor script
// is purely for policy orchestration.
//
// Errors: failed actor invokes return an InvokeResult whose err field
// carries the failure (a value channel, mirroring StreamChunk for
// streams — spore host methods cannot return Go errors). Scripts check
// result.err instead of guessing from nil. Actor invoke timeouts are
// governed by GatewayInvokeTimeout (or the request deadline when live).
type ScriptInterceptor struct {
	app AppHost

	mu     sync.Mutex // guards source/gen and pool draining
	source string
	gen    uint64

	slots chan struct{}                // bounds concurrent script executions
	pool  chan *interceptorRuntimeItem // reusable, per-VM runtime items
}

// interceptorPoolSize caps how many script executions (and thus spore VMs)
// may run concurrently per ScriptInterceptor. Requests beyond the cap wait
// for a free slot (bounded by their request context) instead of piling
// unbounded VMs.
const interceptorPoolSize = 8

// interceptorRuntimeItem couples a spore runtime with its own script
// context and the source generation it was loaded with.
type interceptorRuntimeItem struct {
	rt   *script.Runtime
	gctx *gatewayScriptContext
	gen  uint64
}

// NewScriptInterceptor creates a ScriptInterceptor from source text. The
// source is validated eagerly: a compile error fails construction.
func NewScriptInterceptor(app AppHost, source string) (*ScriptInterceptor, error) {
	s := &ScriptInterceptor{
		app:   app,
		slots: make(chan struct{}, interceptorPoolSize),
		pool:  make(chan *interceptorRuntimeItem, interceptorPoolSize),
	}
	s.mu.Lock()
	s.source = source
	s.mu.Unlock()
	item, err := s.newItem(source)
	if err != nil {
		return nil, err
	}
	s.pool <- item
	return s, nil
}

// newItem builds a fresh runtime item: independent spore runtime, its own
// gateway script context, full capability binding, and the given source
// loaded.
func (s *ScriptInterceptor) newItem(source string) (*interceptorRuntimeItem, error) {
	rt, err := script.NewRuntime()
	if err != nil {
		return nil, fmt.Errorf("gateway script interceptor: create runtime: %w", err)
	}
	gctx := &gatewayScriptContext{app: s.app, rt: rt}
	if err := bindGatewayScriptCapabilities(rt, gctx); err != nil {
		_ = rt.Close()
		return nil, fmt.Errorf("gateway script interceptor: bind capabilities: %w", err)
	}
	if err := rt.LoadSource("interceptor", source); err != nil {
		_ = rt.Close()
		return nil, fmt.Errorf("gateway script interceptor: load source: %w", err)
	}
	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()
	return &interceptorRuntimeItem{rt: rt, gctx: gctx, gen: gen}, nil
}

// acquire takes an execution slot (honoring ctx cancellation) and returns
// a pooled runtime item, creating a fresh one when the pool is empty.
func (s *ScriptInterceptor) acquire(ctx context.Context) (*interceptorRuntimeItem, error) {
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("gateway script interceptor: waiting for script slot: %w", ctx.Err())
	}
	select {
	case item := <-s.pool:
		s.mu.Lock()
		gen := s.gen
		s.mu.Unlock()
		if item.gen != gen {
			// Stale source (Reload happened); discard and build fresh.
			_ = item.rt.Close()
			item, err := s.newItemFromCurrent()
			if err != nil {
				s.releaseSlot()
				return nil, err
			}
			return item, nil
		}
		return item, nil
	default:
		item, err := s.newItemFromCurrent()
		if err != nil {
			s.releaseSlot()
			return nil, err
		}
		return item, nil
	}
}

// newItemFromCurrent loads the current source into a fresh runtime item.
func (s *ScriptInterceptor) newItemFromCurrent() (*interceptorRuntimeItem, error) {
	s.mu.Lock()
	source := s.source
	s.mu.Unlock()
	return s.newItem(source)
}

// release returns an item to the pool (unless it is stale or the pool is
// full) and frees the execution slot.
func (s *ScriptInterceptor) release(item *interceptorRuntimeItem) {
	defer s.releaseSlot()
	if item == nil {
		return
	}
	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()
	if item.gen != gen {
		_ = item.rt.Close()
		return
	}
	select {
	case s.pool <- item:
	default:
		_ = item.rt.Close()
	}
}

func (s *ScriptInterceptor) releaseSlot() {
	select {
	case <-s.slots:
	default:
	}
}

// Before runs the script's before function on a pooled runtime.
// A non-empty string return aborts the gateway request with ErrGatewayDenied.
// Script runtime or execution errors are returned as-is (surfaced as 500).
func (s *ScriptInterceptor) Before(ctx context.Context, req *GatewayRequest) error {
	item, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer s.release(item)

	item.gctx.setCallCtx(ctx)
	defer item.gctx.setCallCtx(nil)

	result, err := item.rt.Call("before", item.gctx, gatewayRequestToMap(req))
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

// After runs the script's after function on a pooled runtime.
// Errors are swallowed; After must not block the HTTP response path.
func (s *ScriptInterceptor) After(ctx context.Context, req *GatewayRequest, resp *GatewayResponse) {
	// After runs after the response is computed; the request context may
	// already be done, so slot acquisition waits unboundedly instead of
	// being aborted by ctx cancellation.
	item, err := s.acquire(context.Background())
	if err != nil {
		return
	}
	defer s.release(item)

	item.gctx.setCallCtx(ctx)
	defer item.gctx.setCallCtx(nil)

	_, _ = item.rt.Call("after", item.gctx, gatewayRequestToMap(req), gatewayResponseToMap(resp))
}

// Reload hot-swaps the interceptor script: it bumps the source
// generation, drains pooled runtimes, and validates the new source
// eagerly by loading one fresh runtime.
func (s *ScriptInterceptor) Reload(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate the new source eagerly on a fresh runtime.
	rt, err := script.NewRuntime()
	if err != nil {
		return fmt.Errorf("gateway script interceptor: create runtime: %w", err)
	}
	gctx := &gatewayScriptContext{app: s.app, rt: rt}
	if err := bindGatewayScriptCapabilities(rt, gctx); err != nil {
		_ = rt.Close()
		return fmt.Errorf("gateway script interceptor: bind capabilities: %w", err)
	}
	if err := rt.LoadSource("interceptor", source); err != nil {
		_ = rt.Close()
		return fmt.Errorf("gateway script interceptor: load source: %w", err)
	}

	s.source = source
	s.gen++
	item := &interceptorRuntimeItem{rt: rt, gctx: gctx, gen: s.gen}

	// Drain stale pooled items; in-flight stale ones are closed lazily
	// on release.
	for {
		select {
		case old := <-s.pool:
			_ = old.rt.Close()
			continue
		default:
		}
		break
	}
	s.pool <- item
	return nil
}

// bindGatewayScriptCapabilities registers the minimal gospore capability
// surface that interceptor scripts need.
func bindGatewayScriptCapabilities(rt *script.Runtime, gctx *gatewayScriptContext) error {
	// StreamChunk — used by stream adapters.
	if err := rt.BindStruct("gospore", "StreamChunk", cell.StreamChunk{}); err != nil {
		return fmt.Errorf("bind StreamChunk: %w", err)
	}

	// InvokeResult — the value-channel outcome of unary actor invokes.
	if err := rt.BindStruct("gospore", "InvokeResult", cell.InvokeResult{}); err != nil {
		return fmt.Errorf("bind InvokeResult: %w", err)
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
				Returns: []schema.TypeDesc{{Kind: schema.TypeKindStruct, Name: "InvokeResult"}},
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
