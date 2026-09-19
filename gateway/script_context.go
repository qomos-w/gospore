package gateway

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/spore/identity"
	"github.com/qomos-w/spore/script"
)

// gatewayScriptContext is the host-backed interface exposed to interceptor
// scripts. It is a slimmed-down version of cell.ScriptContext: it supports
// actor lookup and policy checks but omits lifecycle operations (Spawn,
// Stop, Watch) because the Gateway is not itself an actor.
type gatewayScriptContext struct {
	app      AppHost
	rt       *script.Runtime
	refCache map[string]cell.ActorRef
	// callCtx is the gateway request context active for the current
	// script execution (Before/After). Actor invocations started by the
	// script inherit its deadline; a request that is already done falls
	// back to Background bounded by GatewayInvokeTimeout.
	callCtx context.Context
	mu      sync.Mutex
}

// actorInvokeCtx derives the context for a synchronous actor invoke made
// from an interceptor script. It honors the request deadline when the
// request is still live, and otherwise applies GatewayInvokeTimeout as the
// bound.
func (g *gatewayScriptContext) actorInvokeCtx() (context.Context, context.CancelFunc) {
	g.mu.Lock()
	base := g.callCtx
	g.mu.Unlock()
	if base != nil {
		if _, hasDeadline := base.Deadline(); hasDeadline && base.Err() == nil {
			return base, func() {}
		}
	}
	return context.WithTimeout(context.Background(), GatewayInvokeTimeout)
}

// streamInvokeCtx derives the context for a stream invoke. Streams are not
// capped by GatewayInvokeTimeout (their lifetime is managed via the
// returned Stream), but they still die with the request when one is live.
func (g *gatewayScriptContext) streamInvokeCtx() context.Context {
	g.mu.Lock()
	base := g.callCtx
	g.mu.Unlock()
	if base != nil && base.Err() == nil {
		return base
	}
	return context.Background()
}

// setCallCtx records the request context for the duration of one script
// execution.
func (g *gatewayScriptContext) setCallCtx(ctx context.Context) {
	g.mu.Lock()
	g.callCtx = ctx
	g.mu.Unlock()
}

func (g *gatewayScriptContext) Self() string {
	if g.app.Self() == nil {
		return ""
	}
	return g.app.Self().ID().String()
}


func (g *gatewayScriptContext) LookupID(idStr string) cell.ActorRef {
	if g.app.Tree() == nil {
		return g.deadRef("actor tree unavailable")
	}
	cid, err := identity.ParseCanonicalID(idStr)
	if err != nil {
		return g.deadRef(fmt.Sprintf("parse actor id %q: %v", idStr, err))
	}
	r, ok := g.app.Tree().LookupID(id.From(cid))
	if !ok {
		return g.deadRef(fmt.Sprintf("actor %s not found", idStr))
	}
	return g.wrapRef(r)
}

func (g *gatewayScriptContext) LookupService(name string) cell.ActorRef {
	r, ok := g.app.LookupService(name)
	if !ok {
		return g.deadRef(fmt.Sprintf("service %q not found", name))
	}
	return g.wrapRef(r)
}

// deadRef returns a non-nil ActorRef whose invokes always fail. Host code
// must never return a nil ActorRef to scripts: method calls on nil host
// objects panic the VM ("invalid object handle").
func (g *gatewayScriptContext) deadRef(reason string) cell.ActorRef {
	adapter := &gatewayActorRefAdapter{gctx: g, deadReason: reason}
	if g.rt != nil {
		_ = g.rt.RegisterHostInterfaceInstance("gospore", "ActorRef", adapter)
	}
	return adapter
}

func (g *gatewayScriptContext) Namespace() string {
	return g.app.Namespace()
}

func (g *gatewayScriptContext) CheckPolicy(roleStr, scope string) actor.PolicyCheckResult {
	if g.app.PolicyStore() == nil {
		return actor.PolicyCheckResult{Allow: false, Found: false}
	}
	allow, found := g.app.PolicyStore().Evaluate(id.Role(roleStr), scope)
	return actor.PolicyCheckResult{Allow: allow, Found: found}
}

func (g *gatewayScriptContext) PolicyVersion() uint64 {
	if g.app.PolicyStore() == nil {
		return 0
	}
	return g.app.PolicyStore().Version()
}

func (g *gatewayScriptContext) wrapRef(r ref.Ref) cell.ActorRef {
	if r == nil {
		return nil
	}
	key := r.ID().String()
	g.mu.Lock()
	if g.refCache == nil {
		g.refCache = make(map[string]cell.ActorRef)
	}
	if cached, ok := g.refCache[key]; ok {
		g.mu.Unlock()
		return cached
	}
	adapter := &gatewayActorRefAdapter{targetRef: r, gctx: g}
	if g.rt != nil {
		_ = g.rt.RegisterHostInterfaceInstance("gospore", "ActorRef", adapter)
	}
	g.refCache[key] = adapter
	g.mu.Unlock()
	return adapter
}

// gatewayActorRefAdapter implements cell.ActorRef by delegating to ref.Ref.
// It runs in the Gateway process, not inside a Cell, so it uses the App's
// public Invoke surface rather than the low-level cell dispatch path.
// Invoke failures travel through cell.InvokeResult.Err (value channel);
// lookup misses produce a dead ref (deadReason set) instead of nil.
type gatewayActorRefAdapter struct {
	targetRef  ref.Ref
	gctx       *gatewayScriptContext
	deadReason string
}

func (a *gatewayActorRefAdapter) Invoke(callID string, payload any) cell.InvokeResult {
	if a.deadReason != "" {
		return cell.InvokeResult{Err: fmt.Sprintf("gateway script: invoke %s: %s", callID, a.deadReason)}
	}
	if a.targetRef == nil {
		return cell.InvokeResult{Err: fmt.Sprintf("gateway script: invoke %s: no target ref", callID)}
	}
	ctx, cancel := a.gctx.actorInvokeCtx()
	defer cancel()
	call := a.targetRef.Invoke(ctx, callID, payload)
	if call == nil {
		return cell.InvokeResult{Err: fmt.Sprintf("gateway script: invoke %s: no call returned", callID)}
	}
	defer call.Close()
	val, err := call.Recv()
	if err != nil {
		return cell.InvokeResult{Err: fmt.Sprintf("gateway script: invoke %s: %v", callID, err)}
	}
	if b, ok := val.([]byte); ok {
		// Same script-visible normalization as the cell host (string
		// under a configured codec).
		return cell.InvokeResult{Value: cell.ScriptNormalizeValue(b, true)}
	}
	return cell.InvokeResult{Value: cell.ScriptNormalizeValue(val, true)}
}

func (a *gatewayActorRefAdapter) InvokeStream(callID string, payload any) cell.Stream {
	if a.deadReason != "" {
		return cell.DeadStream{Reason: a.deadReason}
	}
	if a.targetRef == nil {
		return nil
	}
	call := a.targetRef.Invoke(a.gctx.streamInvokeCtx(), callID, payload)
	if call == nil {
		return nil
	}
	sa := &gatewayStreamAdapter{call: call}
	if a.gctx.rt != nil {
		_ = a.gctx.rt.RegisterHostInterfaceInstance("gospore", "Stream", sa)
	}
	return sa
}

// gatewayStreamAdapter bridges invoke.Call to cell.Stream.
type gatewayStreamAdapter struct {
	call *invoke.Call
}

func (s *gatewayStreamAdapter) Recv() cell.StreamChunk {
	if s.call == nil {
		return cell.StreamChunk{Err: "not found", Ok: true}
	}
	val, err := s.call.Recv()
	if err == io.EOF {
		return cell.StreamChunk{Ok: false}
	}
	if err != nil {
		return cell.StreamChunk{Err: err.Error(), Ok: true}
	}
	if b, ok := val.([]byte); ok {
		return cell.StreamChunk{Data: cell.ScriptNormalizeValue(b, true), Ok: true}
	}
	return cell.StreamChunk{Data: cell.ScriptNormalizeValue(val, true), Ok: true}
}

func (s *gatewayStreamAdapter) Close() bool {
	if s.call == nil {
		return false
	}
	return s.call.Close() == nil
}
