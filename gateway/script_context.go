package gateway

import (
	"context"
	"io"
	"sync"
	"time"

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
	mu       sync.Mutex
}

func (g *gatewayScriptContext) Self() string {
	if g.app.Self() == nil {
		return ""
	}
	return g.app.Self().ID().String()
}


func (g *gatewayScriptContext) LookupID(idStr string) cell.ActorRef {
	if g.app.Tree() == nil {
		return nil
	}
	cid, err := identity.ParseCanonicalID(idStr)
	if err != nil {
		return nil
	}
	r, ok := g.app.Tree().LookupID(id.From(cid))
	if !ok {
		return nil
	}
	return g.wrapRef(r)
}

func (g *gatewayScriptContext) LookupService(name string) cell.ActorRef {
	r, ok := g.app.LookupService(name)
	if !ok {
		return nil
	}
	return g.wrapRef(r)
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
	adapter := &gatewayActorRefAdapter{targetRef: r, app: g.app, rt: g.rt}
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
type gatewayActorRefAdapter struct {
	targetRef ref.Ref
	app       AppHost
	rt        *script.Runtime
}

func (a *gatewayActorRefAdapter) Invoke(callID string, payload any) any {
	if a.targetRef == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	call := a.targetRef.Invoke(ctx, callID, payload)
	if call == nil {
		return nil
	}
	defer call.Close()
	val, err := call.Recv()
	if err != nil {
		return nil
	}
	return val
}

func (a *gatewayActorRefAdapter) InvokeStream(callID string, payload any) cell.Stream {
	if a.targetRef == nil {
		return nil
	}
	// Actor invocations do not use context timeouts; lifetime is managed
	// by the caller via the returned Stream's Close/Cancel methods.
	call := a.targetRef.Invoke(context.Background(), callID, payload)
	if call == nil {
		return nil
	}
	sa := &gatewayStreamAdapter{call: call}
	if a.rt != nil {
		_ = a.rt.RegisterHostInterfaceInstance("gospore", "Stream", sa)
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
	return cell.StreamChunk{Data: val, Ok: true}
}

func (s *gatewayStreamAdapter) Close() bool {
	if s.call == nil {
		return false
	}
	return s.call.Close() == nil
}
