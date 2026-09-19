package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/spore/identity"
)

// Per-request session actors, invoke wrappers, and target resolution.
func (s *Server) sessionGate(scope string) (ref.Ref, func()) {
	if s.app == nil {
		return nil, func() {}
	}
	name := fmt.Sprintf("gatesession-%s-%d-%04x", scope, s.gateSeq.Add(1), rand.Uint32()&0xffff)
	r, err := s.app.SpawnGatewaySession(name)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("gateway: gatesession spawn failed; falling back to root caller", "err", err)
		}
		return nil, func() {}
	}
	return r, func() {
		if err := s.app.DestroyGatewaySession(r); err != nil && s.logger != nil {
			s.logger.Warn("gateway: gatesession destroy failed", "err", err)
		}
	}
}

// invokeAs resolves the caller switch: gatesession ref when the gateway
// managed to spawn one, legacy bare-target invoke otherwise.
func (s *Server) invokeAs(caller, targetRef ref.Ref, ctx context.Context, callID string, payload any, hdrs map[string]string) *invoke.Call {
	if caller != nil {
		return s.app.InvokeAsCaller(caller, targetRef, callID, payload, hdrs)
	}
	return targetRef.Invoke(ctx, callID, payload, hdrs)
}

// invokeActor resolves the target actor for callID, invokes it, and returns
// the raw response body. Shared by HTTP and WebSocket handlers.
//
// If target is non-empty it is treated as a canonical ActorID hex string and
// addresses the actor with that ID directly; the service fallback is used
// only when target is empty. If from is non-empty, scoped service lookup is
// attempted from that actor before falling back to the global service.
func (s *Server) invokeActor(ctx context.Context, caller ref.Ref, callID, target, from string, payload any, customerID, role, subject string, timeoutMs int64) ([]byte, error) {
	service := serviceFromCallID(callID)
	targetRef, ok := s.resolveTarget(service, callID, target, from)
	if !ok {
		return nil, errServiceNotFound
	}
	hdrs := map[string]string{
		"gospore.caller_role": role,
	}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	if s.binaryOnly {
		hdrs["gospore.reply_encoding"] = "binary"
	}
	timeout := GatewayInvokeTimeout
	if timeoutMs > 0 {
		timeout = time.Duration(timeoutMs) * time.Millisecond
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, timeout)
	defer timeoutCancel()

	call := s.invokeAs(caller, targetRef, timeoutCtx, callID, payload, hdrs)
	if call == nil {
		return nil, errInvokeFailed
	}
	defer call.Close()

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-timeoutCtx.Done():
			call.Cancel()
		case <-done:
		}
	}()

	raw, err := call.RecvRaw()
	if errors.Is(err, invoke.ErrCallCancelled) {
		return nil, fmt.Errorf("invoke %s: gateway timeout after %s", callID, timeout)
	}
	return raw, err
}

// beginSubscription resolves the target and starts an actor invocation for
// streaming. The caller reads chunks from the returned Call.
// Shared by HTTP SSE and WebSocket handlers. caller is the connection's
// gatesession ref (nil falls back to root-caller semantics).
func (s *Server) beginSubscription(ctx context.Context, caller ref.Ref, callID, target, from string, payload any, customerID, role, subject string) (*invoke.Call, error) {
	service := serviceFromCallID(callID)
	targetRef, ok := s.resolveTarget(service, callID, target, from)
	if !ok {
		return nil, errServiceNotFound
	}
	hdrs := map[string]string{
		"gospore.caller_role": role,
	}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	if s.binaryOnly {
		hdrs["gospore.reply_encoding"] = "binary"
	}
	call := s.invokeAs(caller, targetRef, ctx, callID, payload, hdrs)
	if call == nil {
		return nil, errInvokeFailed
	}
	return call, nil
}

// resolveTarget finds the actor Ref for a service call.
//
// If target is non-empty it is parsed as a canonical ActorID hex string and
// looked up via the App's actor tree. A non-empty but unresolvable target
// fails the lookup outright (no fallback to service) so misrouting surfaces
// immediately rather than being masked.
//
// When target is empty and from is a valid actor ID, the tree is asked for
// the nearest scoped service visible to that caller. This lets external
// callers reach subtree-scoped services such as "project" by naming a
// descendant actor in whose ancestor chain the service is exposed.
//
// If neither target nor from resolves, the original behaviour is preserved:
// first try LookupService(service); then fall back to the App's root actor
// (Self).
func (s *Server) resolveTarget(service, callID, target, from string) (ref.Ref, bool) {
	if target != "" {
		cid, err := identity.ParseCanonicalID(target)
		if err != nil {
			return nil, false
		}
		if r, ok := s.app.Tree().LookupID(id.From(cid)); ok {
			return r, true
		}
		return nil, false
	}
	if from != "" && service != "" {
		cid, err := identity.ParseCanonicalID(from)
		if err == nil {
			if callerRef, ok := s.app.Tree().LookupID(id.From(cid)); ok {
				if r, ok := s.app.Tree().LookupScopedService(callerRef, service); ok {
					return r, true
				}
			}
		}
	}
	if service != "" {
		if r, ok := s.app.LookupService(service); ok {
			return r, true
		}

	}
	if self := s.app.Self(); self != nil {
		return self, true
	}
	return nil, false
}

// withCORS wraps a handler to allow cross-origin requests from any origin.
// This lets desktop webviews (wails://, file://, etc.) reach the gateway.
