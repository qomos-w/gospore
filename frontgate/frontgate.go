// Package frontgate provides a standard boundary actor for gospore apps.
// It exposes frontgate.invoke, consumed by @qomos/gospore-client:
//
//   frontgate.invoke (unary) — forwards calls to target actors
//
// Streaming calls are invoked directly against the target actor; the
// generated client.ts emits typed subscribe() wrappers that bypass
// frontgate for lower latency and higher concurrency.
//
// Auth is pluggable via AuthProvider. A nil provider means anonymous.
package frontgate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/spore/identity"
)

const defaultInvokeTimeout = 30 * time.Second

// Actor is the boundary actor. Embed it in your app and pass it to Spawn.
type Actor struct {
	actor.Host
	cfg Config
}

// Config holds runtime dependencies for the frontgate actor.
type Config struct {
	// AuthProvider validates tokens. When nil, NopAuthProvider is used.
	AuthProvider AuthProvider
}

// New creates a frontgate Actor bound to cfg.
func New(cfg Config) *Actor {
	if cfg.AuthProvider == nil {
		cfg.AuthProvider = NopAuthProvider{}
	}
	return &Actor{cfg: cfg}
}

func (a *Actor) Type() string { return "frontgate" }

// OnStart registers frontgate.invoke.
func (a *Actor) OnStart(ctx actor.Context) error {
	if err := ctx.Register("frontgate.invoke", a.handleInvoke); err != nil {
		return fmt.Errorf("frontgate: register invoke: %w", err)
	}
	return nil
}

// ------------------------------------------------------------------
// Request types (mirrored in the generated TS client)
// ------------------------------------------------------------------

// InvokeReq is the payload for frontgate.invoke.
type InvokeReq struct {
	Target  string `json:"target"`  // service name or path
	CallID  string `json:"callID"`
	Payload any    `json:"payload"`
	Token   string `json:"_token,omitempty"`
}

// ------------------------------------------------------------------
// Handlers
// ------------------------------------------------------------------

func (a *Actor) handleInvoke(ctx actor.Context, req InvokeReq) (any, error) {
	invokeCtx, cancel := context.WithTimeout(ctx.Lifecycle(), defaultInvokeTimeout)
	defer cancel()

	identity, err := a.cfg.AuthProvider.Authenticate(invokeCtx, req.Token)
	if err != nil {
		return nil, fmt.Errorf("frontgate.auth: %w", err)
	}

	target := a.resolveTarget(ctx, req.Target)
	if target == nil {
		return nil, fmt.Errorf("frontgate: target %q not found", req.Target)
	}

	headers := map[string]string{
		"gospore.caller_role": string(identity.Role),
		"gospore.caller_subject": identity.Subject,
		"gospore.caller_kind": identity.Kind.String(),
	}
	call := target.Invoke(invokeCtx, req.CallID, req.Payload, headers)
	if call == nil {
		return nil, fmt.Errorf("frontgate: invoke %q on %q returned nil", req.CallID, req.Target)
	}
	defer call.Close()

	val, err := call.Final(invokeCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			call.Cancel()
		}
		return nil, fmt.Errorf("frontgate: %w", err)
	}
	return val, nil
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

func (a *Actor) resolveTarget(ctx actor.Context, target string) ref.Ref {
	if r, ok := ctx.LookupService(target); ok {
		return r
	}
	cid, err := identity.ParseCanonicalID(target)
	if err != nil {
		return nil
	}
	if r, ok := ctx.LookupID(id.From(cid)); ok {
		return r
	}
	return nil
}
