// Package gateway is the HTTP entry point for external callers.
//
// Gateway provides an optional layer that sits between the network and the
// actor system: it handles HTTP protocol concerns, authentication, and
// user-defined interception before forwarding calls to business actors.
//
// GatewayInterceptor is the plug-in surface for cross-cutting policies
// (quota, rate limiting, billing, audit). The default is a no-op; users
// inject custom interceptors via app.WithGatewayHTTP.
package gateway

import (
	"context"
	"errors"
	"time"
)

// GatewayRequest carries the information available to an interceptor when
// an external HTTP request arrives.
//
// Target is an optional gospore ActorID (canonical 32-character lowercase hex).
// When non-empty the gateway resolves the request to the actor with that ID
// instead of the leading-segment Service. Used by callers addressing a
// specific actor instance rather than a service group.
//
// From is an optional gospore ActorID used for subtree-scoped service lookup.
// When Target is empty and From is set, the gateway walks the ancestor chain
// of the From actor looking for a scoped service matching Service. This lets
// external callers reach services registered with ExposeToChildren by naming
// a descendant actor in whose subtree the service is visible.
type GatewayRequest struct {
	CustomerID string
	Role       string
	Service    string
	CallID     string
	Target     string
	From       string
	Payload    any
	Headers    map[string]string
}

// GatewayResponse carries the result of a gateway-handled call, delivered
// to interceptors in the After hook.
type GatewayResponse struct {
	Body     any
	Error    error
	Duration time.Duration
}

// ErrGatewayDenied is the sentinel error returned by Before implementations
// when a request should be rejected with 403 Forbidden. Any other error type
// is treated as an internal failure and surfaced as 500.
var ErrGatewayDenied = errors.New("gateway: request denied")

// GatewayInterceptor is the user-customizable policy hook for external
// calls entering the App.
//
// Before runs synchronously before the business actor is invoked. A non-nil
// error aborts the call and the error is returned to the HTTP caller.
// Implementations should wrap ErrGatewayDenied to signal an auth/policy
// rejection (403); any other error is treated as an internal failure (500).
// After runs asynchronously (in its own goroutine) after the call completes,
// making it suitable for audit logging and billing without blocking the
// response path.
type GatewayInterceptor interface {
	Before(ctx context.Context, req *GatewayRequest) error
	After(ctx context.Context, req *GatewayRequest, resp *GatewayResponse)
}

// nopInterceptor is the default zero-overhead interceptor.
type nopInterceptor struct{}

func (n *nopInterceptor) Before(ctx context.Context, req *GatewayRequest) error { return nil }
func (n *nopInterceptor) After(ctx context.Context, req *GatewayRequest, resp *GatewayResponse) {
}

// Nop returns the default no-op interceptor. It is used when the user does
// not configure any interceptors.
func Nop() GatewayInterceptor { return &nopInterceptor{} }
