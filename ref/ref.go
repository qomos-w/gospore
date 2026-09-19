// Package ref defines the cross-actor reference type. Ref is the only
// way one actor reaches another: it carries identity (ActorID), the
// stable address (Path), the optional service role (from ctx.Expose),
// and the unified Invoke entry point.
//
// Refs are produced by gospore (Spawn / Lookup); user code never
// constructs a Ref directly.
package ref

import (
	"context"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
)

// Ref is a reference to another actor. It is opaque from the caller's
// perspective: the only intended operation is Invoke.
type Ref interface {
	// ID returns the canonical 128-bit identifier of the target actor.
	// Uniquely identifies the actor instance within an App run.
	ID() id.ActorID

	// Service reports the service role the target self-declared via
	// ctx.Expose. If the target has not Exposed, ok is false.
	Service() (name string, ok bool)

	// Invoke is the single entry point for actor-to-actor communication.
	//
	// payload accepts two equivalent forms:
	//   - a Go value of the registered request type T (gospore encodes
	//     it via the App's codec, using the schema looked up from the
	//     callable's CallableDesc input).
	//   - a []byte already encoded for the callable's input schema; it
	//     is passed through as Frame.Body unchanged.
	//
	// Optional headers maps are merged into the outgoing Frame.Headers
	// field, allowing transport-layer metadata (e.g. caller_role) to
	// cross the invocation boundary. Multiple maps are merged left-to-
	// right; later values overwrite earlier ones.
	//
	// The returned Stream's consumption mode depends on the callable:
	//   - Close immediately without Recv ⇒ fire-and-forget.
	//   - Recv once ⇒ unary; subsequent Recv returns io.EOF.
	//   - Recv repeatedly until io.EOF ⇒ streaming.
	//
	// Creation-time failures (callID unregistered, target_dead,
	// serialization error) surface from the first Recv / RecvRaw on the
	// returned Stream, NOT from Invoke itself.
	Invoke(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call
}
