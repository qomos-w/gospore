package actor

// Compile-time execution-mode registration.
//
// The reflection-based ctx.Register infers the execution mode from the
// handler's first parameter type (Context → stateful, PureContext →
// stateless). That inference has no compile-time guarantee: a handler
// written for one mode but declared with the other context type compiles
// fine and silently runs on the wrong lane — a stateless-lane handler
// touching actor state races the owner lane without any diagnostic.
//
// The generic functions below pin the mode in their signatures: the
// handler's context parameter type must match the function exactly, so a
// mode mismatch is a compile error. They are thin sugar over the same
// registration pipeline — schema extraction, options, and dispatch are
// unchanged; only the mode is no longer left to reflection to guess.

// RegisterStateful registers a unary handler that runs on the actor's
// serialized owner lane and may read or mutate actor state.
//
//	f	err := actor.RegisterStateful[MyReq, MyResp](ctx, "svc.call",
//		func(ctx actor.Context, req MyReq) (MyResp, error) { ... })
//
// The closure's first parameter must be actor.Context; passing a
// PureContext handler does not compile.
func RegisterStateful[Req, Resp any](ctx Registrar, callID string, h func(ctx Context, req Req) (Resp, error), opts ...RegisterOption) error {
	return ctx.Register(callID, h, opts...)
}

// RegisterStateless registers a unary handler that runs on a fork
// goroutine outside the actor's serialization and must not touch actor
// state.
//
//	f	err := actor.RegisterStateless[MyReq, MyResp](ctx, "svc.pure",
//		func(ctx actor.PureContext, req MyReq) (MyResp, error) { ... })
//
// The closure's first parameter must be actor.PureContext; passing a
// Context handler does not compile.
func RegisterStateless[Req, Resp any](ctx Registrar, callID string, h func(ctx PureContext, req Req) (Resp, error), opts ...RegisterOption) error {
	return ctx.Register(callID, h, opts...)
}
