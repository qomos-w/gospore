package contracts

// CallableDispatcher routes an explicitly-named callable invocation to its handler.
type CallableDispatcher interface {
	InvokeCallable(ctx Expose, callable string, req interface{})
}

// CapabilityActor is an actor that exposes a full capability schema and can
// dispatch explicitly-named callables.
type CapabilityActor interface {
	CapabilityProvider
	CallableDispatcher
}
