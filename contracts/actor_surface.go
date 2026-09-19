package contracts

import "github.com/qomos-w/gospore/model"

// Identity exposes the runtime-assigned identity of the current actor.
type Identity interface {
	Self() model.ActorRef
	Sender() model.ActorRef
	TransID() uint32
}

// InvocationExpose exposes actor-local invocation operations.
type InvocationExpose interface {
	Invoke(to model.ActorRef, req interface{}) Stream
	Emit(msg interface{})
	Done()
	Error(err error)
}

// Children exposes actor-local child lifecycle operations.
type Children interface {
	Spawn(spec model.ActorSpec) model.ActorRef
	StopChild(ref model.ActorRef)
}

// Watch exposes death-watch operations.
type Watch interface {
	Watch(ref model.ActorRef)
	Unwatch(ref model.ActorRef)
}

// Expose is the caller-facing actor surface injected by the runtime.
type Expose interface {
	Identity
	InvocationExpose
	Children
	Watch
}

// FuncExpose extends the base actor surface with invocation streaming controls.
type FuncExpose interface {
	Expose
}
