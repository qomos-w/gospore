package model

type ActorPlacement struct {
	Ref     ActorRef
	Runtime RuntimeID
	Version uint64
}

func NewActorPlacement(ref ActorRef, runtime RuntimeID) ActorPlacement {
	return ActorPlacement{Ref: ref, Runtime: runtime}
}

func (p *ActorPlacement) Update(runtime RuntimeID) {
	p.Runtime = runtime
	p.Version++
}

type ActorRoute struct {
	Ref     ActorRef
	Runtime RuntimeID
	Addr    RuntimeAddr
}

func NewActorRoute(ref ActorRef, runtime RuntimeID, addr RuntimeAddr) ActorRoute {
	return ActorRoute{Ref: ref, Runtime: runtime, Addr: addr}
}

func FromPlacement(p ActorPlacement, info RuntimeInfo) ActorRoute {
	return ActorRoute{Ref: p.Ref, Runtime: p.Runtime, Addr: info.Addr}
}
