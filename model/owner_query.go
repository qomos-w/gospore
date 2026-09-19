package model

// OwnerQueryFilter controls which actors are returned by an owner query.
type OwnerQueryFilter struct {
	Owner  Principal
	States []ActorState
}
