package contracts

import "github.com/qomos-w/gospore/model"

// ProtocolRegistry stores and resolves callable protocol descriptors by actor
// type and callable name.
type ProtocolRegistry interface {
	// Register stores the callable protocols for one actor type.
	Register(actorType string, protocols []model.CallableProtocol)

	// RegisterSchema stores the capability schema for one actor type.
	RegisterSchema(actorType string, schema model.CapabilitySchema)

	// Lookup finds one callable protocol by its callable name.
	Lookup(callableName string) (model.CallableProtocol, bool)

	// LookupSchema finds one capability schema by schema identity.
	LookupSchema(schemaID, version string) (model.CapabilitySchema, bool)

	// LookupRoute finds one callable protocol by schema-aware route key.
	LookupRoute(key model.CapabilityRouteKey) (model.CallableProtocol, bool)

	// ForActor returns all protocols registered for one actor type.
	ForActor(actorType string) []model.CallableProtocol

	// RoutesForActor returns all schema-aware route keys for one actor type.
	RoutesForActor(actorType string) []model.CapabilityRouteKey

	// All returns every registered callable protocol across all actor types.
	All() []model.CallableProtocol
}

// CapabilityProvider allows an actor to declare its full capability schema at runtime.
type CapabilityProvider interface {
	CapabilitySchema() *model.CapabilitySchema
}
