package schema

import (
	spore "github.com/qomos-w/spore/schema"
)

// GosporeManifest extends spore.Manifest with gospore-specific contract
// surfaces: projection snapshots and event streams.
//
// The JSON wire format is a superset of the spore combined-form manifest
// so that spore-gen-ts can consume the base layers (schemas + callables)
// while gospore-gen-ts adds projections + events.
type GosporeManifest struct {
	spore.Manifest
	Projections []ProjectionDecl `json:"projections,omitempty"`
	Events      []EventDecl      `json:"events,omitempty"`
}

// ProjectionDecl describes one component field tagged with
// `gospore:"component"` that is published as a projection snapshot.
type ProjectionDecl struct {
	// Namespace is the wire namespace for the projection type (typically
	// the actor's path prefix, e.g. "skillmanager").
	Namespace string `json:"namespace"`
	// ActorPath is the actor's tree path (e.g. "/skillmanager").
	ActorPath string `json:"actorPath"`
	// Component is the Go field name (e.g. "Skills").
	Component string `json:"component"`
	// SchemaID is the assigned schema ID for the component's payload type.
	SchemaID uint64 `json:"schemaId"`
	// SchemaName is the exported schema/type name for this projection payload.
	SchemaName string `json:"schemaName,omitempty"`
	// Type is the full payload type descriptor for this projection.
	Type spore.TypeDesc `json:"type"`
	// Mode is the delivery mode: "delta" or "full".
	Mode string `json:"mode"`
	// Visibility controls whether this projection is exported to frontend
	// codegen. One of: "internal", "diagnostic", "admin", "public".
	Visibility string `json:"visibility,omitempty"`
}

// EventDecl describes one event kind registered via
// ctx.RegisterEventKind(kind, example).
type EventDecl struct {
	// Namespace is the wire namespace for the event type.
	Namespace string `json:"namespace"`
	// ActorPath is the actor's tree path that owns this event kind.
	ActorPath string `json:"actorPath,omitempty"`
	// Kind is the event wire identifier (e.g. "turn").
	Kind string `json:"kind"`
	// SchemaID is the assigned schema ID for the event payload type.
	SchemaID uint64 `json:"schemaId"`
	// ServiceName is the exposed service name of the registering cell, if any.
	// When non-empty the codegen emits onService(serviceName, kind); when
	// empty it emits onInstance(actorId, kind) with an actorId parameter.
	ServiceName string `json:"serviceName,omitempty"`
	// Visibility controls whether this event type is exported to frontend
	// codegen. One of: "internal", "diagnostic", "admin", "public".
	Visibility string `json:"visibility,omitempty"`
	// Loop is the logical runtime loop that owns this event kind by default.
	Loop string `json:"loop,omitempty"`
}
