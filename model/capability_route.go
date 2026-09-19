package model

// CapabilityRouteKey identifies one callable within a capability schema.
type CapabilityRouteKey struct {
	SchemaID     string `json:"schema_id,omitempty"`
	Version      string `json:"version,omitempty"`
	CallableName string `json:"callable_name,omitempty"`
}

// CapabilityInvokeRequest carries an explicitly-routed invocation into a
// callable actor. The runtime injects this as the req payload when the caller
// provides a callable name to Invoke.
type CapabilityInvokeRequest struct {
	Route CapabilityRouteKey `json:"route"`
	Body  interface{}        `json:"body,omitempty"`
}
