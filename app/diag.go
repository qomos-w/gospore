package app

// Diagnostic codes raised on the App construction surface. Surfaced via
// Error frames and structured logs; callers compare against these
// constants rather than matching message strings.
const (
	// DiagInvalidNamespace indicates the namespace passed to app.New does
	// not satisfy the namespace format rules (must match
	// schema.ValidNamespace, must not be a reserved prefix such as "app"
	// or "_gospore_*"). Emitted at App construction time before any
	// actor tree is created.
	DiagInvalidNamespace = "gospore.app.invalid_namespace"
)
