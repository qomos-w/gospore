package transport

// Diagnostic codes raised on the transport surface. Surfaced via Error
// frames and structured logs; callers compare against these constants
// rather than matching message strings.
const (
	// DiagVisibilityDenied indicates an inbound Call frame targets a
	// callable whose Visibility does not permit the caller's access tier
	// (e.g., a browser attempting to call an Internal or Admin handler).
	// Enforced by the Cell-tier dispatch before the handler runs; the
	// transport layer surfaces this on the Reply frame.
	DiagVisibilityDenied = "gospore.transport.visibility_denied"
)
