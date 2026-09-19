package handler

// Diagnostic codes raised on the handler dispatch surface. Surfaced via
// Error frames and structured logs; callers compare against these
// constants rather than matching message strings.
const (
	// DiagPureOverflow indicates a stateless (PureContext) handler's
	// response exceeded the maximum allowed payload size for a single
	// invocation. Stateless handlers run in forked goroutines without
	// actor-state coupling, so overflow cannot be retried in-place.
	DiagPureOverflow = "gospore.handler.pure_overflow"
	// DiagWriteInStateless indicates a stateless handler attempted to
	// mutate actor state (e.g., via AttachComponent or a write-path
	// Context method). Stateless handlers receive PureContext, which
	// intentionally omits mutation methods — the Cell-tier dispatch
	// catches any escape and emits this diagnostic.
	DiagWriteInStateless = "gospore.handler.write_in_stateless"
)
