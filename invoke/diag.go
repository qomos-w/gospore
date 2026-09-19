package invoke

// Diagnostic codes raised on the invoke (caller-side) surface. Surfaced
// via Error frames and structured logs; callers compare against these
// constants rather than matching message strings.
const (
	// DiagTimeout indicates the caller's Invoke call exceeded its deadline
	// before the target responded. The caller receives an error wrapping
	// this code; the Cell-tier invoke machinery cleans up the pending
	// response slot. Per §4.10 invoke-timeout semantics.
	DiagTimeout = "gospore.invoke.timeout"
	// DiagCallIDNotFound indicates a response frame arrived carrying a
	// callID that has no pending slot in the caller's response table.
	// This can happen when the caller already timed out and cleaned up,
	// or when the callID was forged / corrupted on the wire.
	DiagCallIDNotFound = "gospore.invoke.callid_not_found"
	// DiagTargetDead indicates the target actor's Cell has stopped (or
	// its address is no longer routable) before the invocation could be
	// delivered. The caller receives an error wrapping this code rather
	// than hanging indefinitely.
	DiagTargetDead = "gospore.invoke.target_dead"
)
