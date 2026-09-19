package actor

// HandlerMode discriminates how a script-registered handler executes.
// Go handlers infer their mode from the first parameter type
// (Context → stateful, PureContext → stateless); RegisterScript needs
// this enum because script source cannot carry that type signal.
type HandlerMode int

const (
	// ModeStateful runs the handler on the cell goroutine; it may mutate
	// actor fields. Serialized; one in-flight call at a time per actor.
	ModeStateful HandlerMode = iota
	// ModeStateless runs the handler on a forked goroutine; it may NOT
	// mutate actor fields. Concurrent calls are permitted.
	ModeStateless
)

// String returns the canonical wire-format spelling of m — "stateful" /
// "stateless". These two strings are the source of truth for any
// surface that needs to spell a HandlerMode in human-readable form
// (script registration error messages, future annotation entries, etc.);
// codegen + transport surfaces stay byte-identical by routing through
// this method instead of inlining string literals. Returns "" for an
// out-of-range value so a forgotten case surfaces as an empty string.
func (m HandlerMode) String() string {
	switch m {
	case ModeStateful:
		return "stateful"
	case ModeStateless:
		return "stateless"
	}
	return ""
}
