// Package plan exposes the Plan Node — a per-call, short-lived child
// actor that wraps a single outbound Invoke so that it is observable
// (Watch / Projection), controllable (Stop / Timeout), and supervised
// like any other actor in the tree.
//
// Plan is NOT a new mechanism: it is the standard composition of
// Spawn + Invoke + Stream + Watch + Projection, hardcoded by gospore
// into one built-in actor type. See ARCHITECTURE.md §4.22 and the
// three-role distinction (Host / Plan / Proxy) for context.
package plan

// State is the lifecycle position of a Plan Node.
//
// Transitions:
//
//	Created → Pending → Running → Completed
//	                       │
//	                       ├──→ Failed     (handler business err)
//	                       └──→ Cancelled  (Stop / ctx cancel / timeout)
type State int

const (
	// StatePending — the node is mounted under its parent but Start has
	// not been called. Default state on creation (unless WithAutoStart).
	StatePending State = iota
	// StateRunning — Start has been called; the wrapped Invoke is in
	// progress.
	StateRunning
	// StateCompleted — natural termination; Result has been captured
	// (unary) or Recv reached EOF (streaming).
	StateCompleted
	// StateFailed — handler returned a business error.
	StateFailed
	// StateCancelled — Stop was called, parent ctx was cancelled, or
	// the WithTimeout deadline elapsed.
	StateCancelled
)

// String returns the canonical wire-format spelling of s — "pending" /
// "running" / "completed" / "failed" / "cancelled". The five strings
// are the single source of truth for any surface that needs to spell a
// State in human-readable form: PlanComponent projection (§4.18 tag
// `gospore:"component"` exposes State as a typed field, but the
// stringification feeds projection.field reads, log lines, and any
// downstream codegen annotation that wants the State name). Returns
// "" for an out-of-range value so a forgotten case surfaces visibly
// rather than as a stale spelling — same fallback policy used by
// actor.Visibility.String / actor.HandlerMode.String.
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateRunning:
		return "running"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	case StateCancelled:
		return "cancelled"
	}
	return ""
}

// IsTerminal reports whether s is one of the three terminal states
// (Completed / Failed / Cancelled). The Cell-tier executor uses this
// to decide whether Done should close, whether auto-cleanup should
// fire (in the absence of WithKeepAfterDone), and whether further
// Start / Stop calls are no-ops. Out-of-range values return false:
// an unknown state cannot be assumed terminal without proof.
func (s State) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCancelled:
		return true
	}
	return false
}

// CanStart reports whether Node.Start is allowed to fire from s. Per
// §4.22 the only valid Pending → Running transition starts from
// StatePending; every other state (already Running, or any terminal
// state) is rejected with DiagStartWrongState. Out-of-range values
// return false: an unknown state cannot be assumed safe to Start.
func (s State) CanStart() bool {
	return s == StatePending
}
