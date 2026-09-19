package plan

import "errors"

// Diagnostic codes raised by the plan layer. Surfaced via returned
// errors and structured logs; callers compare against these constants
// rather than matching message strings. Mapped onto §7.5 of
// ARCHITECTURE.md.
const (
	// DiagInvalidTarget is returned when ctx.Plan is called with a
	// target Ref that is already dead (Stop has completed, or the
	// target actor never existed). The Cell-tier dispatcher checks
	// liveness before mounting the Node; this constant pins the wire
	// spelling so transports can route the validation envelope without
	// matching a free-form message.
	//
	// §7.5 row: Validation tier.
	DiagInvalidTarget = "gospore.plan.invalid_target"

	// DiagStartWrongState is returned by Node.Start when the Node is
	// not in StatePending. Per §4.22 the Pending → Running transition
	// is the only valid Start path; calls from Running / Completed /
	// Failed / Cancelled are runtime errors. CanStart locks the rule
	// at the leaf so any Cell-tier executor can guard the transition
	// without re-encoding the comparison.
	//
	// §7.5 row: Runtime tier.
	DiagStartWrongState = "gospore.plan.start_wrong_state"

	// DiagTimeout is returned (and surfaced as the ErrorMsg of the
	// projected PlanComponent) when a Node has not reached a terminal
	// State by its WithTimeout deadline. The Cell-tier executor
	// transitions Pending / Running → Cancelled and tags ErrorMsg with
	// this string before closing Done. Pinned at the leaf so projection
	// observers and event subscribers see a stable, externally
	// recognisable code rather than per-deployment phrasing.
	//
	// §7.5 row: Runtime tier.
	DiagTimeout = "gospore.plan.timeout"

	// DiagStreamingResultUnavailable is the wire-format counterpart of
	// the in-process ErrStreamingResultUnavailable sentinel: returned
	// when Result is invoked on a Node wrapping a streaming callable.
	// In-process Go callers compare against the sentinel via errors.Is;
	// transport / observers / scripts compare against this constant.
	//
	// §7.5 row: Runtime tier.
	DiagStreamingResultUnavailable = "gospore.plan.streaming_result_unavailable"
)

// MapErrToDiag returns the §7.5 wire-format diagnostic code that
// corresponds to err, or "" when err is nil or has no plan-tier
// wire code.
//
// Coverage:
//   - ErrStreamingResultUnavailable → DiagStreamingResultUnavailable
//
// DiagInvalidTarget / DiagStartWrongState / DiagTimeout have no paired
// sentinel — they are emitted by the Cell-tier executor at runtime
// (liveness check, state-transition guard, deadline expiry), not
// returned as Go errors from this package. Uses errors.Is so the
// helper composes with wrapped sentinels.
func MapErrToDiag(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrStreamingResultUnavailable):
		return DiagStreamingResultUnavailable
	}
	return ""
}
