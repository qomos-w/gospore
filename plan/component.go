package plan

import (
	"time"

	"github.com/qomos-w/gospore/id"
)

// PlanComponent is the projected snapshot of a Plan Node. It is tagged
// with `gospore:"component"` on the Plan Node actor type, so any
// observer can read or watch it via §4.18 projection.
//
// Cross-actor / cross-script monitoring example:
//
//	snap := gospore.projection.get(planNodeID)
//	if snap.State == plan.StateRunning { ... }
type PlanComponent struct {
	// State is the current lifecycle position; mirrors Node.State().
	State State
	// CallID is the wrapped Invoke's callID.
	CallID string
	// CallerID is the actor that called ctx.Plan (i.e. the Plan Node's
	// parent in the tree).
	CallerID id.ActorID
	// TargetID is the actor receiving the wrapped Invoke.
	TargetID id.ActorID
	// StartedAt is the wall-clock time of the Pending → Running
	// transition. Zero before Start.
	StartedAt time.Time
	// EndedAt is the wall-clock time of the terminal transition. Zero
	// while non-terminal.
	EndedAt time.Time
	// ErrorMsg carries the failure reason on StateFailed / StateCancelled;
	// empty otherwise.
	ErrorMsg string
}
