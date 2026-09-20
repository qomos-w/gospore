// Package supervisor encodes the failure-handling policy for an actor's
// children. A Supervisor is consulted when a child handler panics; on
// business errors (handler returned err) gospore does NOT consult the
// supervisor.
package supervisor

import (
	"sync"
	"time"

	"github.com/qomos-w/gospore/ref"
)

// Decision is a supervisor's verdict on a failed child.
type Decision int

const (
	// Resume keeps the child running with its current state.
	Resume Decision = iota
	// Restart cascades stop, rebuilds the actor via its factory, and
	// runs OnStart again.
	Restart
	// Stop terminates the child for good.
	Stop
	// Escalate hands the failure to the supervisor's own parent
	// supervisor. At the App root this triggers App.Shutdown.
	Escalate
)

// String returns the canonical wire-format spelling of d — "resume" /
// "restart" / "stop" / "escalate". Cell-tier diagnostic logs and
// projection / event surfaces ("ChildRestarted{decision: …}") emit the
// supervisor verdict by name; pinning the strings here keeps producer
// (Cell) and consumer (logs / observers / scripts) on a single source
// of truth. Returns "" for an out-of-range value so a forgotten case
// surfaces visibly rather than as a stale spelling — same fallback
// policy used by actor.Visibility.String / actor.HandlerMode.String /
// plan.State.String.
func (d Decision) String() string {
	switch d {
	case Resume:
		return "resume"
	case Restart:
		return "restart"
	case Stop:
		return "stop"
	case Escalate:
		return "escalate"
	}
	return ""
}

// IsTerminating reports whether the verdict releases the child from
// its parent's supervision tree. Stop terminates the child directly;
// Escalate hands the failure to the parent's supervisor and the child
// is gone regardless of how the parent decides. Resume and Restart
// keep the child mounted (Restart rebuilds it under the same Path /
// Ref slot). Cell-tier executors use this to decide whether the child
// is removed from the Tree before evaluating the next message; out-of-
// range values return false (an unknown verdict cannot be assumed to
// release the child).
func (d Decision) IsTerminating() bool {
	switch d {
	case Stop, Escalate:
		return true
	}
	return false
}

// Supervisor decides how to handle a child failure.
type Supervisor interface {
	Decide(child ref.Ref, reason error) Decision
}

// NewOneForOne returns a supervisor that restarts up to maxRetries
// times within the within window; further failures Escalate.
//
// Default arguments (used when App is constructed without a custom
// supervisor) are max=10 within 1m. The budget is shared across all
// children of the supervisor — once exceeded, the next failure of any
// child Escalates.
func NewOneForOne(maxRetries int, within time.Duration) Supervisor {
	return &oneForOne{
		max:    maxRetries,
		window: within,
		now:    time.Now,
	}
}

type oneForOne struct {
	max    int
	window time.Duration
	// now is injectable for tests; production binds to time.Now in
	// NewOneForOne.
	now func() time.Time

	mu     sync.Mutex
	stamps []time.Time
}

func (s *oneForOne) Decide(_ ref.Ref, _ error) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.now()
	cutoff := t.Add(-s.window)
	// Drop stamps older than the window. stamps is naturally sorted
	// (we always Append) so a single forward scan suffices.
	keep := s.stamps[:0]
	for _, ts := range s.stamps {
		if ts.After(cutoff) {
			keep = append(keep, ts)
		}
	}
	s.stamps = append(keep, t)
	if len(s.stamps) > s.max {
		return Escalate
	}
	return Restart
}
