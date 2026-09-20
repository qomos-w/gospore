package supervisor

import (
	"errors"
	"testing"
	"time"
)

// TestOneForOne_WindowPruning verifies that failures older than the
// configured window are dropped from the budget so the supervisor can
// recover after a quiet period. Uses an injected clock to drive the
// "elapsed time" deterministically.
func TestOneForOne_WindowPruning(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var clock time.Time
	s := &oneForOne{
		max:    2,
		window: time.Minute,
		now:    func() time.Time { return clock },
	}
	boom := errors.New("boom")

	// Two failures back-to-back fit within the budget.
	clock = base
	if got := s.Decide(nil, boom); got != Restart {
		t.Fatalf("call 1: got %v, want Restart", got)
	}
	clock = base.Add(time.Second)
	if got := s.Decide(nil, boom); got != Restart {
		t.Fatalf("call 2: got %v, want Restart", got)
	}

	// A third failure within the window blows the budget.
	clock = base.Add(2 * time.Second)
	if got := s.Decide(nil, boom); got != Escalate {
		t.Fatalf("call 3 inside window: got %v, want Escalate", got)
	}

	// After the window elapses, the prior stamps drop out and the next
	// failure is back in budget.
	clock = base.Add(2 * time.Minute)
	if got := s.Decide(nil, boom); got != Restart {
		t.Fatalf("call 4 after window: got %v, want Restart", got)
	}
}
