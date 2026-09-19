package actor

import "time"

// Clock is the injectable time source. App injects a real clock by
// default; tests can substitute a fake clock via app.WithClock(...) so
// time-dependent assertions stay deterministic.
//
// Both methods must be safe for concurrent use.
type Clock interface {
	// Now returns the current wall-clock time.
	Now() time.Time
	// After returns a channel that receives the time-of-fire after
	// `d` has elapsed. Real implementations delegate to time.After;
	// fakes may return a channel they fire manually so tests can
	// fast-forward without sleeping.
	After(d time.Duration) <-chan time.Time
}

// RealClock is the default Clock used by App when no Clock has been
// injected via app.WithClock. It delegates to the standard library.
type RealClock struct{}

// Now returns time.Now().
func (RealClock) Now() time.Time { return time.Now() }

// After returns time.After(d).
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Compile-time check.
var _ Clock = RealClock{}
