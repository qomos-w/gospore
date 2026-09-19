package cell

// Monitor is an optional observability hook for handler invocations. When a
// non-nil Monitor is provided in Config, OnCall is invoked after every
// handler completes — including panics. The callback must not acquire locks
// (zero-lock constraint) so it can be called from both the cell goroutine
// (stateful handlers) and forked goroutines (stateless handlers) without
// adding latency to the dispatch path.
type Monitor interface {
	OnCall(cellID, callable string, err error)
}
