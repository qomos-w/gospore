package plan

import (
	"context"
	"errors"

	"github.com/qomos-w/gospore/ref"
)

// ErrStreamingResultUnavailable is returned by Result when called on a
// Plan Node wrapping a streaming callable. Streaming callers must use
// Recv until io.EOF instead.
var ErrStreamingResultUnavailable = errors.New("gospore/plan: Result unavailable on streaming callable; use Recv")

// Node is the Plan Node interface — a child actor that wraps a single
// outbound Invoke. The Node is itself an actor (Ref returns its own
// reference), so it can be Watched, looked up by Path, and supervised
// like any other actor in the tree.
//
// Lifetime: created by ctx.Plan; reaches a terminal State (Completed /
// Failed / Cancelled), at which point Done() closes. Unless
// WithKeepAfterDone was passed, the node auto-stops after the terminal
// transition.
type Node interface {
	// Ref returns this Plan Node's own actor Ref. Watching this Ref
	// streams Plan-Node lifecycle events; Lookup by Path resolves to
	// the same Ref.
	Ref() ref.Ref

	// State returns the current lifecycle state. Snapshot only — the
	// state may change immediately after this call returns.
	State() State

	// Start transitions Pending → Running. Returns nil if the call was
	// dispatched (the wrapped Invoke is now in flight); a non-nil error
	// if the node is already past Pending or the Invoke could not be
	// dispatched.
	Start(ctx context.Context) error

	// Stop transitions any non-terminal state to Cancelled. Idempotent
	// after a terminal state — returns nil if already terminal.
	Stop() error

	// Recv returns the next streaming Reply chunk. io.EOF signals the
	// terminal transition (Completed / Failed). Used for streaming
	// callables; on a unary callable, Recv returns the single reply
	// then io.EOF on the next call.
	Recv() (any, error)

	// Result blocks until the node reaches a terminal State, then
	// returns the unary reply value (Completed) or the business error
	// (Failed / Cancelled). Returns ErrStreamingResultUnavailable when
	// invoked on a streaming callable.
	Result() (any, error)

	// Done returns a channel that is closed when the node enters a
	// terminal State. Useful for select-driven coordination without
	// blocking on Result / Recv.
	Done() <-chan struct{}

	// Target returns the Ref the wrapped Invoke is sent to.
	Target() ref.Ref

	// CallID returns the wrapped Invoke's callID.
	CallID() string

	// Payload returns the input payload as it was passed to ctx.Plan
	// (cached on creation). Useful for diagnostics and replay tooling.
	Payload() any
}

// RecvResult is one item produced by RecvChan.
type RecvResult struct {
	Value any
	Err   error
}

// RecvChan bridges Node.Recv into a channel so callers can multiplex plan
// output with timers or other cancellation signals in a select loop.
func RecvChan(ctx context.Context, node Node) <-chan RecvResult {
	out := make(chan RecvResult, 1)
	go func() {
		defer close(out)
		for {
			v, err := node.Recv()
			if ctx.Err() != nil {
				return
			}
			select {
			case out <- RecvResult{Value: v, Err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return out
}
