package actor

// Subscription is the unified handle for long-lived subscription streams
// (projection.Watch, events.Subscribe). It differs from invoke.Stream in
// purpose, not in shape: Stream is a single-call response stream;
// Subscription is a continuous-interest stream that the producer keeps
// pushing to until the consumer Closes it.
//
// Recv blocks for the next value and returns io.EOF when the subscription
// has terminated (closed by either side or producer-finished).
// Close is the consumer's terminate signal. Done closes when the
// subscription has reached its terminal state.
type Subscription[T any] interface {
	Recv() (T, error)
	Close() error
	Done() <-chan struct{}
}
