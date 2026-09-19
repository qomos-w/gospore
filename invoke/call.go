package invoke

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// CallMode discriminates the three invocation shapes exposed through a
// single Call handle.
type CallMode string

const (
	// CallModeTell — no response expected; Close immediately.
	CallModeTell CallMode = "tell"
	// CallModeUnary — exactly one reply value, then terminal.
	CallModeUnary CallMode = "unary"
	// CallModeStream — zero or more chunks, then terminal.
	CallModeStream CallMode = "stream"
)

// ErrCallCancelled is returned by Call observers once the call has been
// explicitly cancelled.
var ErrCallCancelled = errors.New("invoke: call cancelled")

// Call is the unified caller-side handle for one invocation. It wraps a
// Stream and exposes the same consumption surface regardless of whether the
// underlying callable is tell, unary, or streaming.
//
// Consumption patterns:
//
//	tell   : call.Cancel() or ignore; <-call.Done() closes immediately.
//	unary  : call.Final(ctx) for the single reply.
//	stream : call.Next(ctx) in a loop until io.EOF, then call.Final(ctx).
//
// Cancel is idempotent and safe from any goroutine.
type Call struct {
	mode   CallMode
	stream Stream

	mu         sync.Mutex
	err        error
	done       chan struct{}
	doneClosed bool
	cancelled  bool

	// unary cache — value() triggers lazy consumption via Once.
	unaryOnce  sync.Once
	unaryValue any
	unaryErr   error

	// finalTimeout bounds Final when the caller's context carries no
	// deadline. Zero means unbounded. See WithFinalTimeout.
	finalTimeout time.Duration
}

// CallOption mutates a Call at construction time.
type CallOption func(*Call)

// WithFinalTimeout installs a fallback deadline used by Final when the
// context passed to Final has no deadline of its own. An explicit caller
// deadline always wins. Zero or negative disables the fallback.
func WithFinalTimeout(d time.Duration) CallOption {
	return func(c *Call) {
		if d > 0 {
			c.finalTimeout = d
		}
	}
}

// NewCall wraps a Stream into a unified Call handle. The mode must be
// supplied by the caller because it cannot be inferred from the Stream
// alone.
func NewCall(mode CallMode, stream Stream, opts ...CallOption) *Call {
	c := &Call{
		mode:   mode,
		stream: stream,
		done:   make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// Mode returns the call's shape.
func (c *Call) Mode() CallMode { return c.mode }

// Done returns a channel that is closed when the call reaches a terminal
// state (natural completion, error, or cancellation).
func (c *Call) Done() <-chan struct{} { return c.done }

// Err returns the terminal error, or nil if the call completed cleanly.
func (c *Call) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// value returns the unary result value. For tell and stream it returns nil.
// For unary, this triggers lazy consumption of the underlying Stream via
// Once. Safe to call multiple times; the result is memoised.
//
// Unexported because it blocks without context awareness — use Final(ctx)
// from outside the package.
func (c *Call) value() any {
	if c.mode != CallModeUnary {
		return nil
	}
	c.unaryOnce.Do(func() {
		if c.stream == nil {
			c.unaryErr = errors.New("invoke: no stream")
			c.mu.Lock()
			c.setErrLocked(c.unaryErr)
			c.closeDoneLocked()
			c.mu.Unlock()
			return
		}
		c.unaryValue, c.unaryErr = Once(c.stream)
		c.mu.Lock()
		c.setErrLocked(c.unaryErr)
		c.closeDoneLocked()
		c.mu.Unlock()
	})
	return c.unaryValue
}

// Next returns the next streaming chunk.
// Only valid for CallModeStream. Returns io.EOF when the stream ends.
// Returns ErrCallCancelled if Cancel was called.
func (c *Call) Next(ctx context.Context) (any, error) {
	if c.mode != CallModeStream {
		return nil, errors.New("invoke: Next only valid for streaming calls")
	}
	if c.isCancelled() {
		return nil, ErrCallCancelled
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if c.stream == nil {
		return nil, errors.New("invoke: no stream")
	}
	v, err := c.stream.Recv()
	if err == io.EOF {
		c.closeDone()
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// Final blocks until the call reaches a terminal state and returns the
// final value. For unary, this is the single reply value. For stream,
// it drains all remaining chunks and returns (nil, nil). For tell,
// it returns an error.
//
// Final is safe to call after the stream has already been fully consumed
// via Next.
func (c *Call) Final(ctx context.Context) (any, error) {
	// Fallback deadline: a context with no deadline (typically an actor
	// lifecycle context) would otherwise block Final forever when the
	// target hangs — the primary ingredient of cross-owner-loop
	// deadlocks. An explicit caller deadline is never overridden.
	timeoutCtx := ctx
	fallbackArmed := false
	if c.finalTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			timeoutCtx, cancel = context.WithTimeout(ctx, c.finalTimeout)
			defer cancel()
			fallbackArmed = true
		}
	}
	// If ctx is cancelled while we are blocked in Recv, the inner
	// streamImpl has no way to notice — it selects on s.ch (target
	// replies) and s.done (Close/Cancel). Start a goroutine that
	// translates ctx cancellation into a Cancel call so Recv unblocks.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-timeoutCtx.Done():
			c.Cancel()
		case <-stop:
		}
	}()
	// translateCancellation maps the cancellation-shaped errors produced
	// when the fallback deadline fires onto the caller-visible cause, so
	// a timeout reads as context.DeadlineExceeded instead of io.EOF.
	// ErrCallCancelled is translated whenever the consumer context is
	// done: the ctx watcher above is what calls Cancel in that window, so
	// callers can classify the outcome with errors.Is(err, ctx.Err()).
	// io.EOF keeps the narrower fallbackArmed gate — EOF is also the
	// legitimate void-End shape and must not be misread as cancellation
	// without the fallback deadline suspicion.
	translateCancellation := func(err error) error {
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrCallCancelled) && timeoutCtx.Err() != nil {
			return timeoutCtx.Err()
		}
		if fallbackArmed && errors.Is(err, io.EOF) && timeoutCtx.Err() != nil {
			return timeoutCtx.Err()
		}
		return err
	}

	switch c.mode {
	case CallModeTell:
		return nil, errors.New("invoke: Final not available for tell")
	case CallModeUnary:
		c.value() // trigger lazy consumption; Recv unblocks via Cancel above
		return c.unaryValue, translateCancellation(c.unaryErr)
	case CallModeStream:
		if c.isCancelled() {
			return nil, ErrCallCancelled
		}
		if c.stream == nil {
			return nil, errors.New("invoke: no stream")
		}
		for {
			_, err := c.stream.Recv()
			if err == io.EOF {
				if c.isCancelled() {
					return nil, translateCancellation(ErrCallCancelled)
				}
				c.closeDone()
				return nil, nil
			}
			if err != nil {
				return nil, translateCancellation(err)
			}
		}
	}
	return nil, errors.New("invoke: unknown call mode")
}

// Cancel cancels the call, closes the underlying Stream, and closes Done.
// Idempotent; safe from any goroutine.
func (c *Call) Cancel() {
	c.mu.Lock()
	if c.cancelled {
		c.mu.Unlock()
		return
	}
	c.cancelled = true
	c.setErrLocked(ErrCallCancelled)
	c.mu.Unlock()

	if c.stream != nil {
		if cs, ok := c.stream.(interface{ Cancel() error }); ok {
			_ = cs.Cancel()
		} else {
			_ = c.stream.Close()
		}
	}
	c.closeDone()
}

func (c *Call) isCancelled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelled
}

func (c *Call) closeDone() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeDoneLocked()
}

func (c *Call) closeDoneLocked() {
	if c.doneClosed {
		return
	}
	c.doneClosed = true
	close(c.done)
}

func (c *Call) setErrLocked(err error) {
	if err != nil && c.err == nil {
		c.err = err
	}
}

// --- Stream interface delegation ---
// Call implements invoke.Stream so callers that still speak Recv/RecvRaw/Close
// continue to work without migration. New code should prefer Next/Final/Value.

// Recv delegates to the underlying Stream. Safe for tell/unary/stream.
func (c *Call) Recv() (any, error) {
	if c.stream == nil {
		return nil, errors.New("invoke: no stream")
	}
	return c.stream.Recv()
}

// RecvRaw delegates to the underlying Stream.
func (c *Call) RecvRaw() ([]byte, error) {
	if c.stream == nil {
		return nil, errors.New("invoke: no stream")
	}
	return c.stream.RecvRaw()
}

// Close delegates to the underlying Stream. Idempotent.
func (c *Call) Close() error {
	if c.stream == nil {
		return nil
	}
	return c.stream.Close()
}

var _ Stream = (*Call)(nil)
