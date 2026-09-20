package mailbox

import "errors"

// ErrFull is returned by PushUser when the user lane is at capacity.
// Senders should treat this as backpressure: drop, retry with backoff,
// or surface it as a stream-level error to the caller.
var ErrFull = errors.New("gospore/mailbox: user lane full")

// ErrClosed is returned by Pop when the mailbox has been Close()d while
// no frames remain. Cells use this to distinguish a normal shutdown
// from a caller-supplied ctx cancellation.
var ErrClosed = errors.New("gospore/mailbox: closed")
