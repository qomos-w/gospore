package mailbox

import "errors"

// Diagnostic codes raised on the mailbox path. They are surfaced via
// Error frames; callers compare against these constants rather than
// matching error strings.
const (
	// DiagMailboxFull indicates the user lane was at capacity when a
	// PushUser attempt arrived. The corresponding Go error is ErrFull.
	DiagMailboxFull = "gospore.mailbox.full"
)

// MapErrToDiag returns the §7.5 wire-format diagnostic code that
// corresponds to err, or "" when err is nil or has no mailbox-tier
// wire code.
//
// Coverage:
//   - ErrFull → DiagMailboxFull
//
// ErrClosed intentionally returns "" — mailbox closure is a lifecycle
// event, not a wire-diagnosable error. Uses errors.Is so the helper
// composes with wrapped sentinels.
func MapErrToDiag(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrFull):
		return DiagMailboxFull
	}
	return ""
}
