package resource

import "errors"

// Diagnostic codes raised on the resource path. Surfaced via Error
// frames and structured logs; callers compare against these constants
// rather than matching message strings.
const (
	// DiagFrozen indicates a Set arrived after the registry was frozen.
	// The corresponding Go error is ErrFrozen.
	DiagFrozen = "gospore.resource.frozen"
	// DiagUnknown indicates Get / MustGet was called with a Key that
	// has no binding.
	DiagUnknown = "gospore.resource.unknown"
	// DiagTypeMismatch indicates the value bound to a Key has a runtime
	// type different from the Key's declared T. Generic Get returns
	// (zero, false); MustGet panics.
	DiagTypeMismatch = "gospore.resource.type_mismatch"
)

// MapErrToDiag returns the §7.5 wire-format diagnostic code that
// corresponds to err, or "" when err is nil or has no resource-tier
// wire code.
//
// Coverage:
//   - ErrFrozen       → DiagFrozen
//   - ErrTypeMismatch → DiagTypeMismatch
//
// DiagUnknown intentionally has no paired sentinel — it is emitted by
// the lookup-side consumer (MustGet panic / Get returning false), not
// by Set. Uses errors.Is so the helper composes with wrapped sentinels.
func MapErrToDiag(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrFrozen):
		return DiagFrozen
	case errors.Is(err, ErrTypeMismatch):
		return DiagTypeMismatch
	}
	return ""
}
