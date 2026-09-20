package service

import "errors"

// Diagnostic codes raised on the service registry surface. Surfaced
// via Error frames and structured logs; callers compare against these
// constants rather than matching message strings or sentinel errors
// directly when they need to discriminate at the wire layer.
const (
	// DiagInvalidName indicates a service name failed the
	// `^[a-z][a-z0-9_-]*$` format check. Maps to ErrInvalidServiceName
	// at the Go API surface.
	DiagInvalidName = "gospore.service.invalid_name"
	// DiagNameTaken indicates Register / Expose was called with a name
	// already bound to another actor. Maps to ErrServiceNameTaken at
	// the Go API surface.
	DiagNameTaken = "gospore.service.name_taken"
	// DiagUnknownName indicates LookupService (or its remote variant)
	// was called with a name not currently registered.
	DiagUnknownName = "gospore.service.unknown_name"
	// DiagScopedConflict indicates a scoped service registration
	// conflicts with an existing global or scoped service in the same
	// subtree. Maps to ErrScopedConflict at the Go API surface.
	DiagScopedConflict = "gospore.service.scoped_conflict"
)

// MapErrToDiag returns the §7.5 wire-format diagnostic code that
// corresponds to err, or "" when err is nil or has no service-tier
// wire code.
//
// Coverage:
//   - ErrInvalidServiceName → DiagInvalidName
//   - ErrServiceNameTaken   → DiagNameTaken
//   - ErrScopedConflict     → DiagScopedConflict
//
// DiagUnknownName intentionally has no paired sentinel — it is emitted
// by the lookup-side consumer when Lookup returns false, not by Register.
// Uses errors.Is so the helper composes with wrapped sentinels.
func MapErrToDiag(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrInvalidServiceName):
		return DiagInvalidName
	case errors.Is(err, ErrServiceNameTaken):
		return DiagNameTaken
	case errors.Is(err, ErrScopedConflict):
		return DiagScopedConflict
	}
	return ""
}
