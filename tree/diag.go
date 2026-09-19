package tree

import "errors"

// Diagnostic codes raised on the actor tree path. Surfaced via Error
// frames and structured logs; callers compare against these constants
// rather than matching message strings.
//
// The codes are defined here as soon as the tree package's contract
// surface lands; the tree implementation (M11 items 1-4) emits them
// from Spawn / Stop validation paths, while the constants themselves
// already participate in cross-process Error frames.
const (
	// DiagCycle indicates Spawn would create a parent/child cycle (the
	// proposed parent is the new actor itself or one of its descendants).
	DiagCycle = "gospore.tree.cycle"
	// DiagNameTaken indicates Spawn was called with a name already in
	// use among the parent's existing children.
	DiagNameTaken = "gospore.tree.name_taken"
	// DiagNotChild indicates Stop(target) was called with a target that
	// is neither Self nor any descendant of Self. Self-stop and
	// descendant-stop are the only legal scopes.
	DiagNotChild = "gospore.tree.not_child"
)

// MapErrToDiag returns the §7.5 wire-format diagnostic code that
// corresponds to err, or "" when err is nil or has no tree-tier
// wire code.
//
// Coverage:
//   - ErrCycle     → DiagCycle
//   - ErrNameTaken → DiagNameTaken
//
// ErrUnknownParent and ErrUnknownTarget intentionally return "" — they
// are internal validation errors (parent not in tree / target not in
// tree) that do not carry §7.5 wire codes. DiagNotChild has no paired
// sentinel — it is emitted by Stop when the target scope check fails,
// using the constant directly rather than through a sentinel. Uses
// errors.Is so the helper composes with wrapped sentinels.
func MapErrToDiag(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrCycle):
		return DiagCycle
	case errors.Is(err, ErrNameTaken):
		return DiagNameTaken
	}
	return ""
}
