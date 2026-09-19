package actor

import "fmt"

// ValidateReplaceReq runs the pure-data half of the
// `_gospore_.script_replace` request handler (§4.20 line 2634-2643).
// It owns one invariant: DropState MUST be true. Any false / missing
// flag returns a DiagReplaceWithoutDrop-tagged error, mirroring §7.5's
// Validation-row mapping for the diagnostic.
//
// nil request is illegal — script_replace cannot be invoked with no
// payload (the Cell harness rejects this earlier in the dispatch path,
// but the data-layer guard reflects the same invariant for callers
// that drive Validate directly, e.g. in tests or when transports want
// to pre-validate before submitting).
//
// Definition itself is NOT shape-checked here: the §4.20 destructive
// replacement allows removal / mode change / schema change of every
// data part, and a nil Definition (wipe the data layer entirely) is a
// legal opt-out. Definition shape — when present — is validated by
// ValidateDefinition at OnStart-time after the Cell accepts the swap;
// reusing the same data-layer rules through the same code path keeps
// the §4.20 state machine simpler than a parallel "replace-time" rule
// set.
func ValidateReplaceReq(req *ReplaceReq) error {
	if req == nil {
		return fmt.Errorf("%s: ReplaceReq is nil; %s requires a payload",
			DiagReplaceWithoutDrop, CallIDReplace)
	}
	if !req.DropState {
		return fmt.Errorf("%s: %s requires DropState=true; refusing to drop component state silently",
			DiagReplaceWithoutDrop, CallIDReplace)
	}
	return nil
}
