package actor

import "strings"

// ServiceNameFor derives the service name for a callable by matching the
// first segment of callID (the namespace) against the cell's declared
// domain names. Returns "" when the first segment is not a declared
// domain — callers (routing layer) treat empty as "agent-local, fall
// back to ctx.Self()".
//
// Matching uses an exact first-segment comparison (SplitN(callID, ".", 2)[0]),
// NOT strings.HasPrefix, so "app" and "appmanager" never collide.
func ServiceNameFor(domains []string, callID string) string {
	seg := callID
	if i := strings.IndexByte(callID, '.'); i > 0 {
		seg = callID[:i]
	} else {
		return "" // single-segment (no dot): agent-local, no domain
	}
	for _, d := range domains {
		if seg == d {
			return d
		}
	}
	return ""
}
