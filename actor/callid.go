package actor

import "strings"

// CallID is the dotted-namespace identifier of a callable.
// Format: `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$` — a single segment is
// legal and denotes an actor-local callable (no external service
// namespace); two or more segments denote externally callable
// namespaced callables. Examples: "tool.use", "auth.login_v2",
// "db.user.fetch", "memory_save" (local).
//
// Reserved namespaces:
//   - "app.*"       — owned by App root actor (§4.17).
//   - "_gospore_.*" — owned by gospore internal actors.
//
// The Register / RegisterScript APIs accept a plain string; CallID is
// the typed parser/helper for code that needs to inspect the namespace
// or terminal name of a callable identifier.
type CallID string

// Namespace returns everything before the last '.' in the CallID.
// For "db.user.fetch" returns "db.user"; for a single-segment string
// (which is invalid as a CallID) returns "".
func (c CallID) Namespace() string {
	s := string(c)
	i := strings.LastIndex(s, ".")
	if i < 0 {
		return ""
	}
	return s[:i]
}

// Name returns everything after the last '.' in the CallID.
// For "db.user.fetch" returns "fetch"; for a string with no '.'
// returns the whole string.
func (c CallID) Name() string {
	s := string(c)
	i := strings.LastIndex(s, ".")
	if i < 0 {
		return s
	}
	return s[i+1:]
}

// ReservedNamespaceApp is the App-root reserved first segment. Only
// the root actor (the App itself) may register callables under
// "app.*"; non-root attempts return DiagCallableReservedNamespace.
const ReservedNamespaceApp = "app"

// ReservedNamespaceGospore is the gospore-internal reserved first
// segment. Users should avoid this prefix; it is not enforced as a
// hard rejection (§4.17 leaves room for internal evolution) but is
// surfaced here so handler / app wiring can recognise it.
const ReservedNamespaceGospore = "_gospore_"

// ValidCallID reports whether s matches the callable format
// `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$` — one or more segments, each
// starting with a lowercase letter, then lowercase letters / digits /
// underscores. A single segment is a legal actor-local callable ID.
// Hyphens are NOT permitted inside a callable segment (the hyphen rule
// is for service names, not callIDs).
//
// Implemented by hand to avoid pulling regexp into the actor package's
// hot path; the table-test in tdd/actor_test.go locks the boundary
// behaviour. Callers that need a typed result should compare against
// DiagCallableInvalidID.
func ValidCallID(s string) bool {
	if s == "" {
		return false
	}
	segments := 0
	segStart := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if segStart {
			if !(c >= 'a' && c <= 'z') {
				return false
			}
			segments++
			segStart = false
			continue
		}
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_':
		case c == '.':
			segStart = true
		default:
			return false
		}
	}
	if segStart {
		// Trailing '.' — last byte closed a segment without producing
		// content for the next one.
		return false
	}
	return segments >= 1
}

// IsReservedNamespace reports whether s's first dot-separated segment
// is one of the reserved prefixes ("app" / "_gospore_") and returns
// that segment along with the boolean. Behaviour is independent of
// ValidCallID: a malformed string with an "app." prefix still trips
// the reserved check, since the registration pipeline must reject the
// reserved-namespace violation before the format error reaches the
// caller (per ARCHITECTURE.md §7.5 ordering). Single-segment callables
// exactly equal to a reserved name ("app" / "_gospore_") also trip the
// check.
func IsReservedNamespace(s string) (string, bool) {
	if s == ReservedNamespaceApp || s == ReservedNamespaceGospore {
		return s, true
	}
	first := FirstSegment(s)
	if first == "" {
		return "", false
	}
	switch first {
	case ReservedNamespaceApp, ReservedNamespaceGospore:
		return first, true
	}
	return "", false
}

// FirstSegment returns the first dot-separated segment of s, or "" if
// s contains no dot (single-segment strings are not valid callIDs and
// have no notion of "first segment" for namespace purposes). The empty
// string is also handled — returns "".
//
// Exposed because both IsReservedNamespace and MatchesAppNamespace need
// the same primitive, and Register's diagnostic messages quote the
// extracted first segment (e.g., "callID first segment %q does not
// match app namespace %q").
func FirstSegment(s string) string {
	i := strings.IndexByte(s, '.')
	if i <= 0 {
		// i < 0: no dot; i == 0: leading dot (callID is malformed but
		// the namespace check should still report empty first segment
		// rather than panic).
		return ""
	}
	return s[:i]
}

// MatchesAppNamespace reports whether callID's first dot-separated
// segment equals the App's declared namespace. The Register pipeline
// uses this AFTER the IsReservedNamespace gate has cleared — reserved
// callIDs ("app.*" and "_gospore_.*") have separate enforcement
// (root-only, internal-only) and must not be checked against the user
// namespace.
//
// Returns false on either empty input. An empty namespace argument
// would trivially match callIDs that have no dot ("" == "") but that
// is never a legitimate state — the App always has a non-empty
// namespace by construction (validated in app.New) — so the empty
// guard prevents misuse from silently succeeding.
//
// Maps to actor.DiagCallableNamespaceMismatch when this returns false
// in the Register pipeline's not-reserved branch.
func MatchesAppNamespace(callID, namespace string) bool {
	if callID == "" || namespace == "" {
		return false
	}
	return FirstSegment(callID) == namespace
}

// ValidNamespace reports whether s matches the single-segment namespace
// identifier format `^[a-z][a-z0-9_]*$`. Used by app.New (WithNamespace
// validation), schema.New, and actor.ValidateDefinition.
//
// Reserved-name rejection ("app" / "_gospore_") is layered by callers
// — keep the predicate orthogonal so cross-app code that legitimately
// references reserved namespaces can still format-check them.
//
// Implemented by hand to avoid pulling regexp into the actor package's
// hot path; mirrors ValidCallID's per-segment scanner with a single
// segment instead of two-or-more.
func ValidNamespace(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 0 {
			if !(c >= 'a' && c <= 'z') {
				return false
			}
			continue
		}
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_':
		default:
			return false
		}
	}
	return true
}
