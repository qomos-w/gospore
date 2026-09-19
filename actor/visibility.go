package actor

// Visibility controls whether a callable, component, event, or schema type
// is exported to the frontend codegen pipeline. It is a compile-time
// (manifest / tool-chain surface) concern only — runtime authorization is
// handled by PolicyStore. Visibility defaults to Internal; callers must
// explicitly opt in to broader exposure.
type Visibility int

const (
	// VisibilityInternal means the item is NOT exported to frontend codegen.
	// This is the zero value and the default for every registration surface.
	VisibilityInternal Visibility = iota
	// VisibilityDiagnostic means the item is exported for diagnostic / admin
	// tooling but not for end-user frontend code.
	VisibilityDiagnostic
	// VisibilityAdmin means the item is exported for admin frontend builds.
	VisibilityAdmin
	// VisibilityPublic means the item is exported for all frontend builds.
	VisibilityPublic
)

// String returns the canonical wire-format spelling of v — "internal" /
// "diagnostic" / "admin" / "public". These four strings are the source of
// truth for manifest generation and codegen filtering. Returns "" for an
// out-of-range value so a forgotten case surfaces as an empty string.
func (v Visibility) String() string {
	switch v {
	case VisibilityInternal:
		return "internal"
	case VisibilityDiagnostic:
		return "diagnostic"
	case VisibilityAdmin:
		return "admin"
	case VisibilityPublic:
		return "public"
	}
	return ""
}

// ParseVisibility converts a canonical wire-format spelling back to the
// corresponding Visibility constant. It is the inverse of String.
// The second result is false for any unrecognized token.
func ParseVisibility(s string) (Visibility, bool) {
	switch s {
	case "internal":
		return VisibilityInternal, true
	case "diagnostic":
		return VisibilityDiagnostic, true
	case "admin":
		return VisibilityAdmin, true
	case "public":
		return VisibilityPublic, true
	}
	return VisibilityInternal, false
}
