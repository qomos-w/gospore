package projection

// FingerprintMode selects the algorithm used to detect projected-field
// changes after a stateful handler returns. Picked at App construction
// time via app.WithProjectionFingerprint.
type FingerprintMode int

const (
	// FingerprintBytes (default) — spore binary-encode every projected
	// field then bytes.Equal. Cheapest at small to medium component
	// sizes (KB-scale).
	FingerprintBytes FingerprintMode = iota
	// FingerprintHash — xxhash 64-bit fingerprint. Same algorithmic
	// shape as Bytes but with smaller RAM footprint; recommended for
	// 100KB-1MB components.
	FingerprintHash
	// FingerprintPerField — track dirty bits per field; only re-hash
	// fields the handler actually touched. Recommended for MB-scale
	// components and very high-frequency stateful handlers.
	FingerprintPerField
)

// ValidFingerprintMode reports whether m is one of the three enumerated
// FingerprintMode constants (FingerprintBytes / FingerprintHash /
// FingerprintPerField). The predicate is the boundary check
// app.WithProjectionFingerprint applies before storing the option;
// rejection maps to the gospore.projection.invalid_fingerprint_mode
// diagnostic so a caller passing an unknown integer fails at App
// construction instead of silently degrading at runtime.
func ValidFingerprintMode(m FingerprintMode) bool {
	switch m {
	case FingerprintBytes, FingerprintHash, FingerprintPerField:
		return true
	}
	return false
}

// String returns the canonical wire-format spelling of m — "bytes" /
// "hash" / "per_field". Snake-case "per_field" mirrors the §7.5
// multi-word wire-token convention rather than the camel-cased Go
// identifier FingerprintPerField. Surfaced on App config dumps,
// projection diagnostic logs, and any telemetry that exposes the
// chosen fingerprint algorithm in human-readable form. Returns ""
// for an out-of-range value so a forgotten case surfaces visibly
// rather than as a stale spelling — same fallback policy used by
// actor.Visibility.String / actor.HandlerMode.String /
// plan.State.String / supervisor.Decision.String /
// message.FrameKind.String / id.IdentityKind.String /
// actor.WatchKind.String.
func (m FingerprintMode) String() string {
	switch m {
	case FingerprintBytes:
		return "bytes"
	case FingerprintHash:
		return "hash"
	case FingerprintPerField:
		return "per_field"
	}
	return ""
}
