package projection

// Diagnostic codes raised on the State Channel surface. Surfaced via
// Update frames (UpdateGapTooLarge) and structured logs; callers compare
// against these constants rather than matching message strings.
const (
	// DiagGapTooLarge is emitted when a Watch caller's since_version is
	// older than DeltaWindow.OldestVersion (the requested deltas have
	// been evicted). The default Store responds by emitting an
	// UpdateGapTooLarge frame followed by an UpdateFullSnapshot rather
	// than failing the call — the constant is exposed so external
	// transports can map it onto their own error envelopes.
	//
	// Mirrors gospore.events.gap_too_large on the Event Channel side;
	// both follow ARCHITECTURE.md §4.18 / §4.23 reconnect rules.
	DiagGapTooLarge = "gospore.projection.gap_too_large"

	// DiagVersionOverflow is the placeholder code for the (theoretical)
	// uint64 Version counter wrapping. ARCHITECTURE.md §7.5 lists this
	// as Internal — at 1M ++/sec it takes ~580k years to wrap, so this
	// code primarily exists to keep the diagnostic table exhaustive.
	DiagVersionOverflow = "gospore.projection.version_overflow"

	// DiagTagInvalid covers the three structural failures ScanComponents
	// can detect at spawn time: tagged field is unexported / not
	// addressable, two tagged fields share a name, or the receiver is
	// not a pointer to a struct. Cell promotes a scanner error to this
	// code and refuses to spawn the actor.
	DiagTagInvalid = "gospore.projection.tag_invalid"

	// DiagUnregisteredSchema is emitted when a tagged field's type is
	// not present in app.Schemas() — projection requires a spore
	// TypeDesc to encode the field. ScanComponents itself does not
	// perform schema lookup (no app handle here); Cell layers the
	// check on top using the (Type, Name) the scanner reports.
	DiagUnregisteredSchema = "gospore.projection.unregistered_schema"

	// DiagFieldNotExposed is emitted at runtime (script side) when
	// gospore.projection.field("foo") names an actor field that is
	// not tagged. The mechanism lives in the projection.Store
	// implementation; the constant lives here so transports map it.
	DiagFieldNotExposed = "gospore.projection.field_not_exposed"

	// DiagInvalidFingerprintMode is emitted when WithProjectionFingerprint
	// receives a FingerprintMode value outside the FingerprintBytes /
	// FingerprintHash / FingerprintPerField triple. The check happens
	// in app.WithProjectionFingerprint Option processing; the constant
	// lives here so transports can map the validation error onto their
	// own envelopes.
	DiagInvalidFingerprintMode = "gospore.projection.invalid_fingerprint_mode"

	// DiagUnstableComponentType is emitted when a tagged field's type
	// cannot be stably encoded by spore — currently only Go native
	// `map`, whose iteration order is not deterministic and so makes
	// fingerprints non-reproducible. ScanComponents does not detect
	// this (no spore handle); Cell layers the check after schema
	// lookup. ARCHITECTURE.md §4.18.
	DiagUnstableComponentType = "gospore.projection.unstable_component_type"
)
