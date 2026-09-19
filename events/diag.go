package events

// Diagnostic codes raised on the Event Channel surface. Surfaced via
// Error frames and structured logs; callers compare against these
// constants rather than matching message strings.
const (
	// DiagGapTooLarge is emitted when a Subscribe / Recent caller's
	// since_seqno is older than Ring.OldestSeqNo (the requested events
	// have been evicted). The default Store responds by emitting a gap
	// marker followed by live delivery rather than failing the call —
	// the constant is exposed so external transports can map it onto
	// their own error envelopes.
	//
	// Mirrors gospore.projection.gap_too_large on the State Channel
	// side; both follow ARCHITECTURE.md §4.23 / §4.18 reconnect rules.
	DiagGapTooLarge = "gospore.events.gap_too_large"

	// DiagInvalidKinds is emitted when a Subscribe / Recent caller
	// passes a WatchKind value that is not one of the 11 enumerated
	// constants. The default Store rejects the call before subscription
	// rather than silently returning an empty stream — surfaces the
	// caller mistake at the boundary instead of producing zero results
	// that look like "no events of that kind yet". Mapped to the §7.5
	// Validation row.
	DiagInvalidKinds = "gospore.events.invalid_kinds"
)
