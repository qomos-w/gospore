package actor

// EventVisibilitySource discriminates where the visibility of an event
// Record originates per ARCHITECTURE.md §4.23. Records do not carry an
// explicit visibility field — the events.Store consults the source
// indicated below before forwarding a Record to a subscriber.
type EventVisibilitySource int

const (
	// EventVisibilityUnknown is the zero value returned for invalid
	// WatchKinds (zero, WatchAll, OR'd combinations, out-of-range
	// bits). Callers should validate via ValidWatchKind before relying
	// on the source classification.
	EventVisibilityUnknown EventVisibilitySource = iota

	// EventVisibilityFromCallable means visibility is inherited from
	// the registered callable's RegisterOption visibility (the same
	// Public / AdminOnly / Diagnostic chosen at Register time). Applies
	// to WatchInvokeIn{Started, Ended, Panicked} and
	// WatchInvokeOut{Started, Ended, Cancelled}.
	EventVisibilityFromCallable

	// EventVisibilityFromProps means visibility is inherited from the
	// emitting actor's Props.EventVisibility — default Internal,
	// configurable via PropsFromFunc(...).WithEventVisibility(...).
	// Applies to WatchTerminated, WatchStarted, WatchRestarted.
	EventVisibilityFromProps

	// EventVisibilityFromChild means visibility is inherited from the
	// child actor's own Props.EventVisibility, not the parent's.
	// Applies to WatchChildSpawned and WatchChildTerminated.
	EventVisibilityFromChild
)

// WatchKindVisibilitySource maps each of the 12 WatchKind constants to
// the source from which an event Record of that Kind inherits its
// visibility. Per §4.23:
//
//   - InvokeIn{Started, Ended, Panicked} / InvokeOut{Started, Ended,
//     Cancelled} — visibility = the registered callable's visibility.
//   - Terminated / Started / Restarted / Stopped — visibility = the emitting
//     actor's Props.EventVisibility (default Public).
//   - ChildSpawned / ChildTerminated — visibility = the child actor's
//     Props.EventVisibility (not the parent's).
//
// Returns EventVisibilityUnknown for any input that is not one of the
// 12 enumerated single-bit constants — including the zero value,
// WatchAll, OR'd combinations, and out-of-range bits. Callers should
// gate inputs through ValidWatchKind first; the Unknown sentinel
// exists so a missed validation surfaces visibly rather than silently
// defaulting to a wrong source bucket.
func WatchKindVisibilitySource(k WatchKind) EventVisibilitySource {
	switch k {
	case WatchInvokeInStarted, WatchInvokeInEnded, WatchInvokeInPanicked,
		WatchInvokeOutStarted, WatchInvokeOutEnded, WatchInvokeOutCancelled:
		return EventVisibilityFromCallable
	case WatchTerminated, WatchStarted, WatchRestarted, WatchStopped:
		return EventVisibilityFromProps
	case WatchChildSpawned, WatchChildTerminated:
		return EventVisibilityFromChild
	}
	return EventVisibilityUnknown
}
