package actor

import (
	"time"

	"github.com/qomos-w/gospore/ref"
)

// WatchKind is the bitmask passed to Context.Watch to select event Kinds.
// Empty (no kinds) defaults to WatchTerminated for back-compat. Events
// are delivered as system messages into the watcher's mailbox and
// processed by a stateful handler.
type WatchKind int

const (
	// WatchTerminated fires after the target's OnStop has returned.
	WatchTerminated WatchKind = 1 << iota
	// WatchStarted fires after the target's OnStart has returned.
	WatchStarted
	// WatchRestarted fires after a supervisor Restart has completed.
	WatchRestarted

	// WatchInvokeInStarted fires when target receives a Call frame and
	// the handler is about to run.
	WatchInvokeInStarted
	// WatchInvokeInEnded fires when the handler finishes (including
	// business error returns).
	WatchInvokeInEnded
	// WatchInvokeInPanicked fires when the handler panics; mutually
	// exclusive with WatchInvokeInEnded for that call.
	WatchInvokeInPanicked

	// WatchInvokeOutStarted fires when ref.Invoke / ctx.Plan is about
	// to send the Call frame.
	WatchInvokeOutStarted
	// WatchInvokeOutEnded fires when End/Reply has been received
	// (terminal state).
	WatchInvokeOutEnded
	// WatchInvokeOutCancelled fires on Stream.Close / ctx.Done /
	// Plan.Stop before terminal completion.
	WatchInvokeOutCancelled

	// WatchChildSpawned fires when this actor adopts a new child
	// (including Plan Nodes).
	WatchChildSpawned
	// WatchChildTerminated fires when one of this actor's children
	// terminates.
	WatchChildTerminated

	// WatchStopped fires when a target transitions to idle (Stop called,
	// background work halted but actor still in tree).
	WatchStopped

	// WatchUserEvent marks Records produced by ctx.EmitEvent (registered
	// event kinds). The Record's Event field carries the registered kind
	// name; the payload itself travels on the event bus — the ring keeps
	// metadata history, mirroring how invoke records store CallID but
	// not arguments.
	WatchUserEvent

	// WatchAll subscribes to every Kind.
	WatchAll WatchKind = -1
)

// ValidWatchKind reports whether k is one of the 12 enumerated single-bit
// Kind constants (WatchTerminated / WatchStarted / WatchRestarted /
// WatchInvokeIn{Started,Ended,Panicked} / WatchInvokeOut{Started,Ended,
// Cancelled} / WatchChild{Spawned,Terminated} / WatchUserEvent). The
// predicate intentionally rejects WatchAll, the zero value, and OR'd
// combinations of bits — it is the per-entry check the events.Store
// applies to each element of the `kinds []WatchKind` argument of
// Subscribe / Recent (and that Context.Watch's variadic form applies
// entry-by-entry).
//
// Mismatches map to the gospore.events.invalid_kinds diagnostic so a
// caller passing an unknown bit pattern fails at the boundary instead of
// silently receiving an empty stream.
func ValidWatchKind(k WatchKind) bool {
	switch k {
	case WatchTerminated, WatchStarted, WatchRestarted,
		WatchInvokeInStarted, WatchInvokeInEnded, WatchInvokeInPanicked,
		WatchInvokeOutStarted, WatchInvokeOutEnded, WatchInvokeOutCancelled,
		WatchChildSpawned, WatchChildTerminated, WatchStopped, WatchUserEvent:
		return true
	}
	return false
}

// ParseWatchKind converts a canonical wire-format spelling back to the
// corresponding WatchKind constant. It is the inverse of String.
func ParseWatchKind(s string) (WatchKind, bool) {
	switch s {
	case "terminated":
		return WatchTerminated, true
	case "started":
		return WatchStarted, true
	case "restarted":
		return WatchRestarted, true
	case "invoke_in_started":
		return WatchInvokeInStarted, true
	case "invoke_in_ended":
		return WatchInvokeInEnded, true
	case "invoke_in_panicked":
		return WatchInvokeInPanicked, true
	case "invoke_out_started":
		return WatchInvokeOutStarted, true
	case "invoke_out_ended":
		return WatchInvokeOutEnded, true
	case "invoke_out_cancelled":
		return WatchInvokeOutCancelled, true
	case "child_spawned":
		return WatchChildSpawned, true
	case "child_terminated":
		return WatchChildTerminated, true
	case "stopped":
		return WatchStopped, true
	case "all":
		return WatchAll, true
	}
	return 0, false
}

// String returns the canonical wire-format spelling of k for the 11
// enumerated single-bit Kinds plus WatchAll: "terminated" / "started"
// / "restarted" / "invoke_in_started" / "invoke_in_ended" /
// "invoke_in_panicked" / "invoke_out_started" / "invoke_out_ended" /
// "invoke_out_cancelled" / "child_spawned" / "child_terminated" /
// "all". Snake-case spellings mirror the §7.5 multi-word wire-token
// convention rather than the camel-cased Go identifier — this is the
// vocabulary events.Record dump / projection telemetry / Cell logs
// emit when naming a Kind. The zero value, OR'd combinations, and
// any non-canonical bit pattern return "" — same fallback policy
// used by actor.Visibility.String / actor.HandlerMode.String /
// plan.State.String / supervisor.Decision.String / message.FrameKind
// .String / id.IdentityKind.String. Combined kinds (WatchTerminated|
// WatchStarted) deliberately collapse to "" rather than producing a
// composite "terminated|started" — the events boundary already
// rejects OR'd masks via ValidWatchKind, so any code reaching String
// with a composite is already on the error path.
func (k WatchKind) String() string {
	switch k {
	case WatchTerminated:
		return "terminated"
	case WatchStarted:
		return "started"
	case WatchRestarted:
		return "restarted"
	case WatchInvokeInStarted:
		return "invoke_in_started"
	case WatchInvokeInEnded:
		return "invoke_in_ended"
	case WatchInvokeInPanicked:
		return "invoke_in_panicked"
	case WatchInvokeOutStarted:
		return "invoke_out_started"
	case WatchInvokeOutEnded:
		return "invoke_out_ended"
	case WatchInvokeOutCancelled:
		return "invoke_out_cancelled"
	case WatchChildSpawned:
		return "child_spawned"
	case WatchChildTerminated:
		return "child_terminated"
	case WatchStopped:
		return "stopped"
	case WatchAll:
		return "all"
	}
	return ""
}

// Lifecycle events.

// Terminated is delivered when a watched target's OnStop has finished.
type Terminated struct {
	Of     ref.Ref
	Reason error
}

// Started is delivered when a watched target's OnStart has finished.
type Started struct {
	Of ref.Ref
}

// Restarted is delivered when a supervisor has restarted a watched target.
type Restarted struct {
	Of     ref.Ref
	Reason error
}

// Inbound (request side) events.

// InvokeInStarted fires on the target when a Call frame arrives and
// the handler is about to execute.
type InvokeInStarted struct {
	Of, Caller ref.Ref
	CallID     string
	SchemaNS   string
	SchemaID   uint64
}

// InvokeInEnded fires on the target after the handler returns (any err
// included).
type InvokeInEnded struct {
	Of, Caller ref.Ref
	CallID     string
	Duration   time.Duration
	Err        error
}

// InvokeInPanicked fires on the target if the handler panics; mutually
// exclusive with InvokeInEnded for that call.
type InvokeInPanicked struct {
	Of     ref.Ref
	CallID string
	Reason string
}

// Outbound (caller side) events.

// InvokeOutStarted fires on the caller when ref.Invoke / ctx.Plan
// is about to emit the Call frame.
type InvokeOutStarted struct {
	Of, Target ref.Ref
	CallID     string
	CorID      uint64
}

// InvokeOutEnded fires on the caller when End/Reply (terminal) arrives.
type InvokeOutEnded struct {
	Of, Target ref.Ref
	CallID     string
	CorID      uint64
	Duration   time.Duration
	Err        error
}

// InvokeOutCancelled fires on the caller when Stream.Close / ctx.Done
// / Plan.Stop terminates a call before reply.
type InvokeOutCancelled struct {
	Of, Target ref.Ref
	CallID     string
	CorID      uint64
	Reason     string
}

// Child lifecycle events.

// ChildSpawned fires on the parent when a child actor is adopted.
type ChildSpawned struct {
	Of, Parent ref.Ref
}

// ChildTerminated fires on the parent when one of its children terminates.
type ChildTerminated struct {
	Of, Parent ref.Ref
	Reason     error
}
