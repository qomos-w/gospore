package scriptbridge

// Annotation key constants applied by ScriptBridge to every dynamically
// generated `gospore.invoke.<callID>` Capability method per
// ARCHITECTURE.md §4.19. Static methods on `gospore.projection` /
// `gospore.events` / `gospore.app` carry only AnnotationExecution; the
// dynamic invoke namespace carries all four.
//
// The strings are wire format — codegen consumers (gospore-ts, future
// SDKs) parse them by exact match — so they live as code constants
// rather than inline string literals to keep producer / consumer
// synchronized through a single source of truth.
const (
	// AnnotationExecution carries the callable's mode: one of
	// ExecutionUnary / ExecutionStreaming / ExecutionTell.
	AnnotationExecution = "gospore.execution"
	// AnnotationNamespace carries the App namespace owning the
	// callable (the first segment of the CallID).
	AnnotationNamespace = "gospore.namespace"
	// AnnotationFrameKind carries the frame kind the handler emits
	// from the user-visible subset: one of FrameKindCall /
	// FrameKindReply / FrameKindEnd. (message.FrameKind's Error /
	// Cancel / System kinds are runtime-only and never advertised on
	// a callable's annotation surface.)
	AnnotationFrameKind = "gospore.frame_kind"
	// AnnotationVisibility carries the callable's outward reach:
	// matches actor.Visibility.String() — one of "internal" /
	// "public" / "admin" / "diagnostic".
	AnnotationVisibility = "gospore.visibility"
)

// Execution-mode value strings for AnnotationExecution. Unary /
// Streaming match spore's CallableMode wire format byte-for-byte;
// Tell is gospore-specific (one-way fire-and-forget invocation has
// no spore equivalent).
const (
	ExecutionUnary     = "unary"
	ExecutionStreaming = "streaming"
	ExecutionTell      = "tell"
)

// Frame-kind value strings for AnnotationFrameKind. Only the three
// user-visible kinds appear here — the wire-only Error / Cancel /
// System kinds are never advertised on a callable's annotation
// surface (they are emitted by the Cell, not by user handlers).
const (
	FrameKindCall  = "call"
	FrameKindReply = "reply"
	FrameKindEnd   = "end"
)
