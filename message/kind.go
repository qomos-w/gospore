// Package message defines the Frame wire format and FrameKind taxonomy.
//
// Frame is the unit of exchange between Cells (and across Transport in
// future phases). Wire encoding is fixed:
//
//	| From(16B) | To(16B) | Kind(1B) | CorID(8B BE) | Seq(4B BE) |
//	| uvarint(CallIDLen)   | CallID bytes |
//	| uvarint(SchemaNSLen) | SchemaNS bytes |
//	| SchemaID(4B BE)      |
//	| uvarint(BodyLen)     | Body bytes   |
//	| uvarint(HeaderCount) | { uvarint(KLen) K | uvarint(VLen) V } * HeaderCount |
package message

// FrameKind discriminates the meaning of a Frame.
// Call / System carry a CallID; Reply / End / Error / Cancel pair to
// the originating Call by CorID alone (their CallID is empty).
type FrameKind uint8

const (
	// KindCall opens a new invocation. Carries CallID + (SchemaNS, SchemaID)
	// + Body (the encoded request).
	KindCall FrameKind = 1
	// KindReply carries a value frame. Seq increments per chunk in streaming
	// mode; in unary mode there is exactly one Reply followed by KindEnd.
	KindReply FrameKind = 2
	// KindError terminates the call with an error frame.
	KindError FrameKind = 3
	// KindEnd terminates the call normally (no further frames).
	KindEnd FrameKind = 4
	// KindCancel cancels an in-flight call. Sender → recipient signal.
	KindCancel FrameKind = 5
	// KindSystem carries a system message (lifecycle event, watch trigger,
	// etc.). CallID is filled with the system event identifier.
	KindSystem FrameKind = 6
)

// String returns the canonical wire-format spelling of k — "call" /
// "reply" / "error" / "end" / "cancel" / "system". The first three
// match scriptbridge.FrameKindCall / FrameKindReply / FrameKindEnd
// byte-for-byte (the user-visible subset advertised on every dynamic
// `gospore.invoke.<callID>` annotation). The last three are the wire-
// only kinds emitted by the Cell, never advertised on a callable's
// annotation surface; their spellings live here so logs, projection,
// and transport tracing have a single source of truth for all six.
// Returns "" for an out-of-range value so a forgotten case surfaces
// visibly rather than as a stale spelling — same fallback policy used
// by actor.Visibility.String / actor.HandlerMode.String /
// plan.State.String / supervisor.Decision.String.
func (k FrameKind) String() string {
	switch k {
	case KindCall:
		return "call"
	case KindReply:
		return "reply"
	case KindError:
		return "error"
	case KindEnd:
		return "end"
	case KindCancel:
		return "cancel"
	case KindSystem:
		return "system"
	}
	return ""
}
