package message

// Diagnostic codes raised on the streaming wire path. They are surfaced
// via Error frames; callers compare against these constants rather than
// matching error strings.
//
// The full diagnostic taxonomy lives in ARCHITECTURE.md §7.5; only the
// stream-tier codes are owned by the message package because they are
// intrinsic to the Frame protocol itself.
const (
	// DiagStreamCancelled indicates the stream was cancelled by the
	// initiating client (KindCancel observed).
	DiagStreamCancelled = "gospore.stream.cancelled"

	// DiagStreamPeerClosed indicates the peer closed the stream while
	// the local side still expected more frames.
	DiagStreamPeerClosed = "gospore.stream.peer_closed"

	// DiagStreamModeLocked indicates a stream that was already used in
	// one decoded mode (Recv vs RecvRaw) had the other mode requested.
	DiagStreamModeLocked = "gospore.stream.mode_locked"

	// DiagFrameMalformed indicates a frame with invalid payload metadata.
	DiagFrameMalformed = "gospore.frame.malformed"
)
