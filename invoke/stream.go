// Package invoke holds the caller-side streaming abstractions returned by
// ref.Ref.Invoke. Stream unifies fire-and-forget / unary / streaming
// invocation consumption under a single Recv / RecvRaw / Close surface;
// the caller does not need to know in advance which mode the callable
// uses.
package invoke

// Stream is the caller-side handle for one invocation's response.
//
// Consumption modes:
//   - Don't Recv, just Close ⇒ fire-and-forget (call lifecycle ends on Close).
//   - Recv once ⇒ unary: returns the value, second Recv returns io.EOF.
//   - Recv repeatedly until io.EOF ⇒ streaming: each Recv yields one chunk.
//
// Recv and RecvRaw are MUTUALLY EXCLUSIVE for the lifetime of one Stream:
// once a caller has used either, the other returns gospore.stream.mode_locked.
// Recv decodes via codec into a Go value; RecvRaw returns the original
// Frame.Body bytes for forwarding scenarios that want to skip a decode/encode
// round-trip. Use Recv unless you specifically need raw bytes.
//
// Creation-time errors (callID unregistered, target dead, encoding error)
// surface from the first Recv / RecvRaw, never from Invoke directly.
type Stream interface {
	Recv() (any, error)
	RecvRaw() ([]byte, error)
	Close() error
}
