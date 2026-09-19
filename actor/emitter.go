package actor

// Emitter is the streaming sender side. Handlers that declare an Emitter
// parameter receive an instance whose Send pushes one chunk per call;
// chan-full conditions block. Done closes when the caller cancels.
//
// Lifecycle: when the handler returns nil gospore emits an End frame;
// when it returns err gospore emits an Error frame. On Stream.Close
// or ctx.Done by the caller, Emitter.Done closes — the handler should
// observe and return promptly.
type Emitter interface {
	Send(chunk any) error
	Done() <-chan struct{}
}

// EmitterT is the typed convenience form of Emitter, parameterized by
// the chunk type R. Handlers that prefer compile-time chunk-type
// safety declare EmitterT[R] instead of Emitter.
//
// Note: EmitterT[R] embeds Emitter so that the reflection-based Run
// closure can pass a non-generic emitterImpl to handlers that declare
// EmitterT[R]. This means Send accepts any rather than R at the
// interface level; the compile-time type safety comes from the
// handler's own parameter declaration.
type EmitterT[R any] interface {
	Emitter
}
