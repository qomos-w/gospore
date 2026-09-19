package actor

// Logger is the structured logger surface exposed via Context.
// The shape mirrors slog: leveled methods that accept a message plus
// alternating key/value pairs. Implementations are free to be slog
// adapters or any other structured-logger backend.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// LogHook receives a fully-structured log entry produced by the runtime.
// It is called after the runtime has resolved the caller (file:line) at the
// correct stack depth, so the caller string is always accurate.
//
// When a LogHook is registered via app.WithLogHook, the defaultLogger
// forwards every log entry to the hook instead of writing to stdout.
// This lets downstream consumers (e.g. myxos) capture structured logs
// into a ring buffer or emit JSON without re-implementing actor.Logger.
type LogHook func(level string, msg string, caller string, fields map[string]any)
