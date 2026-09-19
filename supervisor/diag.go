package supervisor

// Diagnostic codes raised on the supervision surface. Surfaced via Error
// frames and structured logs; callers compare against these constants
// rather than matching message strings.
const (
	// DiagEscalate indicates a supervisor returned Escalate and the
	// failure has been handed to the parent's supervisor. At the App
	// root this triggers App.Shutdown. Emitted by the Cell-tier
	// supervision loop after Decide returns Escalate.
	DiagEscalate = "gospore.supervision.escalate"
)
