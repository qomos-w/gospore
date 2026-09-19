package model

type Directive int

const (
	RestartDirective Directive = iota
	StopDirective
	ResumeDirective
	EscalateDirective
)

func (d Directive) String() string {
	switch d {
	case RestartDirective:
		return "restart"
	case StopDirective:
		return "stop"
	case ResumeDirective:
		return "resume"
	case EscalateDirective:
		return "escalate"
	default:
		return "unknown"
	}
}

type ChildStats struct {
	Ref          ActorRef
	RestartCount int
}

type Terminated struct{ Ref ActorRef }
