package model

type Principal struct {
	Name string
	Kind PrincipalKind
}

type PrincipalKind int

const (
	PrincipalSystem PrincipalKind = iota
	PrincipalAgent
	PrincipalHuman
	PrincipalProgram
)

func NewPrincipal(name string, kind PrincipalKind) Principal {
	return Principal{Name: name, Kind: kind}
}

func (k PrincipalKind) String() string {
	switch k {
	case PrincipalSystem:
		return "System"
	case PrincipalAgent:
		return "Agent"
	case PrincipalHuman:
		return "Human"
	case PrincipalProgram:
		return "Program"
	default:
		return "Unknown"
	}
}

func (p Principal) String() string {
	return p.Kind.String() + ":" + p.Name
}
