package model

// HostCommandKind identifies the kind of a host-level command.
type HostCommandKind int

const (
	HostCmdSpawn HostCommandKind = iota
	HostCmdStop
	HostCmdPause
	HostCmdResume
	HostCmdRestart
	HostCmdFreeze
	HostCmdUnfreeze
	HostCmdDestroy
)

func (k HostCommandKind) String() string {
	switch k {
	case HostCmdSpawn:
		return "spawn"
	case HostCmdStop:
		return "stop"
	case HostCmdPause:
		return "pause"
	case HostCmdResume:
		return "resume"
	case HostCmdRestart:
		return "restart"
	case HostCmdFreeze:
		return "freeze"
	case HostCmdUnfreeze:
		return "unfreeze"
	case HostCmdDestroy:
		return "destroy"
	default:
		return "unknown"
	}
}

// HostCommand is a struct-based, serializable control command targeting an actor host.
type HostCommand struct {
	Kind           HostCommandKind `json:"kind"`
	Target         ActorRef        `json:"target"`
	Spec           *ActorSpec      `json:"spec,omitempty"`
	Reason         string          `json:"reason,omitempty"`
	ActorPrincipal Principal       `json:"actor_principal"`
	Involuntary    bool            `json:"involuntary"`
}

// ActorCommandKind identifies the kind of an actor-level command.
type ActorCommandKind int

const (
	ActorCmdWatch ActorCommandKind = iota
	ActorCmdUnwatch
)

func (k ActorCommandKind) String() string {
	switch k {
	case ActorCmdWatch:
		return "watch"
	case ActorCmdUnwatch:
		return "unwatch"
	default:
		return "unknown"
	}
}

// ActorCommand is a struct-based, serializable control command targeting an actor's behavior.
type ActorCommand struct {
	Kind      ActorCommandKind `json:"kind"`
	Target    ActorRef         `json:"target"`
	Watcher   ActorRef         `json:"watcher,omitempty"`
	Principal Principal        `json:"principal"`
}

// PlacementCommandKind identifies the kind of a placement-level command.
type PlacementCommandKind int

const (
	PlacementCmdAssign PlacementCommandKind = iota
	PlacementCmdMigrate
	PlacementCmdEvacuate
)

func (k PlacementCommandKind) String() string {
	switch k {
	case PlacementCmdAssign:
		return "assign"
	case PlacementCmdMigrate:
		return "migrate"
	case PlacementCmdEvacuate:
		return "evacuate"
	default:
		return "unknown"
	}
}

// PlacementCommand is a struct-based, serializable control command for actor placement.
type PlacementCommand struct {
	Kind      PlacementCommandKind `json:"kind"`
	Target    ActorRef             `json:"target"`
	Runtime   RuntimeID            `json:"runtime"`
	Principal Principal            `json:"principal"`
}
