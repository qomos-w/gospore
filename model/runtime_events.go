package model

import "time"

type MemberEventType int

const (
	MemberJoined MemberEventType = iota
	MemberLeft
	MemberDown
	MemberRecovered
)

type MemberEvent struct {
	Type    MemberEventType
	Runtime RuntimeStatus
	Reason  string
}

type MigrationEvent struct {
	Ref    ActorRef
	From   RuntimeID
	To     RuntimeID
	Status string
}

type RuntimeEventType int

const (
	EventRuntimeJoin RuntimeEventType = iota
	EventRuntimeLeave
	EventRuntimePause
	EventRuntimeResume
	EventRuntimeDrain
	EventRuntimeDown
)

type RuntimeEvent struct {
	Type    RuntimeEventType
	Runtime RuntimeStatus
	Reason  string
}

type ActorGlobalEventType int

const (
	EventActorSpawned ActorGlobalEventType = iota
	EventActorDestroyed
	EventActorMigrated
	EventActorPaused
	EventActorResumed
	EventActorRestarted
	EventActorError
)

type ActorState int

const (
	ActorCreated ActorState = iota
	ActorRunning
	ActorPaused
	ActorStopped
	ActorError
)

type ActorEventType int

const (
	EventStateChange ActorEventType = iota
	EventMessageSent
	EventMessageRecv
	EventError
	EventSpawn
	EventStop
)

type ActorEvent struct {
	Ref       ActorRef
	Parent    ActorRef
	Type      ActorEventType
	State     ActorState
	Detail    interface{}
	Timestamp time.Time
}

type ActorMetrics struct {
	MsgSent      int64
	MsgRecv      int64
	BytesSent    int64
	BytesRecv    int64
	Errors       int64
	Restarts     int
	CreatedAt    time.Time
	LastActiveAt time.Time
}

type ActorErrorInfo struct {
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
	Kind      string    `json:"kind,omitempty"`
}

type ActorInfo struct {
	Ref       ActorRef
	Type      string
	State     ActorState
	Parent    ActorRef
	Children  []ActorRef
	Runtime   RuntimeID
	Owner     Principal
	Metrics   ActorMetrics
	LastError *ActorErrorInfo
}

type TreeSnapshot struct {
	Info     *ActorInfo
	Children []*TreeSnapshot
}

type ActorStatus struct {
	Ref       ActorRef
	Type      string
	State     ActorState
	Runtime   RuntimeID
	Parent    ActorRef
	Children  []ActorRef
	Owner     Principal
	CreatedAt time.Time
	Metrics   ActorMetrics
}

type ActorGlobalEvent struct {
	Type    ActorGlobalEventType
	Actor   ActorStatus
	Runtime RuntimeID
}

type ScheduleStrategy int

const (
	BalanceStrategy ScheduleStrategy = iota
	PackStrategy
	SpreadStrategy
)

type TopologyEventType int

const (
	EventTopologyStatusChange TopologyEventType = iota
	EventTopologyRebalance
	EventTopologyScaleOut
	EventTopologyScaleIn
)

type TopologyEvent struct {
	Type   TopologyEventType
	Status TopologyStatus
	Detail interface{}
}
