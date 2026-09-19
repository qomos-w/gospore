package model

import "time"

// ObservationConfig configures runtime-owned actor observation behavior.
type ObservationConfig struct {
	SubscriptionBuffer int
	EmitMessageEvents  bool
}

type ActorPatchOp string

const (
	ActorOpAddActor      ActorPatchOp = "ADD_ACTOR"
	ActorOpRemoveActor   ActorPatchOp = "REMOVE_ACTOR"
	ActorOpSetState      ActorPatchOp = "SET_STATE"
	ActorOpSetParent     ActorPatchOp = "SET_PARENT"
	ActorOpAddChild      ActorPatchOp = "ADD_CHILD"
	ActorOpRemoveChild   ActorPatchOp = "REMOVE_CHILD"
	ActorOpUpdateMetrics ActorPatchOp = "UPDATE_METRICS"
	ActorOpSetType       ActorPatchOp = "SET_TYPE"
	ActorOpSetRuntime    ActorPatchOp = "SET_RUNTIME"
	ActorOpSetLastError  ActorPatchOp = "SET_LAST_ERROR"
)

type ActorPatchEntry struct {
	Op      ActorPatchOp `json:"op"`
	Ref     ActorRef     `json:"ref"`
	Parent  ActorRef     `json:"parent,omitempty"`
	Runtime RuntimeID    `json:"runtime,omitempty"`
	Value   interface{}  `json:"value,omitempty"`
}

type ActorPatch struct {
	Epoch uint64            `json:"epoch"`
	Ops   []ActorPatchEntry `json:"ops"`
}

type ActorMetricsDelta struct {
	MsgSentDelta   int64      `json:"msg_sent_delta,omitempty"`
	MsgRecvDelta   int64      `json:"msg_recv_delta,omitempty"`
	BytesSentDelta int64      `json:"bytes_sent_delta,omitempty"`
	BytesRecvDelta int64      `json:"bytes_recv_delta,omitempty"`
	ErrorsDelta    int64      `json:"errors_delta,omitempty"`
	RestartsSet    *int       `json:"restarts_set,omitempty"`
	CreatedAt      *time.Time `json:"created_at,omitempty"`
	LastActiveAt   *time.Time `json:"last_active_at,omitempty"`
}

type ActorInfoSeed struct {
	Type    string
	State   ActorState
	Parent  ActorRef
	Runtime RuntimeID
	Owner   Principal
	Metrics ActorMetrics
	Schema  *CapabilitySchema
}

type ActorObserveScope struct {
	Roots   []ActorRef   `json:"roots,omitempty"`
	Types   []string     `json:"types,omitempty"`
	States  []ActorState `json:"states,omitempty"`
	Runtime []RuntimeID  `json:"runtime,omitempty"`
}

type ActorSubscription struct {
	ID    string
	Scope ActorObserveScope
	C     <-chan ActorPatch
}

type ActorSyncMode string

const (
	ActorSyncFull        ActorSyncMode = "full"
	ActorSyncIncremental ActorSyncMode = "incremental"
)

type ActorSyncResult struct {
	Mode     ActorSyncMode `json:"mode"`
	Epoch    uint64        `json:"epoch"`
	Snapshot *TreeSnapshot `json:"snapshot,omitempty"`
	Patches  []ActorPatch  `json:"patches,omitempty"`
}
