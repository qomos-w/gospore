package model

import "time"

// ShutdownSerializeMode controls what runtime action happens after one shutdown tree is captured.
type ShutdownSerializeMode int

const (
	ShutdownSerializeOnly ShutdownSerializeMode = iota
	ShutdownSerializeAndStop
	ShutdownSerializeAndDestroy
)

// ShutdownTreeOptions configures one shutdown-tree capture operation.
type ShutdownTreeOptions struct {
	Mode           ShutdownSerializeMode
	FreezeTimeout  time.Duration
	CaptureTimeout time.Duration
	Strict         bool
}

// ShutdownTreeResult reports the outcome of one shutdown-tree capture operation.
type ShutdownTreeResult struct {
	Roots       []ActorRef
	Runtime     RuntimeID
	CapturedAt  time.Time
	NodeCount   int
	Persisted   bool
	ArtifactKey string
}

// ShutdownTreePackage is the versioned serialized artifact for one captured actor tree or runtime.
type ShutdownTreePackage struct {
	Version    int
	Runtime    RuntimeID
	Roots      []ActorRef
	CapturedAt time.Time
	Mode       ShutdownSerializeMode
	Nodes      []ShutdownTreeNode
}

// ShutdownTreeNode stores one serialized actor node inside a shutdown tree package.
type ShutdownTreeNode struct {
	Ref            ActorRef
	Type           string
	Parent         ActorRef
	Children       []ActorRef
	Owner          Principal
	CaptureOrder   int
	LifecycleState ActorState
	Recoverable    bool
	State          []byte
	CaptureError   string
}
