package model

import "time"

type RuntimeID uint32

type RuntimeAddr struct {
	Host string
	Port int
}

type RuntimeInfo struct {
	ID   RuntimeID
	Name string
	Addr RuntimeAddr
}

type SnapshotStorageClass string

type RuntimeState int

const (
	RuntimeJoining RuntimeState = iota
	RuntimeOnline
	RuntimeDraining
	RuntimePaused
	RuntimeSuspect
	RuntimeDown
)

type RuntimeStatus struct {
	RuntimeInfo
	State      RuntimeState
	ActorCount int
	Load       float64
	CPU        float64
	Memory     float64
	LastSeen   time.Time
	Uptime     time.Duration
}

type TopologyHealth int

const (
	TopologyHealthy TopologyHealth = iota
	TopologyDegraded
	TopologyCritical
)

type TopologyStatus struct {
	Health       TopologyHealth
	RuntimeCount int
	OnlineCount  int
	ActorCount   int64
	LoadAvg      float64
}
