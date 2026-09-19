package contracts

import "github.com/qomos-w/gospore/model"

// FailureDetector detects remote runtime availability.
type FailureDetector interface {
	IsAvailable(runtime model.RuntimeID) bool
	OnDetection(cb func(runtime model.RuntimeID, state model.RuntimeState))
	Confirm(runtime model.RuntimeID) error
}

// RuntimeOperator provides lifecycle operations for the local runtime host.
type RuntimeOperator interface {
	Join(topologyAddr string) error
	Bootstrap(seeds []string) error
	Leave() error
	Shutdown() error
}

// RuntimeManager provides administrative operations over runtimes in the topology.
type RuntimeManager interface {
	List() []model.RuntimeStatus
	Get(runtime model.RuntimeID) (*model.RuntimeStatus, error)
	Remove(runtime model.RuntimeID) error
	Drain(runtime model.RuntimeID) error
	Pause(runtime model.RuntimeID) error
	Resume(runtime model.RuntimeID) error
	Events() <-chan model.RuntimeEvent
}
