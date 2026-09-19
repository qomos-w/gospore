package contracts

import "github.com/qomos-w/gospore/model"

// Recoverable lets an actor save and restore its state.
type Recoverable interface {
	SaveState() ([]byte, error)
	RestoreState(data []byte) error
}

// Snapshotter stores and loads persisted actor state.
type Snapshotter interface {
	Save(ref model.ActorRef, state []byte) error
	Load(ref model.ActorRef) ([]byte, error)
	Delete(ref model.ActorRef) error
}

// SnapshotResolver chooses the snapshot backend for one actor target.
type SnapshotResolver interface {
	ResolveSnapshotter(target model.SnapshotTarget) Snapshotter
}
