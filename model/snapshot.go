package model

// SnapshotTarget describes the actor metadata used to resolve a snapshot backend.
type SnapshotTarget struct {
	Ref          ActorRef
	Type         string
	Owner        Principal
	StorageClass SnapshotStorageClass
}
