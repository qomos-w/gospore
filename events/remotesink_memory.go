package events

import (
	"sync"

	"github.com/qomos-w/gospore/id"
)

// MemoryRemoteSink is a test-double RemoteSink that buffers Records in
// memory. It is suitable for single-process tests of cross-App event
// forwarding and as the reference implementation for Phase 2 remote
// event delivery.
//
// In production Phase 2 deployments MemoryRemoteSink is replaced by
// a transport-backed sink that serialises Records and pushes them
// across the network to subscribing Apps.
// NewMemoryRemoteSink creates an empty MemoryRemoteSink.
func NewMemoryRemoteSink() *MemoryRemoteSink {
	return &MemoryRemoteSink{records: make([]memoryRecord, 0)}
}

type MemoryRemoteSink struct {
	mu      sync.Mutex
	records []memoryRecord
}

type memoryRecord struct {
	ActorID id.ActorID
	Rec     Record
}

// Push appends the record to the in-memory buffer.
func (s *MemoryRemoteSink) Push(actorID id.ActorID, rec Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, memoryRecord{ActorID: actorID, Rec: rec})
}

// Records returns a snapshot of all pushed records.
func (s *MemoryRemoteSink) Records() []struct {
	ActorID id.ActorID
	Rec     Record
} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]struct {
		ActorID id.ActorID
		Rec     Record
	}, len(s.records))
	for i, r := range s.records {
		out[i] = struct {
			ActorID id.ActorID
			Rec     Record
		}{ActorID: r.ActorID, Rec: r.Rec}
	}
	return out
}

var _ RemoteSink = (*MemoryRemoteSink)(nil)
