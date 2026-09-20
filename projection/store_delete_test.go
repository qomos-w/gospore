package projection

import (
	"io"
	"testing"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/spore/binding"
)

func deleteTestID(slot uint16) id.ActorID {
	return id.NewCanonical(slot, 0, func() uint64 { return 1 }).Next()
}

func deleteTestSnapshot(aid id.ActorID, version uint64) Snapshot {
	return Snapshot{
		ActorID: aid,
		Version: version,
		Fields: map[string]binding.ViewProjection{
			"comp": {Fields: map[string]any{"state": "completed"}},
		},
	}
}

// TestStore_Delete_RemovesStateAndClosesWatchers confirms Delete removes
// the actor's projection entry and terminates its subscribers instead of
// leaving a tombstone that accumulates for the process lifetime.
func TestStore_Delete_RemovesStateAndClosesWatchers(t *testing.T) {
	s := NewStore(0)
	aid := deleteTestID(7)

	s.Publish(aid, deleteTestSnapshot(aid, 1))

	if _, ok := s.Get(aid); !ok {
		t.Fatal("pre-delete: snapshot missing")
	}

	sub, err := s.Watch(aid)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if _, err := sub.Recv(); err != nil { // drain initial full snapshot
		t.Fatalf("drain initial snapshot: %v", err)
	}

	s.Delete(aid)

	if _, ok := s.Get(aid); ok {
		t.Fatal("post-delete: snapshot still present")
	}
	if s.Has(aid) {
		t.Fatal("post-delete: Has still true")
	}
	if _, err := sub.Recv(); err != io.EOF {
		t.Fatalf("watcher Recv after delete: err = %v, want io.EOF", err)
	}
}

// TestStore_Delete_IdempotentAndRepublishable confirms a second Delete is
// a no-op and a republish after Delete recreates the entry (restart
// semantics).
func TestStore_Delete_IdempotentAndRepublishable(t *testing.T) {
	s := NewStore(0)
	aid := deleteTestID(8)

	s.Publish(aid, deleteTestSnapshot(aid, 1))
	s.Delete(aid)
	s.Delete(aid) // must not panic

	s.Publish(aid, deleteTestSnapshot(aid, 2))
	if _, ok := s.Get(aid); !ok {
		t.Fatal("republish after delete: snapshot missing")
	}
}
