package events

import (
	"testing"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

func TestRingEvictionCounted(t *testing.T) {
	r := NewRing(3)
	for i := uint64(1); i <= 3; i++ {
		r.Push(Record{SeqNo: i, Kind: actor.WatchStarted})
	}
	if got := r.Evicted(); got != 0 {
		t.Fatalf("Evicted before overflow = %d, want 0", got)
	}
	r.Push(Record{SeqNo: 4, Kind: actor.WatchStarted})
	if got := r.Evicted(); got != 1 {
		t.Fatalf("Evicted after first overflow = %d, want 1", got)
	}
	r.Push(Record{SeqNo: 5, Kind: actor.WatchStarted})
	r.Push(Record{SeqNo: 6, Kind: actor.WatchStarted})
	if got := r.Evicted(); got != 3 {
		t.Fatalf("Evicted after three overflows = %d, want 3", got)
	}
	if r.OldestSeqNo() != 4 || r.LatestSeqNo() != 6 {
		t.Fatalf("window = [%d,%d], want [4,6]", r.OldestSeqNo(), r.LatestSeqNo())
	}
}

func TestStoreRingStatsAggregates(t *testing.T) {
	s := NewStore(2)
	busy, err := id.Parse("01000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	quiet, err := id.Parse("02000000000000000000000000000002")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		s.Push(busy, Record{Kind: actor.WatchStarted})
	}
	s.Push(quiet, Record{Kind: actor.WatchStarted})

	stats := s.RingStats()
	if len(stats) != 2 {
		t.Fatalf("RingStats len = %d, want 2", len(stats))
	}
	// Sorted by Evicted descending: busy (3 evictions) before quiet (0).
	if stats[0].ActorID != busy || stats[0].Evicted != 3 || stats[0].Capacity != 2 || stats[0].Len != 2 {
		t.Fatalf("stats[0] = %+v", stats[0])
	}
	if stats[1].ActorID != quiet || stats[1].Evicted != 0 {
		t.Fatalf("stats[1] = %+v", stats[1])
	}
}
