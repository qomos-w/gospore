package events

import (
	"sync"
	"testing"
	"time"

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

// blockingSink blocks inside Push until unblocked, signalling first entry.
type blockingSink struct {
	once     sync.Once
	entered  chan struct{}
	unblock  chan struct{}
}

func (b *blockingSink) Push(id.ActorID, Record) {
	b.once.Do(func() { close(b.entered) })
	<-b.unblock
}

// TestPushRemoteSinkOutsideLock pins that RemoteSink dispatch happens
// outside the store lock: a wedged (e.g. network-blocked) sink must not
// serialise other actors' ring writes behind it. The wedged sink still
// blocks its own caller's return (implementations are contractually
// non-blocking; this store-side guarantee is about lock scope).
func TestPushRemoteSinkOutsideLock(t *testing.T) {
	s := NewStore(4)
	s.RemoteSink = &blockingSink{entered: make(chan struct{}), unblock: make(chan struct{})}

	a, err := id.Parse("01000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	b, err := id.Parse("02000000000000000000000000000002")
	if err != nil {
		t.Fatal(err)
	}

	// First push parks inside the sink dispatch.
	parked := make(chan struct{})
	go func() {
		defer close(parked)
		s.Push(a, Record{Kind: actor.WatchStarted})
	}()
	sink := s.RemoteSink.(*blockingSink)
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sink never entered")
	}

	// The store lock must be free: a lock-taking read completes while the
	// dispatch is wedged, and a push for another actor lands in its ring.
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Push(b, Record{Kind: actor.WatchStarted})
	}()
	deadline := time.After(2 * time.Second)
	for {
		if recs := s.Recent(b, 0, nil, 1); len(recs) == 1 && recs[0].SeqNo == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Push ring write serialized behind a blocked RemoteSink — dispatch must be outside the store lock")
		case <-time.After(10 * time.Millisecond):
		}
	}

	close(sink.unblock)
	<-done
	<-parked
}

