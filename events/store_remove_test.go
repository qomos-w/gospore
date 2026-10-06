package events

import (
	"sync"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

// TestRemove_EvictsRingAndClosesSubscribers pins the eviction-on-destroy
// contract: Remove drops the actor's ring (Recent/RingStats see nothing)
// and terminates lingering subscribers with EOF instead of leaving them
// parked on a stream that can never produce another record.
func TestRemove_EvictsRingAndClosesSubscribers(t *testing.T) {
	s := NewStore(8)
	a, err := id.Parse("01000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	s.Push(a, Record{Kind: actor.WatchStarted})
	if recs := s.Recent(a, 0, nil, 0); len(recs) != 1 {
		t.Fatalf("Recent before Remove = %d records, want 1", len(recs))
	}

	sub, err := s.Subscribe(a, 0, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	s.Remove(a)

	if recs := s.Recent(a, 0, nil, 0); recs != nil {
		t.Fatalf("Recent after Remove = %v, want nil", recs)
	}
	if stats := s.RingStats(); len(stats) != 0 {
		t.Fatalf("RingStats after Remove = %d entries, want 0", len(stats))
	}
	if _, err := sub.Recv(); err == nil {
		t.Fatal("Recv after Remove succeeded, want EOF")
	}
}

// TestRemove_IdempotentUnknownActor covers the no-op paths: Remove of a
// never-seen actor and a double Remove must not panic.
func TestRemove_IdempotentUnknownActor(t *testing.T) {
	s := NewStore(8)
	b, err := id.Parse("01000000000000000000000000000002")
	if err != nil {
		t.Fatal(err)
	}

	s.Remove(b) // unknown actor

	s.Push(b, Record{Kind: actor.WatchStarted})
	s.Remove(b)
	s.Remove(b) // second Remove after eviction

	if recs := s.Recent(b, 0, nil, 0); recs != nil {
		t.Fatalf("Recent after double Remove = %v, want nil", recs)
	}
}

// TestRing_LazyAllocation pins that a ring is allocated on first Push,
// never on Subscribe/Recent alone: subscribing to a terminated (or
// never-emitting) actor must not pin a full-capacity ring.
func TestRing_LazyAllocation(t *testing.T) {
	s := NewStore(8)
	a, err := id.Parse("01000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}

	sub, err := s.Subscribe(a, 5, nil) // replay path touches a nil ring
	if err != nil {
		t.Fatalf("Subscribe with replay on bare entry: %v", err)
	}
	if recs := s.Recent(a, 0, nil, 0); recs != nil {
		t.Fatalf("Recent on subscribe-only actor = %v, want nil", recs)
	}
	if stats := s.RingStats(); len(stats) != 0 {
		t.Fatalf("RingStats on subscribe-only actor = %d entries, want 0", len(stats))
	}
	_ = sub.Close()

	s.Push(a, Record{Kind: actor.WatchStarted})
	if stats := s.RingStats(); len(stats) != 1 {
		t.Fatalf("RingStats after first Push = %d entries, want 1", len(stats))
	}
}

// TestRemove_ConcurrentWithPush is a -race smoke test: Push on one
// goroutine while Remove runs must stay consistent, and the final state
// after the last Remove must be empty.
func TestRemove_ConcurrentWithPush(t *testing.T) {
	s := NewStore(8)
	a, err := id.Parse("01000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.Push(a, Record{Kind: actor.WatchStarted})
			}
		}
	}()

	s.Remove(a)
	s.Remove(a)
	close(stop)
	wg.Wait()
	s.Remove(a)

	deadline := time.After(2 * time.Second)
	for {
		if recs := s.Recent(a, 0, nil, 1); recs == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("Recent not empty after final Remove")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
