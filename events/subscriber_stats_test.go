package events

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

// TestSubscriberDroppedCounted pins the slow-consumer observability: a
// full subscriber buffer must count its losses (fan-out and replay
// paths), not silently swallow them.
func TestSubscriberDroppedCounted(t *testing.T) {
	s := NewStore(16)
	aid, err := id.Parse("01000000000000000000000000000000")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// Subscribe without draining: the 16-slot buffer fills, then every
	// further Push drops and counts.
	sub, err := s.Subscribe(aid, 0, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	for i := 0; i < 40; i++ {
		s.Push(aid, Record{Kind: actor.WatchStarted, Timestamp: time.Now()})
	}

	stats := s.SubscriberStats()
	if len(stats) != 1 {
		t.Fatalf("SubscriberStats len = %d, want 1", len(stats))
	}
	if stats[0].Dropped == 0 {
		t.Fatal("Dropped = 0, want > 0 after buffer overflow")
	}
	if stats[0].BufferCap != cap(sub.(*eventSubscription).ch) {
		t.Fatalf("BufferCap mismatch: %d", stats[0].BufferCap)
	}
}

// TestStoreCloseTearsDownSubscribers pins the shutdown contract: Close
// releases every subscriber (Recv reports EOF) and is idempotent.
func TestStoreCloseTearsDownSubscribers(t *testing.T) {
	s := NewStore(16)
	aid, aidErr := id.Parse("01000000000000000000000000000000")
	if aidErr != nil {
		t.Fatalf("Parse: %v", aidErr)
	}
	sub, subErr := s.Subscribe(aid, 0, nil)
	if subErr != nil {
		t.Fatalf("Subscribe: %v", subErr)
	}
	s.Push(aid, Record{Kind: actor.WatchStarted, Timestamp: time.Now()})

	s.Close()
	s.Close() // idempotent

	if _, err := sub.Recv(); err == nil {
		t.Fatal("Recv after store Close unexpectedly succeeded, want EOF")
	}
	if _, err := sub.Recv(); err == nil {
		t.Fatal("second Recv after Close succeeded, want persistent EOF")
	}
}
