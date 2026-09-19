package invoke

import (
	"testing"

	"github.com/qomos-w/gospore/message"
)

// TestPendingTable_SnapshotCounters verifies the lifetime outcome counters:
// terminal End/Error frames, send failures, and early closes each increment
// exactly their own counter, and in-flight drops back to zero.
func TestPendingTable_SnapshotCounters(t *testing.T) {
	pt := NewPendingTable()

	pt.Register(1, "svc.a", string(CallModeUnary), make(chan message.Frame, 4))
	pt.Register(2, "svc.b", string(CallModeStream), make(chan message.Frame, 4))

	s := pt.Snapshot()
	if s.InFlight != 2 || s.Registered != 2 {
		t.Fatalf("after register: InFlight=%d Registered=%d, want 2/2", s.InFlight, s.Registered)
	}
	if s.InFlightByMode["unary"] != 1 || s.InFlightByMode["stream"] != 1 {
		t.Fatalf("InFlightByMode=%v, want unary=1 stream=1", s.InFlightByMode)
	}
	if s.InFlightByCall["svc.a"] != 1 || s.InFlightByCall["svc.b"] != 1 {
		t.Fatalf("InFlightByCall=%v, want svc.a=1 svc.b=1", s.InFlightByCall)
	}

	// Terminal End releases slot 1 and counts a completion.
	if !pt.Deliver(message.Frame{Kind: message.KindEnd, CorID: 1}) {
		t.Fatal("Deliver end: got false, want true")
	}
	// Terminal Error releases slot 2 and counts a failure.
	if !pt.Deliver(message.Frame{Kind: message.KindError, CorID: 2, Body: []byte("boom")}) {
		t.Fatal("Deliver error: got false, want true")
	}

	s = pt.Snapshot()
	if s.InFlight != 0 {
		t.Fatalf("InFlight after terminals: got %d, want 0", s.InFlight)
	}
	if len(s.InFlightByMode) != 0 || len(s.InFlightByCall) != 0 {
		t.Fatalf("breakdowns after terminals: mode=%v call=%v, want empty", s.InFlightByMode, s.InFlightByCall)
	}
	if s.Completed != 1 || s.Errored != 1 {
		t.Fatalf("Completed=%d Errored=%d, want 1/1", s.Completed, s.Errored)
	}

	// A send failure and an early close each hit their own counter.
	pt.Register(3, "svc.c", string(CallModeTell), make(chan message.Frame, 1))
	pt.SendFailed(3)
	pt.Register(4, "svc.d", string(CallModeTell), make(chan message.Frame, 1))
	pt.Unregister(4)

	s = pt.Snapshot()
	if s.SendFailed != 1 || s.ClosedEarly != 1 || s.InFlight != 0 {
		t.Fatalf("SendFailed=%d ClosedEarly=%d InFlight=%d, want 1/1/0", s.SendFailed, s.ClosedEarly, s.InFlight)
	}

	// Unregister of an unknown CorID must not count as an early close.
	pt.Unregister(999)
	if s := pt.Snapshot(); s.ClosedEarly != 1 {
		t.Fatalf("ClosedEarly after unknown unregister: got %d, want 1", s.ClosedEarly)
	}
}

// TestPendingTable_SnapshotStallCounters verifies dropped-frame and stalled
// eviction counters using a shrunken overflow cap.
func TestPendingTable_SnapshotStallCounters(t *testing.T) {
	orig := ovFrameCap
	ovFrameCap = 2
	defer func() { ovFrameCap = orig }()

	pt := NewPendingTable()
	ch := make(chan message.Frame, 1)
	pt.Register(7, "svc.e", string(CallModeStream), ch)
	ch <- message.Frame{Kind: message.KindReply, CorID: 7} // fill the buffer

	// Two deliveries park in overflow, the third exceeds the cap → eviction.
	if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7}) {
		t.Fatal("first overflow Deliver: got false, want true")
	}
	if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7}) {
		t.Fatal("second overflow Deliver: got false, want true")
	}
	if pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7}) {
		t.Fatal("third overflow Deliver past cap: got true, want false")
	}

	s := pt.Snapshot()
	// Eviction dropped the three overflowed frames plus one buffered frame
	// displaced by the injected terminal error.
	if s.DroppedFrames != 4 {
		t.Fatalf("DroppedFrames=%d, want 4", s.DroppedFrames)
	}
	if s.EvictedStalled != 1 {
		t.Fatalf("EvictedStalled=%d, want 1", s.EvictedStalled)
	}
	if s.InFlight != 0 || s.InFlightByCall["svc.e"] != 0 {
		t.Fatalf("stalled slot not removed from breakdowns: %v", s)
	}
	if len(s.RecentEvictions) != 1 || s.RecentEvictions[0].CallID != "svc.e" ||
		s.RecentEvictions[0].Reason != EvictionStalled || s.RecentEvictions[0].Drops != 4 {
		t.Fatalf("RecentEvictions=%+v, want one stalled svc.e record with Drops=4", s.RecentEvictions)
	}
}

// TestPendingTable_RecentEvictionsRing verifies eviction attribution: stalled
// and capacity evictions both land in the recent ring with callID/mode/CorID,
// and the ring keeps only the newest recentEvictionsCap records.
func TestPendingTable_RecentEvictionsRing(t *testing.T) {
	orig, origCap := ovFrameCap, maxPendingSlots
	ovFrameCap = 1
	maxPendingSlots = 4
	defer func() { ovFrameCap, maxPendingSlots = orig, origCap }()

	pt := NewPendingTable()

	// One stalled eviction.
	ch := make(chan message.Frame, 1)
	pt.Register(7, "svc.stream", string(CallModeStream), ch)
	ch <- message.Frame{Kind: message.KindReply, CorID: 7}
	pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7}) // first overflow frame
	pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7}) // past cap → evict

	// Capacity evictions: fill past the cap so older slots get evicted.
	for i := 0; i < recentEvictionsCap; i++ {
		pt.Register(uint64(100+i), "svc.bulk", string(CallModeUnary), make(chan message.Frame, 1))
	}

	s := pt.Snapshot()
	// One stalled eviction plus four capacity evictions (CorIDs 100..103
	// evicted as 104..107 register past the cap of 4).
	if len(s.RecentEvictions) != 5 {
		t.Fatalf("RecentEvictions len=%d, want 5: %+v", len(s.RecentEvictions), s.RecentEvictions)
	}
	if r := s.RecentEvictions[0]; r.CallID != "svc.stream" || r.Reason != EvictionStalled || r.CorID != 7 {
		t.Fatalf("first record = %+v, want the stalled svc.stream eviction", r)
	}
	for i, r := range s.RecentEvictions[1:] {
		if r.CallID != "svc.bulk" || r.Reason != EvictionCapacity || r.Mode != "unary" {
			t.Fatalf("record %d = %+v, want capacity eviction of svc.bulk", i+1, r)
		}
		if r.CorID != uint64(100+i) {
			t.Fatalf("record %d CorID=%d, want %d (oldest-first eviction order)", i+1, r.CorID, 100+i)
		}
		if r.At.IsZero() {
			t.Fatal("eviction record missing timestamp")
		}
	}
}
