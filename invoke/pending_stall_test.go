package invoke

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/message"
)

// TestPendingTable_DeliverNeverBlocks verifies Deliver parks frames in the
// slot's overflow instead of waiting on a full caller buffer: the serial
// replyLoop of the owning Cell must never spend time on one slow consumer.
func TestPendingTable_DeliverNeverBlocks(t *testing.T) {
	orig := ovFrameCap
	ovFrameCap = 64
	defer func() { ovFrameCap = orig }()

	pt := NewPendingTable()
	ch := make(chan message.Frame, 1)
	pt.Register(1, "", "", ch)
	ch <- message.Frame{Kind: message.KindReply, CorID: 1} // fill the buffer

	start := time.Now()
	for i := 0; i < 32; i++ {
		if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 1}) {
			t.Fatalf("Deliver %d into full buffer: got false, want true (overflow)", i)
		}
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deliver stalled %v; overflow enqueue must not block", elapsed)
	}
	if !pt.Has(1) {
		t.Fatal("slot evicted despite overflow being under cap")
	}
}

// TestPendingTable_SlowConsumerLosesNothing verifies a consumer that lags
// behind the producer still receives every frame exactly once and in order:
// the overflow FIFO is pumped into the buffer as the consumer drains.
func TestPendingTable_SlowConsumerLosesNothing(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 4)
	pt.Register(7, "", "", ch)
	pump := pt.pumpFor(7)

	const total = 500
	go func() {
		for i := 0; i < total; i++ {
			if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7, Body: []byte{byte(i)}}) {
				t.Errorf("Deliver %d failed", i)
				return
			}
		}
		pt.Deliver(message.Frame{Kind: message.KindEnd, CorID: 7})
	}()

	for i := 0; i < total; i++ {
		frame := <-ch
		pump()
		if frame.Kind != message.KindReply || len(frame.Body) != 1 || frame.Body[0] != byte(i) {
			t.Fatalf("frame %d: got kind=%v body=%v, want KindReply body=[%d]", i, frame.Kind, frame.Body, i)
		}
	}
	end := <-ch
	pump()
	if end.Kind != message.KindEnd {
		t.Fatalf("terminal frame: got kind %v, want KindEnd", end.Kind)
	}
	// Deliver removes the mapping after enqueue, which races the consumer's
	// read of the terminal frame — poll for the deregistration.
	deadline := time.Now().Add(2 * time.Second)
	for pt.Has(7) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pt.Has(7) {
		t.Fatal("slot still registered after terminal frame")
	}
}

// TestPendingTable_EvictStalledSlot verifies that a slot whose overflow
// blows past ovFrameCap is evicted: the CorID mapping is removed and a
// terminal KindError frame is injected so the caller's Recv unblocks.
func TestPendingTable_EvictStalledSlot(t *testing.T) {
	orig := ovFrameCap
	ovFrameCap = 4
	defer func() { ovFrameCap = orig }()

	pt := NewPendingTable()
	ch := make(chan message.Frame, 2)
	pt.Register(42, "", "", ch)
	// Pre-fill so every Deliver hits the overflow path.
	ch <- message.Frame{Kind: message.KindReply, CorID: 42, Body: []byte("old-1")}
	ch <- message.Frame{Kind: message.KindReply, CorID: 42, Body: []byte("old-2")}

	for i := 0; i < 4; i++ {
		if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 42}) {
			t.Fatalf("Deliver %d under cap: got false, want true", i)
		}
	}
	// Fifth overflow append exceeds the cap: eviction, Deliver reports false.
	if pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 42}) {
		t.Fatal("Deliver past overflow cap: got true, want false")
	}

	if pt.Has(42) {
		t.Fatal("Has(42): stalled slot not evicted past overflow cap")
	}

	// The caller now drains: the OLDEST frame was dropped to make room and
	// the terminal error was appended, so drain order is the surviving
	// reply followed by the eviction terminal.
	got := <-ch
	if got.Kind != message.KindReply {
		t.Fatalf("first drained frame: Kind=%v, want KindReply (surviving queued frame)", got.Kind)
	}
	got2 := <-ch
	if got2.Kind != message.KindError {
		t.Fatalf("second drained frame: Kind=%v, want KindError (eviction terminal)", got2.Kind)
	}
	if got2.CorID != 42 {
		t.Fatalf("eviction terminal CorID=%d, want 42", got2.CorID)
	}
}

// TestPendingTable_GlobalCapEvictsOldest verifies that when the table is at
// maxPendingSlots, Register evicts the oldest slot (lowest CorID): the
// mapping is removed, a terminal KindError frame unblocks that caller, and
// the table size stays at the cap.
func TestPendingTable_GlobalCapEvictsOldest(t *testing.T) {
	origCap := maxPendingSlots
	maxPendingSlots = 4
	defer func() { maxPendingSlots = origCap }()

	pt := NewPendingTable()
	channels := make(map[uint64]chan message.Frame)
	for corID := uint64(10); corID < 14; corID++ {
		ch := make(chan message.Frame, 1)
		channels[corID] = ch
		pt.Register(corID, "", "", ch)
	}
	if n := pt.Len(); n != 4 {
		t.Fatalf("Len = %d, want 4", n)
	}

	// Register one more: oldest slot (10) is evicted, new slot (99) added.
	newCh := make(chan message.Frame, 1)
	pt.Register(99, "", "", newCh)
	if n := pt.Len(); n != 4 {
		t.Fatalf("Len after cap-evict = %d, want 4", n)
	}
	if pt.Has(10) {
		t.Fatal("oldest slot 10 still registered after cap eviction")
	}
	if !pt.Has(99) {
		t.Fatal("new slot 99 not registered")
	}
	if !pt.Has(11) || !pt.Has(12) || !pt.Has(13) {
		t.Fatal("non-oldest slots were evicted")
	}

	// The evicted caller's channel receives a terminal error frame.
	select {
	case env := <-channels[10]:
		if env.Kind != message.KindError || env.CorID != 10 {
			t.Fatalf("evicted slot received %+v, want KindError CorID=10", env)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for eviction error frame")
	}
}

// TestPendingTable_GlobalCapReRegisterNoEvict verifies that re-registering
// an existing CorID at capacity replaces the slot's channel without
// evicting any other slot.
func TestPendingTable_GlobalCapReRegisterNoEvict(t *testing.T) {
	origCap := maxPendingSlots
	maxPendingSlots = 3
	defer func() { maxPendingSlots = origCap }()

	pt := NewPendingTable()
	for corID := uint64(1); corID <= 3; corID++ {
		pt.Register(corID, "", "", make(chan message.Frame, 1))
	}

	replacement := make(chan message.Frame, 1)
	pt.Register(2, "", "", replacement)
	if n := pt.Len(); n != 3 {
		t.Fatalf("Len = %d, want 3", n)
	}
	for corID := uint64(1); corID <= 3; corID++ {
		if !pt.Has(corID) {
			t.Fatalf("slot %d missing after re-register", corID)
		}
	}
	// Delivery routes to the replacement channel.
	if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 2}) {
		t.Fatal("Deliver to re-registered slot failed")
	}
	select {
	case env := <-replacement:
		if env.CorID != 2 {
			t.Fatalf("replacement channel received %+v", env)
		}
	default:
		t.Fatal("replacement channel empty after Deliver")
	}
}
