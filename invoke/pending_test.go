package invoke

import (
	"testing"

	"github.com/qomos-w/gospore/message"
)

// TestPendingTableAutoReleaseOnEnd confirms a slot is released as soon as the
// terminal frame (KindEnd) is delivered, so callers who never Close
// (fire-and-forget Tell) or who read to EOF without Close do not leak. The
// channel stays readable after the slot is gone — only the CorID mapping is
// removed.
func TestPendingTableAutoReleaseOnEnd(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 4)
	pt.Register(7, "", "", ch)

	// Data frames keep the slot registered.
	if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7, Body: []byte("chunk")}) {
		t.Fatal("Deliver reply: got false, want true")
	}
	if !pt.Has(7) {
		t.Fatal("Has(7): slot released before terminal frame delivered")
	}

	// Terminal frame releases the slot but the buffered reply stays readable.
	if !pt.Deliver(message.Frame{Kind: message.KindEnd, CorID: 7}) {
		t.Fatal("Deliver end: got false, want true")
	}
	if pt.Has(7) {
		t.Fatal("Has(7): slot still registered after terminal frame delivered")
	}
	if got := <-ch; string(got.Body) != "chunk" {
		t.Fatalf("buffered reply: got %q, want %q", got.Body, "chunk")
	}
	if got := <-ch; got.Kind != message.KindEnd {
		t.Fatalf("buffered end: got Kind=%v, want KindEnd", got.Kind)
	}
	// A late frame for a released CorID is dropped.
	if pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 7}) {
		t.Fatal("Deliver after release: got true, want false")
	}
}

// TestPendingTableAutoReleaseOnError mirrors the End case for failure:
// KindError is terminal too, so the slot is released on delivery.
func TestPendingTableAutoReleaseOnError(t *testing.T) {
	pt := NewPendingTable()
	pt.Register(9, "", "", make(chan message.Frame, 1))

	if !pt.Deliver(message.Frame{Kind: message.KindError, CorID: 9, Body: []byte("boom")}) {
		t.Fatal("Deliver error: got false, want true")
	}
	if pt.Has(9) {
		t.Fatal("Has(9): slot still registered after error delivered")
	}
}

// TestPendingTableDataFramesKeepSlot confirms non-terminal frames never
// release the slot, preserving streaming semantics.
func TestPendingTableDataFramesKeepSlot(t *testing.T) {
	pt := NewPendingTable()
	pt.Register(11, "", "", make(chan message.Frame, 4))
	for i := 0; i < 3; i++ {
		if !pt.Deliver(message.Frame{Kind: message.KindReply, CorID: 11, Body: []byte("x")}) {
			t.Fatalf("Deliver reply %d: got false, want true", i)
		}
	}
	if !pt.Has(11) {
		t.Fatal("Has(11): slot released by data frames")
	}
}
