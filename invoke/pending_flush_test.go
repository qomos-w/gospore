package invoke

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/message"
)

func TestFlushTerminatesWaitingSlots(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 16)
	pt.Register(7, "test.slow", "unary", ch)

	received := make(chan message.Frame, 1)
	go func() {
		received <- <-ch
	}()

	if n := pt.Flush("cell destroyed"); n != 1 {
		t.Fatalf("Flush returned %d, want 1", n)
	}

	select {
	case frame := <-received:
		if frame.Kind != message.KindError {
			t.Errorf("frame kind = %v, want KindError", frame.Kind)
		}
		if frame.CorID != 7 {
			t.Errorf("CorID = %d, want 7", frame.CorID)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting reader was not unblocked by Flush")
	}

	if pt.Len() != 0 {
		t.Errorf("Len() = %d, want 0", pt.Len())
	}
	snap := pt.Snapshot()
	if snap.InFlight != 0 {
		t.Errorf("snapshot InFlight = %d, want 0", snap.InFlight)
	}
	if snap.Flushed != 1 {
		t.Errorf("snapshot Flushed = %d, want 1", snap.Flushed)
	}
	if len(snap.RecentEvictions) != 1 || snap.RecentEvictions[0].Reason != EvictionShutdown {
		t.Errorf("RecentEvictions = %+v, want one shutdown record", snap.RecentEvictions)
	}
	if pt.Deliver(message.Frame{Kind: message.KindEnd, CorID: 7}) {
		t.Error("Deliver after Flush should find no slot")
	}
}

func TestFlushEmptyTable(t *testing.T) {
	pt := NewPendingTable()
	if n := pt.Flush("nothing"); n != 0 {
		t.Fatalf("Flush on empty table returned %d, want 0", n)
	}
}

func TestFlushFullBufferStillTerminates(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 16)
	pt.Register(1, "test.slow", "stream", ch)
	for i := 0; i < cap(ch); i++ {
		ch <- message.Frame{Kind: message.KindReply, CorID: 1}
	}

	if n := pt.Flush("cell destroyed"); n != 1 {
		t.Fatalf("Flush returned %d, want 1", n)
	}

	// One queued reply is dropped to make room for the terminal error, so
	// the channel still holds exactly cap(ch) frames, one of them terminal.
	drained, terminal := 0, 0
	for {
		select {
		case f := <-ch:
			drained++
			if f.Kind == message.KindError {
				terminal++
			}
		default:
			if drained != cap(ch) {
				t.Fatalf("drained %d frames, want %d", drained, cap(ch))
			}
			if terminal != 1 {
				t.Fatalf("terminal frames = %d, want 1", terminal)
			}
			return
		}
	}
}
