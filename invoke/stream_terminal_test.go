package invoke

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/qomos-w/gospore/message"
)

// deliver frames for one call: a data reply followed by the terminal frame.
func deliverStream(pt *PendingTable, corID uint64, terminal message.Frame) {
	pt.Deliver(message.Frame{Kind: message.KindReply, CorID: corID, Body: []byte("chunk")})
	pt.Deliver(terminal)
}

// TestRecvTerminalStickyAfterEnd reproduces the host-bridge hang: Deliver
// deregisters the slot on the terminal frame and never closes s.ch, so once
// Recv consumed the KindEnd frame a second Recv — Final's drain loop after
// Next already saw EOF — used to park until ctx cancellation. The terminal
// latch must replay io.EOF immediately.
func TestRecvTerminalStickyAfterEnd(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 4)
	const corID = 11
	pt.Register(corID, "test.call", "stream", ch)
	deliverStream(pt, corID, message.Frame{Kind: message.KindEnd, CorID: corID})

	s := NewStream(ch, corID, pt, nil)
	if _, err := s.Recv(); err != nil {
		t.Fatalf("first Recv (data frame): %v", err)
	}
	if _, err := s.Recv(); err != io.EOF {
		t.Fatalf("second Recv (terminal): got %v, want io.EOF", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.Recv()
		done <- err
	}()
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("third Recv after consumed terminal: got %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv after consumed terminal frame parked — terminal latch missing")
	}
}

// TestFinalAfterNextDrainReturns covers the exact handleHostBridgeStream
// shape: drain via Call.Next until io.EOF, then call Final to consume the
// close frame. Before the latch, Final blocked for the caller's whole
// budget (observed as the deterministic 30s reverse-call timeout).
func TestFinalAfterNextDrainReturns(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 4)
	const corID = 12
	pt.Register(corID, "test.call", "stream", ch)
	deliverStream(pt, corID, message.Frame{Kind: message.KindEnd, CorID: corID})

	call := NewCall(CallModeStream, NewStream(ch, corID, pt, nil))
	ctx := context.Background()
	for {
		_, err := call.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := call.Final(ctx); err != nil {
			t.Errorf("Final after drained stream: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Final after Next consumed the terminal frame did not return — drain Recv parked")
	}
}

// TestRecvTerminalStickyOnError mirrors the End case for KindError: the
// latched error replays on repeated reads instead of parking.
func TestRecvTerminalStickyOnError(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 2)
	const corID = 13
	pt.Register(corID, "test.call", "stream", ch)
	pt.Deliver(message.Frame{Kind: message.KindError, CorID: corID, Body: []byte("boom")})

	s := NewStream(ch, corID, pt, nil)
	if _, err := s.Recv(); err == nil || err.Error() != "boom" {
		t.Fatalf("first Recv: got %v, want boom", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.Recv()
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || err.Error() != "boom" {
			t.Fatalf("second Recv: got %v, want boom", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv after consumed error terminal parked — terminal latch missing")
	}
}

// TestRecvRawTerminalStickyAfterEnd mirrors the End latch for the raw
// consumer (sshmanager-style host invokes read with RecvRaw).
func TestRecvRawTerminalStickyAfterEnd(t *testing.T) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 2)
	const corID = 14
	pt.Register(corID, "test.call", "unary", ch)
	pt.Deliver(message.Frame{Kind: message.KindEnd, CorID: corID})

	s := NewStream(ch, corID, pt, nil)
	if _, err := s.RecvRaw(); err != io.EOF {
		t.Fatalf("first RecvRaw: got %v, want io.EOF", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.RecvRaw()
		done <- err
	}()
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("second RecvRaw: got %v, want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RecvRaw after consumed terminal parked — terminal latch missing")
	}
}
