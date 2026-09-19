package invoke

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/qomos-w/gospore/message"
)

// Cancel must be distinguishable from a legitimate void-handler End: the
// gateway's unary invoke path calls Cancel on timeout and then reads
// RecvRaw — returning io.EOF there masked the timeout as an empty success
// reply (frontend resolved undefined). See gateway/server.go invokeActor.

func TestCancelReturnsErrCallCancelledFromRecvRaw(t *testing.T) {
	ch := make(chan message.Frame, 1)
	pt := NewPendingTable()
	stream := NewStream(ch, 1, pt, nil)

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.RecvRaw()
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	if err := stream.(interface{ Cancel() error }).Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrCallCancelled) {
			t.Fatalf("RecvRaw after Cancel = %v, want ErrCallCancelled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RecvRaw did not unblock after Cancel")
	}
}

func TestCancelReturnsErrCallCancelledFromRecv(t *testing.T) {
	ch := make(chan message.Frame, 1)
	pt := NewPendingTable()
	stream := NewStream(ch, 1, pt, nil)

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	stream.(interface{ Cancel() error }).Cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrCallCancelled) {
			t.Fatalf("Recv after Cancel = %v, want ErrCallCancelled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv did not unblock after Cancel")
	}
}

// A terminal frame that raced with Cancel must still be delivered: the
// callee's real outcome wins over the teardown.
func TestQueuedTerminalFrameWinsOverCancel(t *testing.T) {
	ch := make(chan message.Frame, 4)
	pt := NewPendingTable()
	stream := NewStream(ch, 1, pt, nil)

	ch <- message.Frame{Kind: message.KindReply, PayloadMode: message.PayloadModeValue, Body: []byte("late-reply")}
	ch <- message.Frame{Kind: message.KindEnd}
	stream.(interface{ Cancel() error }).Cancel()

	raw, err := stream.RecvRaw()
	if err != nil {
		t.Fatalf("RecvRaw after Cancel with queued reply: %v", err)
	}
	if string(raw) != "late-reply" {
		t.Fatalf("reply = %q, want late-reply", raw)
	}
	if _, err := stream.RecvRaw(); !errors.Is(err, io.EOF) {
		t.Fatalf("second RecvRaw = %v, want io.EOF (End frame)", err)
	}
}

// Close (caller cleanup after reading) keeps the void-handler contract:
// io.EOF, never ErrCallCancelled.
func TestCloseStillYieldsEOF(t *testing.T) {
	ch := make(chan message.Frame, 1)
	pt := NewPendingTable()
	stream := NewStream(ch, 1, pt, nil)
	stream.Close()

	if _, err := stream.RecvRaw(); !errors.Is(err, io.EOF) {
		t.Fatalf("RecvRaw after Close = %v, want io.EOF", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after Close = %v, want io.EOF", err)
	}
}
