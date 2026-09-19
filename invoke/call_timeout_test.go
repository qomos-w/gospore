package invoke

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/qomos-w/gospore/message"
)

// hangingStream is a Stream whose Recv blocks until Close/Cancel.
type hangingStream struct {
	done chan struct{}
}

func newHangingStream() *hangingStream {
	return &hangingStream{done: make(chan struct{})}
}

func (s *hangingStream) Recv() (any, error) {
	<-s.done
	return nil, io.EOF
}

func (s *hangingStream) RecvRaw() ([]byte, error) {
	<-s.done
	return nil, io.EOF
}

func (s *hangingStream) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return nil
}

// Cancel implements the cancellable-stream contract so Call.Cancel unblocks Recv.
func (s *hangingStream) Cancel() error { return s.Close() }



// When Final receives a context without a deadline and the Call carries a
// fallback timeout, Final must return context.DeadlineExceeded instead of
// blocking forever on a target that never replies.
func TestFinalFallbackTimeoutUnblocksDeadlinelessContext(t *testing.T) {
	c := NewCall(CallModeUnary, newHangingStream(), WithFinalTimeout(50*time.Millisecond))
	start := time.Now()
	_, err := c.Final(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("fallback timeout fired too late: %v", elapsed)
	}
}

// An explicit caller deadline must never be overridden by the fallback:
// with a 150ms deadline and a 50ms fallback the call must survive past the
// fallback window and expire on its own deadline.
func TestFinalExplicitDeadlineWinsOverFallback(t *testing.T) {
	c := NewCall(CallModeUnary, newHangingStream(), WithFinalTimeout(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Final(ctx)
	elapsed := time.Since(start)
	// Legacy error shape for explicit-deadline callers is io.EOF; what
	// matters here is that the fallback did not fire early.
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected legacy io.EOF shape, got %v", err)
	}
	if elapsed < 140*time.Millisecond {
		t.Fatalf("explicit deadline was overridden by fallback: expired after %v", elapsed)
	}
}

// With the fallback disabled (no option / non-positive), Final keeps the
// legacy behavior: it blocks until the caller context dies.
func TestFinalFallbackDisabledBlocksUntilCallerCancel(t *testing.T) {
	c := NewCall(CallModeUnary, newHangingStream(), WithFinalTimeout(0))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.Final(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error after caller cancel")
	}
	if elapsed < 40*time.Millisecond {
		t.Fatal("Final returned before caller cancellation")
	}
}

// The fallback only bounds Final; streaming consumption via Next must not
// be affected by the configured fallback timeout.
func TestFinalFallbackDoesNotAffectNext(t *testing.T) {
	ch := make(chan message.Frame, 2)
	pt := NewPendingTable()
	stream := NewStream(ch, 1, pt, nil)
	c := NewCall(CallModeStream, stream, WithFinalTimeout(50*time.Millisecond))

	// A chunk is available well past the 50ms fallback window; Next must
	// still deliver it and later see the stream end, unaffected by the
	// fallback bound.
	time.Sleep(80 * time.Millisecond)
	ch <- message.Frame{Kind: message.KindReply, PayloadMode: message.PayloadModeValue, Body: []byte("chunk")}
	ch <- message.Frame{Kind: message.KindEnd}
	v, err := c.Next(context.Background())
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if string(v.([]byte)) != "chunk" {
		t.Fatalf("unexpected chunk %v", v)
	}
	if _, err := c.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after stream end, got %v", err)
	}
}

// A unary reply that arrives before the fallback expires returns normally.
func TestFinalFallbackSuccessBeforeTimeout(t *testing.T) {
	ch := make(chan message.Frame, 2)
	pt := NewPendingTable()
	stream := NewStream(ch, 1, pt, nil)
	c := NewCall(CallModeUnary, stream, WithFinalTimeout(2*time.Second))

	ch <- message.Frame{Kind: message.KindReply, PayloadMode: message.PayloadModeValue, Body: []byte("ok")}
	ch <- message.Frame{Kind: message.KindEnd}
	v, err := c.Final(context.Background())
	if err != nil {
		t.Fatalf("Final failed: %v", err)
	}
	if string(v.([]byte)) != "ok" {
		t.Fatalf("unexpected reply %v", v)
	}
}
