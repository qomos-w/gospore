package transport

import (
	"testing"

	"github.com/qomos-w/gospore/message"
)

func TestMemoryRemotePairWithBufferBackpressure(t *testing.T) {
	a, b := NewMemoryRemotePairWithBuffer(2)
	_ = b
	frame := message.Frame{}
	if err := a.Send(frame); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	if err := a.Send(frame); err != nil {
		t.Fatalf("send 2: %v", err)
	}
	if err := a.Send(frame); err == nil {
		t.Fatal("send 3 should hit full buffer")
	}
	// Draining one slot releases backpressure.
	<-b.Receive()
	if err := a.Send(frame); err != nil {
		t.Fatalf("send after drain: %v", err)
	}
}

func TestMemoryRemotePairDefaultCapacity(t *testing.T) {
	a, _ := NewMemoryRemotePair()
	if got := cap(a.outbound); got != DefaultInboundCapacity {
		t.Fatalf("default buffer = %d, want %d", got, DefaultInboundCapacity)
	}
}

func TestMemoryRemotePairWithBufferInvalidFallsBack(t *testing.T) {
	a, _ := NewMemoryRemotePairWithBuffer(0)
	if got := cap(a.outbound); got != DefaultInboundCapacity {
		t.Fatalf("zero buffer = %d, want default %d", got, DefaultInboundCapacity)
	}
}
