package transport

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/message"
)

func TestHTTPWithBufferRejectsWhenFull(t *testing.T) {
	h, err := NewHTTPWithBuffer("", 1)
	if err != nil {
		t.Fatalf("NewHTTPWithBuffer: %v", err)
	}
	defer func() { _ = h.Close() }()

	sender, err := NewHTTP("http://" + h.ListenAddr())
	if err != nil {
		t.Fatalf("NewHTTP sender: %v", err)
	}
	defer func() { _ = sender.Close() }()

	frame := message.Frame{}
	if err := sender.Send(frame); err != nil {
		t.Fatalf("send 1: %v", err)
	}

	// Buffer holds 1; the next POST must surface 503 backpressure.
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := sender.Send(frame)
		if err == nil {
			if time.Now().After(deadline) {
				t.Fatal("second send never hit full buffer")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}

	// Drain the buffer; the next send succeeds again.
	<-h.Receive()
	if err := sender.Send(frame); err != nil {
		t.Fatalf("send after drain: %v", err)
	}
}

func TestHTTPDefaultCapacity(t *testing.T) {
	h, err := NewHTTP("")
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	defer func() { _ = h.Close() }()
	if got := cap(h.inbound); got != DefaultInboundCapacity {
		t.Fatalf("default inbound capacity = %d, want %d", got, DefaultInboundCapacity)
	}
}
