package cell

import (
	"sync/atomic"
	"testing"

	"github.com/qomos-w/gospore/message"
)

// TestReplyRegistry_DeliverRoutesFrame verifies that deliver forwards
// frames through the routeReply hook and returns the hook's result.
func TestReplyRegistry_DeliverRoutesFrame(t *testing.T) {
	var got message.Frame
	rr := newReplyRegistry(func(f message.Frame) bool {
		got = f
		return true
	})

	frame := message.Frame{Kind: message.KindReply, CorID: 42, Body: []byte("hi")}
	if !rr.deliver(frame) {
		t.Fatal("deliver returned false, want true")
	}
	if got.CorID != 42 {
		t.Fatalf("CorID = %d, want 42", got.CorID)
	}
}

// TestReplyRegistry_DeliverWithoutHookReturnsFalse verifies that when
// no routeReply hook is wired, deliver returns false silently.
func TestReplyRegistry_DeliverWithoutHookReturnsFalse(t *testing.T) {
	rr := newReplyRegistry(nil)
	frame := message.Frame{Kind: message.KindReply, CorID: 1}
	if rr.deliver(frame) {
		t.Fatal("deliver returned true without hook, want false")
	}
}

// TestReplyRegistry_RegisterAndCancel verifies the full cancel lifecycle:
// registerCancel -> cancel invokes the function and returns true ->
// subsequent cancel returns false.
func TestReplyRegistry_RegisterAndCancel(t *testing.T) {
	rr := newReplyRegistry(nil)

	var fired atomic.Bool
	rr.registerCancel(7, func() { fired.Store(true) })
	if rr.len() != 1 {
		t.Fatalf("len = %d, want 1", rr.len())
	}

	if !rr.cancel(7) {
		t.Fatal("cancel returned false, want true")
	}
	if !fired.Load() {
		t.Fatal("cancel function was not invoked")
	}
	if rr.len() != 0 {
		t.Fatalf("len after cancel = %d, want 0", rr.len())
	}

	if rr.cancel(7) {
		t.Fatal("second cancel returned true, want false")
	}
}

// TestReplyRegistry_UnregisterPreventsCancel verifies that unregisterCancel
// removes a callback so a later cancel is a no-op.
func TestReplyRegistry_UnregisterPreventsCancel(t *testing.T) {
	rr := newReplyRegistry(nil)

	var fired atomic.Bool
	rr.registerCancel(3, func() { fired.Store(true) })
	rr.unregisterCancel(3)

	if rr.cancel(3) {
		t.Fatal("cancel after unregister returned true, want false")
	}
	if fired.Load() {
		t.Fatal("cancel function fired after unregister")
	}
}

// TestReplyRegistry_MultiCorIDIsolation verifies that cancel callbacks are
// keyed strictly by CorID and do not leak across slots.
func TestReplyRegistry_MultiCorIDIsolation(t *testing.T) {
	rr := newReplyRegistry(nil)

	var aFired, bFired atomic.Bool
	rr.registerCancel(1, func() { aFired.Store(true) })
	rr.registerCancel(2, func() { bFired.Store(true) })

	rr.cancel(1)
	if !aFired.Load() {
		t.Fatal("callback A did not fire")
	}
	if bFired.Load() {
		t.Fatal("callback B leaked")
	}
}

// TestReplyRegistry_ClearRemovesAll verifies that clear wipes every
// registered callback.
func TestReplyRegistry_Clear(t *testing.T) {
	rr := newReplyRegistry(nil)

	var fired atomic.Bool
	rr.registerCancel(1, func() { fired.Store(true) })
	rr.registerCancel(2, func() { fired.Store(true) })
	rr.clear()

	if rr.len() != 0 {
		t.Fatalf("len after clear = %d, want 0", rr.len())
	}
	if rr.cancel(1) || rr.cancel(2) {
		t.Fatal("cancel after clear returned true")
	}
}

// TestReplyRegistry_DeliverConcurrentWithCancel verifies that deliver
// and cancel can race without deadlock or data races.
func TestReplyRegistry_DeliverConcurrentWithCancel(t *testing.T) {
	rr := newReplyRegistry(func(f message.Frame) bool { return true })

	var fired atomic.Int32
	for i := uint64(0); i < 100; i++ {
		rr.registerCancel(i, func() { fired.Add(1) })
	}

	done := make(chan struct{})
	go func() {
		for i := uint64(0); i < 100; i++ {
			rr.deliver(message.Frame{Kind: message.KindReply, CorID: i})
		}
		close(done)
	}()
	for i := uint64(0); i < 100; i++ {
		rr.cancel(i)
	}
	<-done
}
