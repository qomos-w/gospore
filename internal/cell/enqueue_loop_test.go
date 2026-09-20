package cell

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// RegisterLoopViaStart registers a custom loop through the start context
// (the only sanctioned registration path).
func (c *Cell) RegisterLoopViaStart(t *testing.T, name string) {
	t.Helper()
	ctx := NewStartContextForTest(c)
	if err := ctx.RegisterLoop(name, actor.ModeStateful); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}
}

// TestEnqueueLoopDeadCellBailsEarly pins the bounded-patience exit: when
// the cell is already dead (done closed — Run returned during teardown),
// a full custom lane must bail out immediately on the done signal
// instead of burning the full 5s deadline against a handler that can
// never drain again.
func TestEnqueueLoopDeadCellBailsEarly(t *testing.T) {
	c := New(Config{Namespace: "test-ns"})
	c.RegisterLoopViaStart(t, "stuck")

	// Fill the lane's queue without running its loop: block the single
	// consumer by never starting it (enqueueLoop spawns customLoop on
	// first use — occupy the queue from the lane struct directly).
	c.loopsMu.Lock()
	lane := c.loops["stuck"]
	lane.q = make(chan mailbox.Envelope, 1)
	lane.closed = false
	q := lane.q
	q <- mailbox.Envelope{Frame: message.Frame{Kind: message.KindCall, CallID: "x.fill"}}
	c.loopsMu.Unlock()

	// Kill the cell, then time a failing enqueue: it must return false
	// promptly via the done branch, not after the 5s deadline.
	close(c.done)
	start := time.Now()
	if c.enqueueLoop("stuck", mailbox.Envelope{Frame: message.Frame{Kind: message.KindCall, CallID: "x.over"}}) {
		t.Fatal("enqueueLoop on a dead cell returned true, want false")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("enqueueLoop on dead cell took %v, want immediate done-branch exit", elapsed)
	}
}
