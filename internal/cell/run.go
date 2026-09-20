package cell

import (
	"context"
	"fmt"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// Cell state constants for the lifecycle state machine.
const (
	cellCreated     int32 = 0
	cellInitialized int32 = 1
	cellStarted     int32 = 2
	cellIdle        int32 = 3 // Stop called; background work halted, callable still responsive
	cellStopped     int32 = 4 // Destroy called; removed from tree, fully inert
)

// ownerQueueCapacity bounds the owner-loop mailbox that serializes stateful
// calls. Sized well above the system/reply lanes to absorb bursts when many
// callers fan in on a single owner (e.g. many agents concurrently reading
// workspace.wiki_*). This is a safety buffer only; throughput is fixed by
// making those callables stateless (parallel), not by growing this queue.
const ownerQueueCapacity = 256

// Run is the Cell's main entry. It starts the internal ingress loops and
// blocks until ctx is cancelled.
func (c *Cell) Run(ctx context.Context) {
	defer close(c.done)
	runCtx, runCancel := context.WithCancel(ctx)
	c.setRunCancel(runCancel)
	c.openIngressLoops()
	<-runCtx.Done()
	c.draining.Store(true)
	c.drainIngressLoops()
}

// setRunCancel/getRunCancel serialize access to runCancel across the Run
// goroutine and the system lane (handleIdle/handleDestroy call it while
// Run may still be writing it during startup, or restarting).
func (c *Cell) setRunCancel(fn context.CancelFunc) {
	c.cancelMu.Lock()
	c.runCancel = fn
	c.cancelMu.Unlock()
}

func (c *Cell) callRunCancel() {
	c.cancelMu.Lock()
	fn := c.runCancel
	c.cancelMu.Unlock()
	if fn != nil {
		fn()
	}
}

// Recv is the unified message entry point. Called directly by the transport
// layer (no mailbox queue). It classifies the message and routes it:
//
//	SystemMsg                → systemQ  → systemLoop
//	KindCall + stateless     → goroutine (tracked by pureWG)
//	KindCall + stateful      → ownerQ   → ownerLoop
//	KindReply/Error/End/Cancel → replyQ → replyLoop
func (c *Cell) Recv(env mailbox.Envelope) error {
	if _, ok := env.Payload.(mailbox.SystemMsg); ok {
		return c.enqueueSystem(env)
	}
	switch env.Frame.Kind {
	case message.KindCall:
		return c.handleCall(env)
	case message.KindReply, message.KindError, message.KindEnd, message.KindCancel:
		return c.enqueueReply(env)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Queue helpers
// ---------------------------------------------------------------------------

// selfID returns the cell actor's ref id, or "<nil>" when the self ref is not
// set. Used in queue-overflow diagnostics so callers can identify the actor.
func (c *Cell) selfID() string {
	if c.self == nil {
		return "<nil>"
	}
	return c.self.ID().String()
}

func (c *Cell) enqueueSystem(env mailbox.Envelope) (err error) {
	c.systemQMu.Lock()
	closed := c.systemQClosed
	q := c.systemQ
	if !closed {
		closed = q == nil
	}
	// Do not send outside the mutex: openIngressLoops/drain may close the old
	// queue concurrently, and a send on a stale reference after close races
	// runtime.closechan (detected by -race in TestSystemE2EDemoAppJSONPath).
	// Holding the lock for the send guarantees the queue cannot be closed
	// between lookup and send — the closers all close under the same lock.
	if closed {
		c.systemQMu.Unlock()
		return fmt.Errorf("cell: system queue closed")
	}
	select {
	case q <- env:
		c.noteLaneDepth(laneNameSystem, len(q))
		c.systemQMu.Unlock()
		return nil
	default:
		actorID := c.selfID()
		qlen, qcap := len(q), cap(q)
		if !isCriticalSystemMsg(env.Payload) {
			c.systemQMu.Unlock()
			if c.logger != nil {
				c.logger.Warn("cell: system queue full, dropped non-critical", "actor", actorID, "queued", qlen, "cap", qcap)
			}
			return fmt.Errorf("cell: system queue full, dropped non-critical (actor=%s, queued=%d/%d)", actorID, qlen, qcap)
		}
		// Critical system message: bounded patience under the mutex (the
		// documented close-race invariant requires the send to stay inside
		// the lock). An unbounded block here would freeze the SENDER's
		// goroutine for as long as systemLoop is stuck — e.g. a slow
		// OnStart wedging every parent spawning children serially.
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case q <- env:
			c.noteLaneDepth(laneNameSystem, len(q))
			c.systemQMu.Unlock()
			return nil
		case <-timer.C:
			c.systemQMu.Unlock()
			if c.logger != nil {
				c.logger.Warn("cell: system queue full after 5s timeout, critical message refused",
					"actor", actorID, "queued", qlen, "cap", qcap)
			}
			return fmt.Errorf("cell: system queue full after 5s timeout, critical message refused (actor=%s, queued=%d/%d)", actorID, qlen, qcap)
		}
	}
}

func (c *Cell) handleCall(env mailbox.Envelope) error {
	handlers := c.Handlers()
	if handlers == nil {
		reply := c.makeReply(env)
		reply(message.Frame{Kind: message.KindError, Body: []byte("gospore.cell: handler table not initialized")})
		reply(message.Frame{Kind: message.KindEnd})
		return nil
	}
	inv, ok := handlers.Lookup(env.Frame.CallID)
	if !ok {
		reply := c.makeReply(env)
		reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf("gospore.cell: call ID %q not registered", env.Frame.CallID))})
		reply(message.Frame{Kind: message.KindEnd})
		return nil
	}
	loop := c.resolveCallLoop(env, inv)
	switch loop {
	case actor.DefaultLoopPure:
		// Stateless: run directly in a goroutine so the caller never blocks.
		c.pureWG.Add(1)
		go func() {
			defer c.pureWG.Done()
			reply := c.makeReply(env)
			c.dispatchCall(env, reply)
		}()
		return nil
	case actor.DefaultLoopOwner:
		return c.enqueueOwner(env)
	default:
		if !c.enqueueLoop(loop, env) {
			// Lane full after bounded patience (or draining). Reply an
			// explicit error; queueing onto the owner lane would run the
			// handler concurrently with the busy custom lane.
			reply := c.makeReply(env)
			reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf(
				"cell: custom lane %q unavailable (full or draining), call dropped explicitly", loop))})
			reply(message.Frame{Kind: message.KindEnd})
			return nil
		}
		return nil
	}
}

func (c *Cell) makeReply(env mailbox.Envelope) func(message.Frame) {
	return func(f message.Frame) {
		f.CorID = env.Frame.CorID
		if c.deliveryHost != nil && env.Sender != nil {
			if err := c.deliveryHost.Deliver(env.Sender, mailbox.Envelope{Frame: f}); err != nil && c.logger != nil {
				c.logger.Error("cell: reply delivery failed", "callID", env.Frame.CallID, "corID", f.CorID, "err", err)
			}
		}
	}
}

func (c *Cell) enqueueOwner(env mailbox.Envelope) (err error) {
	// Stamp the enqueue time so the owner lane's queue-wait metric can be
	// computed at dispatch (per-(lane, callID) observability).
	env.EnqueuedAt = time.Now()
	c.ownerQMu.Lock()
	q := c.ownerQ
	if q == nil {
		// No live owner queue means the cell is between drain and reopen
		// (restart/idle). Dropping the message silently wedges every caller
		// until its timeout — reply an error instead so callers can retry.
		actorID := c.selfID()
		c.ownerQMu.Unlock()
		body := fmt.Sprintf("gospore.cell: owner queue not ready (actor=%s, call=%s)", actorID, env.Frame.CallID)
		reply := c.makeReply(env)
		reply(message.Frame{Kind: message.KindError, Body: []byte(body)})
		reply(message.Frame{Kind: message.KindEnd})
		if c.logger != nil {
			c.logger.Warn("cell: owner queue not ready", "actor", actorID, "callID", env.Frame.CallID)
		}
		return fmt.Errorf("cell: owner queue not ready (actor=%s, call=%s)", actorID, env.Frame.CallID)
	}
	select {
	case q <- env:
		c.noteOwnerWatermark(len(q), cap(q))
		c.noteLaneDepth(laneNameOwner, len(q))
		c.ownerQMu.Unlock()
		return nil
	default:
		actorID := c.selfID()
		qlen, qcap := len(q), cap(q)
		c.ownerQMu.Unlock()
		if c.logger != nil {
			c.logger.Warn("cell: owner queue full", "actor", actorID, "callID", env.Frame.CallID, "queued", qlen, "cap", qcap)
		}
		body := fmt.Sprintf("gospore.cell: owner queue full (actor=%s, call=%s, queued=%d/%d)", actorID, env.Frame.CallID, qlen, qcap)
		reply := c.makeReply(env)
		reply(message.Frame{Kind: message.KindError, Body: []byte(body)})
		reply(message.Frame{Kind: message.KindEnd})
		return fmt.Errorf("cell: owner queue full (actor=%s, call=%s, queued=%d/%d)", actorID, env.Frame.CallID, qlen, qcap)
	}
}

// noteOwnerWatermark logs owner-queue saturation once per crossed level
// (≥75%, ≥90%) and resets when the queue drains below 75%, so a backlog
// shows in the logs before overflow — without per-enqueue spam.
func (c *Cell) noteOwnerWatermark(qlen, qcap int) {
	if qcap <= 0 || qlen*4 < qcap*3 {
		c.ownerWater.Store(0)
		return
	}
	level := int32(1)
	if qlen*10 >= qcap*9 {
		level = 2
	}
	if prev := c.ownerWater.Swap(level); prev >= level {
		return
	}
	if c.logger != nil {
		c.logger.Warn("cell: owner queue high-water", "actor", c.selfID(), "queued", qlen, "cap", qcap, "level", level)
	}
}

func (c *Cell) enqueueReply(env mailbox.Envelope) (err error) {
	// Hold replyQMu across the send: drainIngressLoops closes the queue
	// concurrently and a send racing close() panics (and leaks a timer).
	// Holding the lock serializes send-vs-close so a closed channel is
	// either never touched or safely detected via the closed flag.
	c.replyQMu.Lock()
	defer c.replyQMu.Unlock()
	if c.replyQ == nil || c.replyQClosed {
		// Between drain and reopen (restart/idle): delivering to the
		// pending table directly settles the caller's promise — the
		// queue's consumer would do exactly this, and dropping the
		// frame instead wedges the caller until its timeout.
		if c.replyReg != nil {
			c.replyReg.deliver(env.Frame)
			return nil
		}
		return fmt.Errorf("cell: reply queue not ready (corID=%d)", env.Frame.CorID)
	}
	q := c.replyQ
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case q <- env:
		c.noteLaneDepth(laneNameReply, len(q))
		return nil
	case <-timer.C:
		if c.logger != nil {
			c.logger.Warn("cell: reply queue full after 5s timeout",
				"actor", c.selfID(), "corID", env.Frame.CorID, "queued", len(q), "cap", cap(q))
		}
		return fmt.Errorf("cell: reply queue full after 5s timeout")
	}
}

func (c *Cell) enqueueLoop(name string, env mailbox.Envelope) bool {
	// Stamp once: the queue-wait metric measures from first attempt.
	env.EnqueuedAt = time.Now()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		c.loopsMu.Lock()
		lane, ok := c.loops[name]
		if !ok {
			c.loopsMu.Unlock()
			return false
		}
		if lane.q == nil {
			lane.q = make(chan mailbox.Envelope, 64)
			lane.wg.Add(1)
			go c.customLoop(name, lane)
		}
		if lane.closed || lane.q == nil {
			c.loopsMu.Unlock()
			return false
		}
		q := lane.q
		c.loopsMu.Unlock()
		select {
		case q <- env:
			c.noteLaneDepth(name, len(q))
			return true
		default:
		}
		// A full custom lane means its handler is busy. Bounded patience —
		// NEVER execute the handler on another goroutine: a stateful handler
		// running concurrently with its lane is an actor-model violation
		// (unsynchronized state access). Callers turn the eventual false
		// into an explicit error reply.
		//
		// Waiting on timer/ticker channels (not time.Sleep) so a dead
		// cell (Run returned, done closed) bails out immediately instead
		// of burning the full 5s deadline on a handler that can never
		// make progress.
		select {
		case <-deadline.C:
			if c.logger != nil {
				c.logger.Warn("cell: custom lane queue full after 5s timeout",
					"actor", c.selfID(), "lane", name, "callID", env.Frame.CallID, "queued", len(q), "cap", cap(q))
			}
			return false
		case <-c.done:
			return false
		case <-tick.C:
		}
	}
}
