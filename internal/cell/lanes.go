package cell

import (
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// Cell state constants for the lifecycle state machine.
// Ingress lane goroutines and their drain protocol.
// ---------------------------------------------------------------------------
// Ingress loops
// ---------------------------------------------------------------------------

func (c *Cell) openIngressLoops() {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()

	// Drain any messages from pre-existing queues (created in New()) into
	// fresh queues, then start goroutines on the fresh queues. This two-step
	// dance avoids a race between go cell.Run() and cell.Recv(Init) in the
	// App startup path, AND supports the idle → drainIngressLoops →
	// openIngressLoops cycle (channels are closed after drain, so we need
	// fresh ones).
	//
	// Step 1 – ownerQ: drain old → new. The owner loop is NOT started here;
	// handleStart starts it after OnStart completes so business messages
	// never race the actor's startup state reconstruction.
	c.ownerQMu.Lock()
	oldOwnerQ := c.ownerQ
	c.ownerQ = make(chan mailbox.Envelope, ownerQueueCapacity)
	c.ownerQClosed = false
	// Snapshot under the lock: Run's drainIngressLoops nils c.ownerQ under
	// the same lock while this loop is still replaying, and reading the
	// field unsynchronized in the select below races it.
	newOwnerQ := c.ownerQ
	c.ownerQMu.Unlock()
	if oldOwnerQ != nil {
		close(oldOwnerQ)
		for env := range oldOwnerQ {
			select {
			case newOwnerQ <- env:
			default:
			}
		}
	}

	// Step 2 – systemQ: drain old → new, start systemLoop.
	c.systemQMu.Lock()
	oldSysQ := c.systemQ
	c.systemQ = make(chan mailbox.Envelope, 64)
	c.systemQClosed = false
	newSysQ := c.systemQ
	c.systemQMu.Unlock()
	if oldSysQ != nil {
		close(oldSysQ)
		for env := range oldSysQ {
			select {
			case newSysQ <- env:
			default:
			}
		}
	}
	c.systemWG.Add(1)
	go c.systemLoop()

	// Step 3 – replyQ: drain old → new, start replyLoop.
	c.replyQMu.Lock()
	oldReplyQ := c.replyQ
	c.replyQ = make(chan mailbox.Envelope, 64)
	c.replyQClosed = false
	newReplyQ := c.replyQ
	c.replyQMu.Unlock()
	if oldReplyQ != nil {
		close(oldReplyQ)
		for env := range oldReplyQ {
			select {
			case newReplyQ <- env:
			default:
			}
		}
	}
	c.replyWG.Add(1)
	go c.replyLoop()

	c.loopsMu.Lock()
	loops := make([]*loopLane, 0, len(c.loops))
	for _, lane := range c.loops {
		loops = append(loops, lane)
	}
	// Close lanes under loopsMu: enqueueLoop checks lane.closed and sends
	// under the same lock, so a send can never race the close (send on a
	// closed channel panics).
	for _, lane := range loops {
		if lane.q == nil {
			continue
		}
		if !lane.closed {
			lane.closed = true
			close(lane.q)
		}
	}
	c.loopsMu.Unlock()
	for _, lane := range loops {
		lane.wg.Wait()
	}
	c.loopsMu.Lock()
	for _, lane := range c.loops {
		lane.q = nil
		lane.closed = false
	}
	c.loopsMu.Unlock()
}

// ownerLoop drains the business ingress queue and invokes handlers one at a
// time. Because the queue is consumed by exactly one goroutine, stateful
// handlers are naturally serialised without additional synchronisation.
func (c *Cell) ownerLoop() {
	defer c.ownerWG.Done()
	// Capture the queue once: drainIngressLoops nils the field under
	// ownerQMu while a running loop would otherwise race the field read.
	// The captured channel is only ever closed (never reassigned) for this
	// loop's lifetime, so ranging it is safe.
	c.ownerQMu.Lock()
	q := c.ownerQ
	c.ownerQMu.Unlock()
	for env := range q {
		reply := c.makeReply(env)
		c.dispatchCall(env, reply)
	}
}

// systemLoop drains system messages and invokes handleSystem.
func (c *Cell) systemLoop() {
	defer c.systemWG.Done()
	// Capture once under the lock (drain nils the field concurrently).
	c.systemQMu.Lock()
	q := c.systemQ
	c.systemQMu.Unlock()
	for env := range q {
		if sys, ok := env.Payload.(mailbox.SystemMsg); ok {
			c.withLaneBusy(laneNameSystem, func() { c.handleSystem(c.lifecycleCtx, sys) })
		}
	}
}

// replyLoop drains async completion traffic independently from the owner loop.
func (c *Cell) replyLoop() {
	defer c.replyWG.Done()
	// Capture once under the lock (drain nils the field concurrently).
	c.replyQMu.Lock()
	q := c.replyQ
	c.replyQMu.Unlock()
	for env := range q {
		switch env.Frame.Kind {
		case message.KindReply, message.KindError, message.KindEnd:
			c.withLaneBusy(laneNameReply, func() { c.routeToWaitingStream(env) })
		case message.KindCancel:
			c.withLaneBusy(laneNameReply, func() { c.cancelStream(env) })
		}
	}
}

func (c *Cell) customLoop(name string, lane *loopLane) {
	defer lane.wg.Done()
	for env := range lane.q {
		c.invokeLoop(name, env)
	}
}

// drainIngressLoopsExceptSystem is drainIngressLoops without touching the
// system queue. It is safe to call from within the systemLoop goroutine
// (typically via handleIdle) because it does not wait for systemWG.
func (c *Cell) drainIngressLoopsExceptSystem() {
	c.drainMu.Lock()

	c.ownerQMu.Lock()
	if !c.ownerQClosed {
		c.ownerQClosed = true
	}
	ownerQ := c.ownerQ
	c.ownerQ = nil
	c.ownerQMu.Unlock()
	if ownerQ != nil {
		close(ownerQ)
	}

	c.replyQMu.Lock()
	if !c.replyQClosed {
		c.replyQClosed = true
	}
	replyQ := c.replyQ
	c.replyQ = nil
	c.replyQMu.Unlock()
	if replyQ != nil {
		close(replyQ)
	}

	c.loopsMu.Lock()
	loops := make([]*loopLane, 0, len(c.loops))
	for _, lane := range c.loops {
		loops = append(loops, lane)
	}
	for _, lane := range loops {
		if lane.q == nil {
			continue
		}
		if !lane.closed {
			lane.closed = true
			close(lane.q)
		}
	}
	c.loopsMu.Unlock()

	c.drainMu.Unlock()

	c.ownerWG.Wait()
	c.replyWG.Wait()
	for _, lane := range loops {
		lane.wg.Wait()
	}
	// Note: we intentionally do NOT wait for pureWG here.
	// Stateless goroutines do not mutate actor state, and cancelLifecycle()
	// already signaled them to stop via ctx.Done(). Waiting for them would
	// delay Destroy processing (and thus DAG updates) for no benefit.
}

// drainIngressLoops closes the internal ingress queues and waits for the
// owner/system/reply loops to finish their current work. Safe to call multiple times.
func (c *Cell) drainIngressLoops() {
	c.drainMu.Lock()

	// Close and nil out ownerQ.
	c.ownerQMu.Lock()
	if !c.ownerQClosed {
		c.ownerQClosed = true
	}
	ownerQ := c.ownerQ
	c.ownerQ = nil
	c.ownerQMu.Unlock()
	if ownerQ != nil {
		close(ownerQ)
	}

	// Close and nil out systemQ.
	c.systemQMu.Lock()
	if !c.systemQClosed {
		c.systemQClosed = true
	}
	systemQ := c.systemQ
	c.systemQ = nil
	c.systemQMu.Unlock()
	if systemQ != nil {
		close(systemQ)
	}

	// Close and nil out replyQ.
	c.replyQMu.Lock()
	if !c.replyQClosed {
		c.replyQClosed = true
	}
	replyQ := c.replyQ
	c.replyQ = nil
	c.replyQMu.Unlock()
	if replyQ != nil {
		close(replyQ)
	}

	// Close custom loops.
	c.loopsMu.Lock()
	loops := make([]*loopLane, 0, len(c.loops))
	for _, lane := range c.loops {
		loops = append(loops, lane)
	}
	for _, lane := range loops {
		if lane.q == nil {
			continue
		}
		if !lane.closed {
			lane.closed = true
			close(lane.q)
		}
	}
	c.loopsMu.Unlock()

	c.drainMu.Unlock()

	// Wait for goroutines to exit (no lock held — safe to block).
	c.ownerWG.Wait()
	c.systemWG.Wait()
	c.replyWG.Wait()
	for _, lane := range loops {
		lane.wg.Wait()
	}
	c.pureWG.Wait()
}
