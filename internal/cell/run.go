package cell

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/events"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/gospore/supervisor"
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
	c.runCancel = runCancel
	c.openIngressLoops()
	<-runCtx.Done()
	c.draining.Store(true)
	c.drainIngressLoops()
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
	if c.handlers == nil {
		reply := c.makeReply(env)
		reply(message.Frame{Kind: message.KindError, Body: []byte("gospore.cell: handler table not initialized")})
		reply(message.Frame{Kind: message.KindEnd})
		return nil
	}
	inv, ok := c.handlers.Lookup(env.Frame.CallID)
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
		if c.deliver != nil && env.Sender != nil {
			if err := c.deliver(env.Sender, mailbox.Envelope{Frame: f}); err != nil && c.logger != nil {
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
	deadline := time.Now().Add(5 * time.Second)
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
		if time.Now().After(deadline) {
			if c.logger != nil {
				c.logger.Warn("cell: custom lane queue full after 5s timeout",
					"actor", c.selfID(), "lane", name, "callID", env.Frame.CallID, "queued", len(q), "cap", cap(q))
			}
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

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
	oldOwnerQ := c.ownerQ
	c.ownerQMu.Lock()
	c.ownerQ = make(chan mailbox.Envelope, ownerQueueCapacity)
	c.ownerQClosed = false
	c.ownerQMu.Unlock()
	if oldOwnerQ != nil {
		close(oldOwnerQ)
		for env := range oldOwnerQ {
			select {
			case c.ownerQ <- env:
			default:
			}
		}
	}

	// Step 2 – systemQ: drain old → new, start systemLoop.
	oldSysQ := c.systemQ
	c.systemQMu.Lock()
	c.systemQ = make(chan mailbox.Envelope, 64)
	c.systemQClosed = false
	c.systemQMu.Unlock()
	if oldSysQ != nil {
		close(oldSysQ)
		for env := range oldSysQ {
			select {
			case c.systemQ <- env:
			default:
			}
		}
	}
	c.systemWG.Add(1)
	go c.systemLoop()

	// Step 3 – replyQ: drain old → new, start replyLoop.
	oldReplyQ := c.replyQ
	c.replyQMu.Lock()
	c.replyQ = make(chan mailbox.Envelope, 64)
	c.replyQClosed = false
	c.replyQMu.Unlock()
	if oldReplyQ != nil {
		close(oldReplyQ)
		for env := range oldReplyQ {
			select {
			case c.replyQ <- env:
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

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (c *Cell) handleSystem(ctx context.Context, msg mailbox.SystemMsg) {
	switch m := msg.(type) {
	case mailbox.Init:
		c.handleInit()
	case mailbox.Start:
		c.handleStart()
	case mailbox.Stop:
		c.handleIdle()
		c.handleDestroy()
		return
	case mailbox.Idle:
		c.handleIdle()
	case mailbox.Destroy:
		if c.state.Load() < cellIdle {
			c.handleIdle()
		}
		c.handleDestroy()
		return
	case mailbox.Restart:
		c.handleRestart(ctx, m)
	case mailbox.Terminated:
		c.handleTerminated(m)
	case mailbox.Watch:
		c.handleWatch(m)
	case mailbox.Unwatch:
		c.handleUnwatch(m)
	case mailbox.Escalated:
		c.handleEscalated(m)
	case mailbox.RefreshProjection:
		_, _, _ = c.applyProjectionIfObserved()
	case mailbox.Suspend:
	case mailbox.Resume:
	}
}

func (c *Cell) handleInit() {
	defer func() {
		if r := recover(); r != nil {
			path := "<nil>"
			if c.self != nil {
				path = c.self.ID().String()
			}
			reason := fmt.Errorf("actor panic in OnInit: %v", r)
			c.logger.Error("gospore/cell: actor panic in OnInit", "actorID", path, "reason", r)
			_ = c.supervisor.Decide(c.self, reason)
			c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
		}
	}()

	if init, ok := c.actor.(actor.Initializable); ok {
		initCtx := newInitContext(c)
		if err := init.OnInit(initCtx); err != nil {
			c.logger.Error("gospore/cell: actor OnInit error", "actorID", c.self.ID(), "error", err)
			_ = c.supervisor.Decide(c.self, err)
			c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
			return
		}
	}

	c.state.Store(cellInitialized)
	if c.onInitCompleted != nil {
		c.onInitCompleted()
	}
}

func (c *Cell) handleStart() {
	defer func() {
		if r := recover(); r != nil {
			path := "<nil>"
			if c.self != nil {
				path = c.self.ID().String()
			}
			reason := fmt.Errorf("actor panic in OnStart: %v", r)
			c.logger.Error("gospore/cell: actor panic in OnStart", "actorID", path, "reason", r)
			_ = c.supervisor.Decide(c.self, reason)
			c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
		}
	}()
	c.lifecycleCtx, c.cancelLifecycle = context.WithCancel(context.Background())
	startCtx := newStartContext(c)
	if err := c.actor.OnStart(startCtx); err != nil {
		c.logger.Error("gospore/cell: actor OnStart error", "actorID", c.self.ID(), "error", err)
		_ = c.supervisor.Decide(c.self, err)
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
		return
	}

	slots, err := projection.ScanComponents(c.actor)
	if err == nil && len(slots) > 0 {
		c.setComponentSlots(slots)
		if c.scriptRuntime != nil {
			_ = c.scriptRuntime.bindActorComponents(slots)
		}
	}

	c.emitLifecycleEvent(actor.WatchStarted)
	c.state.Store(cellStarted)
	if c.onStartCompleted != nil {
		c.onStartCompleted()
	}

	// Start the owner loop only after OnStart has fully reconstructed state.
	// Messages enqueued during OnStart wait in ownerQ until now, so stateful
	// handlers never race startup.
	c.ownerWG.Add(1)
	go c.ownerLoop()
}

func (c *Cell) handleIdle() {
	if c.state.Load() >= cellIdle {
		return
	}
	c.emitLifecycleEvent(actor.WatchStopped)
	c.state.Store(cellIdle)
	if c.cancelLifecycle != nil {
		c.cancelLifecycle()
	}
	if c.runCancel != nil {
		c.runCancel()
	}
	// If Run() has already started draining (ctx cancelled), skip queue
	// operations. drainIngressLoops handles all cleanup; we just need to
	// record the state transition and call OnStop.
	if c.draining.Load() {
		if c.replyReg != nil {
			c.replyReg.clear()
		}
		c.safeOnStop()
		return
	}

	c.drainIngressLoopsExceptSystem()
	if c.replyReg != nil {
		c.replyReg.clear()
	}
	c.safeOnStop()
	// Note: we do NOT call openIngressLoops() here. For the Destroy path
	// (which always follows handleIdle except for standalone Idle),
	// reopening queues is wasted work. The systemQ stays open so the
	// current systemLoop goroutine can continue processing messages.
}

func (c *Cell) handleDestroy() {
	// Ensure idle transition happened.
	if c.state.Load() < cellIdle {
		c.handleIdle()
	}
	if c.state.Load() >= cellStopped {
		return
	}
	c.emitLifecycleEvent(actor.WatchTerminated)
	c.state.Store(cellStopped)
	// The reply pipeline is gone, so replies for this actor's in-flight
	// outbound calls can never arrive. Flush the pending table to unblock
	// every waiting Stream with a terminal error instead of a hang.
	if c.invokeTable != nil {
		c.invokeTable.Flush("cell destroyed")
	}
	if c.replyReg != nil {
		c.replyReg.clear()
	}
	c.clearServices()
	c.notifyWatchers()
	if onDestroy, ok := c.actor.(actor.Destroyable); ok {
		_ = onDestroy.OnDestroy(newStopContext(c))
	}
}

func (c *Cell) safeOnStop() {
	defer func() {
		if r := recover(); r != nil {
			_ = c.supervisor.Decide(c.self, fmt.Errorf("actor panic in OnStop: %v", r))
		}
	}()
	_ = c.actor.OnStop(newStopContext(c))
}

func (c *Cell) clearServices() {
	if c.unexposeService != nil {
		for _, name := range c.services {
			c.unexposeService(name)
		}
	} else if c.svcReg != nil {
		for _, name := range c.services {
			c.svcReg.Unregister(name)
		}
	}
	if c.unexposeAllScopedServices != nil {
		c.unexposeAllScopedServices()
	} else if c.unexposeServiceToChildren != nil {
		for _, name := range c.childServices {
			c.unexposeServiceToChildren(name)
		}
	}
	if c.discoveryProvider != nil && c.namespace != "" {
		_ = c.discoveryProvider.Deregister(c.namespace)
	}
	c.services = c.services[:0]
	c.childServices = c.childServices[:0]
}

func (c *Cell) notifyWatchers() {
	if c.watchers.Len() == 0 || c.deliver == nil {
		return
	}
	snapshot := c.watchers.Snapshot()
	for _, w := range snapshot {
		_ = c.deliver(w, mailbox.Envelope{Payload: mailbox.Terminated{Of: c.self}})
	}
}

func (c *Cell) handleRestart(ctx context.Context, m mailbox.Restart) {
	defer func() {
		if r := recover(); r != nil {
			c.recoverAndDecide(r)
		}
	}()
	if c.logger != nil {
		c.logger.Warn("gospore/cell: handleRestart",
			"actorID", c.self.ID().String(),
			"reason", m.Reason.Error(),
		)
	}
	c.pureWG.Wait()
	if c.cancelLifecycle != nil {
		c.cancelLifecycle()
	}
	if c.runCancel != nil {
		c.runCancel()
	}
	_ = c.actor.OnStop(newStopContext(c))

	if fn := c.props.Factory(); fn != nil {
		c.actor = fn()
	}

	c.handlers = c.newHandlerTable()

	c.projectionMu.Lock()
	c.projectionState = nil
	c.projectionVersion = 0
	c.projectionMu.Unlock()

	if c.scriptRuntime != nil {
		c.scriptRuntime = newScriptRuntimeOwner(c.codec, c)
	}

	if init, ok := c.actor.(actor.Initializable); ok {
		if err := init.OnInit(newInitContext(c)); err != nil {
			_ = c.supervisor.Decide(c.self, err)
			c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
			return
		}
	}
	c.state.Store(cellInitialized)
	startCtx := newStartContext(c)
	if err := c.actor.OnStart(startCtx); err != nil {
		_ = c.supervisor.Decide(c.self, err)
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
		return
	}

	if slots, err := projection.ScanComponents(c.actor); err == nil && len(slots) > 0 {
		c.setComponentSlots(slots)
		if c.scriptRuntime != nil {
			_ = c.scriptRuntime.bindActorComponents(slots)
		}
	}

	// Reopen the ingress pipeline. Run() exits after its drain (runCancel
	// above), and every queue was closed by that drain — without reopening,
	// a restarted cell keeps stateful handlers dead forever: enqueueOwner
	// sees a nil queue and owner/system/reply traffic never resumes.
	// The Run goroutine is deliberately NOT re-armed: its only remaining
	// duty is the final drain on ctx cancel, which handleDestroy performs
	// via drainIngressLoops directly.
	c.openIngressLoops()
	// openIngressLoops does not start the owner loop (it is started after
	// OnStart by handleStart); restart replays the same ordering contract.
	c.ownerWG.Add(1)
	go c.ownerLoop()

	c.emitLifecycleEvent(actor.WatchRestarted)
}

func (c *Cell) handleWatch(m mailbox.Watch) {
	if m.Watcher != nil {
		c.AddWatcher(m.Watcher)
	}
}

func (c *Cell) handleUnwatch(m mailbox.Unwatch) {
	c.RemoveWatcher(m.Watcher)
}

func (c *Cell) handleTerminated(m mailbox.Terminated) {
	c.children.Remove(m.Of)
	if w, ok := c.actor.(actor.Watcher); ok {
		w.OnTerminated(newStartContext(c), m.Of)
	}
}

// ---------------------------------------------------------------------------
// Dispatch & Invoke
// ---------------------------------------------------------------------------

func (c *Cell) dispatchCall(env mailbox.Envelope, reply func(message.Frame)) {
	if denied, diag := c.checkPolicy(env); denied {
		c.logger.Error("cell: dispatchCall POLICY_DENIED", "callID", env.Frame.CallID, "diag", diag)
		reply(message.Frame{Kind: message.KindError, Body: []byte(diag)})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}

	switch env.Frame.CallID {
	case actorCallIDReload:
		c.handleControlCallReply(env, reply, func(ie *handler.InvokeEnv) error {
			req, ok, err := decodeReloadReq(ie.Payload)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return newCallContext(c, *ie).applyScriptReload(req)
		})
		return
	case actorCallIDReplace:
		c.handleControlCallReply(env, reply, func(ie *handler.InvokeEnv) error {
			req, ok, err := decodeReplaceReq(ie.Payload)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return newCallContext(c, *ie).applyScriptReplace(req)
		})
		return
	}

	if c.handlers == nil {
		c.logger.Error("cell: dispatchCall NO_HANDLERS", "callID", env.Frame.CallID)
		reply(message.Frame{Kind: message.KindError, Body: []byte("gospore.cell: handler table not initialized")})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	inv, ok := c.handlers.Lookup(env.Frame.CallID)
	if !ok {
		c.logger.Error("cell: dispatchCall NOT_FOUND", "callID", env.Frame.CallID)
		reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf("gospore.cell: call ID %q not registered", env.Frame.CallID))})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}

	loop := c.resolveCallLoop(env, inv)
	switch loop {
	case actor.DefaultLoopPure:
		// Should not reach here — stateless calls are handled in handleCall
		// and run in a goroutine. This is a safety fallback.
		c.timedInvoke(laneNamePure, inv, env, reply)
		return
	case actor.DefaultLoopOwner:
		c.timedInvoke(laneNameOwner, inv, env, reply)
		return
	default:
		if !c.enqueueLoop(loop, env) {
			// Lane full after bounded patience (or draining). Reply an
			// explicit error; executing inline on the caller's goroutine
			// (owner loop or pure fallback) would run the handler
			// concurrently with the busy custom lane.
			reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf(
				"cell: custom lane %q unavailable (full or draining), call dropped explicitly", loop))})
			reply(message.Frame{Kind: message.KindEnd})
		}
		return
	}
}

func (c *Cell) resolveCallLoop(env mailbox.Envelope, inv *handler.Invoker) string {
	if inv == nil {
		return actor.DefaultLoopOwner
	}
	if loop, ok := c.selfLoopOverride(env); ok {
		if !c.isRegisteredLoop(loop) {
			return actor.DefaultLoopOwner
		}
		if loop == actor.DefaultLoopPure && inv.Mode != actor.ModeStateless {
			return actor.DefaultLoopOwner
		}
		return loop
	}
	switch inv.Loop {
	case actor.DefaultLoopPure:
		if inv.Mode == actor.ModeStateless {
			return actor.DefaultLoopPure
		}
		return actor.DefaultLoopOwner
	case actor.DefaultLoopOwner:
		return actor.DefaultLoopOwner
	case "", actor.DefaultLoopReply:
		return actor.DefaultLoopForMode(inv.Mode)
	default:
		if c.isRegisteredLoop(inv.Loop) {
			c.loopsMu.Lock()
			lane, exists := c.loops[inv.Loop]
			c.loopsMu.Unlock()
			if exists && lane != nil && lane.mode == actor.ModeStateless && inv.Mode != actor.ModeStateless {
				return actor.DefaultLoopOwner
			}
			return inv.Loop
		}
		return actor.DefaultLoopOwner
	}
}

func (c *Cell) isRegisteredLoop(name string) bool {
	switch name {
	case actor.DefaultLoopOwner:
		return true
	case actor.DefaultLoopPure:
		return true
	case actor.DefaultLoopReply, "":
		return false
	}
	c.loopsMu.Lock()
	_, ok := c.loops[name]
	c.loopsMu.Unlock()
	return ok
}

func (c *Cell) selfLoopOverride(env mailbox.Envelope) (string, bool) {
	if env.Frame.Headers == nil {
		return "", false
	}
	loop := strings.TrimSpace(env.Frame.Headers["gospore.loop"])
	if loop == "" {
		return "", false
	}
	if env.Sender == nil || c.self == nil || env.Sender.ID() != c.self.ID() {
		return "", false
	}
	if role := strings.TrimSpace(env.Frame.Headers["gospore.caller_role"]); role != "" {
		return "", false
	}
	switch loop {
	case actor.DefaultLoopOwner, actor.DefaultLoopPure:
		return loop, true
	default:
		if c.isRegisteredLoop(loop) {
			return loop, true
		}
		return "", false
	}
}

func (c *Cell) invokeLoop(name string, env mailbox.Envelope) {
	reply := c.makeReply(env)
	if c.handlers == nil {
		reply(message.Frame{Kind: message.KindError, Body: []byte("gospore.cell: handler table not initialized")})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	inv, ok := c.handlers.Lookup(env.Frame.CallID)
	if !ok {
		reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf("gospore.cell: call ID %q not registered", env.Frame.CallID))})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	c.invokeLoopCall(name, inv, env, reply)
}

func (c *Cell) invokeLoopCall(name string, inv *handler.Invoker, env mailbox.Envelope, reply func(message.Frame)) {
	c.pureWG.Add(1)
	defer c.pureWG.Done()
	c.timedInvoke(name, inv, env, reply)
}

func (c *Cell) invokeCall(inv *handler.Invoker, env mailbox.Envelope, reply func(message.Frame)) {
	var invokeErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if c.logger != nil {
					c.logger.Error("gospore/cell: handler panic",
						"actorID", c.self.ID().String(),
						"callID", env.Frame.CallID,
						"panic", r,
						"stack", string(debug.Stack()),
					)
				}
				invokeErr = fmt.Errorf("actor panic: %v", r)
				c.recoverAndDecide(r)
				reply(message.Frame{Kind: message.KindError, Body: []byte(invokeErr.Error())})
				reply(message.Frame{Kind: message.KindEnd})
			}
		}()
		c.emitCallableEvent(actor.WatchInvokeInStarted, inv, env, "")
		var payload any
		if env.Frame.PayloadMode == message.PayloadModeValue {
			payload = env.Frame.Body
			if env.Payload != nil {
				payload = env.Payload
			}
		}
		invokeEnv := handler.InvokeEnv{
			Frame:          env.Frame,
			Caller:         env.Sender,
			Payload:        payload,
			Actor:          c.actor,
			Reply:          reply,
			Codec:          c.codec,
			ReturnSchemaID: inv.ReturnSchemaID,
			ChunkSchemaID:  inv.ChunkSchemaID,
			Identity:       identityFromHeaders(env.Frame.Headers),
			OnCancel: func(corID uint64, cancel func()) {
				c.replyReg.registerCancel(corID, cancel)
			},
			OffCancel: func(corID uint64) {
				c.replyReg.unregisterCancel(corID)
			},
		}
		invokeEnv.Context = newCallContext(c, invokeEnv)
		if inv.Script != nil && c.scriptRuntime != nil {
			if err := c.scriptRuntime.invoke(inv, invokeEnv); err != nil {
				invokeErr = err
				invokeEnv.Reply(message.Frame{Kind: message.KindError, Body: []byte(err.Error())})
				invokeEnv.Reply(message.Frame{Kind: message.KindEnd})
				return
			}
		} else {
			inv.Run(invokeEnv)
		}
		_, _, _ = c.applyProjectionIfObserved()
		c.emitCallableEvent(actor.WatchInvokeInEnded, inv, env, invokeErrString(invokeErr))
	}()
	if c.monitor != nil {
		c.monitor.OnCall(c.cellID(), inv.CallID, invokeErr)
	}
}

func (c *Cell) handleControlCallReply(env mailbox.Envelope, reply func(message.Frame), apply func(*handler.InvokeEnv) error) {
	ie := handler.InvokeEnv{
		Frame:    env.Frame,
		Caller:   env.Sender,
		Payload:  env.Frame.Body,
		Actor:    c.actor,
		Reply:    reply,
		Codec:    c.codec,
		Identity: identityFromHeaders(env.Frame.Headers),
	}
	if err := apply(&ie); err != nil {
		reply(message.Frame{Kind: message.KindError, Body: []byte(err.Error())})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	reply(message.Frame{Kind: message.KindEnd})
}

func decodeReloadReq(payload any) (actor.ReloadReq, bool, error) {
	switch req := payload.(type) {
	case actor.ReloadReq:
		return req, true, nil
	case *actor.ReloadReq:
		if req == nil {
			return actor.ReloadReq{}, true, nil
		}
		return *req, true, nil
	case []byte:
		if len(req) == 0 {
			return actor.ReloadReq{}, false, nil
		}
		var out actor.ReloadReq
		if err := json.Unmarshal(req, &out); err != nil {
			return actor.ReloadReq{}, false, fmt.Errorf("decode %s: %w", actorCallIDReload, err)
		}
		return out, true, nil
	default:
		return actor.ReloadReq{}, false, nil
	}
}

func decodeReplaceReq(payload any) (actor.ReplaceReq, bool, error) {
	switch req := payload.(type) {
	case actor.ReplaceReq:
		return req, true, nil
	case *actor.ReplaceReq:
		if req == nil {
			return actor.ReplaceReq{}, true, nil
		}
		return *req, true, nil
	case []byte:
		if len(req) == 0 {
			return actor.ReplaceReq{}, false, nil
		}
		var out actor.ReplaceReq
		if err := json.Unmarshal(req, &out); err != nil {
			return actor.ReplaceReq{}, false, fmt.Errorf("decode %s: %w", actorCallIDReplace, err)
		}
		return out, true, nil
	default:
		return actor.ReplaceReq{}, false, nil
	}
}

func (c *Cell) cellID() string {
	if c.self == nil {
		return ""
	}
	return c.self.ID().String()
}

func (c *Cell) routeToWaitingStream(env mailbox.Envelope) {
	c.replyReg.deliver(env.Frame)
}

func (c *Cell) cancelStream(env mailbox.Envelope) {
	c.replyReg.cancel(env.Frame.CorID)
}

func (c *Cell) recoverAndDecide(recovered any) {
	if c.logger != nil {
		c.logger.Error("gospore/cell: actor panic recovered",
			"actorID", c.self.ID().String(),
			"panic", recovered,
			"stack", string(debug.Stack()),
		)
	}
	reason := fmt.Errorf("actor panic: %v", recovered)
	decision := c.supervisor.Decide(c.self, reason)
	switch decision {
	case supervisor.Restart:
		c.Recv(mailbox.Envelope{Payload: mailbox.Restart{Reason: reason}})
	case supervisor.Stop:
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
	case supervisor.Escalate:
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
		c.notifyParentEscalation(reason)
	case supervisor.Resume:
	}
}

func (c *Cell) notifyParentEscalation(reason error) {
	if c.parent != nil && c.deliver != nil {
		_ = c.deliver(c.parent, mailbox.Envelope{Payload: mailbox.Escalated{Child: c.self, Reason: reason}})
	} else if c.shutdown != nil {
		c.shutdown(reason)
	}
}

func (c *Cell) handleEscalated(m mailbox.Escalated) {
	decision := c.supervisor.Decide(m.Child, m.Reason)
	switch decision {
	case supervisor.Restart:
		c.Recv(mailbox.Envelope{Payload: mailbox.Restart{Reason: m.Reason}})
	case supervisor.Stop:
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
	case supervisor.Escalate:
		c.notifyParentEscalation(m.Reason)
	case supervisor.Resume:
	}
}

func (c *Cell) now() time.Time {
	if c.clock != nil {
		return c.clock.Now()
	}
	return time.Now()
}

func (c *Cell) emitLifecycleEvent(kind actor.WatchKind) {
	if c.events == nil || !c.events.HasSubscribers(c.self.ID()) {
		return
	}
	c.events.Push(c.self.ID(), events.Record{
		Kind:      kind,
		Timestamp: c.now(),
		ActorID:   c.self.ID(),
	})
}

func (c *Cell) emitCallableEvent(kind actor.WatchKind, inv *handler.Invoker, env mailbox.Envelope, errorMsg string) {
	if c.events == nil || inv == nil || !c.events.HasSubscribers(c.self.ID()) {
		return
	}
	rec := events.Record{
		Kind:      kind,
		Timestamp: c.now(),
		ActorID:   c.self.ID(),
		CallID:    inv.CallID,
		ErrorMsg:  errorMsg,
	}
	if env.Sender != nil {
		rec.Caller = env.Sender.ID()
	}
	c.events.Push(c.self.ID(), rec)
}

func invokeErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

const (
	actorCallIDReload  = "_gospore_.script_reload"
	actorCallIDReplace = "_gospore_.script_replace"
)

func identityFromHeaders(headers map[string]string) id.Identity {
	if len(headers) == 0 {
		return id.Identity{}
	}
	role := id.Role(strings.TrimSpace(headers["gospore.caller_role"]))
	if role == "" {
		return id.Identity{}
	}
	ident := id.Identity{Role: role}
	if kindStr := strings.TrimSpace(headers["gospore.caller_kind"]); kindStr != "" {
		switch kindStr {
		case id.IdentityToken.String():
			ident.Kind = id.IdentityToken
		case id.IdentityAppCertificate.String():
			ident.Kind = id.IdentityAppCertificate
		default:
			ident.Kind = id.IdentityAnonymous
		}
		ident.Subject = strings.TrimSpace(headers["gospore.caller_subject"])
		return ident
	}
	ident.Kind = id.IdentityToken
	ident.Subject = strings.TrimSpace(headers["gospore.caller_subject"])
	return ident
}

func (c *Cell) checkPolicy(env mailbox.Envelope) (bool, string) {
	if c.policyStore == nil {
		return false, ""
	}

	role := ""
	if env.Frame.Headers != nil {
		role = strings.TrimSpace(env.Frame.Headers["gospore.caller_role"])
	}
	isExternal := role != ""
	callID := env.Frame.CallID

	if isExternal && strings.HasPrefix(callID, "_gospore_.") {
		return true, actor.DiagPolicyDenied
	}
	if !isExternal {
		return false, ""
	}

	allow, found := c.policyStore.Evaluate(id.Role(role), callID)
	if found && !allow {
		return true, actor.DiagPolicyDenied
	}
	return false, ""
}

func isCriticalSystemMsg(payload any) bool {
	switch payload.(type) {
	case mailbox.Start, mailbox.Stop, mailbox.Restart, mailbox.Escalated:
		return true
	default:
		return false
	}
}
