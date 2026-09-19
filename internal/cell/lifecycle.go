package cell

import (
	"context"
	"fmt"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/events"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/projection"
)

// Cell state constants for the lifecycle state machine.
// System-message lifecycle: init/start/idle/destroy/restart and event emission.
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
	c.notifyInitOutcome()
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
	c.notifyStartOutcome()

	// Start the owner loop only after OnStart has fully reconstructed state.
	// Messages enqueued during OnStart wait in ownerQ until now, so stateful
	// handlers never race startup.
	//
	// The Add/go happen under ownerQMu with an ownerQClosed re-check: Run's
	// drain may already have closed+niled ownerQ when ctx was cancelled
	// during boot — spawning then would race drain's ownerWG.Wait (illegal
	// Add-after-Wait) and leave an ownerLoop ranging a nil channel forever.
	c.ownerQMu.Lock()
	if !c.ownerQClosed {
		c.ownerWG.Add(1)
		c.ownerQMu.Unlock()
		go c.ownerLoop()
	} else {
		c.ownerQMu.Unlock()
	}
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
	c.callRunCancel()
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
	c.notifyStartOutcome()
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
	if c.svcHost != nil {
		for _, name := range c.services {
			c.svcHost.UnexposeService(c.self, name)
		}
	} else if c.svcReg != nil {
		for _, name := range c.services {
			c.svcReg.Unregister(name)
		}
	}
	if c.svcHost != nil {
		c.svcHost.UnexposeAllScoped(c.self)
	}
	if c.discoveryProvider != nil && c.namespace != "" {
		_ = c.discoveryProvider.Deregister(c.namespace)
	}
	c.services = c.services[:0]
	c.childServices = c.childServices[:0]
}

func (c *Cell) notifyWatchers() {
	if c.watchers.Len() == 0 || c.deliveryHost == nil {
		return
	}
	snapshot := c.watchers.Snapshot()
	for _, w := range snapshot {
		_ = c.deliveryHost.Deliver(w, mailbox.Envelope{Payload: mailbox.Terminated{Of: c.self}})
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
	c.callRunCancel()
	_ = c.actor.OnStop(newStopContext(c))

	c.actorMu.Lock()
	if fn := c.props.Factory(); fn != nil {
		c.actor = fn()
	}
	c.handlers = c.newHandlerTable()
	c.actorMu.Unlock()

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
	// The replayed OnStart counts: a restarted cell must report
	// IsStarted() and close StartDone for any spawn-side waiter —
	// previously the replay skipped both, leaving restart-completed
	// actors stuck in the pre-start state forever.
	c.state.Store(cellStarted)
	c.notifyStartOutcome()

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

func (c *Cell) now() time.Time {
	if c.clock != nil {
		return c.clock.Now()
	}
	return time.Now()
}

func (c *Cell) emitLifecycleEvent(kind actor.WatchKind) {
	if c.events == nil {
		return
	}
	c.events.Push(c.self.ID(), events.Record{
		Kind:      kind,
		Timestamp: c.now(),
		ActorID:   c.self.ID(),
	})
}

func (c *Cell) emitCallableEvent(kind actor.WatchKind, inv *handler.Invoker, env mailbox.Envelope, errorMsg string) {
	if c.events == nil || inv == nil {
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
