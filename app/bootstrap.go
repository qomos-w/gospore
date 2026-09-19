package app

// Run/Shutdown lifecycle, boot-phase delivery, and the receive loop.
import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/resource"
)

// App is the process-level entry point and root actor of one
// gospore service. App embeds actor.Actor: it has its own OnStart /
// OnStop chain, runs handlers in the `app.*` reserved namespace, and
func (a *appImpl) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel.Store(cancel)
	a.runCtx.Store(ctx)
	a.done = make(chan struct{})
	defer close(a.done)

	// Step 1: Apply WithResource pre-population BEFORE OnInit so
	// RootActor can read them, and BEFORE freeze so they can be set.
	for _, fn := range a.cfg.resources {
		_ = fn(a.resReg)
	}

	// Step 2: Create and register the root Cell.
	rootInitialized := make(chan struct{})
	rootStarted := make(chan struct{})
	rootCell := cell.New(cell.Config{
		Self:       a.rootRef,
		Actor:      a.rootActor(),
		Props:      actor.PropsFromFunc(a.rootActor),
		Supervisor: a.defSup,
		Handlers:   a.handlers,
		Deliver:    a.deliver,
		Shutdown: func(reason error) {
			cancel()
		},
		RouteReply:                a.pending.Deliver,
		Events:                    a.eventsStore,
		Projections:               a.projStore,
		PolicyStore:               a.policyStore,
		ServiceRegistry:           a.svcReg,
		DiscoveryProvider:         a.cfg.discoveryProvider,
		DiscoveryAddress:          a.cfg.discoveryAddress,
		LookupService:             a.LookupService,
		ExposeService:             func(name string) error { return a.exposeService(a.rootRef, name) },
		UnexposeService:           func(name string) { a.unexposeService(a.rootRef, name) },
		ExposeServiceToChildren:   func(name string) error { return a.exposeServiceToChildren(a.rootRef, name) },
		UnexposeServiceToChildren: func(name string) { a.unexposeServiceToChildren(a.rootRef, name) },
		UnexposeAllScopedServices: func() { a.unexposeAllScopedServices(a.rootRef) },
		LookupScopedService:       func(name string) (ref.Ref, bool) { return a.lookupScopedService(a.rootRef, name) },
		Resources:                 a.resReg,
		Namespace:                 a.namespace,
		Codec:                     a.codec,
		Schemas:                   a.schemas,
		RootRef:                   a.rootRef,
		PlannerInvoke: func(target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call {
			return a.invokeAs(a.rootRef, target, callID, payload, headers...)
		},
		Clock:            a.cfg.clock,
		Spawn:            a.tree.Spawn,
		Stop:             a.tree.Stop,
		Destroy:          a.tree.Destroy,
		LookupID:         a.tree.LookupID,
		InvokeTable:      a.pending,
		CorIDGen:         a.corIDGen,
		IDGen:            a.gen,
		OnInitCompleted:  func() { close(rootInitialized) },
		OnStartCompleted: func() { close(rootStarted) },
		EventBus:         a.eventBus,
		Logger:           a.cfg.logger,
		LogHook:          a.cfg.logHook,
	})

	a.cellMu.Lock()
	a.cells[a.rootRef.ID()] = rootCell
	a.cellMu.Unlock()

	// Register the cell's Recv function for direct message delivery.
	a.tp.Register(a.rootRef.ID(), rootCell.Recv)

	// Step 3: Start the root Cell loop.
	go rootCell.Run(ctx)

	// Step 3b: Start remote transport receive loop if configured.
	if a.remoteTP != nil {
		go a.receiveLoop(ctx)
	}

	// Phase 1: Init. Push Init to root, which recursively inits all children.
	_ = rootCell.Recv(mailbox.Envelope{Payload: mailbox.Init{}})
	select {
	case <-rootInitialized:
	case <-rootCell.Done():
		// OnInit failed or cell stopped early.
	case <-ctx.Done():
		// Cancelled while boot init was still in flight. The root cell's
		// Done may never close in this window: its drain path can be
		// wedged behind a child whose ingress the same cancellation
		// closed (recursive Spawn waits for child OnInit). Run must
		// observe the cancellation itself.
		log.Printf("gospore/app: context cancelled during init; aborting boot")
		return ctx.Err()
	}

	// A cancellation that raced Phase 1's completion (or landed between
	// phases) must not march the boot through Phase 2's waits.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Phase 2: Start cells.
	// Prefer the start order computed by rootActor.OnInit (which reflects
	// the actual DependsOn topological sort). Fall back to the config
	// option. When neither is set all cells start in parallel (backward-
	// compatible default).
	startOrder := a.childStartOrder
	if len(startOrder) == 0 {
		startOrder = a.cfg.childStartOrder
	}

	if len(startOrder) > 0 {
		// Ordered start: send Start to the root first, then to children in
		// dependency order, waiting for each cell to finish OnStart before
		// starting the next.
		startedWait := time.After(30 * time.Second)
		// Start root cell first.
		a.deliverStart(rootCell.Self().ID(), rootCell)
		if !rootCell.IsStarted() && !rootCell.IsStopped() {
		waitRoot:
			for {
				select {
				case <-startedWait:
					log.Printf("gospore/app: timed out waiting for root cell to start")
					break waitRoot
				case <-rootCell.Done():
					break waitRoot
				default:
					time.Sleep(2 * time.Millisecond)
				}
				if rootCell.IsStarted() || rootCell.IsStopped() {
					break waitRoot
				}
			}
		}
		for _, name := range startOrder {
			actorIDStr, ok := a.childNameToID[name]
			if !ok {
				// Name not in mapping — skip silently (may be a
				// framework-internal cell not spawned by rootActor).
				continue
			}
			aid, err := id.Parse(actorIDStr)
			if err != nil {
				continue
			}
			c := a.getCell(aid)
			if c == nil || c == rootCell {
				continue
			}
			a.deliverStart(aid, c)
			// Wait for this cell to complete OnStart.
			if !c.IsStarted() && !c.IsStopped() {
			waitCell:
				for {
					select {
					case <-startedWait:
						log.Printf("gospore/app: timed out waiting for cell %q to start", name)
						break waitCell
					case <-c.Done():
						break waitCell
					default:
						time.Sleep(2 * time.Millisecond)
					}
					if c.IsStarted() || c.IsStopped() {
						break waitCell
					}
				}
			}
		}
	} else {
		// Parallel start: push Start to ALL cells in parallel (original behavior).
		a.cellMu.RLock()
		allCells := make([]*cell.Cell, 0, len(a.cells))
		for _, c := range a.cells {
			allCells = append(allCells, c)
		}
		a.cellMu.RUnlock()
		for _, c := range allCells {
			if c == rootCell {
				a.deliverStart(rootCell.Self().ID(), rootCell)
			} else {
				a.deliverStart(c.Self().ID(), c)
			}
		}
	}

	// Mark batch init complete so dynamic spawns push Init+Start.
	a.batchInitComplete.Store(true)

	// Deliver Start to any cell whose delivery was deferred during the
	// batch (grandchildren spawned inside an OnStart, or spawns that raced
	// batchInitComplete). Without this sweep such cells stay unstarted
	// forever and WaitForAllCellsStart can never succeed.
	a.deliverDeferredStarts()

	// Wait for ALL cells to complete OnStart, not just root.
	// This guarantees every callable is registered before the
	// gateway accepts external traffic or Ready() returns.
	startedWait := time.After(10 * time.Second)
waitAll:
	for {
		a.cellMu.RLock()
		allReady := true
		for _, c := range a.cells {
			if !c.IsStarted() && !c.IsStopped() {
				allReady = false
				break
			}
		}
		a.cellMu.RUnlock()
		if allReady {
			break waitAll
		}
		select {
		case <-startedWait:
			log.Printf("gospore/app: timed out waiting for all cells to start")
			break waitAll
		case <-rootCell.Done():
			break waitAll
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}

	// Step 4: Freeze resource registry after OnStart completes.
	resource.Freeze(a.resReg)

	// Step 4b: Start external HTTP gateway if configured.
	if a.gatewaySrv != nil {
		go func() {
			if err := a.gatewaySrv.Run(ctx); err != nil {
				log.Printf("gospore/app: gateway exited: %v", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
	case <-rootCell.Done():
	}

	// Step 6: LIFO destroy the entire tree.
	_ = a.tree.Destroy(a.rootRef)

	// Step 9: Close transport, release stores.
	a.tp.Close()
	if a.remoteTP != nil {
		a.remoteTP.Close()
	}
	cancel()
	return ctx.Err()
}

func (a *appImpl) Shutdown(_ context.Context) error {
	a.shutdownOnce.Do(func() {
		if fn, ok := a.cancel.Load().(context.CancelFunc); ok && fn != nil {
			fn()
		}
	})
	return nil
}

// deliverStart sends the Start envelope to a cell exactly once. All Start
// deliveries (allocate's post-batch path, Phase 2 ordered/parallel start,
// and the post-batch sweep) must funnel through here so the sweep cannot
// double-deliver.
func (a *appImpl) deliverStart(aid id.ActorID, c *cell.Cell) {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	if a.startSent[aid] {
		return
	}
	_ = c.Recv(mailbox.Envelope{Payload: mailbox.Start{}})
	a.startSent[aid] = true
}

// deliverDeferredStarts delivers Start to cells allocated during the batch
// whose delivery was deferred (grandchildren spawned inside an OnStart, or
// spawns racing batchInitComplete). Called after Phase 2 and after
// batchInitComplete.Store(true) so no cell is left permanently unstarted.
func (a *appImpl) deliverDeferredStarts() {
	a.cellMu.RLock()
	snapshot := make([]*cell.Cell, 0, len(a.cells))
	for _, c := range a.cells {
		snapshot = append(snapshot, c)
	}
	a.cellMu.RUnlock()
	for _, c := range snapshot {
		a.deliverStart(c.Self().ID(), c)
	}
}

// SetChildStartOrder stores the topological child start order and
// name→actorID mapping computed by the root actor during Phase 1.
func (a *appImpl) SetChildStartOrder(order []string, nameToID map[string]string) {
	a.childStartOrder = order
	a.childNameToID = nameToID
}

// deliver is the cross-actor delivery closure for Cells created by this App.
func (a *appImpl) getCell(aid id.ActorID) *cell.Cell {
	a.cellMu.RLock()
	c := a.cells[aid]
	a.cellMu.RUnlock()
	return c
}

func (a *appImpl) waitForCellStart(r ref.Ref) error {
	if r == nil {
		return nil
	}
	for {
		c := a.getCell(r.ID())
		if c == nil {
			return fmt.Errorf("gospore/app: spawned actor %s not registered", r.ID().String())
		}
		if c.IsStarted() {
			return nil
		}
		if c.IsStopped() {
			return fmt.Errorf("gospore/app: actor %s stopped before completing OnStart", r.ID().String())
		}
		select {
		case <-c.Done():
			// loop re-checks terminal state and reports a stable error
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// receiveLoop drains inbound frames from the remote transport and
// delivers them to local Cells. It runs in its own goroutine and
// exits when the context is cancelled or the Receive channel closes.
func (a *appImpl) receiveLoop(ctx context.Context) {
	ch := a.remoteTP.Receive()
	if ch == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-ch:
			if !ok {
				return
			}
			targetRef, found := a.tree.LookupID(frame.To)
			if !found {
				continue
			}
			sender := &remoteRef{
				aid: frame.From,
				invoke: func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
					mode := a.lookupCallMode(frame.From, callID)
					return invoke.NewCall(mode, a.invoke(targetRef.ID(), frame.From, callID, payload, headers...), invoke.WithFinalTimeout(a.cfg.defaultInvokeTimeout))
				},
			}
			env := mailbox.Envelope{
				Frame:  frame,
				Sender: sender,
			}
			if frame.Kind == message.KindSystem {
				if sys, err := decodeSystemMsg(frame.Body); err == nil {
					env.Payload = sys
				}
			}
			_ = a.deliver(targetRef, env)
		}
	}
}

// remoteRef is a ref.Ref that represents an actor on a remote App.
// It allows the local Cell to route replies back through the remote
// transport without requiring the remote actor to be in the local tree.
