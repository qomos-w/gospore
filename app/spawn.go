package app

// Actor spawn reservation, build, and teardown wired through the tree.
import (
	"context"
	"fmt"
	"time"

	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/mailbox"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
)

// App is the process-level entry point and root actor of one
// gospore service. App embeds actor.Actor: it has its own OnStart /
// OnStop chain, runs handlers in the `app.*` reserved namespace, and
func (a *appImpl) Spawn(props actor.Props, name string) (ref.Ref, error) {
	ref, err := a.tree.Spawn(a.rootRef, props, name)
	if err != nil {
		return nil, err
	}
	if !a.batchInitComplete.Load() || props.AsyncStart() {
		return ref, nil
	}
	return ref, a.waitForCellStart(ref)
}

// HandlerTable satisfies the scriptbridge.Host SPI: scriptbridge
// type-asserts the App to obtain the runtime handler table when
// building the dynamic gospore.invoke capability set. Adapter-only —
// not intended for application code, which should use the Spawn /
// service.Registry surfaces instead.
func (a *appImpl) reserveSpawn(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
	aid := props.ID()
	if aid.IsZero() {
		aid = a.gen.Next()
	}
	// Reserve the child's pending table now, under the tree write lock:
	// allocation-only, non-blocking. Calls issued through the reserved ref
	// (caller = aid) before Build completes already resolve the right table.
	a.invokeTableMu.Lock()
	a.invokeTables[aid] = invoke.NewPendingTable()
	a.invokeTableMu.Unlock()
	return &appRef{
		aid: aid,
		invoke: func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
			mode := a.lookupCallMode(aid, callID)
			headers = a.injectCallerRole(aid, headers)
			return invoke.NewCall(mode, a.invoke(aid, aid, callID, payload, headers...), invoke.WithFinalTimeout(a.cfg.defaultInvokeTimeout))
		},
	}, nil
}

// buildSpawn constructs the cell for a reserved identity: cell creation,
// Init delivery and the OnInit wait. Wired as the tree Allocator's Build
// phase, running WITHOUT the tree write lock.
func (a *appImpl) buildSpawn(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error {
	childRef := reserved

	// Role inheritance: an actor spawned without an explicit role (plan
	// nodes, lanes, and other internal delegation children) inherits the
	// parent cell's role so its outbound planner calls carry the parent's
	// caller identity. Identity is otherwise lost across internal
	// indirection, and callees (e.g. appmanager) classify zero-identity
	// contexts as external.
	if props.Role() == "" {
		if parentCell := a.getCell(parent.ID()); parentCell != nil {
			if role := parentCell.Role(); role != "" {
				props = props.WithRole(role)
			}
		}
	}

	// Create child Cell.
	var childHandlers *handler.Table
	if a.cfg.strictHandlers {
		childHandlers = handler.NewStrictTableWithSchema(a.schemas)
	} else {
		childHandlers = handler.NewTableWithSchema(a.schemas)
	}
	// Per-cell pending table: replies for calls issued by this actor land in
	// its own reply pipeline (frames are addressed To: the caller), so the
	// slot registry lives with the cell. The table was reserved in
	// reserveSpawn; re-create defensively if this build raced a restart.
	childPT := a.invokeTableFor(childRef.ID())
	if childPT == a.pending || childPT == nil {
		childPT = invoke.NewPendingTable()
		a.setInvokeTable(childRef.ID(), childPT)
	}
	childCell := cell.New(cell.Config{
		Self:              childRef,
		Parent:            parent,
		Actor:             props.Factory()(),
		Props:             props,
		Supervisor:        a.defSup,
		Handlers:          childHandlers,
		StrictHandlers:    a.cfg.strictHandlers,
		Host:              a.cellHost(),
		Events:            a.eventsStore,
		Projections:       a.projStore,
		PolicyStore:       a.policyStore,
		ServiceRegistry:   a.svcReg,
		DiscoveryProvider: a.cfg.discoveryProvider,
		Resources:         a.resReg,
		Namespace:         a.namespace,
		Codec:             a.codec,
		Schemas:           a.schemas,
		RootRef:           a.rootRef,
		Clock:             a.cfg.clock,
		InvokeTable:       childPT,
		CorIDGen:          a.corIDGen,
		IDGen:             a.gen,
		EventBus:          a.eventBus,
		Logger:            a.cfg.logger,
		LogHook:           a.cfg.logHook,
	})
	childInitialized := childCell.InitDone()

	aid := childRef.ID()
	a.cellMu.Lock()
	a.cells[aid] = childCell
	a.cellMu.Unlock()

	// Start the child Cell goroutine.
	var cellCtx context.Context
	if rc := a.runCtx.Load(); rc != nil {
		cellCtx = rc.(context.Context)
	} else {
		cellCtx = context.Background()
	}
	go childCell.Run(cellCtx)

	// Register the child's Recv for direct delivery.
	a.tp.Register(aid, childCell.Recv)

	// Phase 1: Trigger OnInit.
	_ = childCell.Recv(mailbox.Envelope{Payload: mailbox.Init{}})

	// Wait for OnInit to complete so the tree is fully built
	// before any OnStart runs. A cell that dies during its own OnInit
	// still counts as built (matches the historical allocate contract).
	// A cancelled cell context unwedges the caller: without this case a
	// child whose OnInit never observes cancellation wedges the whole
	// recursive Spawn chain (and Run's Phase 1) forever.
	select {
	case <-childInitialized:
	case <-childCell.Done():
		return nil
	case <-cellCtx.Done():
		return fmt.Errorf("gospore/app: spawn of %s aborted: context cancelled during OnInit", childRef.ID())
	}

	// Phase 2: Trigger OnStart (only after batch init is complete).
	// During the batch, Start delivery is Phase 2's job; register the cell
	// as pending so deliverDeferredStarts can deliver it after the batch
	// (re-checking the flag under pendingMu closes the allocate/Store race).
	a.pendingMu.Lock()
	if a.batchInitComplete.Load() {
		a.pendingMu.Unlock()
		a.deliverStart(aid, childCell)
	} else {
		a.pendingStart[aid] = childCell
		a.pendingMu.Unlock()
	}

	return nil
}

// abortSpawn tears down a reservation whose Build failed or whose parent
// vanished mid-build. Runs after Build returned, so it observes the
// reservation's final state; terminate is idempotent for identities that
// were never built.
func (a *appImpl) abortSpawn(reserved ref.Ref) {
	_ = a.terminate(reserved)
}

// appAllocator adapts appImpl to the tree.Allocator phase protocol.
type appAllocator struct{ a *appImpl }

func (al *appAllocator) Reserve(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
	return al.a.reserveSpawn(parent, props, name)
}

func (al *appAllocator) Build(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error {
	return al.a.buildSpawn(parent, props, name, reserved)
}

func (al *appAllocator) Abort(reserved ref.Ref) {
	al.a.abortSpawn(reserved)
}

// idleCell is the Tree Idler closure. Pushes Idle to a Cell without
// removing it from the indices.
func (a *appImpl) idleCell(r ref.Ref) error {
	if r == nil {
		return nil
	}
	if recv, ok := a.tp.Lookup(r.ID()); ok {
		_ = recv(mailbox.Envelope{Payload: mailbox.Idle{}})
	}
	return nil
}

// terminate is the Tree Terminator closure. Destroys a Cell.
func (a *appImpl) terminate(r ref.Ref) error {
	if r == nil {
		return nil
	}

	a.cellMu.Lock()
	childCell, ok := a.cells[r.ID()]
	delete(a.cells, r.ID())
	a.cellMu.Unlock()

	if ok && childCell != nil {
		// For root cell, send Destroy via Recv; for children, same path.
		if recv, ok := a.tp.Lookup(r.ID()); ok {
			_ = recv(mailbox.Envelope{Payload: mailbox.Destroy{}})
		}
		// Wait for graceful shutdown.
		select {
		case <-childCell.Done():
		case <-time.After(a.cfg.terminateTimeout):
			// Graceful path is wedged (systemLoop blocked — typically a
			// slow OnInit) while Run's drain already closed the queues, so
			// the queued Stop/Destroy will never be processed. Force the
			// terminal callbacks so resources opened during OnInit are
			// closed instead of leaking; ForceCleanup is idempotent and
			// races the message path safely (exactly-once callbacks).
			childCell.ForceCleanup()
		}
	}

	// Drop the actor's pending-table registry entry only after the cell is
	// down (handleDestroy has flushed the table itself). Root keeps its
	// entry: it aliases the App table.
	a.removeInvokeTable(r.ID())
	a.tp.Unregister(r.ID())
	return nil
}

// injectCallerRole checks whether any of the provided headers already set
// gospore.caller_role.  If not, and the caller actor has a non-empty role
// configured via Props.WithRole, it appends a header map carrying that role.
