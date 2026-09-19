// Package app is the gospore process-level entry point.
//
// An App is the root actor of one logical service: it owns the
// actor Tree, the SchemaSet, the Service / Resource / Projection /
// Events stores, the Codec, and a single Namespace. Every gospore
// runtime artifact lives under exactly one App.
//
// Construct with New(opts...). Run blocks until ctx cancellation,
// Shutdown, or root-actor escalation; both paths guarantee LIFO
// teardown of the entire actor tree before returning.
//
// See ARCHITECTURE.md §4.17 for the full semantics, the startup
// timeline, and the `app.*` reserved Call ID namespace.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/gateway"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/discovery"
	"github.com/qomos-w/gospore/events"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/resource"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/gospore/service"
	"github.com/qomos-w/gospore/supervisor"
	"github.com/qomos-w/gospore/transport"
	"github.com/qomos-w/gospore/tree"
	tschema "github.com/qomos-w/spore/schema"
)

// App is the process-level entry point and root actor of one
// gospore service. App embeds actor.Actor: it has its own OnStart /
// OnStop chain, runs handlers in the `app.*` reserved namespace, and
// is the parent of every top-level user actor.
type App interface {
	actor.Actor

	// Namespace returns the App's required namespace identifier
	// (matches `^[a-z][a-z0-9_]*$` and is neither `app` nor
	// `_gospore_`). Used as the SchemaSet owner namespace and the
	// allowed first segment of every user-registered Call ID.
	Namespace() string

	// Self returns the root actor's Ref (path == "/").
	Self() ref.Ref

	// Spawn creates a new child directly under the root actor. name
	// must be unique among existing root children.
	//
	// The returned Ref is tree-registered before Spawn returns. During
	// steady-state runs (after batch init), Spawn also waits for the
	// child's OnStart to complete before returning, unless the Props opt
	// into AsyncStart.
	Spawn(props actor.Props, name string) (ref.Ref, error)

	// LookupService returns the Ref currently exposing the named
	// service in this App. Service names are single-segment and do
	// NOT contain dots.
	LookupService(name string) (ref.Ref, bool)

	// RemoteRef returns a Ref for an actor that lives in a remote
	// App. The returned Ref routes through the App's cross-process
	// transport; Invoke calls transparently marshal frames and await
	// replies. caller is the local ActorID that appears as Frame.From.
	RemoteRef(actorID id.ActorID) ref.Ref

	// Codec returns the wire codec used by this App.
	Codec() codec.Codec

	// Schemas returns the App's SchemaSet, the read-write registry
	// for this App's namespace plus any Imported foreign namespaces.
	Schemas() schema.Set

	// Tree returns the actor tree — the sole owner of the
	// (ActorID → cell) and (Path → cell) indices.
	Tree() tree.Tree

	// Service returns the service-name registry populated by
	// ctx.Expose calls.
	Service() service.Registry

	// Resources returns the App's typed resource table. Writeable
	// during OnStart, frozen for the rest of the run.
	Resources() resource.Registry

	// Projections returns the State Channel store: per-actor
	// component snapshots + version + delta ring.
	Projections() projection.Store

	// Events returns the Event Channel store: per-actor invocation
	// and lifecycle records.
	Events() events.Store

	// PolicyStore returns the App's runtime permission evaluator.
	// Scripts access it through the gospore.policy capability.
	PolicyStore() actor.PolicyStore

	// WaitForAllCellsStart blocks until every cell has completed OnStart
	// or stopped. Useful for callers that need a complete callable surface
	// before proceeding.
	WaitForAllCellsStart(timeout time.Duration) error

	// ExportGosporeManifest produces a manifest describing every callable,
	// projection component, and event kind registered across the App's
	// actor tree. Suitable for feeding into gospore-gen-ts.
	ExportGosporeManifest() (schema.GosporeManifest, error)

	// ManifestJSON returns the raw manifest JSON bytes passed via
	// WithManifestImport, or nil if no manifest was configured.
	ManifestJSON() []byte

	// HandlerTableFor returns the handler table for the actor identified
	// by r. Returns (nil, false) when the actor is not live.
	HandlerTableFor(r ref.Ref) (*handler.Table, bool)

	// ActorType returns the Type() of the actor identified by aid,
	// or "" if the actor is not live.
	ActorType(aid id.ActorID) string

	// Run blocks the calling goroutine until ctx cancellation,
	// Shutdown, or a root-actor escalation triggers App teardown.
	// On return the entire actor tree has reached its terminal
	// state and every OnStop has completed.
	Run(ctx context.Context) error

	// Shutdown is the explicit teardown entry point. Safe to call
	// from any goroutine, including handlers. Idempotent — second
	// and later calls are no-ops. Does not wait for completion;
	// callers that need to wait should block on Run's return.
	Shutdown(ctx context.Context) error

	// GatewayServer returns the HTTP gateway's *gateway.Server,
	// or nil if no gateway is configured. Callers can use it to
	// accept non-HTTP transport sessions (e.g. Wails IPC) via
	// ServeSession. Must not be called before Run.
	GatewayServer() *gateway.Server

	// SpawnGatewaySession spawns a dedicated caller cell under root for
	// one gateway connection ("gatesession"). Calls the connection issues
	// should go through InvokeAsCaller with the returned Ref so their
	// reply slots live in this actor's pending table: a slow consumer
	// then backpressures only its own cell's reply pipeline instead of
	// the root pipeline shared by every session.
	SpawnGatewaySession(name string) (ref.Ref, error)

	// InvokeAsCaller issues a call on behalf of caller, so the target's
	// ctx.Caller() resolves to the calling actor and replies land in the
	// caller's own pending table. See invokeAs.
	InvokeAsCaller(caller, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call

	// DestroyGatewaySession tears a gatesession cell down. Its pending
	// table is flushed with terminal errors, unblocking any in-flight
	// Stream still waiting for replies.
	DestroyGatewaySession(r ref.Ref) error

	// SetChildStartOrder configures the Phase 2 Start order. Called
	// by the root actor's OnInit callback with the topologically sorted
	// child names and name→actorID mapping. When set, Phase 2 sends
	// Start in dependency order and waits for each cell to complete
	// before starting the next.
	SetChildStartOrder(order []string, nameToID map[string]string)
}

// Sentinel errors for App construction.
var (
	// ErrInvalidNamespace is returned by New when the namespace is empty,
	// fails format validation, or equals a reserved prefix.
	ErrInvalidNamespace = errors.New("gospore/app: invalid namespace")
)

// New constructs an App from opts. WithNamespace is required;
// omitting it or passing an invalid namespace returns ErrInvalidNamespace.
func New(opts ...Option) (App, error) {
	cfg := config{
		// Default fallback deadline for caller-side Final waits without an
		// explicit context deadline. Overridable via WithDefaultInvokeTimeout;
		// a zero or negative value disables it entirely.
		defaultInvokeTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if !actor.ValidNamespace(cfg.namespace) || cfg.namespace == "app" {
		return nil, fmt.Errorf("%w: %q", ErrInvalidNamespace, cfg.namespace)
	}
	schemas, err := schema.New(cfg.namespace)
	if err != nil {
		return nil, fmt.Errorf("%w: schema init: %v", ErrInvalidNamespace, err)
	}
	if len(cfg.manifestJSON) > 0 {
		var manifest tschema.Manifest
		if err := json.Unmarshal(cfg.manifestJSON, &manifest); err != nil {
			return nil, fmt.Errorf("manifest parse: %w", err)
		}
		if err := schema.ImportFromManifest(schemas, manifest); err != nil {
			return nil, fmt.Errorf("manifest import: %w", err)
		}
	}
	svcReg := service.New()
	resReg := resource.New()
	tp := transport.NewLocal()
	gen := id.NewCanonical(cfg.runtimeSlot, uint16(0), func() uint64 { return uint64(time.Now().UnixMilli()) })
	corIDGen := id.NewCorIDGenerator()
	rootAID := cfg.rootID
	if rootAID.IsZero() {
		rootAID = gen.Next()
	}
	pt := invoke.NewPendingTable()
	rootActor := cfg.rootActor
	if rootActor == nil {
		rootActor = func() actor.Actor { return &actor.Host{} }
	}
	defSup := cfg.defaultSupervisor
	if defSup == nil {
		defSup = supervisor.NewOneForOne(10, time.Minute)
	}
	if cfg.discoveryProvider == nil {
		cfg.discoveryProvider = discovery.NewMemoryProvider(time.Now())
	}
	mbCap := cfg.userMailboxCapacity
	if mbCap == 0 {
		mbCap = 1024
	}
	if cfg.codec == nil {
		cfg.codec = codec.NewMulti(schemas)
	}
	if cfg.clock == nil {
		cfg.clock = actor.RealClock{}
	}
	if cfg.logger == nil {
		cfg.logger = cell.NewDefaultLogger("", cfg.logHook)
	}
	if cfg.terminateTimeout <= 0 {
		cfg.terminateTimeout = 5 * time.Second
	}
	var rootHandlers *handler.Table
	if cfg.strictHandlers {
		rootHandlers = handler.NewStrictRootTableWithSchema(schemas)
	} else {
		rootHandlers = handler.NewRootTableWithSchema(schemas)
	}

	a := &appImpl{
		namespace:    cfg.namespace,
		rootActor:    rootActor,
		handlers:     rootHandlers,
		codec:        cfg.codec,
		schemas:      schemas,
		svcReg:       svcReg,
		resReg:       resReg,
		tp:           tp,
		remoteTP:     cfg.remoteTransport,
		gen:          gen,
		corIDGen:     corIDGen,
		pending:      pt,
		defSup:       defSup,
		mbCap:        mbCap,
		cfg:          cfg,
		eventsStore:  events.NewStore(cfg.eventRingCapacity),
		projStore:    projection.NewStore(cfg.projectionDeltaWindow),
		policyStore:  actor.NewPolicyStore(),
		eventBus:     cell.NewEventBus(),
		cells:        make(map[id.ActorID]*cell.Cell),
		invokeTables: make(map[id.ActorID]*invoke.PendingTable),
		startSent:    make(map[id.ActorID]bool),
		pendingStart: make(map[id.ActorID]*cell.Cell),
	}
	// The root cell's pending table is the App-level table: gateway and
	// App-originated calls (caller = root) resolve here via invokeTableFor.
	a.invokeTables[rootAID] = pt
	if cfg.gatewayInterceptor != nil {
		srv := gateway.NewServer(a, cfg.gatewayInterceptor, cfg.gatewayAddr)
		srv.SetStaticFS(cfg.staticFS)
		srv.SetLogger(cfg.logger)
		if cfg.binaryOnly {
			srv.WithBinaryOnly(true)
		}
		if cfg.urlAuth != nil {
			srv.WithURLAuth(cfg.urlAuth)
		}
		if cfg.logProvider != nil {
			srv.SetLogProvider(cfg.logProvider)
		}
		if cfg.extraRoutes != nil {
			srv.SetExtraRoutes(cfg.extraRoutes)
		}
		if len(cfg.gatewayExtraAddrs) > 0 {
			srv.SetExtraAddrs(cfg.gatewayExtraAddrs)
		}
		a.gatewaySrv = srv
	}
	rootRef := &appRef{
		aid: rootAID,
		invoke: func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
			mode := a.lookupCallMode(rootAID, callID)
			headers = a.injectCallerRole(rootAID, headers)
			return invoke.NewCall(mode, a.invoke(rootAID, rootAID, callID, payload, headers...), invoke.WithFinalTimeout(a.cfg.defaultInvokeTimeout))
		},
	}
	a.rootRef = rootRef
	a.eventBus.SetLogger(cfg.logger)
	a.cancel.Store(context.CancelFunc(func() {}))
	tr, err := tree.New(tree.Config{
		Root:                rootRef,
		Allocator:           &appAllocator{a},
		Idler:               a.idleCell,
		Terminator:          a.terminate,
		LookupGlobalService: a.svcReg.Lookup,
	})
	if err != nil {
		return nil, err
	}
	a.tree = tr
	if err := a.registerEventSubscribeCallable(); err != nil {
		return nil, err
	}
	if err := a.registerProjectionCallables(); err != nil {
		return nil, err
	}
	if err := a.registerCellStatsCallable(); err != nil {
		return nil, err
	}
	return a, nil
}

// appImpl is the concrete App implementation.
type appImpl struct {
	namespace   string
	rootRef     *appRef
	rootActor   func() actor.Actor
	handlers    *handler.Table
	codec       codec.Codec
	schemas     schema.Set
	svcReg      service.Registry
	resReg      resource.Registry
	tp          *transport.Local
	remoteTP    transport.Transport
	tree        tree.Tree
	gen         *id.Canonical
	corIDGen    *id.CorIDGenerator
	pending     *invoke.PendingTable
	defSup      supervisor.Supervisor
	mbCap       int
	cfg         config
	eventsStore *events.StoreImpl
	projStore   *projection.StoreImpl
	policyStore actor.PolicyStore
	eventBus    *cell.EventBus
	gatewaySrv  *gateway.Server

	cellMu sync.RWMutex
	cells  map[id.ActorID]*cell.Cell

	// invokeTableMu guards invokeTables: the per-actor pending tables used
	// to correlate replies for calls issued by each actor. The root entry
	// aliases `pending`, which also serves App/gateway-originated calls.
	invokeTableMu sync.RWMutex
	invokeTables  map[id.ActorID]*invoke.PendingTable

	serviceMu sync.Mutex

	shutdownOnce      sync.Once
	cancel            atomic.Value // stores context.CancelFunc
	done              chan struct{}
	runCtx            atomic.Value // stores context.Context
	batchInitComplete atomic.Bool

	// childStartOrder and childNameToID are populated via the
	// onInitComplete callback from the root actor during Phase 1.
	// Phase 2 uses them to start cells in dependency order.
	childStartOrder []string
	childNameToID   map[string]string

	// pendingMu guards startSent/pendingStart. allocate defers Start for
	// cells created before batchInitComplete (Phase 2 delivers Start
	// explicitly for batch children); deliverDeferredStarts sweeps after
	// the batch so mid-batch dynamic spawns — grandchildren spawned inside
	// an OnStart — cannot be left permanently unstarted.
	pendingMu    sync.Mutex
	startSent    map[id.ActorID]bool
	pendingStart map[id.ActorID]*cell.Cell
}

func (a *appImpl) Namespace() string { return a.namespace }
func (a *appImpl) Self() ref.Ref     { return a.rootRef }
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
func (a *appImpl) HandlerTable() *handler.Table { return a.handlers }

// HandlerTableFor returns the handler table for the live actor identified
// by r. Returns (nil, false) when the actor is not found.
func (a *appImpl) HandlerTableFor(r ref.Ref) (*handler.Table, bool) {
	if r == nil {
		return nil, false
	}
	c := a.getCell(r.ID())
	if c == nil {
		return nil, false
	}
	ht := c.Handlers()
	return ht, ht != nil
}

func (a *appImpl) LookupService(name string) (ref.Ref, bool) {
	if r, ok := a.svcReg.Lookup(name); ok {
		return r, true
	}
	if a.cfg.discoveryProvider != nil {
		instances := a.cfg.discoveryProvider.Resolve(name)
		for _, inst := range instances {
			if inst.RuntimeSlotID == a.cfg.runtimeSlot {
				// Local instance — re-check registry in case sync is behind.
				if r, ok := a.svcReg.Lookup(name); ok {
					return r, true
				}
				continue
			}
			// Remote instance — construct a transparent RemoteRef.
			if !inst.ActorID.IsZero() && a.remoteTP != nil {
				return a.RemoteRef(inst.ActorID), true
			}
		}
	}
	return nil, false
}

func (a *appImpl) exposeService(owner ref.Ref, name string) error {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	if err := a.svcReg.Register(name, owner); err != nil {
		return err
	}
	if err := a.tree.RegisterGlobalService(owner, name, owner); err != nil {
		a.svcReg.Unregister(name)
		return err
	}
	return nil
}

func (a *appImpl) unexposeService(owner ref.Ref, name string) {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	a.svcReg.Unregister(name)
	a.tree.UnregisterGlobalService(owner, name)
}

func (a *appImpl) exposeServiceToChildren(owner ref.Ref, name string) error {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	return a.tree.RegisterScopedService(owner, name, owner)
}

func (a *appImpl) unexposeServiceToChildren(owner ref.Ref, name string) {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	a.tree.UnregisterScopedService(owner, name)
}

func (a *appImpl) unexposeAllScopedServices(owner ref.Ref) {
	a.serviceMu.Lock()
	defer a.serviceMu.Unlock()
	a.tree.UnregisterAllScopedServices(owner)
}

func (a *appImpl) lookupScopedService(caller ref.Ref, name string) (ref.Ref, bool) {
	return a.tree.LookupScopedService(caller, name)
}
func (a *appImpl) Codec() codec.Codec             { return a.codec }
func (a *appImpl) Schemas() schema.Set            { return a.schemas }
func (a *appImpl) Tree() tree.Tree                { return a.tree }
func (a *appImpl) Service() service.Registry      { return a.svcReg }
func (a *appImpl) Resources() resource.Registry   { return a.resReg }
func (a *appImpl) Projections() projection.Store  { return a.projStore }
func (a *appImpl) Events() events.Store           { return a.eventsStore }
func (a *appImpl) PolicyStore() actor.PolicyStore { return a.policyStore }
func (a *appImpl) ManifestJSON() []byte           { return a.cfg.manifestJSON }
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
func (a *appImpl) deliver(target ref.Ref, env mailbox.Envelope) error {
	recv, ok := a.tp.Lookup(target.ID())
	if ok {
		return recv(env)
	}
	if a.remoteTP != nil {
		if sys, ok := env.Payload.(mailbox.SystemMsg); ok {
			body, err := encodeSystemMsg(sys)
			if err != nil {
				return err
			}
			return a.remoteTP.Send(message.Frame{
				To:   target.ID(),
				Kind: message.KindSystem,
				Body: body,
			})
		}
		frame := env.Frame
		if frame.To.IsZero() {
			frame.To = target.ID()
		}
		return a.remoteTP.Send(frame)
	}
	return fmt.Errorf("gospore/app: deliver target %s not registered", target.ID().Canonical())
}
func (a *appImpl) Type() string                { return "app" }
func (a *appImpl) OnInit(actor.Context) error  { return nil }
func (a *appImpl) OnStart(actor.Context) error { return nil }
func (a *appImpl) OnStop(actor.Context) error  { return nil }

// getCell returns the Cell for an actor ID, or nil if not found.
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
type remoteRef struct {
	aid    id.ActorID
	invoke func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call
}

// cancellingStream wraps an invoke.Stream and sends a KindCancel frame
// to the target actor when Cancel() is called. Normal Close() does not
// send cancellation — only the explicit Cancel() path does.
type cancellingStream struct {
	invoke.Stream
	sendCancel func() error
	once       sync.Once
}

func (s *cancellingStream) Cancel() error {
	s.once.Do(func() {
		_ = s.sendCancel()
	})
	if cs, ok := s.Stream.(interface{ Cancel() error }); ok {
		return cs.Cancel()
	}
	return s.Stream.Close()
}

func (r *remoteRef) ID() id.ActorID          { return r.aid }
func (r *remoteRef) Service() (string, bool) { return "", false }
func (r *remoteRef) Invoke(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
	if r.invoke != nil {
		return r.invoke(ctx, callID, payload, headers...)
	}
	return nil
}

var _ ref.Ref = (*remoteRef)(nil)

// RemoteRef returns a Ref for a remote actor. Invoke calls on the
// returned Ref route through the App's remote transport with the
// App's root actor as the caller.
func (a *appImpl) RemoteRef(actorID id.ActorID) ref.Ref {
	return &remoteRef{
		aid: actorID,
		invoke: func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
			mode := a.lookupCallMode(actorID, callID)
			return invoke.NewCall(mode, a.invoke(a.rootRef.ID(), actorID, callID, payload, headers...), invoke.WithFinalTimeout(a.cfg.defaultInvokeTimeout))
		},
	}
}

// invokeAs issues a call on behalf of caller with caller as the message
// sender, so ctx.Caller() in the target's handlers resolves to the calling
// actor instead of the target itself (which is what a bare target.Invoke
// produces). Local targets carry the caller's role header; remote targets
// keep the legacy sender semantics (app root) because remote reply routing
// addresses the App, not an individual local actor.
func (a *appImpl) invokeAs(caller, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call {
	targetID := target.ID()
	if _, ok := a.tree.LookupID(targetID); !ok {
		return target.Invoke(context.Background(), callID, payload, headers...)
	}
	mode := a.lookupCallMode(targetID, callID)
	headers = a.injectCallerRole(caller.ID(), headers)
	return invoke.NewCall(mode, a.invoke(caller.ID(), targetID, callID, payload, headers...), invoke.WithFinalTimeout(a.cfg.defaultInvokeTimeout))
}

// SpawnGatewaySession spawns an empty Host cell under root. The per-cell
// pending table is reserved by the tree allocator's Reserve phase
// (reserveSpawn), so calls issued through the returned ref are isolated
// from the moment Spawn returns.
func (a *appImpl) SpawnGatewaySession(name string) (ref.Ref, error) {
	return a.tree.Spawn(a.rootRef, actor.PropsFromFunc(func() actor.Actor { return &actor.Host{} }), name)
}

// InvokeAsCaller is the gateway-facing wrapper around invokeAs: same
// call-mode lookup, caller-role injection and default Final timeout as
// the planner path.
func (a *appImpl) InvokeAsCaller(caller, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call {
	return a.invokeAs(caller, target, callID, payload, headers...)
}

// DestroyGatewaySession removes the session cell from the tree. The
// terminate path delivers Destroy to the cell, whose handleDestroy
// flushes the pending table (terminal errors to every in-flight call)
// before the registry entry is dropped.
func (a *appImpl) DestroyGatewaySession(r ref.Ref) error {
	return a.tree.Destroy(r)
}

// allocate is the Tree Allocator closure. Creates a Cell for a new actor.
// reserveSpawn allocates the identity for a new child without
// constructing it. Wired as the tree Allocator's Reserve phase: it runs
// under the tree write lock and must not block.
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
	childInitialized := make(chan struct{})
	childStarted := make(chan struct{})
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
		Self:            childRef,
		Parent:          parent,
		Actor:           props.Factory()(),
		Props:           props,
		Supervisor:      a.defSup,
		Handlers:        childHandlers,
		StrictHandlers:  a.cfg.strictHandlers,
		Deliver:         a.deliver,
		RouteReply:      childPT.Deliver,
		Events:          a.eventsStore,
		Projections:     a.projStore,
		PolicyStore:     a.policyStore,
		ServiceRegistry: a.svcReg,
		PlannerInvoke: func(target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call {
			return a.invokeAs(childRef, target, callID, payload, headers...)
		},
		DiscoveryProvider:         a.cfg.discoveryProvider,
		LookupService:             a.LookupService,
		ExposeService:             func(name string) error { return a.exposeService(childRef, name) },
		UnexposeService:           func(name string) { a.unexposeService(childRef, name) },
		ExposeServiceToChildren:   func(name string) error { return a.exposeServiceToChildren(childRef, name) },
		UnexposeServiceToChildren: func(name string) { a.unexposeServiceToChildren(childRef, name) },
		UnexposeAllScopedServices: func() { a.unexposeAllScopedServices(childRef) },
		LookupScopedService:       func(name string) (ref.Ref, bool) { return a.lookupScopedService(childRef, name) },
		Resources:                 a.resReg,
		Namespace:                 a.namespace,
		Codec:                     a.codec,
		Schemas:                   a.schemas,
		RootRef:                   a.rootRef,
		Clock:                     a.cfg.clock,
		Spawn:                     a.tree.Spawn,
		Stop:                      a.tree.Stop,
		Destroy:                   a.tree.Destroy,
		LookupID:                  a.tree.LookupID,
		InvokeTable:               childPT,
		CorIDGen:                  a.corIDGen,
		IDGen:                     a.gen,
		OnInitCompleted:           func() { close(childInitialized) },
		OnStartCompleted:          func() { close(childStarted) },
		EventBus:                  a.eventBus,
		Logger:                    a.cfg.logger,
		LogHook:                   a.cfg.logHook,
	})

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
	select {
	case <-childInitialized:
	case <-childCell.Done():
		return nil
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
func (a *appImpl) injectCallerRole(caller id.ActorID, headers []map[string]string) []map[string]string {
	for _, h := range headers {
		if h != nil {
			if _, ok := h["gospore.caller_role"]; ok {
				return headers
			}
		}
	}
	if c := a.getCell(caller); c != nil {
		if role := c.Role(); role != "" {
			return append(headers, map[string]string{"gospore.caller_role": role})
		}
	}
	return headers
}

// invokeTableFor returns the pending table that correlates replies for calls
// issued by caller. Every spawned actor owns one (reserved at spawn); the
// root entry aliases the App table, which also homes gateway/App-originated
// calls. Unknown callers fall back to the App table so a call can never
// register into a table whose reply routing cannot reach it.
func (a *appImpl) invokeTableFor(caller id.ActorID) *invoke.PendingTable {
	a.invokeTableMu.RLock()
	pt, ok := a.invokeTables[caller]
	a.invokeTableMu.RUnlock()
	if ok {
		return pt
	}
	return a.pending
}

func (a *appImpl) setInvokeTable(aid id.ActorID, pt *invoke.PendingTable) {
	a.invokeTableMu.Lock()
	a.invokeTables[aid] = pt
	a.invokeTableMu.Unlock()
}

func (a *appImpl) removeInvokeTable(aid id.ActorID) {
	if aid == a.rootRef.ID() {
		return
	}
	a.invokeTableMu.Lock()
	delete(a.invokeTables, aid)
	a.invokeTableMu.Unlock()
}

// invoke sends a Call frame to target and returns a Stream backed by
// the pending table. Caller and Sender are resolved through the Tree.
//
// Encode path: []byte payloads pass through unchanged; non-[]byte
// payloads are encoded via the App's codec using the target callable's
// parameter TypeDesc. If the descriptor is not yet available (target
// actor has not finished OnStart) the body falls back to nil.
// Optional headers maps are merged into Frame.Headers left-to-right.
func (a *appImpl) invoke(caller, target id.ActorID, callID string, payload any, headers ...map[string]string) invoke.Stream {
	// Reply frames are addressed To: caller and land in the caller's own
	// reply pipeline, so the slot must live in the caller's pending table.
	pt := a.invokeTableFor(caller)
	var body []byte
	payloadMode := message.PayloadModeValue
	encoding := message.EncodingNone
	var schemaID uint64
	encoded := false
	if b, ok := payload.([]byte); ok {
		desc, reqSchemaID, descFound := a.lookupCallableMeta(target, callID)
		if descFound && len(desc.Parameters) > 0 && codec.IsTBCData(b) {
			// Binary (TBC) payload — pass through as Raw with Binary encoding so
			// the receiving cell can decode it via the multi-codec dispatch path.
			body = b
			payloadMode = message.PayloadModeRaw
			encoding = message.EncodingBinary
			schemaID = reqSchemaID
			encoded = true
		} else {
			body = b
			schemaID = schema.BuiltinBytes
		}
	} else if s, ok := payload.(string); ok {
		body = []byte(s)
		schemaID = schema.BuiltinString
	} else if payload != nil && a.codec != nil {
		desc, requestSchemaID, descFound := a.lookupCallableMeta(target, callID)
		if descFound && len(desc.Parameters) > 0 {
			var err error
			body, err = a.codec.Encode(desc.Parameters[0].Type, payload)
			if err != nil {
				body = nil
			} else {
				encoded = true
				payloadMode = message.PayloadModeRaw
				encoding = a.codec.Encoding()
				schemaID = requestSchemaID
			}
		}
	}

	corID := uint64(a.corIDGen.Next())
	ch := make(chan message.Frame, 16)
	pt.Register(corID, callID, string(a.lookupCallMode(target, callID)), ch)

	callerRef, _ := a.tree.LookupID(caller)
	targetRef, _ := a.tree.LookupID(target)

	hdr := make(map[string]string)
	for _, h := range headers {
		for k, v := range h {
			hdr[k] = v
		}
	}

	envPayload := payload
	if encoded {
		// Body is canonical when the codec successfully encoded a non-[]byte
		// payload — clearing envPayload forces the receiving cell to decode
		// from Body via buildArgs instead of trying to assign the raw Go
		// value (e.g. map[string]any from a JSON gateway) directly to a
		// typed handler param.
		envPayload = nil
	}

	env := mailbox.Envelope{
		Frame: message.Frame{
			From:        caller,
			To:          target,
			Kind:        message.KindCall,
			CorID:       corID,
			CallID:      callID,
			SchemaID:    schemaID,
			PayloadMode: payloadMode,
			Encoding:    encoding,
			Body:        body,
			Headers:     hdr,
		},
		Sender:  callerRef,
		Payload: envPayload,
	}

	if targetRef != nil {
		if err := a.deliver(targetRef, env); err != nil {
			pt.SendFailed(corID)
			return invoke.NewErrorStream(err)
		}
	} else if a.remoteTP != nil {
		if err := a.remoteTP.Send(env.Frame); err != nil {
			pt.SendFailed(corID)
			return invoke.NewErrorStream(err)
		}
	} else {
		pt.SendFailed(corID)
		return invoke.NewErrorStream(fmt.Errorf("target not found"))
	}

	stream := invoke.NewStreamWithTypedDecode(ch, corID, pt, a.codec, a.lookupTypedDecode(target, callID))
	wrapped := &cancellingStream{
		Stream: stream,
		sendCancel: func() error {
			cancelFrame := message.Frame{
				From:   caller,
				To:     target,
				Kind:   message.KindCancel,
				CorID:  corID,
				CallID: callID,
			}
			if targetRef != nil {
				return a.deliver(targetRef, mailbox.Envelope{Frame: cancelFrame})
			} else if a.remoteTP != nil {
				return a.remoteTP.Send(cancelFrame)
			}
			return nil
		},
	}
	return wrapped
}

// lookupCallableMeta resolves the registered descriptor and request schema ID
// for callID on the target actor. Returns (zero, 0, false) when unavailable.
func (a *appImpl) lookupCallableMeta(target id.ActorID, callID string) (tschema.CallableDesc, uint64, bool) {
	targetCell := a.getCell(target)
	if targetCell == nil {
		return tschema.CallableDesc{}, 0, false
	}
	if ht := targetCell.Handlers(); ht != nil {
		desc, ok := ht.Desc(callID)
		if !ok {
			return tschema.CallableDesc{}, 0, false
		}
		inv, _ := ht.Lookup(callID)
		if inv == nil {
			return desc, 0, true
		}
		return desc, inv.RequestSchemaID, true
	}
	return tschema.CallableDesc{}, 0, false
}

// lookupCallableDesc resolves the schema descriptor for callID on the
// target actor. Returns (zero, false) when the cell or descriptor is
// not found — callers must handle the fallback themselves.
func (a *appImpl) lookupCallableDesc(target id.ActorID, callID string) (tschema.CallableDesc, bool) {
	desc, _, ok := a.lookupCallableMeta(target, callID)
	return desc, ok
}

// lookupTypedDecode returns a hook that decodes raw replies for callID on
// target into the concrete Go type derived from the handler (streaming
// ChunkType or the unary first return value). Returns nil when the type
// cannot be determined — Recv then decodes via the generic codec path.
//
// The reply type is discovered at runtime via reflection (lookupReplyType),
// so the hook allocates its decode target with reflect.New per frame;
// callers that know the reply type at compile time should build their
// stream with invoke.NewStreamAs, which decodes through codec.DecodeAs
// without any reflection.
func (a *appImpl) lookupTypedDecode(target id.ActorID, callID string) invoke.TypedDecode {
	replyType := a.lookupReplyType(target, callID)
	if replyType == nil {
		return nil
	}
	return func(schemaID uint64, encoding message.Encoding, body []byte) (any, bool) {
		out := reflect.New(replyType)
		if err := a.codec.DecodeByIDInto(schemaID, encoding, body, out.Interface()); err != nil {
			return nil, false
		}
		return out.Elem().Interface(), true
	}
}

// lookupReplyType returns the concrete Go type the caller should expect
// from reply frames for callID on target. For streaming callables this is
// ChunkType; for unary callables it derives from the handler's first return
// value. Returns nil when the type cannot be determined (remote target,
// script-backed handler, or built-in scalar).
func (a *appImpl) lookupReplyType(target id.ActorID, callID string) reflect.Type {
	targetCell := a.getCell(target)
	if targetCell == nil {
		return nil
	}
	ht := targetCell.Handlers()
	if ht == nil {
		return nil
	}
	inv, ok := ht.Lookup(callID)
	if !ok || inv == nil {
		return nil
	}
	// Streaming: the chunk type is definitive.
	if inv.ChunkType != nil {
		return inv.ChunkType
	}
	// Unary: derive from the handler function's first return value.
	if inv.Fn != nil {
		fnType := reflect.TypeOf(inv.Fn)
		if fnType.Kind() == reflect.Func && fnType.NumOut() > 0 {
			out := fnType.Out(0)
			// If the first return is error and there are 2 returns, the
			// value return is actually the second one (unusual but possible).
			if fnType.NumOut() == 2 && out == errorType {
				out = fnType.Out(1)
			}
			if out.Kind() == reflect.Struct {
				return out
			}
			if out.Kind() == reflect.Pointer && out.Elem().Kind() == reflect.Struct {
				return out.Elem()
			}
		}
	}
	return nil
}

// lookupCallMode resolves the CallMode for a target callable based on
// its registered descriptor. Falls back to CallModeUnary when the
// descriptor is not yet available.
func (a *appImpl) lookupCallMode(target id.ActorID, callID string) invoke.CallMode {
	desc, ok := a.lookupCallableDesc(target, callID)
	if !ok {
		return invoke.CallModeUnary
	}
	switch desc.Mode {
	case tschema.CallableModeStreaming:
		return invoke.CallModeStream
	default:
		// Tell: no returns and no error → fire-and-forget.
		if len(desc.Returns) == 0 && !desc.HasError {
			return invoke.CallModeTell
		}
		return invoke.CallModeUnary
	}
}

// appRef is the concrete ref.Ref implementation produced by App.
type appRef struct {
	aid    id.ActorID
	invoke func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call
}

func (r *appRef) ID() id.ActorID          { return r.aid }
func (r *appRef) Service() (string, bool) { return "", false }
func (r *appRef) Invoke(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
	if r.invoke != nil {
		return r.invoke(ctx, callID, payload, headers...)
	}
	return nil
}

var _ ref.Ref = (*appRef)(nil)

// Option configures App construction.
type Option func(*config)

// config is the internal aggregation of Option values.
type config struct {
	namespace               string
	rootActor               func() actor.Actor
	codec                   codec.Codec
	runtimeSlot             uint16
	rootID                  id.ActorID
	userMailboxCapacity     int
	defaultSupervisor       supervisor.Supervisor
	importedSchemas         []schema.Set
	manifestJSON            []byte
	resources               []func(resource.Registry) error
	clock                   actor.Clock
	discoveryProvider       discovery.Provider
	discoveryAddress        string
	remoteTransport         transport.Transport
	projectionFingerprint   projection.FingerprintMode
	projectionDeltaWindow   int
	eventRingCapacity       int
	defaultMaxPureInflight  int
	defaultStreamBufferSize int
	terminateTimeout        time.Duration
	// defaultInvokeTimeout is the fallback deadline Call.Final applies when
	// the caller context has no deadline. Wired into every NewCall produced
	// by this App so cross-owner-loop synchronous invokes cannot block
	// forever. Zero or negative disables.
	defaultInvokeTimeout time.Duration
	gatewayAddr          string
	gatewayExtraAddrs    []string
	gatewayInterceptor   gateway.GatewayInterceptor
	urlAuth              gateway.URLAuth
	logProvider          gateway.LogProvider
	extraRoutes          func(mux *http.ServeMux)
	staticFS             fs.FS
	logger               actor.Logger
	logHook              actor.LogHook
	strictHandlers       bool
	binaryOnly           bool
	// childStartOrder, when non-nil, lists actor names in the order they
	// should receive Start messages during Phase 2. When nil all cells
	// start in parallel (backward-compatible default).
	childStartOrder []string
}

// WithNamespace sets the App's namespace. Required. Must match
// `^[a-z][a-z0-9_]*$` and not equal `app` or `_gospore_`.
//
// The format check runs in app.New, not here — Option helpers only
// capture arguments into the config struct.
func WithNamespace(ns string) Option {
	return func(c *config) { c.namespace = ns }
}

// WithRootActor injects the user's root actor factory. The factory
// runs after the gospore-internal `app.*` registration step in the
// root OnStart chain. Optional — omitted means an empty root.
//
// The default-empty-root resolution lives in app.New.
func WithRootActor(factory func() actor.Actor) Option {
	return func(c *config) { c.rootActor = factory }
}

// WithRootID pre-assigns a stable ActorID to the root actor. When zero
// (default), the ID generator produces one. Use this when the root actor
// must survive process restarts with a deterministic identity.
func WithRootID(id id.ActorID) Option {
	return func(c *config) { c.rootID = id }
}

// WithCodec overrides the default codec backend.
func WithCodec(c codec.Codec) Option {
	return func(cfg *config) { cfg.codec = c }
}

// WithRemoteTransport injects a cross-process Transport. When set,
// the App routes frames for unknown local targets through this
// transport and starts a receive loop to accept inbound frames.
// Phase 1 defaults to nil (single-process only).
func WithRemoteTransport(tp transport.Transport) Option {
	return func(c *config) { c.remoteTransport = tp }
}

// WithRuntimeSlot fixes this App's RuntimeSlot for ID generation.
// Defaults to 0. In multi-App / multi-process deployments each
// participant must hold a distinct slot to keep ActorIDs globally
// unique.
func WithRuntimeSlot(slot uint16) Option {
	return func(c *config) { c.runtimeSlot = slot }
}

// WithUserMailboxCapacity overrides the per-actor user-lane mailbox
// capacity. Used as the default for actors that do not override it
// via Props.
func WithUserMailboxCapacity(n int) Option {
	return func(c *config) { c.userMailboxCapacity = n }
}

// WithDefaultSupervisor installs the default supervisor used when an
// actor's Props does not specify one. gospore's built-in default is
// supervisor.NewOneForOne(10, time.Minute).
func WithDefaultSupervisor(s supervisor.Supervisor) Option {
	return func(c *config) { c.defaultSupervisor = s }
}

// WithImportedSchemas Imports the given foreign Sets into the App's
// SchemaSet at startup. Useful for wiring cross-App callable types
// before any handler runs.
func WithImportedSchemas(others ...schema.Set) Option {
	return func(c *config) {
		c.importedSchemas = append(c.importedSchemas, others...)
	}
}

// WithManifestImport parses a spore manifest JSON and imports every
// schema into the App's SchemaSet before handlers register callables.
// Pre-registering schemas with manifest IDs ensures RegisterAuto returns
// the manifest ID instead of auto-allocating a new one.
func WithManifestImport(manifestJSON []byte) Option {
	return func(c *config) {
		c.manifestJSON = manifestJSON
	}
}

// WithResource pre-populates the App's resource registry. Equivalent
// to calling resource.Set(reg, key, value) inside RootActor.OnStart.
func WithResource[T any](key resource.Key[T], value T) Option {
	return func(c *config) {
		c.resources = append(c.resources, func(r resource.Registry) error {
			return resource.Set(r, key, value)
		})
	}
}

// WithClock injects a custom Clock. Tests substitute a fake clock to
// keep time-dependent assertions deterministic.
func WithClock(c actor.Clock) Option {
	return func(cfg *config) { cfg.clock = c }
}

// WithDiscoveryProvider installs the cross-App discovery provider.
// Default is in-memory (sufficient for single-process Phase 1).
// Phase 2 deployments substitute gossip / K8s DNS / consul without
// changing the App surface.
func WithDiscoveryProvider(p discovery.Provider) Option {
	return func(c *config) { c.discoveryProvider = p }
}

// WithDiscoveryAddress sets the network address advertised in
// discovery registrations. When set, ctx.Expose includes this
// address in the Instance record so remote peers can route
// frames to this App.
func WithDiscoveryAddress(addr string) Option {
	return func(c *config) { c.discoveryAddress = addr }
}

// WithProjectionFingerprint switches the projection fingerprint
// algorithm. Default is FingerprintBytes.
func WithProjectionFingerprint(mode projection.FingerprintMode) Option {
	return func(c *config) { c.projectionFingerprint = mode }
}

// WithProjectionDeltaWindow sets the delta ring capacity per actor.
// Default is 64. Subscribers reconnecting with `since` older than
// the ring receive gap_too_large.
func WithProjectionDeltaWindow(n int) Option {
	return func(c *config) { c.projectionDeltaWindow = n }
}

// WithEventRingCapacity sets the per-actor event ring capacity.
// Default is 256.
func WithEventRingCapacity(n int) Option {
	return func(c *config) { c.eventRingCapacity = n }
}

// WithDefaultMaxPureInflight sets the default max in-flight count
// for stateless handlers. Default 0 = unlimited. Per-actor Props
// can override.
func WithDefaultMaxPureInflight(n int) Option {
	return func(c *config) { c.defaultMaxPureInflight = n }
}

// WithDefaultStreamBufferSize sets the default streaming-channel
// buffer size for Emitter / Subscription. Default 16. Per-actor
// Props can override.
func WithDefaultStreamBufferSize(n int) Option {
	return func(c *config) { c.defaultStreamBufferSize = n }
}

// WithTerminateTimeout overrides how long the App's terminate path
// waits for a child Cell to drain after pushing Stop on its system
// lane. Default is 5 seconds. Pass a smaller value in unit tests to
// keep teardown snappy; pass a larger one for actors with heavy
// OnStop work (database commits, on-disk flushes). Zero or negative
// values fall back to the 5 s default.
// WithDefaultInvokeTimeout sets the fallback deadline Call.Final applies
// when the context passed to Final carries no deadline of its own. Default
// is 30 seconds. Pass zero or a negative value to disable the fallback
// (Final then blocks until reply, cancellation, or caller ctx death).
func WithDefaultInvokeTimeout(d time.Duration) Option {
	return func(c *config) { c.defaultInvokeTimeout = d }
}

func WithTerminateTimeout(d time.Duration) Option {
	return func(c *config) { c.terminateTimeout = d }
}

// WithGatewayHTTP installs an external-facing HTTP gateway on the App.
// addr is the listen address (e.g. ":8080"); empty binds an ephemeral
// local port. interceptor is invoked on every inbound request; nil
// means no-op.
//
// The gateway starts after the root actor's OnStart completes so that
// all service registrations are visible. It shares the App's context
// for coordinated shutdown.
func WithGatewayHTTP(addr string, interceptor gateway.GatewayInterceptor) Option {
	return func(c *config) {
		c.gatewayAddr = addr
		if interceptor == nil {
			c.gatewayInterceptor = gateway.Nop()
		} else {
			c.gatewayInterceptor = interceptor
		}
	}
}

// WithGatewayExtraAddrs configures additional listen addresses for the HTTP
// gateway. Each address gets its own listener sharing the same handler,
// enabling dual binding (e.g. 127.0.0.1 + LAN IP) without exposing 0.0.0.0.
func WithGatewayExtraAddrs(addrs []string) Option {
	return func(c *config) {
		c.gatewayExtraAddrs = addrs
	}
}

// WithURLAuth installs a per-connection authentication hook on the HTTP
// gateway. Called once per WebSocket connection to extract role and subject
// from URL query parameters.
func WithURLAuth(fn gateway.URLAuth) Option {
	return func(c *config) { c.urlAuth = fn }
}

// WithLogProvider installs a structured log provider for the GET /logs endpoint
// on the HTTP gateway. When nil the /logs endpoint returns 503.
func WithLogProvider(p gateway.LogProvider) Option {
	return func(c *config) { c.logProvider = p }
}

// WithExtraRoutes installs a callback that adds custom HTTP routes to the
// gateway's ServeMux. The callback receives the mux after standard routes
// are registered. When nil or when no gateway is configured, this is a no-op.
func WithExtraRoutes(fn func(mux *http.ServeMux)) Option {
	return func(c *config) { c.extraRoutes = fn }
}

// WithStaticFS installs a static filesystem on the HTTP gateway.
// Files are served from its embedded `dist` directory when the gateway
// is enabled; unknown non-API paths fall back to `dist/index.html`.
func WithStaticFS(fsys fs.FS) Option {
	return func(c *config) { c.staticFS = fsys }
}

// WithStableActorID binds a stable ActorID to a path. When the Tree
// allocates a child at that path, it uses the given ActorID instead of
// generating a fresh one. This keeps well-known actors (root children)
// stable across process restarts.

// WithLogger sets the structured logger used by all actor contexts.
// When nil, a default runtime.Caller-based logger is used.
func WithLogger(l actor.Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithLogHook registers a callback that receives every log entry the
// runtime produces.  The hook is called after the runtime has resolved
// the caller at the correct stack depth, so the caller string is always
// accurate.  When a hook is present the default logger skips stdout and
// forwards the entry to the hook instead.
func WithLogHook(hook actor.LogHook) Option {
	return func(c *config) { c.logHook = hook }
}

// WithStrictHandlers enables strict callable signature validation. When
// true, every registered Go handler must follow the single-struct wire
// rule: after the injected Context, exactly zero or one struct parameter
// (plus an optional Emitter for streaming), and any non-error return must
// be exactly one struct. This makes generated clients and peer protocols
// unambiguous.
func WithStrictHandlers(strict bool) Option {
	return func(c *config) { c.strictHandlers = strict }
}

// WithBinaryOnly forces the WebSocket gateway to reject text frames and
// JSON-encoded struct payloads. All struct request/response payloads must
// be TBC-encoded with a known schema.
func WithBinaryOnly(v bool) Option {
	return func(c *config) { c.binaryOnly = v }
}

// WithChildStartOrder sets the order in which child actors receive Start
// messages during Phase 2 (the OnStart broadcast). When nil (the default),
// all cells receive Start in parallel — the original behavior. The caller
// is responsible for validating the order against the actual child set; an
// unknown name is silently ignored.
func WithChildStartOrder(order []string) Option {
	return func(c *config) {
		if len(order) > 0 {
			c.childStartOrder = append([]string(nil), order...)
		}
	}
}
