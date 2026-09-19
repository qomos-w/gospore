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
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/gateway"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/internal/handler"

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

	// GatewayReady returns a channel that is closed once the HTTP
	// gateway is listening. nil when no gateway is configured.
	// Must not be called before Run.
	GatewayReady() <-chan struct{}

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
		srv.SetWSTimeouts(cfg.gatewayWSTimeouts)
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

func (a *appImpl) Namespace() string            { return a.namespace }
func (a *appImpl) Self() ref.Ref                { return a.rootRef }
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

func (a *appImpl) Codec() codec.Codec             { return a.codec }
func (a *appImpl) Schemas() schema.Set            { return a.schemas }
func (a *appImpl) Tree() tree.Tree                { return a.tree }
func (a *appImpl) Service() service.Registry      { return a.svcReg }
func (a *appImpl) Resources() resource.Registry   { return a.resReg }
func (a *appImpl) Projections() projection.Store  { return a.projStore }
func (a *appImpl) Events() events.Store           { return a.eventsStore }
func (a *appImpl) PolicyStore() actor.PolicyStore { return a.policyStore }
func (a *appImpl) ManifestJSON() []byte           { return a.cfg.manifestJSON }
func (a *appImpl) Type() string                   { return "app" }
func (a *appImpl) OnInit(actor.Context) error     { return nil }
func (a *appImpl) OnStart(actor.Context) error    { return nil }
func (a *appImpl) OnStop(actor.Context) error     { return nil }

// getCell returns the Cell for an actor ID, or nil if not found.
