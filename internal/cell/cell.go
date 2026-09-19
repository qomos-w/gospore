// Package cell implements the per-actor runtime: one goroutine that owns
// the actor's state, reads from its Mailbox, and dispatches inbound Calls.
// Users never import this package — it is internal to the gospore runtime.
package cell

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/discovery"
	"github.com/qomos-w/gospore/events"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/resource"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/gospore/service"
	"github.com/qomos-w/gospore/supervisor"

	"github.com/qomos-w/gospore/internal/handler"

	"github.com/qomos-w/spore/script"
)

// eventMetaEntry stores the reflect type, visibility, and routing metadata for an event kind.
type eventMetaEntry struct {
	Type       reflect.Type
	Visibility actor.Visibility
	Mode       actor.HandlerMode
	Loop       string
}

// timerMetaEntry stores routing metadata for delayed self-calls issued via After.
type timerMetaEntry struct {
	Mode actor.HandlerMode
	Loop string
}

type loopLane struct {
	mode   actor.HandlerMode
	q      chan mailbox.Envelope
	closed bool
	wg     sync.WaitGroup
}

// Cell is the per-actor runtime context. Stateful handlers run on the
// ownerLoop (single goroutine) so actor state needs no locks. Stateless
// handlers run in their own goroutine (tracked by pureWG).
//
// Construction via New(Config). The caller starts the goroutine; the Cell
// runs until ctx is cancelled.
type Cell struct {
	self                      ref.Ref
	parent                    ref.Ref
	actor                     actor.Actor
	props                     actor.Props
	role                      string // default caller role (from Props.Role)
	supervisor                supervisor.Supervisor
	handlers                  *handler.Table
	strictHandlers            bool
	children                  *childSet
	watchers                  *watcherSet
	services                  []string // global service names registered by this actor (for auto-unregister on Stop)
	childServices             []string // scoped service names registered by this actor (for auto-unregister on Stop)
	domains                   []string // domain names declared via RegisterDomain (namespace tracking)
	pureWG                    sync.WaitGroup
	state                     atomic.Int32
	draining                  atomic.Bool
	done                      chan struct{}
	svcReg                    service.Registry
	discoveryProvider         discovery.Provider
	discoveryAddress          string
	lookupService             func(string) (ref.Ref, bool)
	exposeService             func(name string) error
	unexposeService           func(name string)
	exposeServiceToChildren   func(name string) error
	unexposeServiceToChildren func(name string)
	unexposeAllScopedServices func()
	lookupScopedService       func(name string) (ref.Ref, bool)
	resReg                    resource.Registry
	namespace                 string
	codec                     codec.Codec
	schemas                   schema.Reader
	rootRef                   ref.Ref
	clock                     actor.Clock
	monitor                   Monitor
	events                    *events.StoreImpl
	projections               *projection.StoreImpl
	policyStore               actor.PolicyStore
	deliver                   func(ref.Ref, mailbox.Envelope) error
	shutdown                  func(reason error)
	routeReply                func(message.Frame) bool
	projectionVersion         uint64
	projectionState           projection.FieldSnapshot
	projectionMu              sync.Mutex                 // serializes applyProjectionIfObserved across owner/system/pure lanes
	componentSlots            []projection.ComponentSlot // scanned from `gospore:"component"` tags
	componentSlotsMu          sync.RWMutex               // guards componentSlots writes (system lane) vs external readers
	eventMeta                 map[string]eventMetaEntry  // kind → metadata, collected via RegisterEventKind
	eventMetaMu               sync.RWMutex               // protects eventMeta reads from external goroutines
	timerMeta                 map[string]timerMetaEntry  // callID → delayed self-call routing metadata
	timerMetaMu               sync.RWMutex               // protects timerMeta reads from external goroutines
	eventBus                  *EventBus                  // App-scoped user-event broadcast bus; nil disables EmitEvent
	scriptRuntime             *scriptRuntimeOwner
	lifecycleCtx              context.Context
	cancelLifecycle           context.CancelFunc
	runCancel                 context.CancelFunc // cancels Cell.Run's ctx so it exits during Destroy
	spawn                     func(parent ref.Ref, props actor.Props, name string) (ref.Ref, error)
	plannerInvoke             func(target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call
	stop                      func(target ref.Ref) error
	destroy                   func(target ref.Ref) error
	lookupID                  func(id.ActorID) (ref.Ref, bool)
	invokeTable               *invoke.PendingTable
	corIDGen                  *id.CorIDGenerator
	idGen                     *id.Canonical
	onStartCompleted          func()
	onInitCompleted           func()

	replyReg *replyRegistry
	logger   actor.Logger
	logHook  actor.LogHook

	drainMu sync.Mutex
	loopsMu sync.Mutex
	loops   map[string]*loopLane

	ownerQ       chan mailbox.Envelope
	ownerQMu     sync.Mutex
	ownerQClosed bool
	ownerWG      sync.WaitGroup
	// ownerWater tracks the last-logged high-water level (0 none, 1 ≥75%,
	// 2 ≥90%) of the owner queue so saturation is visible in logs BEFORE the
	// queue actually overflows — a slow-jammed owner lane otherwise only
	// shows up as unexplained dispatch latency.
	ownerWater    atomic.Int32
	systemQ       chan mailbox.Envelope
	systemQMu     sync.Mutex
	systemQClosed bool
	systemWG      sync.WaitGroup
	replyQ        chan mailbox.Envelope
	replyQMu      sync.Mutex
	replyQClosed  bool
	replyWG       sync.WaitGroup

	// laneStats holds per-lane observability records (see lane_metrics.go):
	// queue-depth high-water, busy windows for single-goroutine lanes, and
	// bounded rings of recent (callID, waitNs, execNs) handler executions
	// for the owner/system/reply/pure and custom lanes. Entries are created
	// lazily on first activity and guarded by lanesMu.
	laneStats map[string]*laneMetrics
	lanesMu   sync.RWMutex
}

// Config holds the parameters for constructing a Cell. Every field is
// required (non-nil / non-zero) unless explicitly documented as optional.
type Config struct {
	// Self is the actor's own Ref.
	Self ref.Ref
	// Parent is the parent actor's Ref, or nil for root.
	Parent ref.Ref
	// Actor is the user-defined handler instance.
	Actor actor.Actor
	// Props carries the factory and event visibility for this actor.
	Props actor.Props
	// Supervisor decides how to handle child failures.
	Supervisor supervisor.Supervisor
	// Handlers is the per-cell callable table (may be empty at construction
	// time; Register populates it during OnStart).
	Handlers *handler.Table
	// StrictHandlers mirrors the App-level strict handler signature policy.
	// When true, handler tables recreated during cell restart are also strict.
	StrictHandlers bool
	// ServiceRegistry is used to unregister services on Stop. Optional;
	// when nil, services are only cleared from the Cell's internal list.
	ServiceRegistry service.Registry
	// DiscoveryProvider is the cross-App service discovery backend.
	// Optional; when nil, Expose does not publish to discovery and
	// LookupService does not fall back to remote lookup.
	DiscoveryProvider discovery.Provider
	// DiscoveryAddress is the network endpoint advertised in discovery
	// registrations. Optional; when empty, Instance records have no Address.
	DiscoveryAddress string
	// LookupService resolves a service name to a Ref, including cross-App
	// fallback via the discovery provider. Optional; when nil,
	// ctx.LookupService only checks the local ServiceRegistry.
	LookupService func(string) (ref.Ref, bool)
	// ExposeService registers a global service on behalf of this actor.
	// Optional; when nil, ctx.Expose falls back to ServiceRegistry.Register.
	ExposeService func(name string) error
	// UnexposeService unregisters a global service on behalf of this actor.
	// Optional; when nil, clearServices falls back to ServiceRegistry.Unregister.
	UnexposeService func(name string)
	// ExposeServiceToChildren registers a scoped service visible only to this
	// actor's descendants. Optional; when nil, ctx.ExposeToChildren returns an error.
	ExposeServiceToChildren func(name string) error
	// UnexposeServiceToChildren unregisters a single scoped service.
	// Optional; when nil, clearServices skips scoped cleanup.
	UnexposeServiceToChildren func(name string)
	// UnexposeAllScopedServices unregisters all scoped services for this actor.
	// Optional; when nil, clearServices skips scoped cleanup.
	UnexposeAllScopedServices func()
	// LookupScopedService resolves a scoped service visible to this actor.
	// Optional; when nil, ctx.LookupService never resolves scoped services.
	LookupScopedService func(name string) (ref.Ref, bool)
	// Monitor is an optional observability hook for handler invocations.
	// When nil, no observations are emitted.
	Monitor Monitor
	// Deliver sends an envelope to the target actor's mailbox. When nil,
	// watch notifications and cross-actor deliveries are silently dropped.
	// The App/Tree layer provides this closure by capturing the Local
	// transport or equivalent delivery mechanism.
	Deliver func(target ref.Ref, env mailbox.Envelope) error
	// Shutdown is called when this Cell (as root) receives an Escalated
	// notification that its own supervisor escalates. Optional; when nil,
	// root escalation is silently observed.
	Shutdown func(reason error)
	// RouteReply routes a response frame (Reply/Error/End) to the
	// waiting invoke.Stream on the caller side. Optional; when nil,
	// response frames are silently dropped. The App layer wires this
	// to invoke.PendingTable.Deliver.
	RouteReply func(frame message.Frame) bool
	// Events is the events store for lifecycle and callable event emission.
	// Optional; when nil, no events are emitted. When non-nil, the Cell
	// checks HasSubscribers before Record creation.
	Events *events.StoreImpl
	// Projections is the state-channel store. Optional; when nil, projection
	// snapshots are not published and projection Tier-0 gating is disabled.
	Projections *projection.StoreImpl
	// PolicyStore is the App-level permission evaluator. Optional; when nil,
	// script-side policy checks return deny-all.
	PolicyStore actor.PolicyStore
	// Resources is the App-level resource registry. Optional; when nil,
	// ctx.Resources() returns nil.
	Resources resource.Registry
	// Namespace is the App's namespace identifier. Optional; when empty,
	// ctx.Namespace() returns "".
	Namespace string
	// Codec is the App's wire codec. Optional; when nil, ctx.Codec() returns nil.
	Codec codec.Codec
	// Schemas is the App's schema registry (read-only face). Optional;
	// when nil, ctx.Schemas() returns nil.
	Schemas schema.Reader
	// RootRef is the App's root actor Ref. Optional; when nil, ctx.Root() returns nil.
	RootRef ref.Ref
	// Clock is the App's time source. Optional; when nil, ctx.Clock() returns
	// a static clock that yields the zero time.
	Clock actor.Clock
	// SporeRuntime is an optional real Spore script runtime. When set,
	// script handler dispatch goes through Spore instead of the fake
	// runtime. Wiring is the App's responsibility — initialise the runtime
	// and load the actor's script module before passing it to Cell construction.
	SporeRuntime *script.Runtime
	// PlannerInvoke issues a call on behalf of this cell's actor with the
	// cell's Self as the message sender, so ctx.Caller() in the target's
	// handlers resolves to the calling actor. Wired by the App layer.
	// Optional; when nil, planner Call/Stream fall back to target.Invoke,
	// whose sender is the target itself (legacy behavior).
	PlannerInvoke func(target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call
	// Spawn creates a child actor under parent. Wired by the App layer to
	// tree.Tree.Spawn. Optional; when nil, ctx.Spawn returns an error.
	Spawn func(parent ref.Ref, props actor.Props, name string) (ref.Ref, error)
	// Stop idles target without removing it from the tree. Wired by the
	// App layer to tree.Tree.Stop. Optional; when nil, ctx.Stop returns nil.
	Stop func(target ref.Ref) error
	// Destroy removes target and its subtree from the tree. Wired by the
	// App layer to tree.Tree.Destroy. Optional; when nil, ctx.Destroy
	// returns nil.
	Destroy func(target ref.Ref) error
	// LookupID resolves an ActorID to a Ref. Wired by the App layer to
	// tree.Tree.LookupID. Optional; when nil, ctx.LookupID returns (nil, false).
	LookupID func(id.ActorID) (ref.Ref, bool)
	// InvokeTable is the App-level pending table for cross-actor calls.
	// Optional; when nil, ctx.Invoke and ctx.InvokeStream return empty/error.
	InvokeTable *invoke.PendingTable
	// CorIDGen is the App-level CorID generator. Required when InvokeTable is set.
	CorIDGen *id.CorIDGenerator
	// IDGen is the App-level canonical 128-bit ID generator exposed to
	// handlers as ctx.NewID. Optional; when nil, ctx.NewID panics. App
	// always wires this; only narrow test fixtures may leave it nil.
	IDGen *id.Canonical
	// OnStartCompleted is called after the actor's OnStart succeeds and
	// WatchStarted has been emitted. Optional.
	OnStartCompleted func()
	// OnInitCompleted is called after the actor's OnInit succeeds.
	// Optional.
	OnInitCompleted func()
	// EventBus is the App-scoped user-event broadcast bus. Optional;
	// when nil, ctx.EmitEvent returns DiagEventNoBus.
	EventBus *EventBus
	// Logger is the structured logger used by this actor's Context.
	// Optional; when nil, a default runtime.Caller-based logger is used.
	Logger actor.Logger
	// LogHook receives structured entries from the default logger.
	// When non-nil the default logger skips stdout and forwards every
	// entry to the hook instead.
	LogHook actor.LogHook
}

// newHandlerTable returns a handler table respecting the cell's strict policy.
func (c *Cell) newHandlerTable() *handler.Table {
	if c == nil {
		return handler.NewTable()
	}
	if c.schemas != nil {
		if reg, ok := c.schemas.(handler.SchemaRegistrar); ok {
			if c.strictHandlers {
				return handler.NewStrictTableWithSchema(reg)
			}
			return handler.NewTableWithSchema(reg)
		}
	}
	if c.strictHandlers {
		return handler.NewStrictTable()
	}
	return handler.NewTable()
}

// New constructs a Cell from Config. The returned Cell is in the "created"
// state — the caller must start the goroutine to enter the running loop.
func New(cfg Config) *Cell {
	c := &Cell{
		self:                      cfg.Self,
		parent:                    cfg.Parent,
		actor:                     cfg.Actor,
		props:                     cfg.Props,
		role:                      cfg.Props.Role(),
		supervisor:                cfg.Supervisor,
		handlers:                  cfg.Handlers,
		strictHandlers:            cfg.StrictHandlers,
		children:                  newChildSet(),
		watchers:                  newWatcherSet(),
		done:                      make(chan struct{}),
		svcReg:                    cfg.ServiceRegistry,
		discoveryProvider:         cfg.DiscoveryProvider,
		discoveryAddress:          cfg.DiscoveryAddress,
		lookupService:             cfg.LookupService,
		exposeService:             cfg.ExposeService,
		unexposeService:           cfg.UnexposeService,
		exposeServiceToChildren:   cfg.ExposeServiceToChildren,
		unexposeServiceToChildren: cfg.UnexposeServiceToChildren,
		unexposeAllScopedServices: cfg.UnexposeAllScopedServices,
		lookupScopedService:       cfg.LookupScopedService,
		resReg:                    cfg.Resources,
		namespace:                 cfg.Namespace,
		codec:                     cfg.Codec,
		schemas:                   cfg.Schemas,
		rootRef:                   cfg.RootRef,
		clock:                     cfg.Clock,
		monitor:                   cfg.Monitor,
		events:                    cfg.Events,
		projections:               cfg.Projections,
		policyStore:               cfg.PolicyStore,
		deliver:                   cfg.Deliver,
		shutdown:                  cfg.Shutdown,
		routeReply:                cfg.RouteReply,
		spawn:                     cfg.Spawn,
		plannerInvoke:             cfg.PlannerInvoke,
		stop:                      cfg.Stop,
		destroy:                   cfg.Destroy,
		lookupID:                  cfg.LookupID,
		invokeTable:               cfg.InvokeTable,
		corIDGen:                  cfg.CorIDGen,
		idGen:                     cfg.IDGen,
		onStartCompleted:          cfg.OnStartCompleted,
		onInitCompleted:           cfg.OnInitCompleted,
		replyReg:                  newReplyRegistry(cfg.RouteReply),
		loops:                     make(map[string]*loopLane),
		laneStats:                 make(map[string]*laneMetrics),
		eventMeta:                 make(map[string]eventMetaEntry),
		timerMeta:                 make(map[string]timerMetaEntry),
		eventBus:                  cfg.EventBus,
		logger:                    cfg.Logger,
		logHook:                   cfg.LogHook,
		// Pre-create ingress queues so Recv() can enqueue messages before
		// openIngressLoops() starts the goroutines. This avoids a race
		// between go cell.Run() and cell.Recv(Init) in the App startup path.
		ownerQ:  make(chan mailbox.Envelope, ownerQueueCapacity),
		systemQ: make(chan mailbox.Envelope, 64),
		replyQ:  make(chan mailbox.Envelope, 64),
	}
	c.scriptRuntime = newScriptRuntimeOwner(cfg.Codec, c)
	return c
}

// Done returns a channel that is closed when the Cell has fully stopped
// (OnStop returned, services unregistered, watchers notified).
func (c *Cell) Done() <-chan struct{} {
	return c.done
}

// IsStarted reports whether the Cell has completed OnStart.
func (c *Cell) IsStarted() bool {
	return c.state.Load() >= cellStarted
}

// IsStopped reports whether the Cell has been stopped (or never started).
func (c *Cell) IsStopped() bool {
	return c.state.Load() >= cellStopped
}

// IsIdle reports whether the Cell is idle (stopped but not yet destroyed).
func (c *Cell) IsIdle() bool {
	return c.state.Load() >= cellIdle && c.state.Load() < cellStopped
}

// cellStateNames maps the lifecycle state constants (cellCreated …
// cellStopped) to human-readable names, indexed by the raw int32 value.
var cellStateNames = [...]string{
	cellCreated:     "created",
	cellInitialized: "initialized",
	cellStarted:     "started",
	cellIdle:        "idle",
	cellStopped:     "stopped",
}

// StateName returns a human-readable name for the Cell's lifecycle state.
func (c *Cell) StateName() string {
	s := c.state.Load()
	if int(s) >= 0 && int(s) < len(cellStateNames) {
		return cellStateNames[s]
	}
	return "unknown"
}

// CellRuntimeStats is a point-in-time, non-blocking snapshot of the Cell's
// ingress queues and pending outbound invocations. All fields are O(1) reads
// (Go's built-in len/cap on channels, PendingTable.Len under a mutex); the
// values are instantaneous and not strongly consistent — suitable for
// diagnostics/observation, not for correctness decisions.
type CellRuntimeStats struct {
	State          string             // created/initialized/started/idle/stopped
	StateCode      int32              // raw state constant
	OwnerQueueLen  int                // current messages waiting in the owner (stateful) lane
	OwnerQueueCap  int                // owner lane capacity (256)
	SystemQueueLen int                // current messages waiting in the system lane
	SystemQueueCap int                // system lane capacity (64)
	ReplyQueueLen  int                // current frames waiting in the reply lane
	ReplyQueueCap  int                // reply lane capacity (64)
	PendingInvokes int                // outbound invocations this actor is awaiting a response for
	InvokeStats    invoke.InvokeStats // this cell's pending-table snapshot (per-actor registrations)
	// Lanes carries per-lane observability (see lane_metrics.go): queue
	// depth/capacity, the historical depth high-water, the current busy
	// window for single-goroutine lanes, and recent (callID, waitNs,
	// execNs) timing records with p50/p99/max summaries. Additive —
	// consumers of the flat fields above are unaffected. Ordered by name.
	Lanes []LaneRuntimeStats
}

// Stats returns a non-blocking runtime snapshot of the Cell's queue depths
// and pending outbound invocations. Safe to call from any goroutine.
func (c *Cell) Stats() CellRuntimeStats {
	pending := 0
	var inv invoke.InvokeStats
	if c.invokeTable != nil {
		pending = c.invokeTable.Len()
		inv = c.invokeTable.Snapshot()
	}
	// Queue fields are swapped under their mutexes (openIngressLoops /
	// drainIngressLoops); read them under the same locks so a concurrent
	// restart cannot race the diagnostic snapshot.
	c.ownerQMu.Lock()
	ownerQ := c.ownerQ
	c.ownerQMu.Unlock()
	c.systemQMu.Lock()
	systemQ := c.systemQ
	c.systemQMu.Unlock()
	c.replyQMu.Lock()
	replyQ := c.replyQ
	c.replyQMu.Unlock()
	return CellRuntimeStats{
		State:          c.StateName(),
		StateCode:      c.state.Load(),
		OwnerQueueLen:  len(ownerQ),
		OwnerQueueCap:  cap(ownerQ),
		SystemQueueLen: len(systemQ),
		SystemQueueCap: cap(systemQ),
		ReplyQueueLen:  len(replyQ),
		ReplyQueueCap:  cap(replyQ),
		PendingInvokes: pending,
		InvokeStats:    inv,
		Lanes:          c.laneSnapshots(),
	}
}

// Self returns the actor's own Ref.
func (c *Cell) Self() ref.Ref {
	return c.self
}

// Actor returns the user-defined handler instance.
func (c *Cell) Actor() actor.Actor {
	return c.actor
}

// Handlers returns the cell's callable table.
func (c *Cell) Handlers() *handler.Table {
	return c.handlers
}

// Role returns the default caller role configured for this actor.
func (c *Cell) Role() string {
	return c.role
}

// AddService records a global service name for auto-unregister on Stop.
func (c *Cell) AddService(name string) {
	c.services = append(c.services, name)
}

// AddChildService records a scoped service name for auto-unregister on Stop.
func (c *Cell) AddChildService(name string) {
	c.childServices = append(c.childServices, name)
}

// AddDomain records a domain name declared via RegisterDomain.
func (c *Cell) AddDomain(name string) {
	c.domains = append(c.domains, name)
}

// Domains returns a snapshot of the domain names declared by this actor.
func (c *Cell) Domains() []string {
	out := make([]string, len(c.domains))
	copy(out, c.domains)
	return out
}

// Services returns a snapshot of the global service names registered by this actor.
func (c *Cell) Services() []string {
	out := make([]string, len(c.services))
	copy(out, c.services)
	return out
}

// ChildServices returns a snapshot of the scoped service names registered by this actor.
func (c *Cell) ChildServices() []string {
	out := make([]string, len(c.childServices))
	copy(out, c.childServices)
	return out
}

// AddChild records a child actor ref. Called during Spawn.
func (c *Cell) AddChild(r ref.Ref) {
	c.children.Add(r)
}

// RemoveChild removes a child actor ref. Called when the child terminates.
func (c *Cell) RemoveChild(r ref.Ref) {
	c.children.Remove(r)
}

// HasChild reports whether the given ref is a live child.
func (c *Cell) HasChild(r ref.Ref) bool {
	return c.children.Has(r)
}

// Children returns a snapshot of all live child refs.
func (c *Cell) Children() []ref.Ref {
	return c.children.Snapshot()
}

// AddWatcher registers a watcher for this Cell's lifecycle events.
func (c *Cell) AddWatcher(r ref.Ref) {
	c.watchers.Add(r)
}

// RemoveWatcher unregisters a watcher.
func (c *Cell) RemoveWatcher(r ref.Ref) {
	c.watchers.Remove(r)
}

// HasWatcher reports whether the given ref is watching this Cell.
func (c *Cell) HasWatcher(r ref.Ref) bool {
	return c.watchers.Has(r)
}

// Watchers returns a snapshot of all watcher refs.
func (c *Cell) Watchers() []ref.Ref {
	return c.watchers.Snapshot()
}

// EventMeta returns a snapshot of the event kinds registered by this actor.
// The map keys are event kind strings; values are the reflect.Types of the
// example values passed to RegisterEventKind. For visibility information,
// use EventMetaEntries.
func (c *Cell) EventMeta() map[string]reflect.Type {
	c.eventMetaMu.RLock()
	out := make(map[string]reflect.Type, len(c.eventMeta))
	for k, v := range c.eventMeta {
		out[k] = v.Type
	}
	c.eventMetaMu.RUnlock()
	return out
}

// EventMetaEntries returns a snapshot of the event kinds with their
// associated metadata (type + visibility). The returned map is a copy;
// callers may mutate it safely.
func (c *Cell) EventMetaEntries() map[string]eventMetaEntry {
	c.eventMetaMu.RLock()
	out := make(map[string]eventMetaEntry, len(c.eventMeta))
	for k, v := range c.eventMeta {
		out[k] = v
	}
	c.eventMetaMu.RUnlock()
	return out
}

// EventBus returns the App-scoped user-event broadcast bus, or nil
// when none has been wired. Internal-only accessor used by the App
// layer to register the gospore.events.subscribe_service and
// gospore.events.subscribe_instance built-in callables.
func (c *Cell) EventBus() *EventBus {
	return c.eventBus
}

// ComponentSlots returns the scanned `gospore:"component"` fields for
// this actor. The slice is owned by the Cell; callers must not mutate it.
func (c *Cell) ComponentSlots() []projection.ComponentSlot {
	c.componentSlotsMu.RLock()
	defer c.componentSlotsMu.RUnlock()
	return c.componentSlots
}

// setComponentSlots replaces the scanned component slots. Called from the
// system lane (handleStart / restart OnStart) while external readers
// (projection visibility checks, export) may read concurrently.
func (c *Cell) setComponentSlots(slots []projection.ComponentSlot) {
	c.componentSlotsMu.Lock()
	c.componentSlots = slots
	c.componentSlotsMu.Unlock()
}

// nativeCellContext implementation for scriptRuntimeOwner.

func (c *Cell) nativeSelf() ref.Ref         { return c.self }
func (c *Cell) nativeParent() ref.Ref       { return c.parent }
func (c *Cell) nativeNamespace() string     { return c.namespace }
func (c *Cell) nativeDone() <-chan struct{} { return c.done }
func (c *Cell) nativeClock() actor.Clock    { return c.clock }

// eventRoutingKey returns the identity key used for EventBus routing.
// Always the actor's canonical ID — the single source of truth for
// identity in the system. Subscribers resolve via subscribeService
// (service name → actor lookup) or subscribeInstance (actorId directly).
func (c *Cell) eventRoutingKey() string {
	if c == nil {
		return ""
	}
	if c.self != nil {
		return c.self.ID().String()
	}
	return c.namespace
}
func (c *Cell) nativeSpawnFn() func(ref.Ref, actor.Props, string) (ref.Ref, error) {
	return c.spawn
}
func (c *Cell) nativeStopFn() func(ref.Ref) error {
	return c.stop
}
func (c *Cell) nativeDeliverFn() func(ref.Ref, mailbox.Envelope) error {
	return c.deliver
}
func (c *Cell) nativeLookupIDFn() func(id.ActorID) (ref.Ref, bool) {
	return c.lookupID
}
func (c *Cell) nativeLookupServiceFn() func(string) (ref.Ref, bool) {
	if c.svcReg == nil {
		return nil
	}
	return c.svcReg.Lookup
}

func (c *Cell) nativeChildrenFn() func() []ref.Ref {
	return c.Children
}

func (c *Cell) nativeRoot() ref.Ref {
	return c.rootRef
}

func (c *Cell) nativeInvokeTable() *invoke.PendingTable {
	return c.invokeTable
}

func (c *Cell) nativeCorIDGen() *id.CorIDGenerator {
	return c.corIDGen
}

func (c *Cell) nativePolicyStore() actor.PolicyStore {
	return c.policyStore
}

func (c *Cell) nativeProjectionsFn() *projection.StoreImpl {
	return c.projections
}

func (c *Cell) nativeHandlersFn() *handler.Table {
	return c.handlers
}
