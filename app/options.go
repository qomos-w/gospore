package app

// Option configures an App at construction time. See New.
import (
	"io/fs"
	"net/http"
	"time"

	"github.com/qomos-w/gospore/gateway"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/discovery"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/gospore/resource"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/gospore/supervisor"
	"github.com/qomos-w/gospore/transport"
)

// App is the process-level entry point and root actor of one
// gospore service. App embeds actor.Actor: it has its own OnStart /
// OnStop chain, runs handlers in the `app.*` reserved namespace, and
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
	gatewayWSTimeouts    gateway.WSTimeouts
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
//
// The default codec is NewMulti(appSchemas) — bound to the app's
// schema.Set so typed-struct handler params decode receiver-side. A
// custom codec that is not resolver-bound (e.g. a bare codec.NewJSON)
// cannot resolve schema.Set-registered IDs; typed handler parameters
// will fail to decode. Build custom codecs with codec.NewWithResolver
// when handlers use typed struct parameters.
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

// WithGatewayWSTimeouts tunes the WebSocket connection timeouts of the
// HTTP gateway (ping cadence, read-idle, write deadlines, stall horizon,
// close drain, overflow budget, outbound queue capacity). The zero
// value keeps package defaults; see gateway.WSTimeouts. Only meaningful
// together with WithGatewayHTTP.
func WithGatewayWSTimeouts(t gateway.WSTimeouts) Option {
	return func(c *config) {
		c.gatewayWSTimeouts = t
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
