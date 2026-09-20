package scriptbridge

// Static capability method IDs — the eight fully-qualified callable names
// ScriptBridge attaches to its three static Capability groups (projection,
// events, app) per ARCHITECTURE.md §4.19. Each is a wire-format string
// parsed by codegen consumers (gospore-ts, future SDKs) and by spore
// `binding.RegisteredCapability` lookup; they live as code constants
// rather than inline literals to keep producer / consumer surfaces
// synchronized through a single source of truth.
const (
	StaticProjectionGet    = "gospore.projection.get"
	StaticProjectionField  = "gospore.projection.field"
	StaticProjectionWatch  = "gospore.projection.watch"
	StaticEventsSubscribeService  = "gospore.events.subscribe_service"
	StaticEventsSubscribeInstance = "gospore.events.subscribe_instance"
	StaticEventsRecent     = "gospore.events.recent"
	StaticAppLookupPath    = "gospore.app.lookup_path"
	StaticAppLookupService = "gospore.app.lookup_service"
	StaticAppTree          = "gospore.app.tree"
	StaticPolicyCheck      = "gospore.policy.check"
	StaticPolicyVersion    = "gospore.policy.version"
)

// PrefixInvoke is the wire-format prefix every dynamic capability method
// shares: ScriptBridge generates one method per registered user callID
// under this prefix — e.g. CallID `auth.login` → method
// `gospore.invoke.auth.login`. Codegen consumers identify dynamic
// methods by `strings.HasPrefix(method, PrefixInvoke)`. Per §4.19.
const PrefixInvoke = "gospore.invoke."

// StaticMethod is one row of the §4.19 static capability method table.
// ID is the fully-qualified callable name (one of the eight Static*
// constants above); Mode is one of ExecutionUnary / ExecutionStreaming
// (ExecutionTell never appears here — Tell is reserved for dynamic
// user callables).
type StaticMethod struct {
	ID   string
	Mode string
}

// StaticMethods returns the eight static capability methods in the
// canonical declaration order defined by §4.19 (projection: get / field
// / watch → events: subscribe / recent → app: lookup_path /
// lookup_service / tree). The slice is freshly allocated on each call
// so callers may mutate without affecting other consumers; codegen
// iterates this list to emit typed wrappers.
func StaticMethods() []StaticMethod {
	return []StaticMethod{
		{ID: StaticProjectionGet, Mode: ExecutionUnary},
		{ID: StaticProjectionField, Mode: ExecutionUnary},
		{ID: StaticProjectionWatch, Mode: ExecutionStreaming},
		{ID: StaticEventsSubscribeService, Mode: ExecutionStreaming},
		{ID: StaticEventsSubscribeInstance, Mode: ExecutionStreaming},
		{ID: StaticEventsRecent, Mode: ExecutionUnary},
		{ID: StaticAppLookupPath, Mode: ExecutionUnary},
		{ID: StaticAppLookupService, Mode: ExecutionUnary},
		{ID: StaticAppTree, Mode: ExecutionStreaming},
		{ID: StaticPolicyCheck, Mode: ExecutionUnary},
		{ID: StaticPolicyVersion, Mode: ExecutionUnary},
	}
}
