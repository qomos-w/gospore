package handler

import (
	"reflect"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/ref"
	tschema "github.com/qomos-w/spore/schema"
)

// Invoker is the runtime artefact produced for each registered callID.
// One Invoker per callID per cell; its Run closure encapsulates the
// decode/inject/call/encode/reply pipeline derived from the user's
// handler function via reflection.
//
// Mode discriminates execution: stateful invokers run on the cell
// goroutine (serialised one-call-at-a-time), stateless invokers run
// on the runtime-owned pure lane unless explicitly routed back onto
// the owner loop. The mode is inferred from the handler's first
// parameter type (Context vs PureContext) — see ARCHITECTURE.md §4.12.
type Invoker struct {
	// Mode reuses actor.HandlerMode rather than declaring a parallel
	// enum so script registrations and Go registrations share a single
	// vocabulary.
	Mode   actor.HandlerMode
	Loop   string
	CallID string
	Desc   tschema.CallableDesc
	Script *ScriptHandle
	// Visibility controls whether this callable is exported to frontend
	// codegen. Defaults to VisibilityInternal; set via RegisterOption.
	Visibility actor.Visibility
	// Description is the human-readable description of this callable,
	// set via WithDescription RegisterOption. Emitted into the manifest.
	Description string
	// ParamDescs maps parameter names to human-readable descriptions,
	// set via WithParams RegisterOption. Emitted into the manifest.
	ParamDescs map[string]string
	// FinalDesc is the human-readable description of the final return value,
	// set via WithFinalDesc RegisterOption.
	FinalDesc string
	// ChunkDesc is the human-readable description of the streaming chunk type,
	// set via WithChunkDesc RegisterOption.
	ChunkDesc string
	// Effect is the side-effect classification (e.g. "read", "write", "mutate"),
	// set via WithEffect RegisterOption.
	Effect string
	// ServiceName is the route service name, set via WithService RegisterOption.
	ServiceName string
	// ToolName is the LLM-side tool name, set via WithToolName RegisterOption.
	ToolName string
	// RequestSchemaID is the schema ID attached to inbound Call frames
	// when the request payload is codec-serialised.
	RequestSchemaID uint64
	// ParamSchemaIDs is the schema ID for every wire-visible parameter.
	// The first entry equals RequestSchemaID when the handler declares a
	// request parameter.
	ParamSchemaIDs []uint64
	// ReturnSchemaID is the schema ID attached to unary reply frames.
	// Zero means "no typed schema attached".
	ReturnSchemaID uint64
	// ReturnSchemaIDs is the schema ID for every non-error return value.
	// The first entry equals ReturnSchemaID when the handler declares a
	// value return.
	ReturnSchemaIDs []uint64
	// ChunkSchemaID is the schema ID attached to streaming chunk reply
	// frames. Zero means "no typed schema attached".
	ChunkSchemaID uint64
	// ChunkType is the concrete chunk type declared via actor.Streaming[T](),
	// stored for manifest export so the schema builder can register the
	// actual struct even when the handler signature uses the untyped
	// actor.Emitter interface.
	ChunkType reflect.Type
	// ChunkTypeDesc is the spore TypeDesc of the streaming chunk type,
	// used by the emitter to binary-encode chunks. Empty when the callable
	// is not streaming or when no Streaming[T]() option was provided.
	ChunkTypeDesc tschema.TypeDesc
	// CrossApp marks the callable as exposed to external sporecode apps via
	// the peer protocol exchange surface.
	CrossApp bool
	// Fn is the original handler function as supplied to Register.
	// Stored for runtime manifest export so that the manifest builder
	// can re-reflect parameter and return types to produce ObjectDesc
	// schema entries. Nil for script-backed handlers.
	Fn any
	// Run executes the handler against a constructed InvokeEnv. The
	// closure performs decode, context injection, reflective call,
	// result encoding, and Reply emission. Cell guarantees Run is
	// invoked on the right goroutine for the Mode.
	Run func(env InvokeEnv)
}

// ScriptHandle marks an Invoker as script-backed and carries the
// source text the Spore runtime compiles and executes. Go handlers
// leave Script nil and continue through the ordinary direct Run
// closure with no extra dispatch branch beyond a nil check.
type ScriptHandle struct {
	Source string
}

// InvokeEnv is the dispatch context Cell builds for a single Call
// frame and hands to Invoker.Run. It bundles the inbound Frame, the
// caller's Ref, the decoded payload (or raw bytes if decoding is
// deferred), the receiving actor instance (for stateful invocations
// that need to read/write fields via reflection), and the Reply
// callback that emits Reply / Error / End frames back through the
// same transport.
//
// InvokeEnv is allocated per Call and lives only for the duration of
// Run; it is intentionally not safe to retain past Run's return.
type InvokeEnv struct {
	// Frame is the original Call frame. Invoker uses it for CorID,
	// CallID, SchemaNS, SchemaID, and (when Payload is nil) Body.
	Frame message.Frame
	// Caller is the originating actor's Ref. Surfaced to handlers via
	// Context.Caller(). May be nil for system-originated calls.
	Caller ref.Ref
	// Payload is the codec-decoded request value when the cell pre-
	// decodes; otherwise nil and Frame.Body holds the raw bytes.
	Payload any
	// Identity is the external caller identity attached by the transport
	// boundary. Internal actor↔actor calls leave it zero-valued.
	Identity id.Identity
	// Context is the invocation-scoped handler context injected into the
	// first parameter when the handler declares actor.Context or
	// actor.PureContext.
	Context any
	// Actor is the target actor instance. Used by reflection to bind
	// stateful method receivers and emitter targets.
	Actor actor.Actor
	// Reply emits a frame back to the caller's transport. For
	// stateful invocations the cell goroutine owns the call to Reply;
	// for stateless invocations the fork goroutine emits via an
	// independent sender. Always non-nil when Run is invoked.
	Reply func(msg message.Frame)
	// Codec is the App-wide wire codec used to encode the handler's
	// return value into the Reply frame's Body. nil is treated by the
	// run closure as a no-codec environment: []byte / string values still
	// travel as raw bytes (the §4.7 raw-payload passthrough), while any
	// other return shape causes the dispatch to emit a structured Error
	// frame instead of a malformed Reply.
	Codec codec.Codec
	// ReturnSchemaID is the schema ID to attach to unary reply frames.
	ReturnSchemaID uint64
	// ChunkSchemaID is the schema ID to attach to streaming chunk reply
	// frames emitted through actor.Emitter.
	ChunkSchemaID uint64
	// OnCancel registers a cancellation callback for this CorID.
	// The Cell uses this to associate an active stream with its CorID
	// so that a KindCancel frame can interrupt an in-flight emitter.
	OnCancel func(corID uint64, cancel func())
	// OffCancel unregisters the cancellation callback.
	OffCancel func(corID uint64)
}
