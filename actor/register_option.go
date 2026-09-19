package actor

import (
	"fmt"
	"reflect"
	"regexp"
)

const (
	DefaultLoopOwner = "owner"
	DefaultLoopReply = "reply"
	DefaultLoopPure  = "pure"
)

// regCfg accumulates RegisterOption mutations.
// Internal; visible only to the actor / cell wiring code that resolves
// final Register / RegisterScript / AttachComponent settings.
type regCfg struct {
	streaming     bool
	streamChunkTy reflect.Type
	visibility    Visibility
	description   string
	paramDescs    map[string]string
	finalDesc     string
	chunkDesc     string
	modeSet       bool
	mode          HandlerMode
	loop          string
	crossApp      bool
	effect        string
	toolName      string
}

// RegisterOption mutates a regCfg.
// Used by Register, RegisterScript, and AttachComponent.
type RegisterOption func(*regCfg)

// WithVisibility sets the compile-time export visibility for the registered
// item. Defaults to VisibilityInternal if not called.
func WithVisibility(v Visibility) RegisterOption {
	return func(c *regCfg) {
		c.visibility = v
	}
}

// Public marks the registered item as VisibilityPublic — exported to all
// frontend codegen builds.
func Public() RegisterOption { return WithVisibility(VisibilityPublic) }

// AdminOnly marks the registered item as VisibilityAdmin — exported only
// to admin frontend builds.
func AdminOnly() RegisterOption { return WithVisibility(VisibilityAdmin) }

// Diagnostic marks the registered item as VisibilityDiagnostic — exported
// for diagnostic tooling but not end-user frontend code.
func Diagnostic() RegisterOption { return WithVisibility(VisibilityDiagnostic) }

// Internal marks the registered item as VisibilityInternal — the default.
// Provided for explicit documentation; omitting any visibility option
// produces the same effect.
func Internal() RegisterOption { return WithVisibility(VisibilityInternal) }

// CrossApp marks the registered callable as exposed to external sporecode
// apps through the peer protocol exchange. It is orthogonal to frontend
// visibility (Public/Admin/Diagnostic/Internal).
func CrossApp() RegisterOption {
	return func(c *regCfg) {
		c.crossApp = true
	}
}

// WithDescription sets a human-readable description for the registered
// callable. This description is emitted into the manifest and can be
// consumed by tool schema generators (e.g. for LLM function calling).
func WithDescription(desc string) RegisterOption {
	return func(c *regCfg) {
		c.description = desc
	}
}

// WithMode explicitly sets the handler execution mode for registration.
// Go handlers may omit this and let Register infer the mode from the first
// parameter type; when provided, the explicit mode must still match the
// handler signature's Context/PureContext shape.
func WithMode(mode HandlerMode) RegisterOption {
	return func(c *regCfg) {
		c.modeSet = true
		c.mode = mode
	}
}

// WithLoop sets the logical loop route for the registered callable.
// Empty means the runtime default for the resolved mode.
func WithLoop(loop string) RegisterOption {
	return func(c *regCfg) {
		c.loop = loop
	}
}

// ParamDesc describes one callable parameter's human-readable description.
type ParamDesc struct {
	Name        string
	Description string
}

// WithParams adds human-readable descriptions for callable parameters.
// These descriptions are emitted into the manifest schema and can be
// consumed by tool schema generators (e.g. for LLM function calling).
func WithParams(descs ...ParamDesc) RegisterOption {
	return func(c *regCfg) {
		if c.paramDescs == nil {
			c.paramDescs = make(map[string]string, len(descs))
		}
		for _, d := range descs {
			c.paramDescs[d.Name] = d.Description
		}
	}
}

// WithFinalDesc sets a human-readable description for the callable's
// final return value (unary result or streaming completion).
func WithFinalDesc(desc string) RegisterOption {
	return func(c *regCfg) {
		c.finalDesc = desc
	}
}

// WithChunkDesc sets a human-readable description for the streaming
// callable's chunk type. Only meaningful for streaming callables.
func WithChunkDesc(desc string) RegisterOption {
	return func(c *regCfg) {
		c.chunkDesc = desc
	}
}

// Streaming marks the handler as streaming and declares the chunk type T.
// Currently reserved for future codegen pipeline consumption; the core
// handler path detects streaming mode automatically from the Emitter
// parameter signature. Manifest generation uses BuildManifestWithStreaming
// + explicit StreamingDef instead.
func Streaming[T any]() RegisterOption {
	// reflect.TypeOf(zero) returns nil when T is an interface type;
	// the (&zero).Elem() form recovers the static type either way.
	var zero T
	ty := reflect.TypeOf(&zero).Elem()
	return func(c *regCfg) {
		c.streaming = true
		c.streamChunkTy = ty
	}
}

// WithEffect declares the side-effect classification of the callable.
// Values follow the sporecode convention: "read", "write", "mutate".
// The string is passed through without validation at the gospore layer;
// the consumer (tool registry / policy engine) enforces its own vocabulary.
func WithEffect(kind string) RegisterOption {
	return func(c *regCfg) {
		c.effect = kind
	}
}

// toolNameRe is the validation pattern for LLM-side tool names.
// Only ASCII alphanumeric, underscore, and hyphen are allowed.
var toolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// WithToolName declares the LLM-side tool name for the callable.
// The name must match [a-zA-Z0-9_-]+; registration fails if the
// constraint is violated.
func WithToolName(name string) RegisterOption {
	return func(c *regCfg) {
		c.toolName = name
	}
}

// ValidateToolName checks whether name is a valid LLM tool name.
// Returns nil if valid, an error describing the violation otherwise.
func ValidateToolName(name string) error {
	if name == "" {
		return fmt.Errorf("tool name cannot be empty")
	}
	if !toolNameRe.MatchString(name) {
		return fmt.Errorf("tool name %q contains invalid characters; only [a-zA-Z0-9_-] are allowed", name)
	}
	return nil
}
