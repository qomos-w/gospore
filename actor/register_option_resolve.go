package actor

import "reflect"

// ResolveStreamingChunkType reports whether opts contain a Streaming[T]()
// marker and, if so, returns the declared chunk type. The handler table
// (internal/handler/table.go RegisterWithService) consumes this to stamp
// the chunk type and allocate its schema on the callable descriptor; the
// manifest then carries Streaming + ChunkSchemaID, which the sporemind host
// protocol extraction reads (actor.Streaming[T]() is the single source of
// streaming truth - appbinding routes derive from it, not re-declare it).
func ResolveStreamingChunkType(opts ...RegisterOption) (reflect.Type, bool) {
	cfg := regCfg{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	if !cfg.streaming || cfg.streamChunkTy == nil {
		return nil, false
	}
	return cfg.streamChunkTy, true
}

func resolveRegCfg(opts ...RegisterOption) regCfg {
	cfg := regCfg{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	return cfg
}

// ResolveVisibility extracts the visibility from a sequence of
// RegisterOption values. Returns VisibilityInternal when no visibility
// option is present (the zero value default).
func ResolveVisibility(opts ...RegisterOption) Visibility {
	return resolveRegCfg(opts...).visibility
}

// ResolveDescription extracts the description from a sequence of
// RegisterOption values. Returns empty string when no description is set.
func ResolveDescription(opts ...RegisterOption) string {
	return resolveRegCfg(opts...).description
}

// ResolveParamDescs extracts the parameter descriptions from a sequence of
// RegisterOption values. Returns nil when no WithParams option is present.
func ResolveParamDescs(opts ...RegisterOption) map[string]string {
	return resolveRegCfg(opts...).paramDescs
}

// ResolveFinalDesc extracts the final return description from RegisterOption values.
func ResolveFinalDesc(opts ...RegisterOption) string {
	return resolveRegCfg(opts...).finalDesc
}

// ResolveChunkDesc extracts the chunk return description from RegisterOption values.
func ResolveChunkDesc(opts ...RegisterOption) string {
	return resolveRegCfg(opts...).chunkDesc
}

// ResolveMode extracts an explicitly declared handler mode from RegisterOption values.
func ResolveMode(opts ...RegisterOption) (HandlerMode, bool) {
	cfg := resolveRegCfg(opts...)
	if !cfg.modeSet {
		return 0, false
	}
	return cfg.mode, true
}

// ResolveCrossApp reports whether any RegisterOption marks the callable as
// exposed to external sporecode apps.
func ResolveCrossApp(opts ...RegisterOption) bool {
	return resolveRegCfg(opts...).crossApp
}

// ResolveLoop extracts the requested logical loop name from RegisterOption values.
// Empty means the runtime should choose the default loop for the resolved mode.
func ResolveLoop(opts ...RegisterOption) string {
	return resolveRegCfg(opts...).loop
}

// ResolveLoopOrDefault returns loop when non-empty, otherwise the runtime default for mode.
func ResolveLoopOrDefault(mode HandlerMode, opts ...RegisterOption) string {
	loop := ResolveLoop(opts...)
	if loop != "" {
		return loop
	}
	return DefaultLoopForMode(mode)
}

// DefaultLoopForMode returns the default logical loop for the given handler mode.
func DefaultLoopForMode(mode HandlerMode) string {
	switch mode {
	case ModeStateless:
		return DefaultLoopPure
	default:
		return DefaultLoopOwner
	}
}

// ResolveEffect extracts the effect classification from a sequence of
// RegisterOption values. Returns empty string when no WithEffect option is present.
func ResolveEffect(opts ...RegisterOption) string {
	return resolveRegCfg(opts...).effect
}

// ResolveToolName extracts the LLM-side tool name from a sequence of
// RegisterOption values. Returns empty string when no WithToolName option is present.
func ResolveToolName(opts ...RegisterOption) string {
	return resolveRegCfg(opts...).toolName
}
