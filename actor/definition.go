package actor

// Definition is the data-shape of an actor's script-and-component
// declarations. It is the serializable description of "what this
// actor can do beyond its Go body" — script handlers, dynamic
// components, namespace, and meta.
//
// Definition itself is spore-codec-serializable, so it can be
// persisted, distributed over the network, or hot-loaded from disk.
//
// A nil *Definition is a first-class legal state: the actor uses only
// its Go body, while remaining eligible for the unified hot-reload
// protocol (which can introduce script content later).
type Definition struct {
	// Namespace must equal the App.Namespace of the host App; identifies
	// the App-scope this Definition belongs to.
	Namespace string
	// Components are dynamic components declared by this Definition.
	// Each ComponentDecl resolves to a (SchemaID-typed) field on the
	// actor at OnStart time.
	Components []ComponentDecl
	// Handlers are script handlers declared by this Definition. All
	// entries are script-source (Go handlers are registered via the
	// actor's Go OnStart, not through Definition).
	Handlers []HandlerDecl
	// Meta carries free-form metadata (version, author, signature,
	// description, etc.). Not interpreted by gospore; surfaced through
	// projection for tooling.
	Meta map[string]string
}

// ComponentDecl declares a dynamic component (one not present as a Go
// field with `gospore:"component"`).
type ComponentDecl struct {
	// Name is the component's identifier; appears as `self.<Name>`
	// inside script handlers.
	Name string
	// SchemaID identifies the component's record schema. The schema must
	// already be registered in the App's SchemaSet at the time
	// Definition is loaded.
	SchemaID uint64
	// Initial is the initial value for the component (decoded against
	// the schema). nil means zero-initialize per schema rules.
	Initial any
}

// HandlerDecl declares one script handler.
type HandlerDecl struct {
	// CallID is the handler's CallID; must match the App's callID format
	// (^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$) and not collide with the
	// actor's existing Go handlers.
	CallID string
	// Mode discriminates stateful (cell goroutine) vs stateless (fork
	// pool worker). Script source cannot encode this signal, so the
	// Definition must declare it explicitly.
	Mode HandlerMode
	// Source is the script source text. Either Source or Compiled must
	// be non-empty; if both are present, Compiled wins.
	Source string
	// Compiled holds a precompiled spore bytecode artifact. Type is
	// `any` because spore has not yet exposed a public bytecode
	// surface; the field is reserved for the eventual Phase-2 form.
	Compiled any
}
