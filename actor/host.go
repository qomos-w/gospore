package actor

// base provides default no-op OnStart / OnStop / OnInit implementations.
// It is the deepest-embedded type in the actor chain so the gospore
// embed-driver always finds an Actor at the root. It is unexported
// because external users should embed Host, not base.
type base struct{}

// Type is the default — returns empty string. Override in derived types.
func (base) Type() string { return "" }

// OnInit is the default no-op. Override in derived actor types.
func (base) OnInit(Context) error { return nil }

// OnStart is the default no-op. Override in derived actor types.
func (base) OnStart(Context) error { return nil }

// OnStop is the default no-op. Override in derived actor types.
func (base) OnStop(Context) error { return nil }

// OnDestroy is the default no-op. Override in derived actor types.
func (base) OnDestroy(Context) error { return nil }

// Host is the unified-host actor type. Embed it (or use NewHost) to
// participate in the data-driven layer: Definition loading, dynamic
// components, script handlers, and the _gospore_.script_reload /
// script_replace protocols.
//
// Embedding model:
//
//	type AuthActor struct {
//	    actor.Host                 // unified-host shell
//	    privateKey []byte          // Go-only field
//	    Stats *AuthStats `gospore:"component"`  // static component
//	}
//
// The OnStart chain runs Base.OnStart → Host.OnStart → the user's
// OnStart. The middle hop registers Definition handlers (if def != nil)
// and the unified hot-reload protocol on every actor.
//
// An actor with no Definition, no script handlers, and no dynamic
// components is the empty-configuration case of the same model — it
// retains the host shell and remains eligible for the reload protocol.
type Host struct {
	base

	// def is the optional data-layer Definition. nil is a first-class
	// legal state — the actor uses Go-only behavior while still
	// retaining the host shell.
	def *Definition
}

// NewHost constructs a Host wrapping def. Pass to actor.PropsFromFunc
// when you want a Definition-only actor (no Go handler body):
//
//	props := actor.PropsFromFunc(func() actor.Actor {
//	    return actor.NewHost(loadDefinition(...))
//	})
//
// A nil def is a first-class legal argument — the resulting Host uses
// only its Go body while still retaining the unified-host shell.
func NewHost(def *Definition) *Host {
	return &Host{def: def}
}

// SetDefinition attaches a Definition to an embedded Host.
// Must be called before OnStart (typically from the embedding type's
// constructor, prior to Spawn). Calling after OnStart is undefined
// behavior — use the _gospore_.script_replace protocol instead to swap
// definitions on a running actor.
func (h *Host) SetDefinition(def *Definition) {
	h.def = def
}

// Definition returns the Definition currently bound to this Host, or
// nil if none. Exposed for testability and tooling — embedding code
// that wants the live Definition during a handler should consult the
// projection layer instead.
func (h *Host) Definition() *Definition {
	return h.def
}

// OnStart is invoked by the embed-driven OnStart chain. It performs
// the unified-host bootstrap:
//
//  1. If def != nil: validate schema / callID / mode / visibility for
//     every Component / Handler decl.
//  2. The Cell layer auto-binds gospore:"component" Go fields via
//     spore binding — handled outside this method.
//  3. If def != nil: register every HandlerDecl via ctx.RegisterScript.
func (h *Host) OnStart(ctx Context) error {
	if err := ValidateDefinition(h.def); err != nil {
		return err
	}
	if h.def == nil {
		return nil
	}
	for _, component := range h.def.Components {
		if err := ctx.AttachComponent(component.Name, component.SchemaID, component.Initial); err != nil {
			return err
		}
	}
	for _, decl := range h.def.Handlers {
		if err := ctx.RegisterScript(decl.CallID, decl.Source, decl.Mode); err != nil {
			return err
		}
	}
	return nil
}

// OnStop releases spore binding resources and reclaims the script
// runtime allocated to this instance.
func (h *Host) OnStop(Context) error {
	return nil
}
