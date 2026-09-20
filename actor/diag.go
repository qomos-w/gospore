package actor

// Diagnostic codes raised on the actor handler-side surface. Surfaced via
// Error frames and structured logs; callers compare against these
// constants rather than matching message strings.
const (
	// DiagComponentUnknown indicates GetComponent / MustGetComponent /
	// HasComponent was called with a name that is not attached.
	DiagComponentUnknown = "gospore.actor.component_unknown"
	// DiagComponentTypeMismatch indicates the attached component's
	// runtime type differs from the requested T. Generic GetComponent
	// returns (zero, false); MustGetComponent panics.
	DiagComponentTypeMismatch = "gospore.actor.component_type_mismatch"

	// DiagCallableInvalidID is returned by Register when callID does not
	// match the dotted-namespace regex (≥2 lowercase segments, see
	// ValidCallID). Wire-format counterpart of M09 item 2's format clause.
	DiagCallableInvalidID = "gospore.callable.invalid_id"
	// DiagCallableReservedNamespace is returned when a non-root actor
	// attempts to register a callID under "app.*" (or another reserved
	// prefix). See IsReservedNamespace and ARCHITECTURE.md §4.17.
	DiagCallableReservedNamespace = "gospore.callable.reserved_namespace"
	// DiagCallableNamespaceMismatch is returned when callID's first
	// segment differs from the App's declared namespace (and is not in
	// the reserved set). Validated at Register time on the Cell tier.
	DiagCallableNamespaceMismatch = "gospore.callable.namespace_mismatch"
	// DiagCallableSignatureMismatch is returned when a callID is
	// re-registered with a structurally different CallableDesc (different
	// arity, parameter type, return type, HasError, or streaming shape).
	// Cell-tier consistency rule per ARCHITECTURE.md §4.12 step 5; pure
	// comparison logic lives in internal/handler.DescConsistent.
	DiagCallableSignatureMismatch = "gospore.callable.signature_mismatch"
	// DiagCallableModeMismatch is returned when a callID is re-registered
	// with a different HandlerMode (stateful vs stateless). Cell-tier
	// consistency rule per ARCHITECTURE.md §4.12 step 5; pure
	// comparison logic lives in internal/handler.ModeConsistent.
	DiagCallableModeMismatch = "gospore.callable.mode_mismatch"
	// DiagCallableVisibilityMismatch is returned when a callID is
	// re-registered with a different Visibility. Cell-tier consistency
	// rule per ARCHITECTURE.md §4.12 step 5.
	DiagCallableVisibilityMismatch = "gospore.callable.visibility_mismatch"
	// DiagCallableLoopMismatch is returned when a callID is re-registered
	// with a different logical loop route.
	DiagCallableLoopMismatch = "gospore.callable.loop_mismatch"

	// DiagCallableToolNameInvalid is returned when WithToolName is
	// provided with a name that does not match [a-zA-Z0-9_-]+.
	DiagCallableToolNameInvalid = "gospore.callable.tool_name_invalid"

	// DiagCallableDuplicate is returned when a callID is registered
	// more than once with an identical CallableDesc. Cell-tier
	// idempotency rule — a re-registration with the same desc is a
	// no-op, but a duplicate under a different owner path is an error.
	DiagCallableDuplicate = "gospore.callable.duplicate"
	// DiagCallableAnonymousStruct is returned by Register (strict mode)
	// when a param or return is an anonymous struct type. Anonymous
	// structs have no stable name, so the manifest exporter cannot
	// register them in the schema set; downstream binary codec lookup
	// then fails with "missing schema entry". Require a named struct
	// type so the schema ID is stable and discoverable.
	DiagCallableAnonymousStruct = "gospore.callable.anonymous_struct"

	// DiagPolicyInvalid is returned when a Policy reload contains
	// an invalid scope or role format.
	DiagPolicyInvalid = "gospore.policy.invalid"
	// DiagPolicyVersionMismatch is returned when a Policy reload
	// request carries a stale version number.
	DiagPolicyVersionMismatch = "gospore.policy.version_mismatch"
	// DiagPolicyDenied is returned when a Call frame is rejected by
	// the Cell-tier PolicyStore evaluation. Emitted in dispatchCall
	// after handler lookup but before the handler runs; the caller
	// receives an Error frame with this code.
	DiagPolicyDenied = "gospore.policy.denied"

	// DiagWatchInvalidKind indicates Context.Watch was called with a
	// WatchKind that is not a single named bit (zero, OR'd mask, or
	// out-of-range). Emitted by the Cell-tier Watch validation after
	// ValidWatchKind returns false; the predicate lives in this
	// package so Cell importers avoid a circular dependency.
	DiagWatchInvalidKind = "gospore.watch.invalid_kind"

	// DiagActorPanic indicates a stateful handler panicked during
	// execution. Cell-tier recovery wraps the panic value and emits this
	// code on the error frame so the caller can discriminate panics from
	// regular Go errors. Per §4.12 step 3.
	DiagActorPanic = "gospore.actor.panic"

	// DiagEmbedDoubleCall indicates embedded.OnStart / embedded.OnStop was
	// called more than once on the same embedded actor. Cell-tier lifecycle
	// guard per §4.2 — the embed chain must see exactly one Start/Stop pair.
	DiagEmbedDoubleCall = "gospore.actor.embed_double_call"
	// DiagEmbedAmbiguous indicates the embed chain contains two or more
	// candidates that could satisfy a given embedded interface. Cell-tier
	// resolution requires exactly one match per interface.
	DiagEmbedAmbiguous = "gospore.actor.embed_ambiguous"
	// DiagEmbedNotActor indicates a struct embedded via Base is not itself
	// a valid actor (missing required handler methods). Cell-tier
	// validation at spawn time.
	DiagEmbedNotActor = "gospore.actor.embed_not_actor"
	// DiagEmbedUnaddressable indicates an embedded actor cannot receive
	// messages because its address was never allocated. This happens when
	// the embed chain breaks before address assignment.
	DiagEmbedUnaddressable = "gospore.actor.embed_unaddressable"
	// DiagEmbedChainBroken indicates the embed chain has a gap — a middle
	// actor was removed or stopped, breaking the delegation path from outer
	// to inner. Cell-tier chain-integrity check.
	DiagEmbedChainBroken = "gospore.actor.embed_chain_broken"

	// DiagDefinitionInvalid is emitted by ValidateDefinition when the
	// data-shape of a Definition fails one of the structural rules:
	// invalid Namespace, ComponentDecl with empty Name / duplicate
	// Name / zero SchemaID, HandlerDecl with invalid CallID / first
	// segment ≠ Namespace / illegal Mode / missing Source-and-Compiled
	// / duplicate CallID. Schema-presence (DiagUnregisteredSchema) is a
	// separate Cell-tier check that needs app.Schemas() and stays out
	// of this code.
	DiagDefinitionInvalid = "gospore.actor.definition_invalid"

	// DiagIncompatibleReload is emitted by ClassifyReloadDiff when a
	// reload request crosses one of the §4.20 forbidden
	// changes: deleting a script handler, changing a handler's Mode,
	// changing a handler's I/O schema (Cell-tier check, not enforced
	// at the leaf), deleting a dynamic component, changing a dynamic
	// component's SchemaID, or changing the Definition's Namespace.
	// Caller must use _gospore_.script_replace + DropState=true to
	// perform any of these destructive shape changes.
	DiagIncompatibleReload = "gospore.actor.incompatible_reload"

	// DiagReplaceWithoutDrop is emitted by ValidateReplaceReq when an
	// `_gospore_.script_replace` request omits the explicit
	// DropState=true acknowledgement. Per §4.20,
	// destructive replacement must be opt-in: state loss is the
	// caller's responsibility, and forgetting the flag is treated as
	// a user error rather than silent data destruction.
	DiagReplaceWithoutDrop = "gospore.actor.replace_without_drop"

	// DiagSwapFailedRolledBack is emitted by the Cell-tier reload
	// executor when an `_gospore_.script_reload` request begins
	// substituting per-callID invokers and one substitution fails
	// midway: any already-substituted invokers are rolled back to
	// their pre-reload implementations and the definition remains
	// untouched. Per §4.20.
	DiagSwapFailedRolledBack = "gospore.actor.swap_failed_rolled_back"

	// DiagContextStopped is returned when handler code calls a mutation
	// surface (Register, Spawn, After, ...) on the Context delivered to
	// OnStop. The Context exposes only read-only capabilities at that
	// point: the actor is past the point of accepting new state. Read
	// methods (Self, Parent, Logger, ...) continue to work.
	DiagContextStopped = "gospore.actor.context_stopped"

	// DiagEventKindInvalid is returned when RegisterEventKind is called
	// with an empty or otherwise invalid kind string.
	DiagEventKindInvalid = "gospore.event.invalid_kind"
	// DiagEventKindDuplicate is returned when RegisterEventKind is called
	// with a kind that has already been registered on this actor.
	DiagEventKindDuplicate = "gospore.event.duplicate_kind"
	// DiagEventKindUnknown is returned when EmitEvent is called with a
	// kind that was never registered on this actor via RegisterEventKind.
	DiagEventKindUnknown = "gospore.event.unknown_kind"
	// DiagEventPayloadMismatch is returned when EmitEvent's payload type
	// differs from the example value passed to RegisterEventKind.
	DiagEventPayloadMismatch = "gospore.event.payload_mismatch"
	// DiagEventAfterStop is returned when EmitEvent is called on a
	// stopped Context (i.e. inside or after OnStop).
	DiagEventAfterStop = "gospore.event.after_stop"
	// DiagEventNoBus is returned when EmitEvent is called but no
	// EventBus has been wired into the Cell. Indicates a runtime
	// configuration error, not user fault.
	DiagEventNoBus = "gospore.event.no_bus"
	// DiagEventSubscriberLagging is reported by the EventBus when a
	// subscription's buffer was full and Publish dropped events. Drop
	// counts are surfaced two ways: (a) the cumulative count via
	// Subscription.Drops, and (b) the per-iteration LaggingHook installed
	// via EventBus.SetLaggingHook. Hooks should treat this constant as
	// the routing key when forwarding to logs or metrics sinks.
	DiagEventSubscriberLagging = "gospore.event.subscriber_lagging"
	// DiagEventRegisterAfterStart is returned when RegisterEventKind is
	// called outside OnStart (i.e. from a request handler or OnStop).
	// Event-kind registration must complete before any handler runs so
	// the eventMeta table is single-goroutine and lock-free for the
	// EmitEvent read path.
	DiagEventRegisterAfterStart = "gospore.event.register_after_start"
)
