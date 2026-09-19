# gospore Architecture

> gospore is an actor orchestration engine built on top of the [spore](https://github.com/qomos-w/spore) protocol layer. This document describes its design, components, contracts, and wiring.
> 中文版见 [ARCHITECTURE.zh-CN.md](ARCHITECTURE.zh-CN.md)。

---

## Positioning

> **gospore lets actor behavior evolve incrementally from "pure compiled Go" to "fully data-driven".**

Any gospore actor can sit anywhere on this spectrum during its lifetime, reversibly: Go handlers only → Go + a few script handlers / components → Definition + dynamic components. This is a difference in configuration content, not a switch of actor species — every actor shares the same unified host capabilities (script runtime, reload protocol, Definition ingestion surface).

Unified rules:

- Every actor has Host capabilities by default; whether script / Definition / dynamic components are actually enabled is decided by configuration content.
- The empty configuration is a valid first-class state.
- Go remains a first-class hot path: Go callIDs go straight to Go invokers, never through the script runtime.
- Concurrency internals such as pipelines / mailboxes / goroutines are host implementation details, never exposed as user API.

## 0. Summary

### 0.1 Quick start

```go
// Start an App
app, _ := app.New(app.WithNamespace("auth"), app.WithRootActor(&MyActor{}))
app.Run(ctx)

// Minimal actor
type MyActor struct{ actor.Host }
func (a *MyActor) OnStart(ctx actor.Context) error { return nil }
func (a *MyActor) OnStop(ctx actor.Context) error  { return nil }
```

- Stateful handler: `func(ctx actor.Context, req *Req) (*Resp, error)`; stateless: `func(pctx actor.PureContext, req *Req) (*Resp, error)`.
- Streaming: append an `Emitter[*T]` parameter.
- Three iron rules for call IDs: dot-separated with at least two segments; the first segment = App.Namespace; `app.*` is root-only, `_gospore_.*` is reserved for the framework.
- Invocation: `ref.Invoke(ctx, "ns.method", &req)` returns a Stream consumed via `Recv()`.
- Observe current state: `gospore.projection.watch(actor_id, since_version)`; observe historical actions: `gospore.events.subscribe_instance(actor_id, kind)`.

### 0.2 Core design points

1. gospore is an actor orchestration engine: in-process actor lifecycle, unified Invoke, parent-child hierarchy and supervision. One process = one App = one namespace.
2. gospore is not a network framework, database, web server, or config center. The protocol layer reuses spore entirely (`spore/transport.Codec`, `spore/identity.CanonicalID`, `spore/schema.TypeDesc`).
3. Unified Invoke: fire-and-forget / unary / streaming are three consumption forms of the same call.
4. ActorTree + App as Root: all actors form a single tree; closing the App equals a cascading stop of the whole tree.
5. Registration-based actors: `ctx.Register(callID, fn)` inside `OnStart`, not `Receive(msg)`.
6. Dual observability channels: every actor exposes a State Channel (projection, §4.18) and an Event Channel (events, §4.22); the mechanisms are symmetric and support `since`-based gap replay.
7. A call is a child actor: `ctx.Plan(...)` wraps one invocation as a child actor (§4.21).

## 1. Design principles

- **From the classic actor-model tradition**: tree supervision, mailbox serialization, system-lane semantics.
- **Deliberately no weakly-typed machinery**: no untyped PIDs, string topics, or cluster gossip — gospore replaces them with strongly-typed wire contracts (SchemaID + namespace).
- **Inherited from spore**: schema descriptions (TypeDesc/CallableDesc), binary/JSON codecs, the 128-bit CanonicalID, script runtime and binding.
- **Built by gospore**: the Cell runtime, handler table, Tree/Supervisor, Service/Resource registries, Projection/Events dual channels, Plan, App assembly and the Gateway.

## 2. Overall architecture

### 2.1 Layering

```
User actors (Go handlers / script handlers / components)
        │ Register / Plan / Watch / Subscribe
   actor.Context  ── actor.PureContext (stateless view)
        │ implemented by internal/cell
   Cell (internal/cell)  ←── Handler Table (internal/handler)
        │ holds: events ring / projection slots / supervisor / timerMeta
   App (app)  = tree root + assembly root (SchemaSet, Codec, Service/Resource, InvokeTable, Gateway)
        │
   Transport (local / memory-remote / HTTP + Router)  ── Frame (message)
        │
   spore v0.1.0 (schema / codec / identity / script / binding)
```

### 2.2 Key invariants

1. Serialized handlers per Cell: stateful handlers run only on the cell goroutine; stateless handlers may fork concurrently.
2. The Frame is the only wire form: `(SchemaID, PayloadMode, Encoding)` is self-describing; the peer can decode after a SchemaSet Import.
3. `_gospore_.script_reload` swaps serially on the cell goroutine, preserves component state, rolls back on failure (§4.20).
4. LIFO shutdown: closing the App tears down all actors in reverse tree depth.
5. The same callID across actors must match in signature + mode (consistency rules in §4.6 / §4.12).

## 3. Package layout

| Package | Responsibility |
|---|---|
| `actor` | User API: Context/PureContext, Host, Props, Register, Watch, Subscription, reload protocol types, diagnostic codes |
| `app` | Process-level assembly root + root actor; App options, manifest export, system callables (events/projection), gateway wiring |
| `internal/cell` | Actor runtime: queues, system message handling, handler dispatch, script host, projection/event mounting |
| `internal/handler` | Reflection-based registration pipeline and invoker closures (§4.12) |
| `internal/collections` | Generic container utilities |
| `mailbox` | Envelope / SystemMsg / ErrFull and other ingress types (queue implementation lives in internal/cell) |
| `message` | Frame wire format and Kind enum |
| `invoke` | PendingTable, Call/Stream client primitives |
| `promise` | Generic unary/streaming promises |
| `ref` | Actor reference and Invoke entry point |
| `id` | ActorID (= spore CanonicalID), CorID, Identity/Role |
| `schema` | SchemaSet, manifest import/export, builtin system protocols, diagnostic codes |
| `codec` | Frame payload encode/decode entry point |
| `transport` | Transport interface, local / memory-remote / HTTP implementations, Router |
| `tree` | Actor tree structure and scoped service registration |
| `supervisor` | One-for-one supervision decisions |
| `service` / `resource` / `discovery` | The three registries (§4.15–§4.16) |
| `events` | Event Channel storage (Ring + Store + RemoteSink) |
| `projection` | State Channel storage (fingerprint + DeltaWindow) |
| `plan` | Plan Node (a call as a child actor) |
| `scriptbridge` | Static spore Capability binding (§4.19) |
| `gateway` | HTTP/WS boundary, auth, and interceptors (the historical `frontgate/` subset package was removed) |
| `cmd/gospore-gen-ts` | manifest → TS client codegen |
| `web-client` | TypeScript client SDK (not a Go package) |

`integration/` hosts cross-package subsystem integration tests (§8.4). The following historical layers have been removed: `contracts/` (unconsumed contract interface layer), `model/` (public type layer referenced only by contracts), `internal/future/` (zero references, overlapped with promise), and `transport.Server` (placeholder interface without implementation).

## 4. Core types and contracts

### 4.1 Identity and addressing

`id.ActorID` is `spore.identity.CanonicalID` (128-bit), stable across processes. `id.CorID` identifies one invocation chain (CorIDGenerator carries a timestamp-slot). `id.Identity{Role, Kind}` carries the caller's role; Roles support dot-separated inheritance (`agent.coder` effectively inherits `agent`), resolved by string prefix in `Effective()`. The gateway rebuilds Identity inbound from `gospore.caller_role` / `gospore.caller_kind` headers.

### 4.2 Actor and Context

- The `actor.Actor` interface: `OnInit / OnStart / OnStop / OnDestroy`. Embed `actor.Host` for unified host capabilities.
- Embedding chain rule: an actor struct's embed chain must see exactly one Start/Stop pair — `actor.Host` rejects duplicate/missing embedding via `DiagEmbedChain`-type diagnostics.
- `actor.Context` is the full operating surface inside the cell goroutine (Register/Spawn/Watch/Plan/Call/Emitter/Expose/...); `actor.PureContext` is the restricted view for stateless handlers (no state writes).
- Cells provide phase-restricted Context implementations (init/start/stop each refuse out-of-phase operations, e.g. Register outside OnStart returns `DiagRegisterOutsideStart`).
- Pipelining and concurrency details stay out of the Context surface.

### 4.3 Streaming

Streaming handler signatures append `Emitter[*T]`; the Emitter pushes chunks and finishes with `Final`. On the wire this is a run of Frames sharing one CorID (chunk + final); consumers terminate via `io.EOF` from `Stream.Recv()`.

### 4.4 Ref

`ref.Ref` is an actor address handle; `Invoke(callID, payload)` returns a Stream. Refs are minted by Cell/App at spawn time; `App.RemoteRef(actorID)` builds a cross-App reference (automatically via remote transport). Refs cannot be forged by users.

### 4.5 Stream (streaming client)

`Stream.Recv()` blocks for the next frame; unary is the special case of "one Recv". Terminal state is cached: after the first KindEnd, EOF is returned forever.

### 4.6 Schema Set (namespace + 32-bit ID)

`schema.Set` maintains globally unique allocation of namespace → name → 32-bit SchemaID. The same callID across actors must match signature + mode, enforced at registration time by the consistency rule (§4.12 step 5). The manifest (`schema.Manifest`) carries cross-process distribution; `ImportFromManifest` completes the peer-side import. `schema.StructRef(name)` builds a name-referenced struct TypeDesc. The builtin table (`schema/builtin.go`) registers fixed SchemaIDs for system protocols (auth/sysmsg/eventbus/projection watch, etc.).

### 4.7 Codec

The `codec` entry selects spore JSON/binary codecs. The Frame's `Encoding` field declares the payload encoding; `PayloadMode` distinguishes Value (body is the value) from Raw (body is a serialized byte stream).

### 4.8 Frame (message wire format)

`message.Frame`: `(Kind, CallID, SchemaNS, SchemaID, Headers, PayloadMode, Encoding, Body)`. The Kind enum covers call/chunk/final/cancel/watch and more; some fields are Kind-specific. The TS implementation lives in `web-client/src/binary_frame.ts` (the GSF binary form is defined in gateway/frame_binary.go).

#### 4.8.1 Transport interface

`transport.Transport.Send(Frame)` is the send side; implementations are `local` (same-process direct delivery), `memory_remote` (cross-App test stub, may drop frames), `HTTP` (production cross-process, synchronous POST) plus `Router` (slot → target address, Static/Dynamic). connproxy folds external connections into Frame streams. Production-grade connection serving is the host's (gateway) responsibility.

### 4.9 Mailbox (with System Lane)

The mailbox type layer (Envelope/SystemMsg) describes envelopes entering a Cell; the actual queue implementation lives in `internal/cell`: each Cell has three lanes — owner (user messages, default capacity 256), system (system messages, 64), reply (replies, 64) — each an independent channel; the system lane never starves due to user-message backlog. `mailbox.ErrFull` is the backpressure sentinel; lane depth and high-water marks (75%/90%) are exposed via `CellRuntimeStats`.

### 4.10 System messages

`mailbox.SystemMsg`: lifecycle signals such as Init/Start/Idle/Stop/Destroy/Suspend/Resume/Restart are processed serially on the system lane.

### 4.11 Cell

`internal/cell.Cell` is the autonomous runtime of a single actor: it owns the consumption loop over the three lanes, the handler table, watchers, projection slots, event fan-out, the supervision decision entry, and the script host. Cells are constructed by the App at spawn; the App injects runtime callbacks via `cell.Config` (spawn/stop/destroy/deliver/routeReply/service and discovery queries, etc.) — this is known structural debt: Cell is currently in "service locator" shape; the convergence direction is a single Host-view interface, without changing its external semantics.

### 4.12 Handler table + Invoker

`internal/handler.Table` is the per-cell callID → invoker index. The Register pipeline:

1. Reflect over the function signature (parameter/return shapes);
2. Classify execution mode by the first parameter: `actor.Context` → stateful, `actor.PureContext` → stateless;
3. Shape validation: unary / streaming × two modes = 4 allowed signatures; violations return `DiagHandlerShape` diagnostics;
4. Generate Req/Final type descriptions via spore `DescribeGoStruct` into the SchemaSet (RegisterAuto);
5. **Consistency rule**: signature ↔ registered schema must be field-by-field equivalent (the same callID across actors must have identical signatures), otherwise registration is refused;
6. Build the invoker closure (run_closure) into the Table under a write lock.

Lookup is a lock-free hot path; `Desc` serves manifest export.

### 4.13 Supervisor

`supervisor` implements one-for-one: a child panic/error triggers a Decision (restart or stop), executed by the Cell's recover-and-decide path. Supervision does not cross levels — a parent handles direct children only.

### 4.14 Tree

`tree` maintains parent-child relations and the spawn phases (Reserve → Build → Abort, see §4.17), and carries parent-scoped lookup for scoped services (`RegisterScopedService` / `LookupScopedService`).

### 4.15 Service Registry (three-layer resolution model)

Service queries follow a three-layer addressing model: **Service name (role) → InstanceGroupID (instance group) → ActorID (specific instance)**. `service.Registry` manages the globally unique name → ref mapping; `tree` manages the owner→name→ref scoped mapping (`ErrScopedConflict` is defined in the service package and enforced in the tree layer); `discovery.Provider` provides cross-process instance discovery (TTL leases; the memory implementation is for tests). The `gospore.app.lookup_service` / `lookup_path` system callables expose resolution results.

### 4.16 Resource Registry

`resource.Registry` is a freeze-semantics any→any registry: the App freezes it late in startup, after which writes are rejected. Use it for non-actor singletons (DB pools, HTTP clients, etc.).

### 4.17 App (Root Actor + process entry)

The App unifies the assembly root and the root actor:

- **Startup sequence**: construction (option parsing, SchemaSet/Codec/registry assembly, root cell build) → root OnInit → ordered Start (`orderedStart`: children receive deliverStart ordered by spawn depth; `WithAsyncStart` relaxes this) → gateway-ready signal. Child spawn goes through a three-phase allocator: **Reserve** (occupy ID and name, non-blocking) → **Build** (actually build the cell) → **Abort** (roll back the reservation on failure).
- **Reserved namespaces**: `app.*` is root-only (system callables: events/projection/stats, etc.); `_gospore_.*` is framework-internal.
- **Shutdown**: ctx cancel / Shutdown / root escalation → LIFO teardown of the whole tree, then return.
- The App imports all internal packages directly for assembly; that is the assembly root's boundary of responsibility, not a layering violation.

### 4.18 Projection (State Channel)

Per-actor state projection: `ScanComponents` scans `gospore:"component"`-tagged fields → fingerprint comparison (Bytes/Hash/PerField modes, one global choice) → deltas are published only on change. `DeltaWindow` (default 64) retains recent versions; `gospore.projection.watch(actor_id, since_version)` supports live-only / replay-then-live / gap replay regimes. Snapshot evaluation is panic-guarded. wireName is authoritative from the Go struct's json tag (`wireFieldName` semantics), keeping field names stable across languages.

### 4.19 ScriptBridge (spore Capability adaptation)

ScriptBridge exposes host capabilities to scripts as spore Capability groups, five namespaces:

| Namespace | Methods (constants in scriptbridge/methods.go) |
|---|---|
| `gospore.projection` | get / field / watch |
| `gospore.events` | subscribe_service / subscribe_instance / recent |
| `gospore.app` | lookup_path / lookup_service / tree |
| `gospore.policy` | check / version |
| Dynamic prefix | one PrefixInvoke method per user callID |

Method IDs are wire constants, shared as a single source between codegen and spore `binding.RegisteredCapability`.

### 4.20 ScriptActor (unified host + data-driven layer)

Hot-update protocol (constants `actor.CallIDReload` / `actor.CallIDReplace`):

**`_gospore_.script_reload` (compatible extension; refuse on failure, state preserved)**

| Change | Result |
|---|---|
| Add handler / component | Allowed, serial table install |
| Handler source change (mode unchanged) | Allowed |
| Meta change | Allowed, refreshes projection metadata |
| Delete/rename handler or component | **Refused** (`DiagIncompatibleReload`) |
| Handler mode change | **Refused** |

**`_gospore_.script_replace` (destructive)**: any change allowed, but requires an explicit `ReplaceReq{DropState: true}` — the explicit flag itself is the accidental-deletion guard; after execution, script/dynamic-layer state is zeroed while Go fields are preserved.

A nil `ReloadReq.Definition` is invalid. Validation is two-layer: `actor.ValidateDefinition` (pure data) + Cell-level `ClassifyReloadDiff` (old vs new), then a serial swap on the cell goroutine with rollback on failure.

### 4.21 Plan Node (a call as a child actor)

`ctx.Plan(target, callID, payload)` wraps one invocation as a child actor (planNodeActor): it gains independent supervision, Watch, projection, and cancellation, with a short lifecycle (reclaimed when the Invoke finishes). `plan.State` is a five-value lifecycle. Retry/circuit-breaker/pause options were removed (they were documented no-ops); they will be re-introduced together with real semantics, not as placeholders.

### 4.22 Events Channel

Each actor has one `events.Ring` (default 256, override via `WithEventRingCapacity`): invoke / lifecycle / watch metadata Records append by SeqNo. Ring overflow is counted (`Ring.Evicted`) and surfaced per-actor through `gospore.events.stats` (`rings[]`, sorted by evictions, only rings with evictions > 0), so silent history loss is queryable. Subscriptions via `subscribe_service` / `subscribe_instance`; the `since` parameter implements gap replay, with clients auto-switching among live-only / replay-then-live / gap regimes. Records do not embed actor identity (the subscription dimension already determines ownership).

**Always-write contract**: ring history is written regardless of live subscribers — lifecycle (`emitLifecycleEvent`) and callable (`emitCallableEvent`) records land even during idle periods, so gap replay across subscriber-free windows works. `ctx.EmitEvent` (user events) additionally records a `WatchUserEvent` entry whose `Event` field carries the registered kind name; the payload itself travels on the event bus (the ring keeps metadata only, mirroring how invoke records store CallID but not arguments). Chatty actors churn a 256-entry ring correspondingly — `RingStats`/`events.stats` exposes per-actor `Evicted` counters.

**RemoteSink contract**: dispatch happens *outside* the store lock — the sink is invoked concurrently and potentially out of order; implementations must be non-blocking, safe for concurrent use, and buffer internally if ordering matters.

### 4.23 Timer Wheel (timer infrastructure)

`AppContext.After(delay, callID, payload)` registers a delayed self-call: the Cell records it in `timerMeta` (callID → routing metadata) and, on expiry, delivers it to Self as an ordinary Invoke. A stopped Cell refuses new After calls (`DiagEventAfterStop`).

## 5. Invocation semantics (Invoke is the only entry)

### 5.1 Three consumption forms × two execution modes

Fire-and-forget / unary / streaming = client-side consumption differences; stateful / stateless = server-side execution differences. The two axes are orthogonal.

### 5.2 Frame sequence details

Unary: call → final. Streaming: call → chunk* → final. Cancellation: a cancel frame. Server errors are expressed as final(Err).

### 5.2b Execution mode: reflection inference vs compile-time pinning

The mode (stateful/stateless) decides the lane: reflection infers it from the handler's first parameter type (`Context` exact-match → stateful owner lane, `PureContext` → stateless fork goroutines; §4.12). The typed registration API pins it at compile time: `actor.RegisterStateful[Req, Resp](ctx, callID, func(ctx Context, req Req) (Resp, error), ...)` and `actor.RegisterStateless[Req, Resp](...)` — the closure's context parameter type must match the function, so a mode mismatch is a compile error instead of a silent wrong-lane race. The typed functions are thin sugar over the same pipeline (options, schema extraction, dispatch unchanged); reflection inference remains available via the `ctx.Register(callID, handler)` escape hatch for dynamic/scripted registration.

### 5.3 Server-side dispatch

Frame arrives at Cell → system/owner lane routing → Table.Lookup → stateful runs serially on the cell goroutine, stateless forks concurrent goroutines (limits and watermarks in §4.9) → Emitter/chunk return via the reply lane.

### 5.4 Client-side dispatch

`invoke.PendingTable` registers the delivery channel per in-flight CorID; Deliver dispatches by CorID hit (a narrow race window exists between Unregister/Register, avoided by monotonic CorID allocation to prevent reuse). Timeouts are context-driven at the Call layer; stalled slots are reclaimed by the Flush path.

### 5.5 Backpressure

The three lanes are bounded; a full owner lane raises `mailbox.ErrFull` to the caller; a slow consumer only backpressures its own cell's reply pipeline, never the whole system.

## 6. Supervision protocol

- Failure detection points: handler panic (recover-and-decide), OnInit/OnStart returning an error, child termination reported via watch.
- Decision: one-for-one — restart (rebuild the cell, state handled per §4.20 semantics) or stop and escalate.
- Watch/Terminated: `ctx.Watch(target)` receives Terminated by default; the `WatchKind` bitmask extends to more events; watch is independent of the Event Channel (§4.22 is a pull/subscription stream; watch is targeted notification).

## 7. Boundary with spore

### 7.1 Reference direction

gospore → spore is a one-way dependency, aligned with the published `github.com/qomos-w/spore v0.1.0`. spore is unaware of gospore.

### 7.2 Transport boundary

spore provides codecs and binary frame encoding; gospore's transport only moves Frames and does not understand schema semantics. Cross-process routing (Router/HTTP/RemoteRef) is implemented on the gospore side. Inbound frame-channel capacity defaults to `transport.DefaultInboundCapacity` and is overridable per transport (`NewHTTPWithBuffer`, `NewMemoryRemotePairWithBuffer`).

### 7.3 Types + callable registration

gospore uses `spore.DescribeGoStruct` to produce TypeDescs; binding-side Capability registration is handled by scriptbridge (§4.19).

### 7.4 PolicyStore call chain

`gospore.policy.check/version` goes through the ScriptBridge into actor.PolicyStore (the default implementation decides by Role inheritance chain + visibility; default deny).

### 7.5 Diagnostic codes

Diagnostic codes are string constants, namespaced by package prefix, single-sourced in each package's `diag.go`: `actor.Diag*` / `app.Diag*` / `cell.Diag*` / `handler.Diag*` (pure-overflow, stateless-write, etc.) / `invoke.Diag*` / `mailbox.Diag*` / `message.Diag*` / `schema.Diag*` / `service.Diag*` / `plan.Diag*` / `projection.Diag*` / `resource.Diag*` / `tree.Diag*` / `events.Diag*`. The `MapErrToDiag` pattern provides err → Diag mapping per package. New diagnostic codes must land in the corresponding diag.go before use.

### 7.6 Gateway WS session lifecycle (timeout governance)

All WS connection timeouts and buffers flow through one tuning struct, `gateway.WSTimeouts` (`gateway.SetWSTimeouts` on the server, `app.WithGatewayWSTimeouts` at wiring). Defaults in parentheses; every knob overridable per deployment:

| Knob | Meaning | Default |
|---|---|---|
| `PingInterval` | server ping cadence; a pong refreshes the read deadline | 30s |
| `ReadIdle` | no inbound frame (incl. pong) → session torn down | 65s |
| `WriteTimeout` | per-frame write deadline; expiry force-closes | 30s |
| `PingWriteTimeout` | write deadline for ping frames | 5s |
| `StallForceClose` | no write progress at all → force-close (dead peer, not a paused one) | 10m |
| `CloseDrainTimeout` | grace period to flush `outCh` on close | 5s |
| `OverflowBudgetBytes` | byte budget for frames spilled past a full `outCh` (safety valve: exceeded → force-close so the client reconnects and replays via `since`) | 32 MiB |
| `OutChCapacity` | buffered outbound frame channel depth | 64 |

Notable semantics: a ping *write* failure logs a warning and force-closes the connection (it previously exited silently, leaving zombie sessions); a silent client that keeps ponging is considered alive by design. A client-driven repro suite for the stall/deadline paths lives in `gateway/ws_deadline_test.go`.

`Server.ReadyChan()` (and `app.App.GatewayReady()`) may be called from any goroutine, concurrently with `Run` — the channel is created eagerly.

## 8. Public contracts vs internals

### 8.1 Public semantic contracts (immutable)

The actor.Context/PureContext method surface, the Frame wire format, SchemaID allocation rules, reload/replace semantics, the `app.*`/`_gospore_.*` reserved namespaces, and the dual-channel watch/subscribe parameters.

### 8.2 Public SPIs (replaceable)

`discovery.Provider`, `events.RemoteSink`, `actor.PolicyStore`, `actor.Logger`, `actor.Clock`, `transport.Transport`, `gateway.Interceptor`.

### 8.3 Internal implementations (rewritable)

`internal/cell` (queue loops, dispatch, script host assembly), `internal/handler` (reflection pipeline), `internal/collections`. These layers can be rewritten without touching §8.1/§8.2 (known structural debt in §4.11).

### 8.4 Test layout

Per-package tests live with their packages; cross-package subsystem tests in `integration/`; gateway end-to-end behavior tests in `gateway/*_test.go` plus timer-driven integration tests (WS/HTTP agreement, app-level WSTimeouts, transport+gateway same-actor) in `integration/gateway_test.go`; TS client contract tests in `web-client/src/*.test.ts`. The Go↔TS wire contracts are generated from a single source: `internal/wiregen` renders `web-client/src/generated/system_protocol.ts` (system protocol constants/TypeDescs) and `web-client/testdata/frame_vector.json` (a golden frame vector marshaled by the real gateway encoder, consumed by vitest); changing builtin system protocols requires only `go test ./internal/wiregen -update`, and drift on either side fails CI.

## 9. Out of scope / non-goals

- No cluster membership / gossip (cross-process goes over the HTTP transport + the discovery Provider extension point).
- No field-level projection filtering (whole snapshot + fingerprint).
- No user-visible mailbox/pipeline API.
- No untyped invocation: every callID must have a schema.
- Production-grade transport connection servers (TCP/QUIC listeners) are not in gospore — the gateway (WS/HTTP) serves as the entry point.

## 10. Glossary

| Term | Meaning |
|---|---|
| App | Process-level assembly root + root actor, single namespace |
| Cell | Single-actor runtime (internal/cell) |
| Lane | The three queues inside a Cell: owner/system/reply |
| Frame | Wire message unit (message.Frame) |
| State Channel | Projection channel: current state + delta (§4.18) |
| Event Channel | Event channel: historical action Records (§4.22) |
| Plan | The mechanism wrapping one call as a child actor (§4.21) |
| Definition | Data-driven description of an actor (handlers/components/meta), the reload payload |
| Capability | Method group exposed to scripts by spore binding (§4.19) |
| wireName | Cross-language authoritative name of a projection field (Go json tag) |
