# Changelog

## v0.5.0 (2026-09-20)

### Added

- **Subscriber-drop observability** (events store): the non-blocking fan-out drop path (live Push and Subscribe replay) now counts losses per subscription; `StoreImpl.SubscriberStats()` exposes {actor, kinds, dropped, bufferCap}, surfaced via `gospore.events.stats` as `storeSubs`. A slow consumer is distinguishable from a quiet actor even when its ring never overflowed; for desktop bridges it separates Go-side fan-out loss from transport-channel loss.
- **`events.StoreImpl.Close` / `projection.StoreImpl.Close`**: idempotent teardown of every active subscription (projection stops each batch `AfterFunc`, which would otherwise fire after shutdown). Wired into Run teardown and `Shutdown` — the latter may be the only path when Run never completed (abort during boot). `eventSubscription.Recv` now prioritizes done over a non-empty buffer: a closed subscription is deterministically terminal.
- **`Cell.ForceCleanup()` — guaranteed terminal callbacks on abort-during-OnInit**: when the App ctx is cancelled while an actor's OnInit is still running, Run's drain closes the ingress queues and the queued Stop/Destroy is never processed; terminate's timeout branch previously only gave up waiting, leaking handles opened during OnInit. The timeout branch now calls `ForceCleanup()` (synchronous, non-blocking, idempotent, exactly-once against the message path). **Upgrade note for embedders with persistence actors**: OnStop can now run before OnInit's Load completes — an unconditional full-state Save in OnStop will overwrite real records with the zero value (data erasure). Add a loaded-flag guard (skip persistence in OnStop unless Load's success path completed); Save itself stays ungated. Observed and fixed in sporemind (da5d932, 7 actors).
- **Compile-time execution-mode registration** (P1-5): `actor.RegisterStateful[Req, Resp]` / `actor.RegisterStateless[Req, Resp]` pin the handler's execution mode in the function signature — a closure whose context parameter doesn't match does not compile, replacing reflection's silently-inferred wrong-lane hazard. Thin sugar over the same pipeline; `ctx.Register` reflection inference remains for dynamic/scripted paths.
- **Benchmark baselines** outside handler/invoke/promise: wire-frame marshal/unmarshal (small/typical/64KiB/header-heavy; ~214ns small marshal, ~673ns typical), events ring `Push` (~48ns, zero-alloc) and store `Push` (~70ns), transport `Local.Send` (~41ns, zero-alloc) and `MemoryRemote.Send` buffered hop (~448ns incl. buffer-full retry).
- **Cross-host script-value normalization** (P2-13): `cell.ScriptNormalizeValue` — one script-visible shape for callable replies across the cell and gateway script hosts. The gateway previously handed raw `[]byte` (or typed structs the VM panics on) to scripts while the cell host returned a string under a codec; both now normalize identically (string under codec, JSON-decoded otherwise, structs → plain maps). Full host unification was evaluated and rejected (Context surfaces intentionally differ); the value contract is the part that must not drift.
- **WS timeout governance**: all eight gateway WS connection
  timeouts/buffers consolidated into `gateway.WSTimeouts`
  (`gateway.SetWSTimeouts`, `app.WithGatewayWSTimeouts`): ping
  interval, read-idle, write/ping-write deadlines, stall no-progress
  horizon, close drain timeout, overflow byte budget, outbound channel
  capacity. A ping write failure now force-closes the connection with
  a warning instead of exiting silently (zombie-session fix).
- **Events ring overflow observability**: `events.Ring.Evicted()`
  counts overflowed records; `gospore.events.stats` returns a
  `rings[]` array (per-actor evictions, worst first) so silent history
  loss is queryable.
- **Transport inbound capacity is parameterizable**:
  `transport.NewHTTPWithBuffer` / `NewMemoryRemotePairWithBuffer`
  (default `transport.DefaultInboundCapacity`), replacing the
  hardcoded 64-frame channels.
- **`app.App.GatewayReady()`** exposed on the public interface
  (previously only on the concrete implementation): a channel that
  closes when the gateway is listening.
- **Integration test suite** (`integration/gateway_test.go`): first
  cross-facet coverage — typed unary over WS and HTTP agreeing,
  app-level `WSTimeouts` proven to reach real connections (timer-driven
  ReadIdle drop of a non-ponging client), and transport + gateway
  hitting the same actor instance across two Apps.

### Fixed

- **web-client TBC magic stranding** (TS twin of the Go-side fix below, authored by Async Mushroom): `session.ts` still welded wire-version 2 into the 4-byte magic, so spore v0.6.0 frames (`TBC`) failed `isTBCPayload` and event subscriptions (self-describing fallback, no codegen schemaId) delivered raw `Uint8Array` to handlers — every pushed-event reader crashed while invoke replies stayed healthy. Preamble-only recognition + `TBC_HEADER_LEN` mirror the Go-side `codec.IsTBCData` semantics; test fixtures bumped to the current version byte. vitest 216/216, tsc clean.
- **TBC magic stranding on spore v0.6.0** (reported by Async Mushroom from sporemind round-trip failures): `codec.IsTBCData` and the gateway WS binary sniff welded wire-version 2 into a 4-byte magic, so v0.6.0-encoded frames (`TBC\x03…`) were rejected by gospore's own guard. Both now route on the 3-byte `"TBC"` preamble (version byte present); version semantics belong to the spore codec backend (v2 exists there only for precise rejection errors) — a future version bump can no longer silently strand the guard. Pinned by `TestIsTBCData_PreambleOnly`. Also bumps spore to v0.6.0 (registry consolidation — zero direct calls on our side; wireVersion transport+TS dual-sided).
- **Planner/`Final` cancellation cause preserved** (reported from a live sporemind pause incident): consuming-context cancellation surfaced as the bare `invoke.ErrCallCancelled` sentinel, defeating downstream `errors.Is(err, context.Canceled)` classification (a pause was misread as a tool failure / retryable stream error). `planner.Stream` now rejects with `ctx.Err()` when the sentinel arrives with a cancelled consumer context; `Final`'s translation no longer requires the fallback-deadline gate (the io.EOF branch keeps it — EOF is also the legitimate void-End shape). Pinned by `TestPlannerCancel_RejectsWithContextCause`.
- **`gateway.Server.ReadyChan` data race**: lazy channel initialization
  raced `Run`'s close; the channel is now created eagerly in
  `NewServer` and the method is documented concurrent-safe.
- **`events.StoreImpl.Push` called `RemoteSink.Push` while holding the
  global store lock**: a blocking network-backed sink serialised every
  actor's event writes behind one remote. Dispatch now snapshots under
  the lock and runs outside it; the `RemoteSink` contract documents
  concurrent, possibly-reordered invocation.
- **Scoped/global service registration errors** are decided in the
  `service` package (`ScopedRegistrationError`,
  `GlobalRegistrationError`); `tree` no longer re-derives the
  classification (single semantics source).

### Changed

- **`Cell.enqueueLoop` bounded patience no longer busy-waits**: the 20ms `time.Sleep` poll is now a ticker + deadline timer + `<-c.done` select — a dead cell (Run returned during teardown) bails out of a full custom lane immediately instead of burning the full 5s deadline against a handler that can never drain again.
- **Tier-1 file splits** (pure code movement, no behavior change): `app/app.go` 1875→437 lines (options / bootstrap / spawn / services / invoke), `internal/cell/run.go` 1395→329 (lanes / lifecycle / dispatch), `gateway/server.go` 992→334 (server_http / gatesession).
- **`waitForCellStart` polls no more**: spawn waits for OnStart via `Cell.StartDone()` (closed once on the first terminal start outcome; a panicked OnStart defers to the supervisor restart) instead of a 2ms re-check loop — removes per-spawn timer churn and up to 2ms latency. Error contract unchanged. sporemind audited zero coupling.

- **Events ring always-write** (user decision): lifecycle and callable records land in the ring during subscriber-free periods (gap replay across idle windows now works), and `ctx.EmitEvent` records a `WatchUserEvent` entry carrying the registered kind name (payload stays on the bus). New `actor.WatchUserEvent` kind + `events.Record.Event` field; `ValidWatchKind` now enumerates 12 kinds. A `RingStats`/`events.stats` `Evicted` audit is the compensating control for chatty actors.
- **Cell boot-drain data race fixed** (`TestSpawnInheritsParentRole` under `-race`): `handleStart` could `ownerWG.Add` + spawn `ownerLoop` after an immediately-cancelled `Run` had already drained (illegal Add-after-Wait; orphaned loop ranging a nil channel). The spawn now re-checks `ownerQClosed` under `ownerQMu`; `runCancel` access is mutex-guarded across the Run goroutine and the system lane. Pre-existing race, probability-shifted into view by always-write timing.

- `gateway/ws.go` (767 lines) split into `ws_frame.go` (protocol conversion, TBC sniffing, auth codec), `ws_frame_conn.go` (connection machinery: tuning, outCh/overflow, deadlines, drain) and the 67-line upgrade entrypoint — pure code movement, no behavior change.
- ARCHITECTURE.md reconciled with the above (new §7.6 WS lifecycle,
  §4.22 ring contract + known `HasSubscribers` deviation documented,
  §8.4 wire codegen reality, §4.20→4.23 renumbering closing the §4.21
  gap).

## v0.2.0 (2026-09-19)

Focus: correctness of the gateway script-interceptor path, dead-surface
removal, and a single wire-key contract for projections. This is a
breaking release relative to v0.1.x.

### Breaking changes

- **Projection wire keys are json-tag authoritative.** Snapshot keys and
  `projection.ApplyTo` now derive from the field's `json` tag (the
  schema-declared public name shared with TS clients); untagged fields
  keep the legacy `lowerFirst(GoName)` fallback; `json:"-"` fields are
  excluded from snapshots entirely. Consumers that relied on
  `lowerFirst` keys for tagged acronym fields (`ID` → `iD`) must read
  the declared name (`Id`) instead.
- **Removed package `frontgate/`.** Zero consumers in gospore and
  downstream; the gateway covers the unary front-door surface, auth,
  and interceptors.
- **Removed `plan.WithRetry` / `plan.WithCircuitBreaker` /
  `plan.WithPause`** and the corresponding `plan.Config` fields. These
  were documented no-ops; re-introduce when actually implemented.
- **`cell.ActorRef` now returns `invoke.Result` (with `cell.InvokeResult`
  values) instead of `(any, error)`**, so script hosts receive typed
  terminal outcomes (values, errors, dead refs) instead of guessing.

### Fixed

- **Gateway script interceptor** (`a38d667`):
  - Invocations no longer serialize on a single global lock; script VMs
    are pooled, so concurrent gateway calls run concurrently.
  - Script `Invoke` errors are no longer swallowed — outcomes flow back
    through a value channel shared by both host implementations.
  - Hard-coded 30s timeout replaced by the request deadline plus a
    configurable `GatewayInvokeTimeout`.
  - Dead `ActorRef` / closed stream dereferences from scripts no longer
    take down the VM.
- **Typed struct handler params over the script-adapter path**: root
  cause was resolver-less codecs (`codec.NewJSON()` used with
  `app.WithCodec`), which cannot resolve `schema.Set`-registered IDs.
  The trap is now documented on `codec.NewJSON`, `codec.NewBinary`, and
  `app.WithCodec`; the interceptor test suite pins the typed-struct
  contract against the default resolver-bound codec.

### Removed (dead code)

- `contracts/` (9 files, 21 interfaces, no runtime consumers)
- `model/` files only referenced by `contracts/`
- `internal/future` (zero references)
- `transport.Server` (interface with no implementation)
- `frontgate/` (see breaking changes)

### Documentation

- `ARCHITECTURE.md` rewritten against the actual codebase (English, with
  `ARCHITECTURE.zh-CN.md`); section numbering preserved so in-code
  citations (§4.17, §7.5, …) resolve.
- README / README.zh-CN package tables updated for removed packages.

## v0.1.0

Initial public release.
