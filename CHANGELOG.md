# Changelog

## Unreleased

### Added

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
