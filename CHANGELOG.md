# Changelog

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
