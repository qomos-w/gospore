# gospore

A Go actor platform designed so that actor behavior can evolve from fully
compiled Go toward fully data-driven definitions — actors, schemas, typed
payloads, and script bindings under one runtime.

[简体中文](README.zh-CN.md)

> Status: early development. The API surface is complete enough to build real
> applications but not yet frozen; breaking changes land without shims.

## Why gospore

- **Minimal actor surface.** An actor is a struct that embeds `actor.Host` and
  optionally implements `OnStart`/`OnStop`. No dependency containers, no base
  classes to configure.
- **One rule decides execution mode.** A handler whose first parameter is
  `actor.Context` runs stateful (serialized on the actor); `actor.PureContext`
  runs stateless (concurrent, read-only snapshot).
- **CallIDs are the namespace.** `greeter.greet` — actor path, handler name.
  The same dotted name becomes the callable name on the bus.
- **Typed payloads end to end.** Non-`[]byte` payloads are encoded and decoded
  through schema descriptors, so script and Go sides agree on shapes.
- **Observable by construction.** Every actor carries projection snapshots
  (versioned state + delta ring) and an event ring of calls and lifecycle.

## Install

```bash
go get github.com/qomos-w/gospore
```

Requires Go 1.27+.

## 30-second tour

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
)

// 1. Define an actor: embed actor.Host, register handlers in OnStart.
type Greeter struct {
	actor.Host
	count int
}

func (g *Greeter) greet(ctx actor.Context, req string) (string, error) {
	g.count++
	return fmt.Sprintf("hello %s (#%d)", req, g.count), nil
}

func (g *Greeter) OnStart(ctx actor.Context) error {
	_ = ctx.Register("greeter.greet", g.greet)
	_ = ctx.Expose("greeter") // register as a discoverable service
	return nil
}

// 2. Assemble and run.
func main() {
	a, _ := app.New(app.WithNamespace("demo"))

	_, _ = a.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &Greeter{}
	}), "greeter")

	ctx := context.Background()
	go func() { _ = a.Run(ctx) }()

	time.Sleep(50 * time.Millisecond) // wait for the tree to start

	greeterRef, _ := a.LookupService("greeter")
	call := greeterRef.Invoke(ctx, "greeter.greet", "world")
	resp, _ := call.Recv()
	fmt.Println(resp) // "hello world (#1)"
}
```

## Core rules

| Rule | Meaning |
|------|---------|
| Embed `actor.Host` | Every actor type embeds `actor.Host` at its root and gets OnStart/OnStop/Register/Spawn/Expose from it |
| First parameter decides the mode | `actor.Context` → stateful handler (serialized, may mutate); `actor.PureContext` → stateless handler (concurrent, read-only) |
| CallID is the namespace | `greeter.greet`: `greeter` is the actor path, `greet` the handler; the dotted form is the callable name everywhere |

## Calling actors

```go
// unary — wait for one value
resp, err := a.Self().Invoke(ctx, "greeter.greet", "world").Recv()

// streaming — consume chunks until EOF
stream, _ := a.Self().Invoke(ctx, "greeter.subscribe", since)
for {
	chunk, err := stream.Recv()
	if err == io.EOF {
		break
	}
	// handle chunk
}

// fire-and-forget
a.Self().Invoke(ctx, "greeter.ping", nil).Close()
```

## Runtime policy

Call permissions live in a runtime `PolicyStore`, decoupled from registration:

```go
a.PolicyStore().Reload([]actor.Policy{
	{Scope: "demo.greeter", Role: actor.Role("agent"), Allow: true},
})
```

## Script handlers

Go and script handlers mix inside one actor — callers cannot tell which
implementation a CallID resolves to:

```go
func (r *RiskActor) OnStart(ctx actor.Context) error {
	return errors.Join(
		ctx.Register("risk.calc", r.calc),
		ctx.RegisterScript("risk.score", scoreScriptSrc, actor.ModeStateless),
	)
}
```

## HTTP gateway

Expose actors over REST with interceptors:

```go
type auditInterceptor struct{}

func (a *auditInterceptor) Before(ctx context.Context, req *gateway.GatewayRequest) error {
	if req.Role != "agent" {
		return gateway.ErrGatewayDenied // 403
	}
	return nil
}

func (a *auditInterceptor) After(ctx context.Context, req *gateway.GatewayRequest, resp *gateway.GatewayResponse) {
	log.Printf("%s %s %v", req.CallID, req.CustomerID, resp.Duration)
}

a, _ := app.New(
	app.WithNamespace("demo"),
	app.WithRootActor(func() actor.Actor { return &Greeter{} }),
	app.WithGatewayHTTP(":8080", &auditInterceptor{}), // nil interceptor = no-op
)
```

```bash
curl -X POST http://localhost:8080/api/greeter.greet \
  -H "Content-Type: application/json" \
  -H "X-Role: agent" \
  -d '"world"'

curl http://localhost:8080/schema
```

Status codes: `200` ok, `204` no content, `403` interceptor denied, `404`
service not found, `405` method not allowed, `500` actor or interceptor error.

## Package map

| Package | Role |
|---------|------|
| `actor` | Actor core: `Host`, `Context`/`PureContext`, `Props`, registration |
| `app` | Assembly entry: builds tree, codec, schema set, services, policy |
| `tree` / `ref` / `supervisor` | Actor tree, references, supervision |
| `invoke` / `plan` / `mailbox` / `message` | Call primitive (tell/unary/stream), plan nodes, mailboxes, message types |
| `promise` | Future-like result plumbing |
| `discovery` | Service lookup across processes |
| `codec` / `schema` / `contracts` | Payload codecs, type descriptors, wire contracts |
| `events` / `projection` | Event rings and versioned state projections |
| `resource` / `service` | Resource lifecycle, service surface |
| `transport` / `gateway` | Cross-process transports and gateways |
| `web-client` | Browser-side client for the bus |
| `scriptbridge` | Bridges script callables (spore) into actor handlers |
| `model` / `id` / `internal` | Shared models, ID generation, internals |

## Scripting

Behavior can move from Go into data: schema-declared callables implemented in
spore script register beside Go handlers (`ctx.RegisterScript`) and share the
same CallID namespace, codec, and policy. See the
[spore](https://github.com/qomos-w/spore) language layer.

## License

[MIT](LICENSE)
