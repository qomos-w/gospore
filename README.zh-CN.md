# gospore

gospore 是一个 Go actor 平台，目标是让 actor 行为从「全 Go 编译产物」渐进演化到「全数据驱动」——actor、schema、类型化 payload 与脚本绑定运行在同一个运行时里。

[English](README.md)

> 状态：早期开发阶段。API 面已能支撑真实应用，但尚未冻结；允许不兼容变更，不提供兼容层。

## 为什么用 gospore

- **注入面最小**。actor 就是一个嵌入了 `actor.Host` 的 struct，可选实现 `OnStart`/`OnStop`。没有依赖容器，没有需要配置的基类。
- **一条规则决定执行模式**。handler 首参数是 `actor.Context` → stateful（在 actor 上串行）；`actor.PureContext` → stateless（并发，只读快照）。
- **CallID 即命名空间**。`greeter.greet` = actor 路径 + handler 名；同一个点分名字即总线上的 callable 名。
- **端到端类型化 payload**。非 `[]byte` payload 经 schema descriptor 编解码，脚本侧与 Go 侧对形状达成一致。
- **构造即可观测**。每个 actor 自带 projection 快照（带版本的状态 + delta ring）与调用/生命周期事件 ring。

## 安装

```bash
go get github.com/qomos-w/gospore
```

要求 Go 1.27+。

## 30 秒上手

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
)

// 1. 写 actor：嵌入 actor.Host，在 OnStart 注册 handler
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
	_ = ctx.Expose("greeter") // 注册 service，便于查找
	return nil
}

// 2. 启动并运行
func main() {
	a, _ := app.New(app.WithNamespace("demo"))

	_, _ = a.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &Greeter{}
	}), "greeter")

	ctx := context.Background()
	go func() { _ = a.Run(ctx) }()

	time.Sleep(50 * time.Millisecond) // 等 tree 启动完成

	greeterRef, _ := a.LookupService("greeter")
	call := greeterRef.Invoke(ctx, "greeter.greet", "world")
	resp, _ := call.Recv()
	fmt.Println(resp) // "hello world (#1)"
}
```

## 核心规则（就 3 条）

| 规则 | 说明 |
|------|------|
| 嵌入 `actor.Host` | 每个 actor 类型根层嵌入 `actor.Host`，获得 OnStart/OnStop/Register/Spawn/Expose 全部能力 |
| 第一参数决定模式 | `actor.Context` → stateful handler（串行，可改状态）；`actor.PureContext` → stateless handler（并发，只读） |
| CallID 即命名空间 | `greeter.greet` 中 `greeter` 是 actor 路径名，`greet` 是 handler 名；点分格式自动成为总线 callable 名 |

## 调用 actor

```go
// unary —— 等一个值
resp, err := a.Self().Invoke(ctx, "greeter.greet", "world").Recv()

// streaming —— 逐 chunk 消费到 EOF
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

## 运行时权限

调用权限由运行时 `PolicyStore` 独立控制，与注册解耦：

```go
a.PolicyStore().Reload([]actor.Policy{
	{Scope: "demo.greeter", Role: actor.Role("agent"), Allow: true},
})
```

## 脚本 handler

同一个 actor 可以混用 Go handler 与脚本 handler——caller 感知不到某个 CallID 背后是哪种实现：

```go
func (r *RiskActor) OnStart(ctx actor.Context) error {
	return errors.Join(
		ctx.Register("risk.calc", r.calc),
		ctx.RegisterScript("risk.score", scoreScriptSrc, actor.ModeStateless),
	)
}
```

## HTTP Gateway

把 actor 通过 REST 暴露给外部，拦截器可插拔：

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

状态码：`200` 成功、`204` 无内容、`403` 拦截器拒绝、`404` service 未找到、`405` 方法不允许、`500` actor 错误或拦截器内部故障。

## 包结构

| 包 | 职责 |
|----|------|
| `actor` | actor 核心：`Host`、`Context`/`PureContext`、`Props`、注册 |
| `app` | 装配入口：构建 tree、codec、schema set、service、policy |
| `tree` / `ref` / `supervisor` | actor 树、引用、监督 |
| `invoke` / `plan` / `mailbox` / `message` | 调用原语（tell/unary/stream）、plan 节点、邮箱、消息类型 |
| `promise` | future 式结果管道 |
| `discovery` | 跨进程 service 查找 |
| `codec` / `schema` / `contracts` | payload 编解码、类型描述符、线格式契约 |
| `events` / `projection` | 事件 ring 与带版本的状态投影 |
| `resource` / `service` | 资源生命周期、service 面 |
| `transport` / `gateway` / `frontgate` | 跨进程传输与网关 |
| `web-client` | 浏览器侧总线客户端 |
| `scriptbridge` | 把脚本 callable（spore）桥接为 actor handler |
| `model` / `id` / `internal` | 共享模型、ID 生成、内部实现 |

## 脚本化

行为可以从 Go 迁移到数据：schema 声明的 callable 用 spore 脚本实现（`ctx.RegisterScript`），与 Go handler 共享同一 CallID 命名空间、codec 与权限体系。语言层见 [spore](https://github.com/qomos-w/spore)。

## 许可证

[MIT](LICENSE)
