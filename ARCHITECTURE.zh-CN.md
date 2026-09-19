# gospore 架构

> gospore 是建立在 [spore](https://github.com/qomos-w/spore) 协议层之上的 actor 编排引擎。本文档描述其设计、组件、契约与连线方式。
> 英文版见 [ARCHITECTURE.md](ARCHITECTURE.md)。

---

## 项目定位

> **gospore 让 actor 行为从"全 Go 编译产物"渐进演化到"全数据驱动"。**

任意 gospore actor 在生命周期中可处于光谱任一位置且可逆：仅 Go handlers → Go + 少量 script handlers / component → Definition + dynamic component。这是配置内容的差异，不是 actor 物种的切换——所有 actor 共享同一套统一宿主能力（script runtime、reload 协议、Definition 接入面）。

统一规则：

- 所有 actor 默认具备 Host 能力；script / Definition / dynamic component 是否实际启用由配置内容决定。
- 空配置是合法一等状态。
- Go 仍是一等热路径：Go callID 直达 Go invoker，不经过 script runtime。
- pipeline / mailbox / goroutine 等并发细节是宿主内部实现，不暴露为用户 API。

## 0. 摘要

### 0.1 速查

```go
// 启动一个 App
app, _ := app.New(app.WithNamespace("auth"), app.WithRootActor(&MyActor{}))
app.Run(ctx)

// 最小 actor
type MyActor struct{ actor.Host }
func (a *MyActor) OnStart(ctx actor.Context) error { return nil }
func (a *MyActor) OnStop(ctx actor.Context) error  { return nil }
```

- 有状态 handler：`func(ctx actor.Context, req *Req) (*Resp, error)`；无状态：`func(pctx actor.PureContext, req *Req) (*Resp, error)`。
- 流式：追加 `Emitter[*T]` 参数。
- Call ID 三条铁律：点分两段以上；第一段 = App.Namespace；`app.*` 仅 root 可注册，`_gospore_.*` 为框架内部保留。
- 调用：`ref.Invoke(ctx, "ns.method", &req)` 返回 Stream，`Recv()` 消费。
- 观察当下状态：`gospore.projection.watch(actor_id, since_version)`；观察历史动作：`gospore.events.subscribe_instance(actor_id, kind)`。

### 0.2 核心设计点

1. gospore 是 actor 编排引擎：单一进程内 actor 生命周期、统一 Invoke、父子层级与监督。一个进程 = 一个 App = 一个 namespace。
2. gospore 不是网络框架、数据库、Web 服务器或配置中心。协议层完全复用 spore（`spore/transport.Codec`、`spore/identity.CanonicalID`、`spore/schema.TypeDesc`）。
3. 统一 Invoke：fire-and-forget / 单返回 / 流式返回是同一调用的三种消费形态。
4. ActorTree + App 即 Root：全部 actor 单棵树，关闭 App 等价于级联 stop 整棵树。
5. Handler 注册式 actor：在 `OnStart` 内 `ctx.Register(callID, fn)`，而非 `Receive(msg)`。
6. 双通道可观察：每个 actor 暴露 State Channel（projection，§4.18）与 Event Channel（events，§4.23），机制对称、支持 `since` 断线补全。
7. 调用即子 actor：`ctx.Plan(...)` 把一次调用包成 child actor（§4.22）。

## 1. 设计原则

- **承自经典 actor 模型**：树形监督、mailbox 串行化、System Lane 语义。
- **有意不做弱类型机制**：无类型 PID、字符串 topic、集群 gossip——gospore 用 SchemaID + namespace 的强类型 wire 契约替代。
- **承接自 spore**：schema 描述（TypeDesc/CallableDesc）、二进制/JSON codec、CanonicalID 128 位身份、script runtime 与 binding。
- **gospore 自建**：Cell 运行时、Handler 表、Tree/Supervisor、Service/Resource 注册、Projection/Events 双通道、Plan、App 组装与 Gateway。

## 2. 总体架构

### 2.1 分层

```
用户 actor (Go handlers / script handlers / components)
        │ Register / Plan / Watch / Subscribe
   actor.Context  ── actor.PureContext（无状态视图）
        │ 由 internal/cell 提供实现
   Cell (internal/cell)  ←── Handler Table (internal/handler)
        │ 持有: events ring / projection slots / supervisor / timerMeta
   App (app)  = Tree root + 组装根（SchemaSet、Codec、Service/Resource、InvokeTable、Gateway）
        │
   Transport (local / memory-remote / HTTP + Router)  ── Frame (message)
        │
   spore v0.1.0（schema / codec / identity / script / binding）
```

### 2.2 关键不变量

1. Cell 内 handler 串行：stateful handler 只在 cell goroutine 执行；stateless handler 可 fork 并发。
2. Frame 是唯一 wire 形态：`(SchemaID, PayloadMode, Encoding)` 自描述，对端经 SchemaSet Import 即可解码。
3. `_gospore_.script_reload` 在 cell goroutine 内串行替换，保留 component 状态，失败回滚（§4.20）。
4. 关闭语义 LIFO：App 关闭按树深度逆序 teardown 全部 actor。
5. 同 callID 跨 actor 必须签名 + 模式一致（§4.6 / §4.12 一致性规则）。

## 3. 包布局

| 包 | 职责 |
|---|---|
| `actor` | 用户 API：Context/PureContext、Host、Props、Register、Watch、Subscription、reload 协议类型、诊断码 |
| `app` | 进程级组装根 + root actor；App 选项、manifest 导出、系统 callable（events/projection）、网关装配 |
| `internal/cell` | actor 运行时：队列、系统消息处理、Handler 调度、脚本宿主、投影/事件挂载 |
| `internal/handler` | 反射注册管线与 invoker 闭包（§4.12） |
| `internal/collections` | 通用容器工具 |
| `mailbox` | Envelope / SystemMsg / ErrFull 等 ingress 类型（队列实现已并入 internal/cell） |
| `message` | Frame 线格式与 Kind 枚举 |
| `invoke` | PendingTable、Call/Stream 客户端原语 |
| `promise` | 通用一次性/流式 Promise |
| `ref` | Actor 引用与 Invoke 入口 |
| `id` | ActorID（=spore CanonicalID）、CorID、Identity/Role |
| `schema` | SchemaSet、manifest 导入导出、builtin 系统协议、诊断码 |
| `codec` | Frame payload 编解码入口 |
| `transport` | Transport 接口、local / memory-remote / HTTP 实现、Router |
| `tree` | actor 树结构与作用域服务注册 |
| `supervisor` | one-for-one 监督决策 |
| `service` / `resource` / `discovery` | 三类注册中心（§4.15–§4.16） |
| `events` | Event Channel 存储（Ring + Store + RemoteSink） |
| `projection` | State Channel 存储（fingerprint + DeltaWindow） |
| `plan` | Plan Node（调用即子 actor） |
| `scriptbridge` | spore Capability 静态绑定（§4.19） |
| `gateway` | HTTP/WS 边界、鉴权与拦截器（frontgate 已并入） |
| `cmd/gospore-gen-ts` | manifest → TS client codegen |
| `web-client` | TypeScript 客户端 SDK（非 Go 包） |

`integration/` 是跨包子系统集成测试的家（§8.4）。以下历史层已删除：`contracts/`（未被消费的契约接口层）、`model/`（仅被 contracts 引用的公共类型层）、`internal/future/`（零引用，与 promise 重叠）、`transport.Server`（无实现的占位接口）。

## 4. 核心类型与契约

### 4.1 身份与地址

`id.ActorID` 即 `spore.identity.CanonicalID`（128 位），跨进程稳定。`id.CorID` 标识一次调用链（CorIDGenerator 含 timestamp-slot）。`id.Identity{Role, Kind}` 携带调用方角色；Role 支持点分隔继承（`agent.coder` 有效继承 `agent`），`Effective()` 按字符串前缀解析。网关入站从 `gospore.caller_role` / `gospore.caller_kind` header 重建 Identity。

### 4.2 Actor 与 Context

- `actor.Actor` 接口：`OnInit / OnStart / OnStop / OnDestroy`。嵌入 `actor.Host` 获得统一宿主能力。
- 嵌入链规则：actor struct 的嵌入链必须恰好看到一对 Start/Stop——`actor.Host` 通过 `DiagEmbedChain` 类诊断拒绝重复/缺失嵌入。
- `actor.Context` 是 cell goroutine 内的完整操作面（Register/Spawn/Watch/Plan/Call/Emitter/Expose/...）；`actor.PureContext` 是无状态 handler 的受限视图（禁止写状态）。
- Cell 为生命周期阶段提供受限 Context 实现（init/start/stop 阶段各自动拒绝越权操作，例如 OnStart 外 Register 返回 `DiagRegisterOutsideStart`）。
- 管道与并发细节不进 Context 面。

### 4.3 Streaming

流式 handler 签名追加 `Emitter[*T]`；Emitter 按 chunk 推送、`Final` 收尾。wire 上表现为同一 CorID 的一串 Frame（chunk + final），消费端 `Stream.Recv()` 以 `io.EOF` 终止。

### 4.4 Ref

`ref.Ref` 是 actor 地址句柄，`Invoke(callID, payload)` 返回 Stream。Ref 由 Cell/App 在 spawn 时铸造；`App.RemoteRef(actorID)` 构造跨 App 引用（自动走 remote transport）。Ref 不可用户伪造。

### 4.5 Stream（流式客户端）

`Stream.Recv()` 阻塞取下一帧；unary 是"一次 Recv"的特例。Stream 终态缓存：首次 KindEnd 后固定返回 EOF。

### 4.6 Schema Set（namespace + 32-bit ID）

`schema.Set` 维护 namespace → 名字 → 32-bit SchemaID 的全局唯一分配。同 callID 跨 actor 必须签名 + 模式一致，注册期由一致性规则（§4.12 step 5）强制。manifest（`schema.Manifest`）承载跨进程分发；`ImportFromManifest` 完成对端导入。`schema.StructRef(name)` 构造按名引用的 struct TypeDesc。builtin 表（`schema/builtin.go`）登记系统协议（auth/sysmsg/eventbus/projection watch 等）的固定 SchemaID。

### 4.7 Codec

`codec` 入口选择 spore JSON/二进制 codec。Frame 的 `Encoding` 字段声明 payload 编码；`PayloadMode` 区分 Value（body 即值）与 Raw（body 是序列化字节流）。

### 4.8 Frame（消息线格式）

`message.Frame`：`(Kind, CallID, SchemaNS, SchemaID, Headers, PayloadMode, Encoding, Body)`。Kind 枚举覆盖 call/chunk/final/cancel/watch 等；部分字段仅特定 Kind 使用。TS 端实现于 `web-client/src/binary_frame.ts`（GSF 二进制形态由 gateway/frame_binary.go 定义）。

#### 4.8.1 Transport 接口

`transport.Transport.Send(Frame)` 是发送面；实现有 `local`（同进程直投）、`memory_remote`（跨 App 测试桩，可丢帧）、`HTTP`（生产跨进程，同步 POST）+ `Router`（slot → 目标地址，Static/Dynamic 两态）。connproxy 将外部连接折叠为 Frame 流。生产级连接服务器由宿主（gateway）承担。

### 4.9 Mailbox（含 System Lane）

mailbox 类型层（Envelope/SystemMsg）描述进入 Cell 的信封；实际队列实现位于 `internal/cell`：每个 Cell 三条 lane——owner（用户消息，默认 256 容量）、system（系统消息，64）、reply（回复，64），各自独立 channel，系统 lane 永不因用户消息积压而饥饿。`mailbox.ErrFull` 是背压 sentinel；lane 深度与高水位（75%/90%）经 `CellRuntimeStats` 暴露。

### 4.10 系统消息

`mailbox.SystemMsg`：Init/Start/Idle/Stop/Destroy/Suspend/Resume/Restart 等生命周期信号沿 system lane 串行处理。

### 4.11 Cell

`internal/cell.Cell` 是单 actor 的自治运行时：持有三条 lane 的消费循环、Handler 表、watchers、投影槽位、事件推送、supervisor 决策入口与 script 宿主。Cell 由 App 在 spawn 时构造，App 通过 `cell.Config` 注入运行所需回调（spawn/stop/destroy/deliver/routeReply/service 与 discovery 查询等）——这是已知的结构性债务：Cell 目前是"服务定位器"形态，收敛方向是单一 Host 视图接口，但不改变其对外语义。

### 4.12 Handler 表 + Invoker

`internal/handler.Table` 是 per-cell callID → invoker 索引。Register 管线：

1. 反射解析函数签名（参数/返回值形状）；
2. 按第一参数分类执行模式：`actor.Context` → stateful，`actor.PureContext` → stateless；
3. 形状校验：允许 unary / streaming × 两种模式共 4 类签名，违规返回 `DiagHandlerShape` 类诊断；
4. 经 spore `DescribeGoStruct` 生成 Req/Final 类型描述并入 SchemaSet（RegisterAuto）；
5. **一致性规则**：签名 ↔ 已注册 schema 逐字段等价（同 callID 跨 actor 必须签名一致），不等价拒绝注册；
6. 构造 invoker 闭包（run_closure）装入 Table，写锁互斥。

Lookup 无锁读热路径；`Desc` 面向 manifest 导出。

### 4.13 Supervisor

`supervisor` 实现 one-for-one：child panic/error 触发 Decision（重启或停止），由 Cell 的 recover-and-decide 路径执行。监督不跨层级——父只处理直接 child。

### 4.14 Tree

`tree` 维护父子关系与 spawn 阶段（Reserve → Build → Abort，见 §4.17），承载 scoped service 的父子作用域查找（`RegisterScopedService` / `LookupScopedService`）。

### 4.15 Service Registry（三层解析模型）

Service 查询遵循三层地址模型：**Service 名（角色）→ InstanceGroupID（实例组）→ ActorID（具体实例）**。`service.Registry` 管 name → ref 的全局唯一映射；`tree` 管 owner→name→ref 的作用域映射（`ErrScopedConflict` 定义在 service 包、实施在 tree 层）；`discovery.Provider` 提供跨进程实例发现（TTL 租约，memory 实现用于测试）。`gospore.app.lookup_service` / `lookup_path` 系统 callable 暴露解析结果。

### 4.16 Resource Registry

`resource.Registry` 是 freeze 语义的 any→any 注册中心：App 启动后期 freeze，此后写入被拒绝。用于放置非 actor 单例（DB pool、HTTP client 等）。

### 4.17 App（Root Actor + 进程级入口）

App 是组装根与 root actor 的合一：

- **启动时序**：构造（选项解析、SchemaSet/Codec/注册中心装配、root cell 构建）→ root OnInit → 顺序 Start（`orderedStart`：children 按 spawn 深度有序 deliverStart；`WithAsyncStart` 可放宽）→ 网关就绪信号。子 spawn 走三阶段分配器：**Reserve**（占用 ID 与名字，非阻塞）→ **Build**（真正构建 cell）→ **Abort**（失败时回滚占位）。
- **保留命名空间**：`app.*` 仅 root 可注册（系统 callable：events/projection/stats 等）；`_gospore_.*` 为框架内部。
- **关闭**：ctx 取消 / Shutdown / root 升级 → LIFO teardown 整棵树后返回。
- App 直接 import 全部内部包完成装配；这是组装根的职责边界，不是分层违规。

### 4.18 Projection（State Channel）

每个 actor 的状态投影：`ScanComponents` 扫描 `gospore:"component"` tag 字段 → fingerprint（Bytes/Hash/PerField 三模式，全局单选）比较 → 变化才发布 delta。`DeltaWindow`（默认 64）保留近期版本，`gospore.projection.watch(actor_id, since_version)` 支持 live-only / replay-then-live / gap 三种补全 regime。快照求值有 panic 防护。wireName 以 Go struct 的 json tag 为权威（`wireFieldName` 语义），保证跨语言字段名稳定。

### 4.19 ScriptBridge（spore Capability 适配）

ScriptBridge 把宿主能力以 spore Capability 组暴露给脚本，五个命名空间：

| 命名空间 | 方法（常量定义于 scriptbridge/methods.go） |
|---|---|
| `gospore.projection` | get / field / watch |
| `gospore.events` | subscribe_service / subscribe_instance / recent |
| `gospore.app` | lookup_path / lookup_service / tree |
| `gospore.policy` | check / version |
| 动态前缀 | 每个用户 callID 生成一个 PrefixInvoke 方法 |

方法 ID 是 wire 常量，codegen 与 spore `binding.RegisteredCapability` 共用单一来源。

### 4.20 ScriptActor（统一宿主 + 数据驱动层）

热更新协议（常量 `actor.CallIDReload` / `actor.CallIDReplace`）：

**`_gospore_.script_reload`（兼容扩展，失败即拒绝、状态保留）**

| 变更 | 结果 |
|---|---|
| 新增 handler / component | 允许，串行装表 |
| handler 源码变化（模式不变） | 允许 |
| Meta 变化 | 允许，刷新投影元数据 |
| 删除/改名 handler 或 component | **拒绝**（`DiagIncompatibleReload`） |
| handler 模式变化 | **拒绝** |

**`_gospore_.script_replace`（破坏式）**：任意变更允许，但必须显式 `ReplaceReq{DropState: true}`——防误删保护即该显式标志本身；执行后 script/动态层状态清零，Go 字段保留。

`ReloadReq.Definition` 为 nil 非法。校验分两层：`actor.ValidateDefinition`（纯数据）+ Cell 层 `ClassifyReloadDiff`（新旧对比）后在 cell goroutine 内串行 swap，失败回滚。

### 4.22 Plan Node（一次调用即子 actor）

`ctx.Plan(target, callID, payload)` 把一次调用包装为 child actor（planNodeActor）：获得独立监督、Watch、投影、取消能力，短生命周期（Invoke 结束即回收）。`plan.State` 五值生命周期；`plan.Option` 中 Retry/CircuitBreaker/Pause 为 Phase 2 占位（当前 no-op）。

### 4.23 Events Channel

每 actor 一个 `events.Ring`（默认 256，`WithEventRingCapacity` 覆盖）：invoke / lifecycle / watch 元数据 Record 按 SeqNo 追加。`subscribe_service` / `subscribe_instance` 订阅；`since` 参数实现断线补全，客户端在 live-only / replay-then-live / gap 三 regime 间自动切换。Record 不内嵌 actor 身份（订阅维度已决定归属）。RemoteSink 是跨进程推送的预留缝（非阻塞契约由实现方保证）。

### 4.24 Timer Wheel（定时器基础设施）

`AppContext.After(delay, callID, payload)` 注册延迟自调用：Cell 以 `timerMeta`（callID → 路由元数据）登记，到期后按普通 Invoke 投递给 Self。停止后的 Cell 拒绝新的 After（`DiagEventAfterStop`）。

## 5. 调用语义（Invoke 唯一入口）

### 5.1 三种消费形态 × 两种执行模式

fire-and-forget / 单返回 / 流式 = 客户端消费差异；stateful / stateless = 服务端执行差异。两轴正交。

### 5.2 帧序列细节

unary：call → final。streaming：call → chunk* → final。取消：cancel 帧。服务端错误以 final(Err) 表达。

### 5.3 服务端调度

Frame 到达 Cell → system/owner lane 分流 → Table.Lookup → stateful 走 cell goroutine 串行，stateless fork 并发 goroutine（上限与水位见 §4.9）→ Emitter/chunk 经 reply lane 回投。

### 5.4 客户端调度

`invoke.PendingTable` 登记每个在途 CorID 的投递通道；Deliver 按 CorID 命中分发（Unregister/Register 间存在窄竞态窗口，以 CorID 单调分配规避复用）。超时在 Call 层以 context 驱动；stalled slot 由 Flush 路径回收。

### 5.5 反压

三条 lane 有界；owner lane 满即 `mailbox.ErrFull` 上抛调用方；慢消费者只反压自身 cell 的 reply 管线，不拖累全局。

## 6. 监督协议

- 失败检测点：handler panic（recover-and-decide）、OnInit/OnStart 返回错误、watch 上报的 child 终止。
- Decision：one-for-one——重启（重建 cell、状态按 §4.20 语义处理）或停止并升级。
- Watch/Terminated：`ctx.Watch(target)` 默认收 Terminated，`WatchKind` 位掩码扩展更多事件；watch 与 Event Channel 独立（§4.23 是拉/订阅流，watch 是定向通知）。

## 7. 与 spore 的协作边界

### 7.1 引用方向

gospore → spore 单向依赖，版本对齐已发布的 `github.com/qomos-w/spore v0.1.0`。spore 不感知 gospore。

### 7.2 Transport 边界

spore 提供 codec 与二进制帧编解码；gospore 的 transport 只搬运 Frame，不理解 schema 语义。跨进程路由（Router/HTTP/RemoteRef）为 gospore 侧实现。

### 7.3 类型 + Callable 注册

gospore 用 `spore.DescribeGoStruct` 产出 TypeDesc；`binding` 侧 Capability 注册由 scriptbridge 承接（§4.19）。

### 7.4 PolicyStore 调用链路

`gospore.policy.check/version` 经 ScriptBridge 进入 actor.PolicyStore（默认实现按 Role 继承链 + visibility 决策；默认拒绝）。

### 7.5 诊断码

诊断码是字符串常量，命名空间 = 包前缀，全表单一来源为各包 `diag.go`：`actor.Diag*` / `app.Diag*` / `cell.Diag*` / `handler.Diag*`（纯溢出、无态写入等）/ `invoke.Diag*` / `mailbox.Diag*` / `message.Diag*` / `schema.Diag*` / `service.Diag*` / `plan.Diag*` / `projection.Diag*` / `resource.Diag*` / `tree.Diag*` / `events.Diag*`。`MapErrToDiag` 模式在各包提供 err → Diag 映射。新增诊断码必须先入对应 diag.go 再使用。

## 8. 公共契约 vs 内部实现

### 8.1 公共语义契约（不可变）

actor.Context/PureContext 方法面、Frame 线格式、SchemaID 分配规则、reload/replace 语义、`app.*`/`_gospore_.*` 保留命名空间、双通道 watch/subscribe 参数。

### 8.2 公共 SPI（可被替换）

`discovery.Provider`、`events.RemoteSink`、`actor.PolicyStore`、`actor.Logger`、`actor.Clock`、`transport.Transport`、`gateway.Interceptor`。

### 8.3 内部实现（可重写）

`internal/cell`（含队列循环、调度、script 宿主装配）、`internal/handler`（反射管线）、`internal/collections`。这些层可在不触碰 §8.1/§8.2 的前提下重写（已知结构性债务见 §4.11）。

### 8.4 测试布局

单包测试随包；跨包子系统级测试在 `integration/`；gateway 的端到端行为测试在 `gateway/*_test.go`；TS 客户端契约测试在 `web-client/src/*.test.ts`。Go↔TS 契约（帧格式、系统协议常量）当前为双侧手写 + codegen 部分覆盖，改 builtin 系统协议 ID 时必须双侧同步（已知债务）。

## 9. 出局 / 不做

- 不做集群 membership / gossip（跨进程走 HTTP transport + discovery Provider 扩展点）。
- 不做字段级投影过滤（整快照 + fingerprint）。
- 不做用户可见的 mailbox/pipeline API。
- 不做无类型调用：一切 callID 必须有 schema。
- 生产级 transport 连接服务器（TCP/QUIC 监听面）不在 gospore——由 gateway（WS/HTTP）承担入口。

## 10. 名词表

| 术语 | 含义 |
|---|---|
| App | 进程级组装根 + root actor，单 namespace |
| Cell | 单 actor 运行时（internal/cell） |
| Lane | Cell 内三类队列：owner/system/reply |
| Frame | wire 消息单元（message.Frame） |
| State Channel | 投影通道：当前状态 + delta（§4.18） |
| Event Channel | 事件通道：历史动作 Record（§4.23） |
| Plan | 把一次调用包成 child actor 的机制（§4.22） |
| Definition | actor 的数据驱动描述（handlers/components/meta），reload 的载荷 |
| Capability | spore binding 暴露给脚本的方法组（§4.19） |
| wireName | 投影字段的跨语言权威名（Go json tag） |
