# BoltDB Connection State Machine

> 每个 client connection 都是一个状态机。command legality 取决于当前 state，某些 command 会导致 state transition，某些 state 是 mutually exclusive 的。
> 本文档 formalize 这些规则。违反任意一条即为 bug。
> 所有代码变更必须保持这些 transition 规则，或在本文档中显式修订。

---

## 目录

1. [Connection States](#1-connection-states)
2. [State Transition Matrix](#2-state-transition-matrix)
3. [Guard Conditions](#3-guard-conditions)
4. [Lifecycle Integration](#4-lifecycle-integration)
5. [Invalid State Examples](#5-invalid-state-examples)
6. [Invariant Cross-Reference](#6-invariant-cross-reference)
7. [Implementation Notes](#7-implementation-notes)

---

## 1. Connection States

BoltDB 使用多维状态模型替代单一的 enum。状态由 `connState`（`internal/server/handler_core.go:23-63`）中的多个正交字段共同决定。

### 1.1 状态定义

| State | 判别字段 | 含义 |
|-------|---------|------|
| `NORMAL` | `subscriber == nil && !monitoring && !inTransaction` | 默认命令模式，可执行所有非模式受限命令 |
| `MULTI` | `inTransaction == true` | 事务队列模式，非控制命令排队等待 EXEC |
| `SUBSCRIBED` | `subscriber != nil` | PubSub 推送模式，运行独立事件循环 |
| `MONITOR` | `monitoring == true` | 监控流模式，运行独立事件循环 |
| `BLOCKING` | _无独立字段，见 §1.3_ | 阻塞等待外部事件 |
| `CLOSING` | _无独立字段，见 §1.4_ | 连接拆除 / 资源清理 |

### 1.2 互斥规则

| 互斥对 | 原因 |
|--------|------|
| `MULTI` + `SUBSCRIBED` | 事务的 request/response 语义与 PubSub 的 push 流语义不兼容 |
| `MULTI` + `MONITOR` | MONITOR 在 MULTI 中被排队，EXEC 时因不支持而报错 |
| `SUBSCRIBED` + `MONITOR` | 二者均为模式专用循环，实际实现中 PubSub 优先于 MONITOR |
| `BLOCKING` + `MULTI` | 阻塞命令在 MULTI 中排队（不阻塞），EXEC 时才实际执行 |

### 1.3 BLOCKING 语义

BLOCKING **不是 connState 上的持久字段**。Blocking 操作通过 `state.ctx` 传递到 store 层，在 store 的 channel 注册表中等待数据。

从 `NORMAL` 进入 BLOCKING：
- BLPOP / BRPOP / BLMOVE / BRPOPLPUSH / XREAD BLOCK 传入 `state.ctx`
- store 层 `registerBlockingPop` 注册等待 channel，然后 `select` 等待数据 / 超时 / ctx 取消

从 BLOCKING 退出（3 种路径）：

| 退出路径 | 目标状态 | 机制 |
|----------|---------|------|
| 数据到达 | `NORMAL` | `notifyBlockingPop` 通过 channel 发送结果 |
| 超时 | `NORMAL` | timeout channel 触发，调用 `unregisterBlockingPop` |
| 连接关闭 / CLIENT KILL | `CLOSING` | `state.cancel()` 触发 `ctx.Done()`，store 层退出 |

BLOCKING 是**瞬态**（transient），必须最终转换到 `NORMAL` 或 `CLOSING`。

### 1.4 CLOSING 语义

CLOSING **不是 connState 上的持久字段**。连接关闭通过 `state.cancel()` 触发，清理在 `handleConnection` 的 `defer` 中执行（`handler_core.go:457`）。

触发 CLOSING 的事件：
- `QUIT` 命令
- `CLIENT KILL` 外部触发
- 连接读错误（客户端断线）
- `Server.Shutdown()`

清理顺序（固定，见 C4）：
```
cancel() → unregisterConnection() → RemoveSubscriber() → unregisterMonitorClient() → 清理 WATCH → conn.Close()
```

---

## 2. State Transition Matrix

### 2.1 合法转换

| From | Command/Event | To | Guard | 文件:行 |
|------|--------------|----|-------|---------|
| NORMAL | MULTI | MULTI | 不在其他模式 | transaction_commands.go:7 |
| NORMAL | SUBSCRIBE | SUBSCRIBED | 不在 MULTI；不在 BLOCKING | pubsub_commands.go:handleSUBSCRIBE |
| NORMAL | PSUBSCRIBE | SUBSCRIBED | 同上 | pubsub_commands.go:handlePSUBSCRIBE |
| NORMAL | MONITOR | MONITOR | 不在 MULTI；不在 BLOCKING | handler_dispatch.go:992 |
| NORMAL | BLPOP / BRPOP / ... | BLOCKING | 不在 MULTI（排队）；不在 SUBSCRIBED/MONITOR | store/list.go:1443 |
| NORMAL | QUIT | CLOSING | — | handler_dispatch.go:100 |
| MULTI | EXEC | NORMAL | WATCH 脏检测通过；commands 非空时执行队列 | transaction_commands.go:26 |
| MULTI | DISCARD | NORMAL | — | transaction_commands.go:115 |
| MULTI | QUEUED | MULTI | 非控制命令入队 | handler_dispatch.go:70 |
| MULTI | QUIT | CLOSING | 不排队，直接执行 | handler_dispatch.go:72,100 |
| SUBSCRIBED | UNSUBSCRIBE (all) | SUBSCRIBED | **不自动退出 PubSub 模式** | pubsub_commands.go:handleUNSUBSCRIBE |
| SUBSCRIBED | QUIT | CLOSING | 返回 PubSubQuitSignal | pubsub_commands.go:QUIT |
| SUBSCRIBED | PING | SUBSCRIBED | — | pubsub_commands.go:PING |
| SUBSCRIBED | (P)SUBSCRIBE/(P)UNSUBSCRIBE | SUBSCRIBED | — | pubsub_commands.go |
| MONITOR | QUIT | CLOSING | 返回 PubSubQuitSignal | handler_core.go:runMonitorLoop |
| MONITOR | PING | MONITOR | — | handler_core.go:runMonitorLoop |
| BLOCKING | 数据到达 | NORMAL | store 通过 channel 通知 | list.go:1443-1463 |
| BLOCKING | 超时 | NORMAL | timeoutCh 触发 + unregisterBlockingPop | list.go:1494-1508 |
| BLOCKING | ctx 取消 | CLOSING | state.cancel() 触发 | list.go:1496,1503 |
| BLOCKING | CLIENT KILL | CLOSING | targetState.cancel() → ctx.Done() | client_commands.go:148 |
| CLOSING | cleanup 完成 | — | 连接 goroutine 退出 | handler_core.go:457 |

### 2.2 非法转换

这些 transition 会返回错误或被静默拒绝：

| From | Command | 结果 | 错误消息 | 文件:行 |
|------|---------|------|---------|---------|
| NORMAL | EXEC | ❌ 拒绝 | `ERR EXEC without MULTI` | transaction_commands.go:28 |
| NORMAL | DISCARD | ❌ 拒绝 | `ERR DISCARD without MULTI` | transaction_commands.go:117 |
| MULTI | MULTI | ❌ 拒绝 | `ERR MULTI calls can not be nested` | transaction_commands.go:9 |
| MULTI | WATCH（有排队的命令） | ❌ 拒绝 | `ERR WATCH inside MULTI is not allowed` | transaction_commands.go:131 |
| MULTI | 非控制命令 | ➡️ QUEUED（不拒绝，排队） | `QUEUED` | handler_dispatch.go:70 |
| MULTI | SUBSCRIBE | ➡️ QUEUED → EXEC 时报错 | `ERR command 'SUBSCRIBE' not supported in transaction` | handler_dispatch.go:1002 |
| SUBSCRIBED | MULTI | ❌ 拒绝 | `ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context` | pubsub_commands.go:PubSub allowlist |
| SUBSCRIBED | BLPOP | ❌ 拒绝 | 同上 | 同上 |
| SUBSCRIBED | EXEC | ❌ 拒绝 | 同上 | 同上 |
| SUBSCRIBED | SET/GET/... | ❌ 拒绝 | 同上 | 同上 |
| MONITOR | MULTI | ❌ 拒绝 | `ERR only PING / QUIT allowed in this context` | handler_core.go:runMonitorLoop |
| MONITOR | BLPOP | ❌ 拒绝 | 同上 | 同上 |
| MONITOR | SUBSCRIBE | ❌ 拒绝 | 同上 | 同上 |
| BLOCKING | MULTI | ➡️ QUEUED（排队等待 EXEC 时阻塞执行） | 阻塞命令在事务中不阻塞 | handler_dispatch.go:70 |
| BLOCKING | QUIT | ➡️ CLOSING（cancel 中断阻塞） | — | handler_dispatch.go:100 |

### 2.3 状态优先级

当多个状态同时激活（`inTransaction && subscriber != nil`）时，模式循环检查顺序决定实际行为（`handler_core.go:630`）：

```
SUBSCRIBED > MONITOR > NORMAL/MULTI
```

即：如果 `subscriber != nil`，连接进入 PubSub 循环，忽略 transaction 状态。MONITOR 仅在非 PubSub 时检查。

---

## 3. Guard Conditions

### 3.1 NORMAL → MULTI

```
MULTI 允许条件:
- inTransaction == false
- subscriber == nil（隐含，MULTI 在 PubSub 循环中被拒绝）
```

`transaction_commands.go:7`

### 3.2 MULTI → NORMAL (EXEC)

```
EXEC 允许条件:
- inTransaction == true
- watchedKeys 非空时，所有 watchedKeys 不在 dirtyKeys 中
- 执行队列中的每个命令，单个失败不中断整体
```

`transaction_commands.go:26`

### 3.3 MULTI → NORMAL (DISCARD)

```
DISCARD 允许条件:
- inTransaction == true
```

`transaction_commands.go:115`

### 3.4 NORMAL → SUBSCRIBED

```
SUBSCRIBE 允许条件:
- subscriber == nil
- inTransaction == false（隐含，SUBSCRIBE 在 MULTI 中被排队拒绝）
```

`pubsub_commands.go:handleSUBSCRIBE`

SUBSCRIBE 后 main loop 在下一轮 iteration 检测 `subscriber != nil` 并切换到 `runPubSubLoop`（`handler_core.go:632`）。

### 3.5 SUBSCRIBED 命令限制

```
PubSub 模式下允许的命令:
- SUBSCRIBE
- PSUBSCRIBE
- UNSUBSCRIBE
- PUNSUBSCRIBE
- PING
- QUIT
其他所有命令 → ERROR
```

`pubsub_commands.go:PubSub allowlist`

### 3.6 NORMAL → MONITOR

```
MONITOR 允许条件:
- monitoring == false
- args 为空
- inTransaction == false（隐含，MONITOR 在 MULTI 中被排队拒绝）
```

`handler_dispatch.go:992`

### 3.7 MONITOR 命令限制

```
MONITOR 模式下允许的命令:
- PING
- QUIT
其他所有命令 → ERROR
```

`handler_core.go:runMonitorLoop`

### 3.8 BLOCKING 进入条件

```
阻塞命令允许条件:
- subscriber == nil（PubSub 循环拒绝非 PubSub 命令）
- monitoring == false（Monitor 循环拒绝非 Monitor 命令）
- 实际阻塞在 store 层执行，使用 state.ctx 作为取消信号
```

进入 BLOCKING 时注册等待 channel，并 re-check 目标 key 避免 TOCTOU：
`list.go:1443-1463`

### 3.9 CLOSING 触发条件

```
任何路径:
- state.cancel() 调用（QUIT / CLIENT KILL / shutdown / 读错误）
- handleConnection 的 defer cleanup 按 FIXED ORDER 执行
```

---

## 4. Lifecycle Integration

### 4.1 连接生命周期

```
Accept → handleConnection
  │
  ├─ main loop (processRequest → executeCommand)
  │    │
  │    ├─ SUBSCRIBE/PSUBSCRIBE detected → switch to runPubSubLoop
  │    │    ├─ read commands from PubSub command channel
  │    │    ├─ select: message / command / flush / ctx.Done
  │    │    └─ QUIT or ctx.Done → exit PubSub loop → defer cleanup
  │    │
  │    ├─ MONITOR detected → switch to runMonitorLoop
  │    │    ├─ read commands from monitor command channel
  │    │    ├─ select: monitor message / command / flush / ctx.Done
  │    │    └─ QUIT or ctx.Done → exit Monitor loop → defer cleanup
  │    │
  │    ├─ MULTI: inTransaction gate queues commands
  │    │
  │    ├─ BLOCKING: store layer blocks on state.ctx
  │    │
  │    └─ QUIT / ctx.Done → break main loop → defer cleanup
  │
  └─ defer cleanup (FIXED ORDER):
       cancel()
       → unregisterConnection()
       → RemoveSubscriber() (if subscriber != nil)
       → unregisterMonitorClient() (if monitoring)
       → clear watchedKeys from watchMonitors
       → conn.Close()
```

### 4.2 阻塞操作生命周期

```
BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/XREAD BLOCK 被调用
  │
  ├─ 注册等待 channel 到 store（registerBlockingPop）
  ├─ re-check 目标 key（避免 TOCTOU）
  ├─ select {
  │    case <-resultCh:    → 数据到达 → 返回结果 → NORMAL
  │    case <-timeoutCh:   → 超时 → unregister → 返回 nil
  │    case <-ctx.Done():  → 取消 → unregister → 返回错误
  │  }
  └─ cleanup: unregisterBlockingPop（自动清理或 defer）
```

### 4.3 Shutdown 顺序

```
Server.Shutdown()
  │
  ├─ 1. Listener.Close()           → 停止接受新连接
  ├─ 2. cancelAll()               → 所有连接 ctx 取消
  ├─ 3. wait for conn goroutines  → 等待所有 handleConnection 退出
  ├─ 4. Replication.Stop()        → 停止复制
  ├─ 5. PubSub.Close()            → 关闭 PubSub 管理器
  └─ 6. Db.Close()                → 关闭 BadgerDB
```

---

## 5. Invalid State Examples

这些例子说明为什么某些状态组合是禁止的，而非"当前实现没支持"。

### 5.1 SUBSCRIBE 后 MULTI

```
SUBSCRIBE → MULTI

禁止原因:
PubSub 模式将连接语义从 request/response 改为 push 流模式。
MULTI 依赖请求-应答序列的原子性，这与 PubSub 的异步消息推送不兼容。
实现层面：PubSub 循环拒绝所有非白名单命令。
```

### 5.2 WATCH → SUBSCRIBE → EXEC

```
WATCH → SUBSCRIBE → EXEC

禁止原因:
WATCH 依赖连接级别的乐观锁（dirtyKeys 跟踪）。
进入 SUBSCRIBED 模式后，连接切换到 PubSub 循环，EXEC 永远不会被处理。
WATCH 注册的 key 仍在 watchMonitors 中，但 cleanup 仅在连接关闭时执行。
这是资源泄漏风险——WATCH 的注册在 PubSub 模式下成为悬空引用。
```

### 5.3 MONITOR → BLPOP

```
MONITOR → BLPOP

禁止原因:
MONITOR 是专用流模式，仅用于观察所有命令。
BLPOP 需要进入阻塞等待，这与 MONITOR 的观察语义冲突。
实现层面：MONITOR 循环拒绝非 PING/QUIT 命令。
```

### 5.4 MULTI → SUBSCRIBE

```
MULTI → SUBSCRIBE

禁止原因（分层）:
1. 协议层：事务在 EXEC 之前不会实际执行命令。SUBSCRIBE 作为事务命令排队。
2. 语义层：EXEC 时执行 SUBSCRIBE 会导致不可预测的副作用——事务内的订阅。
3. 实现层：executeQueuedCommand 的 default 分支返回错误。
这是"协议层禁止"而非"实现层不支持"。
```

### 5.5 BLOCKING + CLIENT KILL

```
NORMAL → BLPOP → CLIENT KILL (外部)

允许的原因:
CLIENT KILL 通过 targetState.cancel() 触发 ctx.Done()。
BLPOP 的 select 检测到 ctx.Done() 后退出阻塞。
目标连接进入 CLOSING，清理路径正常执行。
这是 cancellation invariant (K1-K7) 的设计核心。
```

### 5.6 NORMAL → MULTI → BLPOP → EXEC

```
NORMAL → MULTI → BLPOP → EXEC

语义:
BLPOP 在 MULTI 中被 QUEUED（不阻塞）。
EXEC 时逐个执行排队命令，BLPOP 此时才实际阻塞。
但 EXEC 在主 goroutine 中执行，BLPOP 的阻塞会导致 EXEC 返回延迟。
这是已知的语义折中——与 Redis 一致。
```

---

## 6. Invariant Cross-Reference

### 6.1 状态相关 Invariants

| 状态方面 | Invariant |
|----------|-----------|
| 清理顺序 | C4（固定 cleanup 顺序） |
| Cancel 幂等 | C2（cancel 最多调用一次） |
| Goroutine 隔离 | C3（每个连接恰好一个 goroutine） |
| MULTI 不可嵌套 | T2 |
| MULTI 命令排队 | T3 |
| EXEC 清空事务 | T7 |
| DISCARD 清空事务 | T8 |
| PubSub 消息不入复制 | P1 |
| PubSub 模式切换 | P2 |
| PubSub 受限命令集 | P3 |
| PubSub 退出 cleanup | P7 |
| Monitor 非阻塞广播 | M1 |
| Monitor 受限命令集 | M2 |
| 阻塞 ctx 响应 | K1 |
| 阻塞超时响应 | K3 |
| CLIENT KILL 使用 cancel | C8 |
| hold state.mu 时不 cancel | OW7 |
| 互斥规则：F1-F8 | F1-F8 |

### 6.2 模式循环对照

| 模式循环 | 所在文件:行 | 命令源 | 消息源 | 退出条件 |
|----------|------------|--------|--------|---------|
| main loop | handler_core.go:368 | 直接读取 bufio.Reader | N/A | ctx.Done / 读错误 |
| PubSub loop | handler_core.go:632 | cmdCh（独立 goroutine 读取） | subscriber.MessageCh | QUIT / ctx.Done |
| Monitor loop | handler_core.go:642 | cmdCh（独立 goroutine 读取） | monitorCh | QUIT / ctx.Done |

---

## 7. Implementation Notes

### 7.1 状态判别字段

BoltDB 使用多维状态而非单一 enum，原因：

1. **多轴正交性**: `inTransaction`（事务）、`subscriber`（PubSub）、`monitoring`（Monitor）可以同时存在（虽然后两者互斥），但单一 enum 需要组合值
2. **渐进式切换**: SUBSCRIBE 在 `executeCommand` 中设置 `subscriber`，但模式切换在 main loop 的下一轮 iteration 才发生。这意味着命令处理函数无法立即知道模式将会切换
3. **历史原因**: 项目从简单 KV 逐步演化，状态字段随功能增加而添加

### 7.2 优先级实现细节

```
handleConnection main loop (handler_core.go:630):

1. processRequest (可能设置 subscriber 或 monitoring)
2. 检查 subscriber != nil → 切换到 PubSub 循环
3. 检查 monitoring → 切换到 Monitor 循环
4. 否则继续 main loop（NORMAL / MULTI / BLOCKING）

关键: PubSub 检查在 Monitor 之前。
如果一个连接同时设置了 subscriber 和 monitoring，PubSub 优先。
当前代码中没有路径会同时设置二者，但实现保证如果发生，PubSub 胜出。
```

### 7.3 BLOCKING 实现的特殊考虑

BLOCKING 未作为显式状态字段存储，意味着：

1. `CLIENT LIST` 不会显示 `b` 标志
2. 无法通过 `CLIENT KILL TYPE` 筛选阻塞连接
3. blocking 状态的可见性仅限于 store 层的 channel 注册表

这对比 Redis 是一个已知的功能差距（Redis 7.0+ 支持 `CLIENT KILL TYPE` 和 `CLIENT LIST` 中的阻塞标志）。

---

## 版本化记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-05-17 | 初始版本：基于 handler.go, list.go, stream.go 实现提取的状态机形式化 |
| v1.1 | 2026-08-24 | `handler.go` 拆分 24 文件：全量更新 `文件:行` 列至 `handler_core/dispatch/transaction_commands/pubsub` 等；`CLIENT KILL` 死锁回归、`SORT` R7 等 8 轮巡检同步 |

### 核验命令

```bash
# 状态机行为由 state fuzz 持续验证
go test -race -timeout 120s ./cmd/integration/ -run TestFuzzServerStateMachineChaos
go test -race -timeout 120s ./cmd/integration/ -run TestFuzzServerBlockingKill
go test -race -timeout 120s ./cmd/integration/ -run TestFuzzServerSubscriberChaos
go test -race -timeout 120s ./cmd/integration/ -run TestFuzzServerConcurrentStateChaos

# 所有 invariant 保持
go test -race -short ./internal/...
```
