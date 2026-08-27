# BoltDB System Invariants

> 正式化的系统行为契约。违反任意一条即为 bug。
> 所有代码变更必须保持这些不变式，或在本文档中显式修订。

---

## 目录

1. [Replication Invariants](#1-replication-invariants)
2. [Backlog Invariants](#2-backlog-invariants)
3. [Connection Lifecycle Invariants](#3-connection-lifecycle-invariants)
4. [Cancellation Invariants](#4-cancellation-invariants)
5. [Transaction Invariants](#5-transaction-invariants)
6. [PubSub Invariants](#6-pubsub-invariants)
7. [Monitor Invariants](#7-monitor-invariants)
8. [Output Buffer Invariants](#8-output-buffer-invariants)
9. [Ownership & Concurrency Invariants](#9-ownership--concurrency-invariants)
10. [Store Layer Invariants](#10-store-layer-invariants)
11. [Forbidden Patterns](#11-forbidden-patterns)

---

## 1. Replication Invariants

### 1.1 Offset Semantics

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| R1 | `masterReplOffset` 严格单调递增，永不为负 | `replication.go:90` `IncrementReplOffset` | 复制状态不一致 |
| R2 | `masterReplOffset` == `backlog.internalOffset` 始终相等 | `replication.go:194-211` Append 与 IncrementReplOffset 配对调用 | 部分同步偏移量计算错误 |
| R3 | 复制偏移量增量 == `len(serialized RESP)` | `replication.go:211` `IncrementReplOffset(len(cmdBytes))` | 主从 offset 不同步 |
| R4 | 每个 PropagateCommand 调用恰好增加一次 offset | `replication.go:179-212` | offset 漂移 |

### 1.2 Propagation Rules

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| R5 | 写命令传播条件：IsMaster && isWriteCommand && 非复制控制命令 | `handler_core.go:709` `shouldProp` / `replication_helper.go:112` `isWriteCommand` | 从节点收到无关流量 |
| R6 | 复制控制命令（REPLICAOF, PSYNC, REPLCONF）永不传播 | `replication_helper.go:124` `shouldPropagateCommand` | 复制循环 |
| R7 | 只读命令永不传播 | `replication_helper.go:112` `isWriteCommand` | 带宽浪费 |
| R8 | MULTI 队列中的写命令在 EXEC 前**不传播** | `transaction_commands.go:7` `handleMULTI` 无传播逻辑 | 从节点事务语义不正确 |
| R9 | EXEC 将事务中的写命令**逐个独立传播**（非 MULTI/EXEC 块） | `transaction_commands.go:58` `handleEXEC` 逐条 PropagateCommand | 从节点执行语义错误 |
| R10 | 传播的 RESP 与原始请求 RESP 字节一致 | `replication.go:194` `serializeCommand` | 从节点解析错误 |
| R11 | PubSub 消息永不进入复制传播 | `replication_helper.go:112` `isWriteCommand` 不包含 SUBSCRIBE/PUBLISH | 从节点收到无用流量 |

### 1.3 Partial Sync Contract

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| R12 | 增量同步条件：replId 匹配 && offset ∈ backlog 可用范围 | `psync.go:30-37` | 错误的全量/增量选择 |
| R13 | 全量同步后 offset = masterReplOffset（当前最新值） | `psync.go:60-62` | 从节点从错误位点开始 |
| R14 | 全量同步发送 RDB 后才注册从节点 | `replication_handler.go:44-128` snapshotOffset→GenerateRDB→FULLRESYNC→RDB→SendBacklog→AddSlave→CatchUp | 从节点在 RDB 完成前收到命令 |

### 1.4 Slave Connection Lifecycle

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| R15 | 从节点断线后自动重连，使用 PSYNC 增量（非全量） | `reconnect.go` `lastReplId lastOffset` | 不必要的全量同步 |
| R16 | 重连退避：1s → 2s → 4s → ... → max 60s | `reconnect.go` | 重连风暴 |
| R17 | 稳定连接 ≥ 30s 后重置退避计数器 | `reconnect.go` | 重连延迟不合理 |

---

## 2. Backlog Invariants

### 2.1 环形缓冲区语义

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| B1 | Backlog 是固定大小环形缓冲区（默认 1MB，最大 512MB） | `backlog.go:9-10` | 内存溢出 |
| B2 | 内部 offset = 已写入总字节数（单调递增，永不回绕） | `backlog.go:14` `rb.offset` | offset 计算错误 |
| B3 | 环形写入位置 = `offset % size`（回绕写指针） | `backlog.go:50` | 数据覆盖错误 |
| B4 | 可用范围 = `[offset - size, offset)`，下限 clamp 到 0 | `backlog.go:74-76` `AvailableStartOffset` | 过期数据被认为可用 |
| B5 | 当 dataLen >= size 时：只保留最后 size 字节（直接覆盖整个 buffer） | `backlog.go:44-47` | 环形写入导致残留旧数据 |
| B6 | `GetRange(start, end)` 校验：start < offset 且 start >= availableStart | `backlog.go:79-84` | 越界读 |

### 2.2 Offset 关系

```
backlog.internalOffset == masterReplOffset        (单调字节计数器)
backlog.writePosition    = offset % size           (回绕写位置,≠ offset)
backlog.availableStart   = max(0, offset - size)   (可读范围下限)
replication.offset       = backlog.internalOffset  (全局单调计数器)

关键：backlog position != replication offset
       backlog position 是环形缓冲区中的写指针，会回绕
       replication offset 是单调计数器，永不回绕
```

---

## 3. Connection Lifecycle Invariants

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| C1 | `connState` 在 `executeCommand()` 中永不为 nil | `handler_dispatch.go:45` `if state==nil return Error` | nil pointer dereference |
| C2 | `connState.cancel()` 最多调用一次 | `context.CancelFunc` 幂等保证 | double close |
| C3 | 每个连接恰好一个 `handleConnection` goroutine | `handler_core.go:317` `ServeTCP` / `368` `handleConnection` | goroutine leak |
| C4 | 连接 cleanup 按固定顺序执行：cancel → unregister → RemoveSubscriber → unwatch → conn.Close | `handler_core.go:457` `handleConnection` defer | 资源泄漏 |
| C5 | PSYNC 接管后，连接关闭责任转移给 `handleSlaveReplicationConnection` | `handler_core.go:384` `replicationOwned` | 连接双重关闭 |
| C6 | Pipeline：先读第一个命令，然后 `reader.Buffered() > 0` 时 drain 剩余 | `handler_core.go:491` `processRequest` + buffered loop | pipeline 乱序 |
| C7 | Pipeline 中所有响应批量 flush（一次 Flush 调用） | `handler_core.go:602` `flushBytes`/`writer.Flush` | 性能退化 |
| C8 | CLIENT KILL 通过 `state.cancel()` 触发目标 ctx 取消，不直接关闭 conn | `client_commands.go:148` `targetState.cancel()` | 竞态关闭 |

---

## 4. Cancellation Invariants

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| K1 | 所有阻塞操作必须 select `case <-ctx.Done()` | `list.go:1500,1540,1590,1637` `stream.go:574,598` | goroutine leak / shutdown hang |
| K2 | `ctx.Done()` 后 goroutine 必须在 ≤ 1s 内退出 | `handler_core.go:336` `Shutdown` | Server.Shutdown hang |
| K3 | 阻塞操作同时响应超时和 ctx 取消：`select { case <-timeoutCh: case <-ctx.Done(): }` | `list.go:1494-1503` | 超时不响应 ctx |
| K4 | 阻塞操作超时后必须 unregister（从等待队列移除） | `list.go:1498,1501` | 悬空 channel/goroutine |
| K5 | `defer cancel()` 必须在创建 ctx 的同一 goroutine 中 | Go 惯例 | 取消传播断裂 |
| K6 | Shutdown 顺序：Listener.Close（`ln.Close` via `ctx.Done`）→ `replMgr.Stop()`（关 slave TCP unblock）→ `cancel()`（root ctx）→ `handler.Shutdown()`（`shuttingDown=1`→`conn.cancel+Close`→`wg.Wait`）→ `backupMgr.Wait()`（in-flight BGSAVE）→ `db.Close()` | `cmd/boltDB/main.go:448` + `handler_core.go:336` `Shutdown` | 资源释放顺序错误 |
| K7 | CLIENT KILL 不会导致 `handleConnection` panic（cancel 安全） | `client_commands.go:148` cancel + Close 幂等 | 竞态 |

### 阻塞操作完整清单

| 操作 | 文件:行 | ctx 响应 | 超时响应 |
|------|---------|----------|---------|
| BLPOP | `store/list.go:1500` | ✅ | ✅ (timeoutCh) |
| BRPOP | `store/list.go:1540` | ✅ | ✅ (timeoutCh) |
| BLMOVE | `store/list.go:1590` | ✅ | ✅ (timeoutCh) |
| BRPOPLPUSH | `store/list.go:1637` | ✅ | ✅ (timeoutCh) |
| BZPOPMIN/BZPOPMAX | handler (store 层) | ✅ | ✅ (timeoutCh) |
| XREAD (blocking) | `store/stream.go:574,598` | ✅ | ✅ (timeoutCh) |
| PubSub loop | `handler_core.go:632` `runPubSubLoop` + `pubsub_commands.go` | ✅ | N/A |
| MONITOR loop | `handler_core.go:642` `runMonitorLoop` | ✅ | N/A |
| Replication slave loop | `replication_handler.go:202` `handleSlaveReplicationConnection` | ✅ | N/A |

---

## 5. Transaction Invariants

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| T1 | MULTI 设置 `state.inTransaction = true`，清空 commands 队列 | `transaction_commands.go:7` `handleMULTI` | 状态错误 |
| T2 | MULTI 不可嵌套 | `transaction_commands.go:9` | 事务状态不一致 |
| T3 | 事务中非控制命令返回 `QUEUED` 并加入 commands 队列 | `handler_dispatch.go:70` `inTransaction` 分支 | 响应错误 |
| T4 | 事务控制命令（MULTI/EXEC/DISCARD/WATCH/UNWATCH/PING/QUIT）不排队 | `handler_dispatch.go:72` | 死锁 |
| T5 | EXEC 空事务返回空数组 `*-1\r\n` / RESP3 Null | `transaction_commands.go:42` `Null{}` vs `NilArray` | 响应格式不一致 |
| T6 | EXEC 先检查 dirty keys，dirty 则返回 `*-1\r\n` 并清空事务 | `transaction_commands.go:31` watchMu dirty check | WATCH 竞争丢失 |
| T7 | EXEC 后清空 `inTransaction` 和 commands | `transaction_commands.go:82` | 事务状态残留 |
| T8 | DISCARD 清空 `inTransaction`、commands 和 transaction | `transaction_commands.go:115` `handleDISCARD` | 事务状态残留 |
| T9 | WATCH 在 MULTI + 有排队命令时拒绝 | `transaction_commands.go:131` | WATCH 语义违反 |
| T10 | `markDirtyKeys` 修改 key 时通知所有 WATCH 该 key 的连接 | `handler_core.go:markDirtyKeys` + `transaction_commands.go:138` | WATCH 竞争丢失 |

---

## 6. PubSub Invariants

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| P1 | PubSub 消息永不进入复制传播 | `replication_helper.go:112` `isWriteCommand` | 从节点收到无用流量 |
| P2 | SUBSCRIBE/PSUBSCRIBE 设置 `state.subscriber` 并切换到 PubSub 循环 | `handler_core.go:632` `runPubSubLoop` | 主循环继续处理命令 |
| P3 | PubSub 模式下仅接受：(P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT | `pubsub_commands.go:handlePUBSUB` 入口校验 | 错误命令在 PubSub 模式执行 |
| P4 | PubSub flush ticker 间隔 100ms | `pubsub_commands.go` ticker | 延迟 / 性能 |
| P5 | PubSub 消息读取在独立 goroutine 中，通过 channel 发回主循环 | `handler_core.go:runPubSubLoop` | 读取阻塞推送 |
| P6 | PubSub 退出时关闭 done channel 确保 reader goroutine 退出 | `handler_core.go:runPubSubLoop` done | goroutine leak |
| P7 | RemoveSubscriber 在连接 cleanup 中执行 | `handler_core.go:466` `RemoveSubscriber` | 悬挂订阅 |
| P8 | 慢订阅者被 OutputBufferLimit 断开 | `handler_core.go:619` `OutputBufferLimit` 检查 | 服务器被慢客户端拖垮 |

---

## 7. Monitor Invariants

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| M1 | MONITOR 广播不阻塞发布者（channel 满时 drop） | `handler_core.go:broadcastToMonitors` `select default` | 发布者被慢 monitor 拖慢 |
| M2 | MONITOR 模式下仅接受 QUIT / PING | `handler_core.go:runMonitorLoop` | 错误命令在 monitor 模式执行 |
| M3 | MONITOR 消息格式 `+timestamp [0 addr] "cmd" "arg1" ...\r\n` | `handler_core.go:broadcastToMonitors` | redis-cli 解析失败 |
| M4 | MONITOR flush ticker 间隔 100ms | `handler_core.go:runMonitorLoop` | 延迟 / 性能 |
| M5 | MONITOR 使用独立 goroutine 读取命令 | `handler_core.go:runMonitorLoop` | 读取阻塞推送 |

---

## 8. Output Buffer Invariants

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| O1 | OutputBufferLimit 是软限制（post-flush 检查，不保证精确截断） | `handler_core.go:619` | 连接可能略微超限 |
| O2 | `outputBytes` 累计跟踪 flush 字节数（`writer.Buffered()` pre-Flush） | `handler_core.go:602` `flushBytes` | 错误计算 |
| O3 | PubSub/MONITOR 模式下 outputBuffer 检查在 flush ticker 中执行 | `handler_core.go:619` + `pubsub_commands.go` | 慢客户端保护失效 |
| O4 | OutputBufferLimit == 0 表示不限制 | `handler_core.go:106` | 零值语义错误 |
| O5 | CLIENT LIST 中 `omem` = `outputBytes`，`oFlags` = `>` 当 outputBytes > limit/2 | `client_commands.go:CLIENT LIST` | 监控信息错误 |

---

## 9. Ownership & Concurrency Invariants

### 9.1 Goroutine Ownership

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| OW1 | `bufio.Reader/Writer` 和 `net.Conn` 由 conn goroutine 独占，不可跨 goroutine 访问 | `handler_core.go:401` 创建在 `handleConnection` | data race |
| OW2 | `connState` 字段在所属 goroutine 内可直接读写（不加锁） | 设计约定 | 不必要的锁竞争 |
| OW3 | 跨 goroutine 访问 `connState` 字段必须持有 `state.mu` | `handler_core.go:24` | data race |
| OW4 | `Handler.conns` 必须通过 `h.connsMu` (RWMutex) 保护 | `handler_core.go:94` | data race |
| OW5 | `Handler.watchMonitors` 必须通过 `h.watchMu` (Mutex) 保护 | `handler_core.go:88` | data race |
| OW6 | `Handler.monitorClients` 必须通过 `h.monitorMu` (Mutex) 保护 | `handler_core.go:100` | data race |
| OW7 | 持有 `state.mu` 时禁止调用 `cancel()` | `client_commands.go:148` (KILL 时先 unlock) | 死锁 |

### 9.2 资源所有权矩阵

```
资源                         所属 goroutine          跨 goroutine 安全?    保护机制
───                         ───────────────         ─────────────────    ────────
connState 实例               handleConnection        部分字段              state.mu
connState.subscriber        conn goroutine           仅创建/销毁           state.mu
connState.monitorCh         conn goroutine           仅写                  state.mu + channel semantics
connState.watchedKeys       conn goroutine           读不安全              state.mu
connState.inTransaction     conn goroutine           读不安全              state.mu
connState.transaction       conn goroutine           读不安全              state.mu
bufio.Reader/Writer         conn goroutine           否（独占）            单 goroutine 保证
net.Conn                    conn goroutine           否（独占）            单 goroutine 保证
Handler.conns               Handler                  是                   h.connsMu (RWMutex)
Handler.watchMonitors       Handler                  是                   h.watchMu (Mutex)
Handler.monitorClients      Handler                  是                   h.monitorMu (Mutex)
Handler.PubSub              Handler                  是                   PubSubManager 内部锁
ReplicationManager.slaves   ReplicationManager       是                   内部锁 + atomic
```

### 9.3 并发安全保证

| # | 保证 | 说明 |
|---|------|------|
| S1 | 同一连接的命令串行执行 | conn goroutine 独占 bufio 和 conn |
| S2 | 不同连接的写操作互不阻塞 | 各自 flush，共享 BadgerDB 事务 |
| S3 | CLIENT LIST 可能错过正在注册/注销的连接 | RWMutex RLock 快照语义 |
| S4 | WATCH 冲突检测是最终一致的 | 写操作完成时标记 dirty，下次 EXEC 检测 |
| S5 | PubSub 广播是 best-effort | 慢订阅者被 output buffer limit 断开 |
| S6 | `closeOnce` 保证 Stop 幂等 | `replication.go:228-230` |
| S7 | `context.CancelFunc` 幂等 | Go 标准库保证 |

---

## 10. Store Layer Invariants

| # | Invariant | Source | Violation |
|---|-----------|--------|-----------|
| ST1 | 每个 Redis key 恰好有一个 TYPE_ 记录和至少一个匹配的数据记录 | `define.go:76-268` `Check()` | 孤立键 |
| ST2 | 所有 key 存储前增加类型前缀（STRING:/LIST:/HASH:/SET:/zset:/JSON:/TS:/stream:/geo:/hll:） | `define.go:23-35` | 键空间冲突 |
| ST3 | TYPE_ 记录与实际数据记录的前缀必须一致（`Check()` 验证） | `define.go:86-221` `extractRawKey` | 类型不一致 |
| ST4 | BadgerDB 关闭超时 = CloseTimeout（2s），防止 doWrites drain bug 挂起测试 | `define.go:20` `CloseWithTimeout` | 测试套件 hang |
| ST5 | 读缓存（LRU，10000 条目，5min TTL）最终一致，可返回过期数据 | `define.go:313` | 读陈旧 |
| ST6 | `GetDB()` 返回裸 BadgerDB 实例，仅限 RDB 备份/复制使用 | `define.go:350-352` | 绕过 store 层封装 |
| ST7 | `ClearAllData` 使用分批迭代删除，每批独立重试（"Writes are blocked"） | `define.go:362-485` | 清理 hang |

### 10.1 类型前缀表

| Redis 类型 | 存储前缀 | 备注 |
|------------|---------|------|
| String | `STRING:` |  |
| List | `LIST:` |  |
| Hash | `HASH:` | `__count__` 计数键为 TTL 探测锚点 |
| Set | `SET:` | `count` 键为 TTL 探测锚点 |
| Sorted Set | `zset:` | `meta` 键为 TTL 探测锚点 |
| JSON | `JSON:` |  |
| TimeSeries | `TS:` | `meta` 键为 TTL 探测锚点 |
| Stream | `stream:` | `meta` 键为 TTL 探测锚点 |
| Geo | `geo:` + `zset:` | 双前缀：`geo:<key>:` 存坐标索引，`zset:<key>:` 存 rank/index 缓存；`RENAME`/`DEL`/`COPY`/`NextStartup` 均需双前缀闭环清理 |
| HyperLogLog | `hll:` |  |
| Type 元数据 | `TYPE_` |  |

---

## 11. Forbidden Patterns

以下模式在任何情况下不允许：

```
F1. [并发违规] 在 conn goroutine 外直接读/写 connState 字段
    必须通过 state.mu

F2. [死锁风险] 在持有 state.mu 时调用 cancel()
    先 unlock state.mu，再调用 cancel()

F3. [UAF] 在 defer cleanup 执行后访问 state 字段
    所有 state 访问必须在 defer return 之前

F4. [所有权违规] 在 handler goroutine 中直接操作 net.Conn
    conn 所有权属于 conn goroutine

F5. [所有权违规] 在非主 goroutine 中创建 bufio.Reader/Writer
    bufio 必须有 goroutine 独占保证

F6. [泄漏] 创建 goroutine 后不保证其退出
    必须通过 ctx.Done() 或 done channel 确保退出

F7. [错误消除] 在复制执行路径上吞掉错误
    executeReplicatedCommand 不返回响应，错误导致主从不一致

F8. [竞态关闭] 同时通过 cancel() 和 conn.Close() 关闭连接
    CLIENT KILL 使用 cancel() 触发 cleanup chain → conn.Close()
    禁止额外直接调用 conn.Close()
```

---

## 版本化记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-05-17 | 从 `docs/architecture/v0.3.md §7` 提取并强化：新增 R1-R17（完整复制契约）、B1-B6（backlog 环形缓冲区形式化）、C1-C8（连接生命周期）、K1-K7（取消模型）、T1-T10（事务）、P1-P8（PubSub）、M1-M5（Monitor）、O1-O5（Output Buffer）、OW1-OW7 + 矩阵（所有权）、ST1-ST7（存储层）、F1-F8（禁止模式） |
| v1.1 | 2026-08-24 | `handler.go` 8824 行拆为 24 文件（`handler_core.go`/`handler_dispatch.go`/`replication_handler.go`/`transaction_commands.go`/`client_commands.go`/…）：全量更新 Source 列行号，消除失效引用；`handler.go` 遗留引用归档于 `docs/plans/archive/` |
| v1.2 | 2026-08-24 | 自主巡检 8 轮：`handler_core double-reader→single`（`TotalInputBytes`）、`authPassword` 热路径缓存（`OW7`）、`waitPauseWindow` Timer 泄漏、`CLIENT KILL` 持锁调 `cancel` 死锁、`PubSub.Clear` shard 泄漏、`K6` 关闭时序对齐 `AGENTS`、EXPIRE/SORT 条件写 R7 等；`SORT/EXPIRE` 复制回归加固 |

### 核验命令

```bash
# 原子性：所有测试通过后本文件中的 invariant 才视为保持
go test -race -short ./internal/...
go test -race ./cmd/integration/...
golangci-lint run --timeout 5m
```
