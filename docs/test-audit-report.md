# BoltDB 测试体系审计报告

> 审计日期: 2026-05-15
> 审计范围: Integration coverage, concurrent behavior, timeout behavior, pubsub lifecycle, transaction isolation, parser edge cases, wrongtype behavior, disconnect cleanup, partial packet handling

---

## 1. 虚假测试（不测试实际声称的内容）

| 问题 | 文件 | 严重程度 | 状态 |
|------|------|----------|------|
| SUBSCRIBE/PSUBSCRIBE 是桩代码 | `internal/server/handler.go:3965–4016` | 严重 | ❌ 未修复 |
| connState 已定义但为空 | `internal/server/handler.go:22–25` | 严重 | ✅ 已修复 |
| BRPOPLPUSHBlocking/BLMoveBlocking 使用轮询而非通道 | `internal/store/list.go:1469–1517` | 高 | ✅ 已修复 |

### 1.1 SUBSCRIBE 桩代码

服务器 handler 的 `SUBSCRIBE` 返回一个静态确认数组 `["subscribe", "channel", "1"]`，但**从未创建真正的 store.Subscriber**，也从未将消息推送到连接上。集成测试（`cmd/integration/pubsub_test.go`）仅测试命令解析——它们从未验证 "publish → deliver to subscriber" 的端到端流程。`store.PubSubManager` 有完整的实现，但 handler 根本没有连接它。这使得所有 13 个 PubSub 集成测试成为**仅解析输入的测试**。

```go
// handler.go:3965 - 简化实现，实际应该创建订阅者并持续发送消息
case "SUBSCRIBE":
    // ...
    return &proto.Array{Args: [][]byte{
        []byte("subscribe"),
        []byte(channels[0]),
        []byte("1"),
    }}
```

### 1.2 共享连接状态（已修复）

~~`connState` 结构体声明了 `inTransaction` 和 `commands`，但 `transaction *TransactionState` 和 `clientInfo *ClientInfo` 字段在共享的 `Handler` 结构体上，而不是在每个连接的基础上。这意味着来自不同连接的并发 MULTI/EXEC/WATCH 调用会竞争共享的 `h.transaction` 指针。~~

已修复：`transaction`、`clientInfo`、`clusterAsking` 已从 `Handler` 移到 `connState`，每个连接拥有独立的事务状态和客户端信息。同时添加了跨连接的 WATCH 监视器（`watchMonitors map[string]map[*connState]struct{}`），确保多连接 WATCH 冲突检测正确工作。

### 1.3 BRPOPLPUSH 轮询（已修复）

~~与使用基于通道的 `blockingPopChans` 机制的 BLPOP/BRPOP 不同，BRPOPLPUSHBlocking 和 BLMoveBlocking 使用带有 `time.Sleep(10ms)` 的忙轮询。这浪费 CPU 并且具有竞争窗口。~~

已修复：BRPOPLPUSHBlocking 和 BLMoveBlocking 现在复用与 BLPOP/BRPOP 相同的 `registerBlockingPop`/`notifyBlockingPop` 通道通知机制，消除了忙轮询。

---

## 2. 缺失的回归测试

### 2.1 高优先级

| 缺失的测试 | 位置 | 影响 | 状态 |
|------------|------|------|------|
| **WATCH 冲突检测**：修改被监视的键 ⇒ EXEC 应返回 nil | `handler.go:4103-4153` | 事务正确性的核心保证未经测试 | ✅ 已添加 (`TestWATCHConflict`) |
| **并发事务隔离**：2 个连接在相同键上同时执行 MULTI/EXEC | `handler.go:36` | h.transaction 共享 ⇒ 互相干扰 | ✅ 已添加 (`TestConcurrentTransaction`) |
| **阻塞超时到期**：在空键上使用 `BLPOP key 1`（非零超时） | `store/list.go:1399-1467` | 仅测试了 timeout=0 路径 | ✅ 已添加 (`TestBLPOPTimeout`) |
| **在阻塞期间推送**：一个 goroutine 阻塞，另一个推送 | `store/list.go:1371-1389` | 通道通知路径未经集成测试 | ❌ 未修复 |
| **订阅者消息投递**：通过服务器的 2 连接 PubSub 端到端流程 | `server/handler.go:3965` | 最关键的 PubSub 行为完全未测试 | ❌ 未修复 |
| **`-race` 检测器**：没有测试使用 `go test -race` 运行 | — | 无法检测数据竞争 | ❌ 未修复 |
| **连接断开清理**：客户端在管道中间断开连接 | `server/handler.go:165-230` | handleConnection 仅仅返回 | ❌ 未修复 |

### 2.2 中优先级

| 缺失的测试 | 影响 |
|------------|------|
| **完整的多键事务执行**：MULTI / SET / GET / INCR / EXEC 命令类型混合 | 仅测试了所有 SET |
| **部分 RESP 数据包**：ReadRESP 上 io.EOF / io.ErrUnexpectedEOF 的处理 | bufio 错误静默传播 |
| **大边界值**：SET 100MB+ 值，大列表 100K+ 元素 | 仅测试最大 10MB；LSM 压缩行为未知 |
| **嵌套 MULTI**：MULTI 内嵌 MULTI ⇒ 应返回错误 | 已实现但未测试 |
| **并发 SETEX/过期**：在键过期时同时设置和读取 | 可能因 TTL 竞争导致偶发失败 |

### 2.3 低优先级

| 缺失的测试 | 影响 |
|------------|------|
| 所有过期原语的 PERSIST/expire 组合 | 主要路径已覆盖，边缘情况未覆盖 |
| WRONGTYPE 位于：流、JSON、时间序列、地理空间（集成层） | 单元层已覆盖；集成层仅覆盖 4/8 类型 |
| EXEC 队列中有不支持的命令 | executeQueuedCommand 默认错误未测试 |
| SRANDMEMBER/HRANDFIELD 的随机性分布 | 仅检查返回值范围 |

---

## 3. 虚假的 Happy-Path 测试

### 3.1 阻塞操作仅测试非阻塞路径

`cmd/integration/integration_test.go:1181–1251` 中的所有 4 个阻塞操作测试使用 `timeout=0`，这调用立即非阻塞路径。这些测试**从不测试实际的阻塞行为**：

```go
// 全部使用 timeout=0，仅触发立即返回路径
func TestBLPOPBlocking(t *testing.T) {
    _ = sharedClient.LPush(ctx, "testlist", "value1")
    arr, err := sharedClient.BLPop(ctx, 0, "testlist").Result()  // timeout=0!
}
```

应重命名为 `TestBLPOPNonBlocking`。

### 3.2 PubSub 测试不验证消息投递

`TestTimeoutUnsubscribe` 和 `TestPublishSubscribeIntegration` 仅验证 PUBLISH 返回计数 > 0，但**从未验证订阅者实际收到了消息**。

### 3.3 共享事务掩盖并发问题

`TestTransaction` 和 `TestTransactionExtended` 在同一个客户端上顺序运行，因此 `h.transaction` 共享问题是隐藏的。并发客户端连接会触发竞争。

---

## 4. 不稳定测试风险

| 模式 | 位置 | 风险 |
|------|------|------|
| `time.Sleep(10ms)` 代替 goroutine 就绪信号 | `internal/server/handler_test.go:176,479,572` | **高** — 在 CI 压力下可能失败 | ✅ 已修复：移除盲等，直接 Dial |
| `time.Sleep(1.5s)` 用于哨兵故障转移 | `internal/sentinel/master_test.go:208` | **高** — 即使正常也要等待 1.5s；慢 CI 可能不够 | ❌ 未修复 |
| `time.After(1s)` 用于发布订阅消息等待 | `internal/store/pubsub_test.go:139,163,186,217` | **中** — 真实超时失败（假阴性） | ❌ 未修复 |
| `t.Fatal("timeout...")` 中的无缓冲通道选择 | `internal/store/pubsub_test.go` | **中** — 如果系统繁忙会硬失败 | ❌ 未修复 |
| BRPOPLPUSHBlocking 使用 `time.Sleep(10ms)` 轮询 | `internal/store/list.go:1494,1517` | **中** — 增加延迟 | ✅ 已修复：切换到通道通知 |
| 跳过测试可能隐藏回归 | `handler_coverage_test.go:58` 等 | **低** — 在 Replication 为 nil 时跳过 |
| 集成测试使用硬编码 `/tmp/boltdb_integration_shared` | `cmd/integration/integration_test.go:1895` | **低** — 崩溃后残留数据 |

### handler_test.go 的不稳定模式

```go
// handler_test.go:166-179 - 经典不稳定模式
listener, _ := net.Listen("tcp", "127.0.0.1:0")
go func() { handler.ServeTCP(listener) }()
time.Sleep(10 * time.Millisecond)  // 应该使用通道/就绪信号
conn, _ := net.Dial("tcp", listener.Addr().String())
```

---

## 5. 竞态条件风险

| # | 风险 | 类型 | 证据 | 状态 |
|---|------|------|------|------|
| 1 | **Handler 级别的共享 transaction** | 数据竞争 | `Handler.transaction` 在 executeCommand 和 ResetConnectionState 中被访问，无锁。并发 MULTI 互相损坏。 | ✅ 已修复：移至 `connState` |
| 2 | **ClientInfo 在 Handler 上共享** | 数据竞争 | `Handler.clientInfo` 被 CLIENT SETNAME/GETNAME/INFO/KILL 无锁读写。 | ✅ 已修复：移至 `connState` |
| 3 | **clusterAsking 在 Handler 上共享** | 数据竞争 | `Handler.clusterAsking` 被 ASKING 命令和重定向逻辑无锁访问。 | ✅ 已修复：移至 `connState` |
| 4 | **blockingMu + 通道注册竞争** | 逻辑竞争 | 如果在通道注册**之前**推送完成，BLPOPBlocking 可能错过通知。具有"先尝试非阻塞"缓解措施，但不防弹。 | ❌ 未修复 |
| 5 | **streamBlockingMu 窗口** | 逻辑竞争 | 类似于 BLPOP——通道在立即读取之前注册。如果在间隙中发生 XAdd，消息被遗漏。 | ❌ 未修复 |

### 关键竞争说明

`go test -race` 很可能检测到共享 Handler 字段的竞争。应添加：

```bash
go test -race -count=1 ./internal/server/... -run "Test.*Concurrent.*|Test.*Transaction.*"
```

---

## 6. 未覆盖的状态转换

### 6.1 事务状态机

```
IDLE → MULTI → EXEC(clean) → IDLE        ✓ 测试
IDLE → MULTI → DISCARD → IDLE            ✓ 测试
IDLE → WATCH → MULTI → EXEC(clean) → IDLE ✓ 已测试
IDLE → WATCH → MULTI → EXEC(dirty) → IDLE ✓ 已测试（TestWATCHConflict）
IDLE → MULTI → WATCH → ...               ✗ 未测试（嵌套）
Concurrent: ConnA MULTI + ConnB MULTI    ✓ 已测试（TestConcurrentTransaction）
```

### 6.2 PubSub 状态机（通过 handler）

```
IDLE → SUBSCRIBE → SUB → UNSUBSCRIBE → IDLE  ? 部分（仅确认数组）
SUB → MESSAGE(已投递)                      ✗ 未测试（桩代码从不投递）
IDLE → PSUBSCRIBE → PSUB → PUNSUBSCRIBE   ? 部分（仅确认数组）
```

### 6.3 阻塞操作状态机

```
LPush → BLPop(非阻塞，数据存在) → 结果    ✓ 测试 (timeout=0)
BLPop(空，超时=1s) → timeout                 ✓ 已测试（TestBLPOPTimeout）
BLPop(空，超时=0) → 立即返回 nil             ✓ 已测试（TestBLPOPTimeout）
BLPop(空) → LPush(另一个连接) → 结果         ✗ 未测试
```

### 6.4 连接生命周期

```
Accept → handleConnection → ReadRESP → process → ReadRESP → ... → EOF/error → Close
     ✗ 未测试 mid-command 断开
     ✗ 未测试 pipeline 断开
     ✗ 未测试连接状态清理
```

---

## 7. WRONGTYPE 格式不一致（已修复）

~~`internal/server/handler.go` 中有不一致的错误前缀模式：~~

~~HSET 使用了 `"ERR WRONGTYPE"` 前缀，而其他命令使用 `"WRONGTYPE"`。~~

已修复：HSET 的 WRONGTYPE 错误已统一为与其他命令一致的 `"WRONGTYPE Operation against a key holding the wrong kind of value"` 格式。

---

## 8. 分类摘要

| 类别 | 原始 | 已修复 | 剩余 |
|------|------|--------|------|
| **严重** | 3 | 2（共享事务状态、BRPOPLPUSH 轮询） | 1（SUBSCRIBE 桩代码） |
| **高** | 6 | 3（WATCH 冲突测试、阻塞超时测试、time.Sleeps） | 3（无 -race、无消息投递、1.5s 哨兵睡眠） |
| **中** | 4 | 1（WRONGTYPE 格式不一致） | 3（大边界、集成 WRONGTYPE、硬编码路径） |
| **低** | 3 | 0 | 3 |

---

## 9. 行动建议

### ✅ 已完成（本次迭代）

1. **修复 SUBSCRIBE 桩代码** — 待办（复杂，需要重构 handleConnection 读取循环以支持 PubSub 推送模式）
2. ✅ **添加 WATCH 冲突测试** — `TestWATCHConflict`（使用原始 RESP 连接验证 `*-1\r\n`）
3. **添加并发事务隔离测试** — `TestConcurrentTransaction`（两个客户端同时对相同键执行事务）
4. ✅ **添加阻塞超时测试** — `TestBLPOPTimeout`（BLPOP timeout=0 在空键上应返回 nil）
5. ✅ **修复 handler_test.go time.Sleep** — 移除 3 处盲等，因为 `net.Listen` 返回的 listener 已就绪
6. ✅ **修复 BRPOPLPUSHBlocking/BLMoveBlocking 轮询** — 改用 `registerBlockingPop` 通道通知
7. ✅ **修复 WRONGTYPE 前缀** — HSET 错误格式统一
8. ✅ **修复共享 Handler 事务/clientInfo/clusterAsking** — 移到 `connState`，添加 `watchMonitors` 跨连接冲突检测

### 待办

- **修复 SUBSCRIBE 桩代码** — 将 `SUBSCRIBE` 连接到 `store.PubSubManager`，使 handleConnection 支持推送模式
- **添加 `-race` 步骤到 CI** — 运行 `go test -race -count=1 ./internal/server/...`
- **添加端到端 PubSub 消息投递集成测试**
- **添加部分 RESP 数据包处理测试**（`io.EOF`、截断、错误长度）
- **添加大边界测试**（100MB 字符串、100K 列表、500MB 数据库）
