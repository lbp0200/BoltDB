# Changelog

## v8.3.0 (2026-05-16) — Architecture Freeze

> **架构冻结版本。** 系统进入"收束"阶段——停止大规模 feature 扩张，建立工程护城河，固化系统规则。

### Architecture Freeze

- **Ownership Rules** — goroutine 拓扑、资源所有权矩阵、跨 goroutine 安全规则、shutdown 顺序
- **Cancellation Rules** — 取消传播链、阻塞操作取消契约、CLIENT KILL 语义、超时取消、安全性要求
- **System Invariants** — 10 条不可变不变式、幂等性保证、并发安全性保证、禁止模式
- 以上全部归档到 `docs/architecture/v0.3.md`

### Engineering Moat

| 防线 | 覆盖 |
|------|------|
| 🔬 Fuzz | RESP parser fuzz（~60 seeds + inline fuzzer）+ Server command sequence fuzz（20 opcodes）+ Server raw bytes fuzz |
| 🌀 Chaos | PubSub / Transaction / Blocking / Replication |
| 📉 Regression | 9 个 package, `go test -race -short` |
| 🔗 Compatibility | go-redis + redis-py 153/153 + redis-cli 53/77 + node-redis 93/110 |
| 📊 Benchmark | 18 项，go-redis 原生 API |

### 新增命令

- `GEORADIUS` — handler 层支持
- `HSCAN` — hash 游标扫描（MATCH/COUNT）
- `GETDEL` — 原子 GET + DEL
- `GETEX` — 原子 GET + EXPIRE（EX/PX/PERSIST）
- `COMMAND` — 返回空数组供客户端自发现

### 兼容性修复

- **QUIT 命令缺失** — node-redis 退出时 ERR unknown command，已修复
- **SMISMEMBER RESP 类型** — bulk string → integer
- **SINTERCARD 参数解析** — 处理 numkeys 参数
- **BZPOPMAX/BZPOPMIN timeout 返回** — `*0\r\n` → `*-1\r\n`
- **WRONGTYPE 覆盖** — Stream 17 个命令 + GEORADIUS + GET/LLEN
- **XAdd 类型覆盖 bug** — txn.Set 前检查现有类型
- **键名含冒号的 Sorted Set 解析** — 新增 `parseZSetIndexKey()` 前缀偏移量解析

### node-redis 兼容套件

- 新增 `scripts/redis_node_compat.mjs` — 110 项测试
- 自动构建 + 启动 BoltDB，支持 `--port` 指定外部实例
- 当前通过率 93/110

### RESP Parser 增强

- RESP2 数组元素类型支持（`:` Integer, `+` SimpleString）
- Fuzz 种子扩充：超大长度、负数、截断、CRLF 变体、null bytes、RESP2 混合数组
- 新增 `FuzzReadRESPInlineCommand`（inline 命令 fuzzer）

### Replication

- Backlog 环形缓冲重写（1MB 默认 / 512MB 最大）
- `SlaveReconnector` 自动重连（exponential backoff 1s→60s）
- Propagation 测试套件（13 个测试）
- Replication Chaos 测试（3 个）

### Performance

- 18 项 benchmark baseline（SET/GET/Pipeline/MGET/LRANGE/PubSub/Transaction/XADD+XRead/Concurrent/INCR/MSET/DEL/HSET/SADD/ZADD）
- `-benchmem` 记录 allocs/op

---

## v8.2.1 (2026-05-15)

### Output Buffer Management

- 新增 `-client-output-buffer-limit` CLI 参数
- 正常连接：每次批量写后检查累计输出，超限断开
- PubSub 连接：每 100ms flush + 累计跟踪
- CLIENT LIST 增加 `omem` / `oFlags`

### MONITOR 命令

- `monitorClients` 注册表 + `broadcastToMonitors()`
- `formatMonitorMessage` — `+timestamp [db addr] cmd args\r\n` 格式

### RESP2 兼容

- PubSub SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE 订阅计数整数化
- EXEC 响应格式修复（双重编码 bug）
- XRead 响应格式修复（嵌套结构拍平 bug）

### 新命令

- `ZDIFF` / `ZRANDMEMBER`

---

## v8.2.0 (2026-05-15)

### 工程护城河初始建立

- **RESP Fuzzing** — `FuzzReadRESP`, `FuzzReadRESPPipeline`, `FuzzReadRESPInlineCommand`
- **Goroutine Leak 测试** — 22 个测试覆盖 PubSub/Blocking/Transaction/Connection/Mixed
- **Chaos 集成测试** — PubSub/Transaction/Blocking Chaos + Mixed All
- **Benchmark Baseline** — 18 个 benchmark
- **Compatibility Suite** — redis-py 153 测试全绿 + redis-cli 兼容

### 架构清理

- 删除 `baseState` fallback：`connState` 永不为 nil
- `panic("nil connState")` fast-fail

### redis-py 兼容修复

- BZPOPMAX/BZPOPMIN timeout 返回 `*0\r\n` → `*-1\r\n`
- LLEN / GET type check → WRONGTYPE
- 键名含冒号的 Sorted Set 解析修复
- SMISMEMBER RESP 类型修复
- SINTERCARD 参数解析修复

---

## v8.1.x (2026-05-14~15)

### v8.1.7

- WRONGTYPE error format 修复（stream/JSON/TS/geo 命令）
- 集成测试扩展

### v8.1.5

- 三个 Redis 兼容性 bug 修复
- 复制流程初始实现

### v8.1.0

- 初始版本：SET/GET 核心功能
- BadgerDB 存储引擎集成
- Redis 协议解析（RESP2）
- 基础 Cluster 支持（16384 slots）
- Sentinel 故障转移
- 主从复制
