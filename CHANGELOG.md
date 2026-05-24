# Changelog

## v8.13.1 (2026-05-24) — Fix: Master Replication Health + Regression Barrier

> **修复：主节点复制健康评分与 regression 屏障超时。** `computeReplicationHealth` 对主节点算出 `SlaveOffset=0` 导致 lag=MasterOffset（数百万），健康分误判为 0.30。修复后主节点跳过 lag 计算，仅用重连计数。`TestRegressionSnapshotFullresyncOffset` 屏障改为使用从节点真实 offset 而非 `pm.Latest().SlaveOffset`。

### 修复

- **health.go: `computeReplicationHealth` 主角色处理** — 主节点 `GetSlaveReplOffset()` 始终返回 0（`slaveReconnector` 为空）。`MasterOffset - SlaveOffset = MasterOffset` 无意义。新增逻辑：`ConnectedSlaves > 0 && SlaveOffset == 0` 时跳过 lag 计算，仅以重连计数评分。
- **`snapshot_fullresync_overlap_test.go` 屏障** — 从 `pm.Latest().SlaveOffset`（恒为 0）改为 `slave.GetSlaveOffset()`（从节点真实 offset），使 `lag < 10000` 条件可满足。

## v8.13.0 (2026-05-24) — Evolution Gate v1 & Replication Handshake

> **演化门禁与复制握手增强版本。** Evolution 分析升级为 Gate 模式——支持 recovery dynamics 追踪、趋势斜率分析、regime shift 检测，CI 门禁区分 PASS/WARN/FAIL。复制握手层新增 PING/REPLCONF GETACK/SELECT 处理，防止主节点超时断连。新增三种 regression 测试覆盖。

### Evolution Gate v1

- **Recovery dynamics**: `EvolutionRun` 新增 `RecoveryVelocity`、`RecoveryDurationSec`、`PersistenceDurationSec`、`OscillationDetected`
- **趋势斜率分析**: `EvolutionReport` 新增 `HealthSlopeRecent`、`RecoveryTimeSlope`、`PersistenceSlope`、`RecoveryVelocitySlope` + 对应 trend 分类
- **Regime shift 检测**: `RegimeShiftToWorse` 标识持续恶化
- **CI Gate 增强**: nightly-soak 中 evolution gate 输出 `::error`/`::warning`/`PASS`，退出码 1 表示 FAIL，阻止部署
- **Soak report**: 集成 recovery dynamics 到 Markdown 报告 + JSON summary

### 复制握手增强

- **PING 处理**: `readCommandLoop` 收到 PING 时回复 PONG，保持连接活跃
- **REPLCONF GETACK 处理**: 回复 `REPLCONF ACK <offset>`，防止主节点因 ACK 超时断连
- **SELECT 处理**: 忽略数据库选择（BoltDB 只有 DB 0），正确跟踪偏移量
- **新增 `writeRespToMaster`**: 向主节点写入 RESP 响应的辅助函数

### Sentinel Metrics 增强

- 新增 `GetFailoverStarted()`、`GetODownReached()`、`GetSDownBroadcasts()`、`GetSDownReceived()` accessor
- `sentinel_regression_test.go`: 所有直接字段访问改为 accessor 调用（`s.Metrics.DetectionCount` → `s.Metrics.GetDetectionCount()`）

### 新增 Regression 测试

| 测试 | 文件 | 覆盖 |
|------|------|------|
| Failover oscillation | `cmd/integration/failover_oscillation_test.go` | 轨迹单调性 + 无振荡 |
| Split-brain hardening | `cmd/integration/split_brain_harden_test.go` | 愈合后 leader 稳定性 + 单调收敛 |
| Backlog exhaustion | `cmd/integration/regressions/backlog_exhaustion_test.go` | FULLRESYNC 回退 + 数据收敛 |

### 文档更新

- `README.md`: SLAVEOF 支持从 ❌ 更新为 ✅，已知限制移除"不支持 SLAVEOF"
- `docs/stability-spec.md`: 新增 3 项 regression 覆盖
- `docs/failures/backlog-exhaustion.md`、`docs/failures/failover-oscillation.md`、`docs/failures/split-brain-convergence.md`: 新增故障分析文档

## v8.12.0 (2026-05-23) — Config Persistence & Engineering Foundations

> **配置持久化与工程基础版本。** Cluster 和 Sentinel 配置现在持久化到磁盘，重启后自动恢复。新增 LSM 压缩/重平衡测试覆盖、100-goroutine 高并发测试、gossip 基础框架、sentinel slave 健康追踪。run-id 格式标准化为 40-char hex。

### Config Persistence

| 模块 | 存储方式 | 自动保存 |
|------|----------|----------|
| **Cluster** | BadgerDB (`cluster:config` key) | AddNode/RemoveNode/AssignSlot/AssignSlotRange/Replicate 自动触发 |
| **Sentinel** | JSON 文件 (`sentinel.conf.json`) | AddMaster 自动触发 |

### 新文件

| 文件 | 说明 |
|------|------|
| `internal/cluster/persistence.go` | Cluster state (de)serialization + BadgerDB read/write |
| `internal/sentinel/persistence.go` | Sentinel state (de)serialization + file I/O |
| `internal/cluster/gossip.go` | Gossip 基础框架（周期性 PING、PFAIL 检测、过期清理） |
| `internal/store/compaction_test.go` | LSM 压缩/重平衡测试（4 tests） |

### Cluster 增强

- **配置持久化**: `SaveConfig()` / `LoadConfig()`，BadgerDB 自动存储
- **Gossip 框架**: 每秒 ping 随机节点，5s 超时标记 PFAIL，60s 移除过期节点
- **自动恢复**: `NewCluster()` 自动调用 `LoadConfig()` 恢复节点表 + 槽位分配

### Sentinel 增强

- **run-id 标准化**: `crypto/rand` 生成 20 字节 → 40-char hex（与 Redis 兼容）
- **配置持久化**: JSON 文件持久化，`NewSentinelWithDataDir(quorum, downAfter, dataDir)`
- **Slave 健康追踪**: `RecordHeartbeat()`、`MarkOffline()`、`RecordInfoError()`，reconnect 计数

### 测试基础设施

- LSM 压缩/重平衡测试（4 new）: `TestCompaction_HeavyWriteAndCompaction`、`TestCompaction_MassDeleteThenVerify`、`TestCompaction_ConcurrentReadWriteDuringCompaction`、`TestCompaction_StoreCheckAfterHeavyRW`
- 高并发集成测试（5 new）: `TestStringConcurrent_HighScaleIncrement`、`TestStringConcurrent_HighScaleReadWrite`、`TestListConcurrent_HighScalePushPop`、`TestHashConcurrent_HighScaleHset`、`TestSetConcurrent_HighScaleSadd`（各 100 goroutines）

### 杂项

- `.gitignore`: `sentinel`/`evolution` 模式改为 root-only（`/sentinel`、`/evolution`），避免误匹配 `internal/sentinel/persistence.go`

## v8.8.0 (2026-05-22) — Temporal Stability Analysis

> **时间稳定性分析版本。** 系统稳定性从静态标量升级为带风险包络和收敛动力学的多维状态空间。新增轨迹分析（slope/oscillation/persistence/recovery）、集群收敛门禁、哨兵指标仪表化。

### Temporal Analysis（新子系统）

| 组件 | 描述 |
|------|------|
| **`Slope`** | HEALTH 一阶导数（线性回归），每维度独立斜率：正=恢复，负=恶化，零=稳定 |
| **`Oscillation`** | 零交叉检测 + 振幅估计，≥3 次符号翻转 + ≥0.01 均值振幅 = 振荡态 |
| **`Persistence`** | 从最新采样向前遍历，测量各维度低于 0.5 的持续时长 |
| **`Recovery`** | 找最近低谷，计算恢复速度（score/sec）、阻尼比（2.0=过阻尼/1.0=临界/0.5=欠阻尼）、欠冲量 |
| **`Trajectory`** | 五态分类器：stable / recovering / degrading / oscillating / stuck |

集成：`pm.EnableTemporalAnalysis()` 激活 `HealthScore()` 自动记录，`pm.TemporalAnalysis()` 输出。

### 多维度健康评分

- **三维独立评分**：`HealthStorage`（L0/背压/波动性）、`HealthReplication`（延迟/重连）、`HealthCluster`（一致性/震荡/分裂）
- **风险包络**：`cluster < 0.4` 时 total 上限为 `cluster + 0.3` — 集群不稳定性比存储压力更危险（扩散性破坏）
- **收敛时间跟踪**：`ConvergenceTime` 记录从最大分歧到收敛的耗时
- **最弱维度输出**：`FormatCompact()` → `0.96 [OK] S=0.90 R=1.00 C=1.00`

### 集群收敛门禁（Degradation Gates）

- **Sentinel 一致性检查** — `MinAgreedFraction` 门禁（FAIL < 目标共识率）
- **Leader Churn 检查** — `MaxLeaderChurn` 门禁（FAIL/WARN 级）
- **分裂脑检测** — `ClusterFragmented` 即时 FAIL
- **维度健康检查** — `checkDimensionHealth()` 输出最弱维度，< 0.4 直接 FAIL

### 哨兵指标仪表化

- 新增 `Metrics` 结构体：`DetectionCount`, `ODownReached`, `FailoverStarted`, `SuccessfulFailovers`, `FailedFailovers`, `LeaderChanges`
- `GetLeaderChanges()` / `GetSuccessfulFailovers()` / `GetDetectionCount()` 访问器
- `FailoverStartedAt` / `FailoverCompletedAt` / `LastStableAt` 时间戳跟踪
- `LeaderStabilizationDuration` — leader 选举后稳定耗时

### 测试

- `TestSplitBrainConvergence` — 收敛性跟踪器 + 三维健康验证
- `TestSentinelRegression` — 哨兵 failover 事件序列 + 指标验证
- `internal/monitor/temporal_test.go` — 43 项单元测试覆盖所有分析器
- 所有 43 项新测试 + 原 11 包单元测试全绿

## v8.4.0 (2026-05-21) — System Convergence Governance

> **系统收敛治理版本。** 从"证明系统正确"升级到"证明系统在压力下收敛"。failure 变成可重放、可比较时间序列的可执行知识。

### 核心架构

| 组件 | 描述 |
|------|------|
| **`internal/monitor/`** | 压力监控库，支持 JSONL 时间线 + 三级退化门禁（WARN/DEGRADED/FAIL） |
| **`cmd/integration/regressions/`** | 可重放回归测试框架，每类 failure 对应独立隔离的 replay 测试 |
| **`docs/state-machine.md`** | 完整形式化状态机文档（connection / replication / sentinel / server lifecycle） |
| **`docs/design-constraints.md`** | 系统设计约束规范（收敛性定义、恢复时间预算、admission policy v0） |

### 收敛性门禁（Degradation Gate）

退化断言升级为 WARN / DEGRADED / FAIL 三级：

| 级别 | 含义 | CI 行为 |
|------|------|---------|
| WARN | 早期信号，系统仍健康 | 日志 |
| DEGRADED | 压力偏离健康范围 | `t.Errorf` 或日志（可配置） |
| FAIL | 硬不变性违反 | `t.Errorf` |

每项不变性检查（goroutine 增量 / ActiveRetries / L0 score / reconnect / monotonic rise）独立判定级别。

### JSONL 时间线

- `SOAK_JSONL_DIR` 环境变量驱动，soak 运行时自动生成 `soak-<timestamp>.jsonl`
- 每个采样行含 16 个字段：goroutine / heap / GC / retry metrics / L0 / replication offsets / backlog / reconnects
- 支持直接喂给 jq / gnuplot / Grafana 做时间序列分析
- 5h soak 产生 ~600 样本点的完整系统行为历史

### 可重放回归套件

| 测试 | 覆盖的 failure | 类型 |
|------|---------------|------|
| `TestRegressionRetryStorm` | retry-storm | 局部压力 |
| `TestRegressionReplicationThrash` | replication-thrash（短分区） | 分布式收敛 |
| `TestRegressionReplicationThrashFullresync` | replication-thrash（长分区→FULLRESYNC） | 分布式收敛 |
| `TestRegressionSnapshotConsistency` | snapshot-inconsistency（全类型语义正确性） | 数据正确性 |
| `TestRegressionSnapshotConcurrentWrites` | snapshot-inconsistency（并发写入一致性） | 数据正确性 |

每个测试：独立 Badger DB + 独立服务器 + 独立 PressureMonitor + `expected_metrics.json` 轨迹文档。

### 复制修复

- **Backlog offset ordering fix** — `CONTINUE` 模式下 slave offset 与 backlog 范围匹配修复
- **Slave reconnector race** — 关闭时 `reconnectLoop` 不再尝试访问已关闭的 DB
- **5h 复制 soak** — 在周期网络分区 + 持续写入下验证收敛性

### 测试基础设施

- `t.Parallel()` 覆盖所有测试包
- 确定性事务冲突测试
- 大规模边界测试（string / collection）
- 完整 WRONGTYPE 覆盖集成测试
- `TestSoak` 独立 soak 测试（可配置 data dir / duration / concurrency）
- Server fuzzing 扩展（9 新 opcode + pipeline + concurrent target）
- node-redis 兼容测试 17 个 false FAIL 修复

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
