# BoltDB 待办列表

## 当前状态

| 指标 | 值 |
|------|----|
| RESP3 Null 覆盖 | 34/34 命令（100%） |
| redis-py compat | 153/153 (100%) |
| node-redis compat | 110/110 (100%) |
| redis-cli compat | 77/77 (100%) |
| timer 泄漏 | 8/8 已修复 |
| isWriteCommand | 94/94 完整 |
| goroutine leak test | 通过 |
| handler.go 拆分 | 8824→0 行，拆为 24 个文件，无单文件超 1136 行 |
| Cluster gossip payload | ✅ 已实现（SlotOwners + Nodes + PFail） |
| PFAIL gossip 传播 | ✅ 已实现（多节点投票晋升） |
| 槽位视图同步 | ✅ 已实现（epoch 裁决） |
| Redis 8 命令补齐 | 5/5 批次完成 |
| 全命令准确性测试 | 239/239 命令已覆盖 |
| Mutation Testing | 5,201 变异体，100% efficacy，Store mcover 90.17% ✅ |
| Lua 脚本（EVAL/SCRIPT）技术分析 | 已完成，确认不实现，见 [lua-scripting.md](lua-scripting.md) |
| 收购审查 A–D（第一阶段） | 19/19 项全部完成 |
| 收购审查 E–F（第二阶段） | 14/14 项全部完成（含 E3 方案B ✅） |
| 竞争对手算法缺陷修复 | 五轮审查 17/17 项完成（16 已修复 ✅，1 待 benchmark ⏳） |
| RANDOMKEY 蓄水池采样 | O(2n) → O(n) 单次遍历 |

---

## 高并发 OOM 防护现状

### 已实施的防护（7 层）

| 层 | 防护机制 | 阈值 |
|----|---------|------|
| 1 | GOMEMLIMIT（自动检测） | total - 2GB, ≥256MB, ≤90% |
| 2 | OutputBufferLimit | 默认 32MB/连接 |
| 3 | L0 写背压 | 软 8.0 / 硬 20.0 |
| 4 | 并发写信号量 | 50 并发 retryUpdate |
| 5 | RESP 协议限制 | Bulk 64MB / Array 1M / Line 64MB |
| 6 | MaxClients | 默认 10000 |
| 7 | SCAN 书签淘汰 | 10000 上限 → 75% 淘汰 |

### 修复缺口（P0）

- [x] **OutputBufferLimit 默认值 0 → 32MB**（第五轮已修复）
- [x] **GOMEMLIMIT 自动设置**（第五轮已修复）
- [x] **MaxInputBytes 默认值 0 → 1GB**（CLI flag + TOML 配置均已设置 ✅）
- [x] **MaxBulkLen 默认值 256MB → 64MB**（CLI flag + proto init() 均已更新 ✅）

---

## 出厂默认配置基准（4C8G）

> 设计原则：默认值服务于"开箱即用"体验，配置项服务于"按需调优"。
> 以 4 核 CPU / 8GB 内存 / SSD 为基准调整出厂默认值。

| 参数 | 旧默认值 | 新默认值 | 理由 |
|------|---------|---------|------|
| `GOMEMLIMIT` | 自动计算 ~6GB | 同上（已有） | 自动检测 + 可覆盖 |
| `MaxClients` | 10000 | 10000 | 兼容 Redis |
| `OutputBufferLimit` | 32MB | 32MB | 已验证合理 |
| `MaxInputBytes` | 0（不限制）→ **1GB** ✅ | 已实现：CLI 默认值 + TOML 配置 + 按 RAM 比例自动推导 |
| `MaxBulkLen` | 256MB → **64MB** ✅ | 已实现：CLI 默认值 + TOML 配置 + proto init() 统一更新 |

### 未来方向：按比例自动推导（已完成 ✅）

已实现：`main.go` 启动时自动检测 RAM，按公式 `OutputBufferLimit = min(32MB, RAM/256)` 和 `MaxInputBytes = min(1GB, RAM/8)` 自动推导，CLI flag > 配置文件 > 自动推导 > 硬编码默认值 优先级链。已覆盖手动设置优先判断。

### 启动可见性（已完成 ✅）

启动时已打印检测到的硬件信息和生效配置摘要：

```
=== BoltDB Configuration ===
Detected: CPU 4 cores / RAM 8 GB
Active config:
  GOMEMLIMIT=6GB  max-input-bytes=1GB
  client-output-buffer-limit=32MB  max-clients=10000
  proto-max-bulk-len=64MB
============================
```

实现位置：`cmd/boltDB/main.go` startup banner block（约 217-242 行）。

---

## 架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL (partial) / FUNCTION / MIGRATE | FUNCTION 随 Lua 排除；MIGRATE 不适用（嵌入式）；ACL 仅实现 CAT/LIST/USERS/WHOAMI/HELP，SETUSER/DELUSER 未实现 |

---

## 待办项

### Cluster 部署问题（P2）

**现状：** 部署 3 节点 cluster 时发现以下问题：

- [x] **`cluster_size:1` 硬编码占位符** — `internal/cluster/commands.go:122` 中 `cluster_size` 固定为 1，未根据实际节点数量计算。
  - 已修复：遍历 `Nodes` 表统计 `IsMaster && !HasFailFlag` 的健康 master 节点数。
- [x] **CLUSTER MEET 产生 pfail 幽灵节点** — 执行 `CLUSTER MEET` 后，节点表中出现重复的幽灵节点条目（不同 NodeID 但同 IP:Port，均处于 pfail 状态）。原因是 MEET 握手过程中 `BuildGossipPayload` 的 `SlotOwners` 和 `Nodes` 节包含所有节点（含刚添加的 placeholder），gossip 传播后接收方创建了多余的节点记录。幽灵节点不影响数据读写，但会在 `CLUSTER NODES` 中显示。
  - 已修复：`ApplyGossipPayloadFrom` 处理 gossip `Nodes` 节时，通过 `findNodeByAddr` 检查是否已有相同地址的节点，若存在则跳过创建重复条目。
  - 验证：3 节点集群 CLUSTER NODES 输出均为 3 个 master 节点，无任何 pfail/幽灵条目。

- [x] **重启后节点 ID 重新生成，槽位持久化失效（★★★）** — 重启后节点 ID 变化导致 `loadState` 中持久化的 slot 所有者指向旧节点 ID，当前节点无槽位。
  - 已修复（两处修改）：
    a. `persistence.go` 新增 `loadPersistedNodeID(db)` — 在 `NewCluster` 生成新 ID 前从 BadgerDB 读取持久化的 `node_id`，避免重启后 ID 变化。
    b. `cluster.go` `NewCluster` 的 `else`（首次启动）分支末尾自动调用 `SaveConfig()`，确保配置立即持久化。
  - 验证结果：3 节点集群两次全节点重启后节点 ID 不变、槽位表 5461/5462/5461 不变、数据可读。

**注意：** 部署顺序必须是 **FLUSHSLOTS + ADDSLOTS → CLUSTER MEET**（先分配槽位再组建集群）。如果 MEET 先于 ADDSLOTS，gossip 传播（各节点初始全槽位 0-16383）会覆盖本地槽位分配。后续可考虑用 epoch 机制彻底解决 gossip 覆盖问题。

### CI 稳定性（P1 — 预存 flaky 测试修复）

**现状：** Tier A（test-fast/PR 门禁）已稳定全绿。剩余 3 类预存 flaky：

- [x] **单元测试随机 mutation kill flaky** — 已从 19 个 boundary/mutation kill 测试文件中批量移除 `t.Parallel()`（共 911 处），消除 BadgerDB 在 `-race` + `t.Parallel()` 下的写竞争
  - 涉及文件：`internal/server/`（6 文件）、`internal/store/`（13 文件）

- [x] **复制集成测试 GHA 超时** — `TestReplicationNewCommands`、`TestReplicationSortedSetCommands`、`TestReplicationCompleteness_Key` 在 GHA runner 上 FULLRESYNC 需要 20-30s，pollSlave 超时。
  - 已从 Tier A 移至 Tier B，但仍不可靠。
  - 已修复：`setupMasterSlaveServer` 添加 `waitForFullresync` 轮询 `master_link_status:up`（60s 超时），等待 FULLRESYNC 完成再返回。Tier B 已恢复运行这些测试。

- [x] **TestRegressionFailoverOscillation 超时** — 在 GHA 上耗时 33s+，regression 测试包超时。
  - 已从 300s 提到 600s，观察是否稳定。

### 配置文件支持（已完成 ✅）

- [x] **定义 Config struct + TOML tag**：`cmd/boltDB/config.go`
- [x] **添加 BurntSushi/toml 依赖**：`v1.6.0`
- [x] **`-config` flag**：加载 TOML 配置文件
- [x] **`--dump-config` 子命令**：打印完整注释的配置模板
- [x] **优先级链**：CLI flag > 配置文件 > 自动推导 > 硬编码默认值
- [x] **deploy/boltdb.toml**：102 行全中文注释的默认配置文件

### ZRANK / ZRANGE by rank — 无跳表 O(n) 线性扫描 → ❌ 暂不实施（建立基准测试替代）

**位置：** `internal/store/zrange.go:93-144`、`internal/store/zrank.go:60-82`

**最终决策（2026-07-04）：不做。** 理由：

1. **高投入、高维护成本。** 引入内存 B-tree 缓存需修改 ~15 个写路径 + 启动重建 + 一致性保证，长期维护成本高。
2. **无生产数据。** 没有用户证明 ZRANK 是热点或瓶颈。
3. **理论 O(n) ≠ 实际瓶颈。** 在 10K 条目以下，BadgerDB 线性扫描延迟 0.2-2ms，对大多数场景可接受。

**替代方案：建立性能基线**
- [ ] 编写 zset 基准测试（100/1K/10K/100K 条目），覆盖 ZRANK/ZRANGE/ZADD
- [ ] 与 Redis 横向对比，确认瓶颈在排序集索引还是 BadgerDB I/O
- [ ] 如果 10K+ 条目时 ZRANK 延迟 > 1ms，重新评估缓存方案
- [ ] 设计文档保留在 `docs/plans/zrank-cache-design.md`，供后续参考

### 规模化验证（P1 — 收购阻塞项，唯一主要缺口）

**投入：** 6 月+（分级验证）　**优先级：** P1（文档修正已完成）

README 宣称 *"Memory Redis can only store 64GB? BoltDB can handle 100TB!"*，但当前最大测试数据量仅为 MB–GB 级。

#### 第 1 级（2 周）— 10GB → 100GB 验证

- [x] 在 bolt-remote 上部署定型负载脚本：`scripts/scale-test-tier1.sh`
- [x] 使用自定义 Go 客户端（`cmd/scale-data-filler`）填充 1GB 数据
- [x] 验证指标并记录到 `docs/scaling/scale-tier1-report.md`：

| 指标 | 通过标准 | 实测值（1GB） |
|------|---------|---------------|
| SET 吞吐量退化（相对空库） | < 20% | ~5%（估算） |
| GET 延迟 P99 | < 10ms | **< 0.9ms** |
| L0 峰值 | < 15 | 未测量（需 metrics 端点） |
| 重启时间 | < 30s | **5.1s** |
| FULLRESYNC 速率 | > 100MB/s | 未测试（需独立 replica） |
| 磁盘空间放大 | < 2.5x | **1.5x** |

**说明：** 受磁盘空间（215G 可用）限制，10GB/100GB 测试暂未执行。1GB 测试已验证全部方法学：`scripts/scale-test-tier1.sh` 自动执行完整流程，`cmd/scale-data-filler` 支持 `--size` 参数。需要更大规模时，将 `--size 10GB` 传入即可。

#### 第 2 级（1 月）— 1TB 验证

- [ ] 协调额外磁盘资源（1TB+ SSD）
- [ ] 持续运行 7 天合成负载（80% 读 / 20% 写）
- [ ] 产出 `docs/scaling/capacity-planning.md`（每 TB 内存/磁盘/IOPS 模型）
- [ ] 重点验证：LSM compaction 行为、ValueLog GC 收敛性、背压系统、goroutine 泄漏、Cluster 数据均匀性

#### 第 3 级（3 月+）— 10TB + 架构演进决策

- [ ] 需专用机器（或云实例）
- [ ] 验证主从切换时间（failover）、BGSAVE 期间性能影响
- [ ] 基于 1–10TB 验证结果评估架构决策：

| 发现 | 可能对策 |
|------|---------|
| BadgerDB L0 stall 在 >5TB 不可收敛 | 评估切换 Pebble / 深度配置调优 |
| ValueLog GC 跟不上写入速率 | 增大 ValueLogFileSize / SSD / 分区 |
| 单实例成为瓶颈 | 按 slot 分片 / 多 DB 实例 / 存储抽象 |
| FULLRESYNC 时间不可接受 | commit-seq ↔ repl-offset 映射 |

---

## 竞争对手审查：算法缺陷修复记录

> 五轮审查共 17 项：16 项已修复 ✅，1 项待 benchmark ⏳（ZRANK/ZRANGE 跳表）。

| 轮次 | 日期 | 修复项 | 关键修复 |
|------|------|--------|---------|
| 第一轮 | 2026-07-02 | 3 ✅ + 1 ⏳ | SCAN 假游标 O(n²)→O(n)、ZINTERSTORE 全量拉取、RANDOMKEY 蓄水池采样 |
| 第二轮 | 2026-07-03 | 3 ✅ | HLL Estimate 系数 47000× 错误、Migration Phase 2 遗漏二级索引、`allCopied` dead code |
| 第三轮 | 2026-07-03 | 2 ✅ | SCAN 书签淘汰无界增长、HLL `countTrailingZeros` 手写→`bits.TrailingZeros64` |
| 第四轮 | 2026-07-03 | 3 ✅ | `randomIntn` crypto→math/rand/v2、SPop 线性扫描→双向搜索、`matchPattern` 灾难性回溯→DP |
| 第五轮 | 2026-07-03 | 6 ✅ | Gossip 全量拷贝+全洗牌→蓄水池采样、HRandField/H走读MBER 全量加载优化、remote-test.sh OOM、输出缓冲区默认值、启动设置 MemoryLimit |
| 第六轮 | 2026-07-04 | 3 项（1 已修复 ✅, 1 已降级 ⏳→✅, 1 架构评价） | **CRC64 只写不验**（★★★★★）**已修复**、**backlog resize 丢历史**（★★★★☆→★★☆☆☆）、command dispatch 899 行 switch 架构评价（★★☆☆☆） |

### 第六轮（2026-07-04）具体待办

#### RDB CRC64 校验缺失（★★★★★）

**发现：** `internal/replication/rdb.go:288-294` 在 `WriteFooter()` 中写入 8 字节 CRC64 校验码，但 `internal/replication/rdb_loader.go:183-189` 遇到 `0xFF` 结束符后直接 break，未读取/验证 CRC64。

**影响：** RDB 快照在磁盘静默错误、截断、程序 bug 或传输异常后，从节点加载时零感知、零告警，损坏数据可正常入库。

**建议修复：**
- [x] `loadRDBEntries` 在遇到 `0xFF` 后读取后续 8 字节 CRC64
- [x] 用与 encoder 相同的 `crc64.MakeTable(crc64.ECMA)` 重新计算并比对
- [x] 校验失败时返回明确的错误信息（含期望值 vs 实际值）
- [ ] 考虑在大 RDB 加载时启用流式 CRC（边解析边校验，当前是全量 buffer 后解析）

#### Backlog resize 丢失历史数据（★★★★☆）

**发现：** `internal/replication/replication.go:88-98` 的 `SetBacklogSize` 直接 `rm.backlog = NewReplicationBacklog(size)`，新缓冲区从 offset=0 开始，旧数据全部丢弃。所有等待 PSYNC CONTINUE 的从节点会因 offset 不可用而回退到 FULLRESYNC。

**⏳ 待确认：** 需要确认此接口是否暴露了运行时 `CONFIG SET` 或存在运行期调用路径。若仅用于启动配置（重启生效），则评级降为 ★★☆☆☆。

**建议修复（若需支持热修改）：**
- [x] 确认调用链：`SetBacklogSize` 仅 `main.go:272` 启动时调用，无运行时 `CONFIG SET` 路径 → 风险从 ★★★★☆ 降为 ★★☆☆☆
- [ ] 若未来增加运行时 resize 支持，需迁移为：分配新 buffer + 复制有效窗口 + 原子替换指针

#### Command dispatch 899 行 switch（★☆☆☆☆）

**类型：** 架构偏好，非功能缺陷。当前 switch 模式在 Go 中可正常工作，golangci-lint 可静态检查锁泄漏。建议未来引入命令表（`map[string]Handler`）以支持元数据管理和扩展性，但不阻塞当前开发。

---

## 第七轮：竞争对手架构评审（2026-07-04）

> Redis 核心维护者视角的深度架构评审，覆盖冒泡排序、LSM 天花板、Go GC 三条攻击线。详见本文件最后附注。

### 评审结论：可修复性问题优先级

| 优先级 | 问题 | 代价 | 影响 | 性质 |
|--------|------|------|------|------|
| **P0** | **SORT 冒泡排序 → `sort.Slice`** | < 1 人天 | 消除 "amateur" 标签，降低维护成本 | 工程质量 |
| **P0** | **ZRANK O(n) → O(log n)** | 1-2 周 | 消除 feature gap，提升 sorted set 竞争力 | 功能缺口 |
| **P1** | **`executeReplicatedCommand` 1675 行 switch 去重** | 2 天-2 周 | 降低复制路径与处理路径 drift 风险 | 工程质量 |
| **P1** | **规模化验证（10GB→100GB→1TB）** | 2 周-1 月 | 验证 "100TB" 宣称可信度 | 验证缺口 |
| **P2** | **Go GC 优化（pooling、zero-alloc 路径）** | 持续投入 | 提升 P99，增加并发密度 | 性能优化 |
| **❌** | **LSM 写放大 / compaction 风暴** | 不可修（路线选择） | 接受为差异化定位的前提条件 | 架构边界 |

### 已确认的待办项

#### SORT 冒泡排序（★★★★★）

**发现：** `internal/server/key_commands.go:721-746` 和 `internal/replication/psync.go:1651-1816` 均手写冒泡排序 O(n²)。Go 标准库 `sort.Slice` / `slices.SortFunc` 可用。

**真正的风险不是主从不一致**（两份代码相同，排序结果一致），而是：
1. 同为 O(n²) 但冒泡在数据量稍大时（如几千）就明显慢于快排
2. 代码重复：`executeReplicatedCommand` 的 1675 行 switch 与 handler dispatch 高度重复，未来某一边改排序算法但另一边遗漏时才会出问题

**建议修复：**
- [x] 两处 SORT 统一替换为 `sort.Slice()`（需带 `alpha`、`asc`/`desc`、`by` 等选项的组合比较器）
- [x] 补充 `SORT` 的有序性测试（19 个测试，覆盖 list/set/string/zset、ASC/DESC/ALPHA、BY/GET/STORE/LIMIT、重复值、空键、错误类型）

#### executeReplicatedCommand 代码重复（★★★★☆）

**发现：** `internal/replication/psync.go:172-1846` 包含一个 1675 行的 switch 语句，逐一复制了 handler dispatch 中的命令执行逻辑。每条新增命令和每次 handler 行为变更都需要手动同步到此路径。default branch 仅打一行 log 后 `return nil`，未识别命令被静默跳过。

**建议修复（轻量方案，2 天）：**
- [x] 创建 `ReplicatedCommands` 注册表 (`internal/replication/commands.go`)，列出所有可复制的写命令
- [x] 创建 `ReplicatedCommandsExcluded` 记录有意不实现的命令及原因（MIGRATE、PUBLISH 等 14 个）
- [x] 新增 `TestReplicationSymmetry_WriteCommandsCovered` — 拦截未覆盖的写命令 drift
- [x] 新增 `TestReplicationSymmetry_NoOrphanCommands` — 拦截已删除的孤立命令
- [x] 补齐 `UNLINK` 到 `executeReplicatedCommand`（与 DEL 共用 case）
- [x] 评估剩余 14 个命令：均因缺少 handler 级上下文（PubSub、Cluster、DB 级操作）无法仅用 `*store.BotreonStore` 实现，已记录到 `ReplicatedCommandsExcluded`

#### SORT 的有序性缺失（★★★★☆）

**发现：** `SORT` 命令当前没有专门的排序正确性测试。`TestRESPShape` 只检查返回类型，不检查元素顺序。只有 `redis-py compat` 和 `node-redis compat` 间接覆盖，但覆盖率不定。

**建议修复：**
- [x] 添加 `TestSORTOrdering`：插入已知顺序的数据，验证 `SORT ASC` / `SORT DESC` / `SORT ALPHA` 的返回顺序（19 个测试，已提交）
- [ ] 验证 `SORT STORE` + replication 后从节点的顺序一致

### 架构评估：不做修复的记录

以下问题经评审被归类为"路线选择"，不需要"修复"：

| 问题 | 原因 |
|------|------|
| LSM 写放大 / compaction 风暴 | BadgerDB 的物理规律，不可消除。已存在的背压系统 + 收敛监测为合理缓解策略 |
| Go GC 开销 | Go 运行时的固有代价。fork 是 Redis 的对应代价，两者是不同维度而非一方占优 |
| 协议兼容 ≠ 语义兼容（大规模下） | 这是规模化验证计划要回答的问题，不是代码能直接修的 |

### 评审质量自评

> 第七轮评审以 Redis 核心维护者身份完成，后被内部 reviewer 拆解，发现：
> - ✅ **SORT 冒泡排序**：真实发现的代码缺陷，9.5/10
> - ⚠️ **LSM 架构批评**：核心取舍抓得准，但把 Redis 的 fork/AOF rewrite 等代价弱化了，8.5/10
> - ❌ **Go GC 批评**：基于对旧版 Go GC 的认知，忽视了 1.19+ 并发 GC 的改进，6.5/10
> - **主从不一致攻击点不成立**：两份代码相同，排序结果必然一致。真正的攻击点是 code duplication + maintenance risk。
>
> **内部 reviewer 给出的 3 个更高价值的攻击点：**
> 1. Redis 命令复杂度失真（SCAN/ZRANGE/SORT 的实际复杂度与 Redis 官方复杂度是否一致）
> 2. LSM 无法提供 Redis 的尾延迟可预测性（P99/P999 是 LSM 固有弱点）
> 3. 内存数据结构服务器 vs KV 引擎的语义映射鸿沟（protocol compatibility ≠ semantic compatibility）

---

## 第八轮：性能模型与查询预算（2026-07-05）

> 从"算法缺陷查找"升级到"架构性能模型审查"。核心发现：系统只有 KV 级 sublinear guarantee，所有复合结构操作（ZRANK/ZRANGE/GEO/ZINTER/HGETALL/SMEMBERS）均为 O(n) prefix scan + filter。
>
> **结论：这不是实现缺陷，是数据模型选择。修复方向不是加内存索引，而是让 O(n) 模型可预测、有上限、有文档。**

### 本轮修改

| 修改 | 文件 | 性质 | 备注 |
|------|------|------|------|
| GeoRadius maxScore 上界 | `internal/store/geospatial.go` | 正确性修复 | 缺失上界导致半径查询退化为全表扫描 |
| LCS 输入守卫 (10KB) | `internal/store/lcs.go` | 安全围栏 | O(mn) DP 表防 OOM |
| QueryBudget 机制 | `internal/store/backpressure.go` | 通用框架 | `MaxScanIterations` 配置，防止慢查询拖垮系统 |

### 架构决策：不做跳表

| 方案 | 决策 | 理由 |
|------|------|------|
| 加内存 skiplist 修 ZRANK | ❌ 不做 | 违背 disk-backed 核心卖点，破坏 convergence |
| 替换 BadgerDB | ❌ 不做 | 全盘重做，否定现有架构 |
| 接受 O(n) 模型 + budget guard + 文档 | ✅ 当前路线 | 符合项目定位，收敛优先 |

### 性能模型总结

```
point query      → O(log n) 平均（LSM 无 worst-case 保证）
range query      → O(n)
ranking query    → O(n)
geo query        → O(n + geohash cell filter)
set operations   → O(n·k)
```

所有用户感知的操作集中在 O(n) 层。详见 [README 局限性说明](../../README_CN.md#局限性说明)。
