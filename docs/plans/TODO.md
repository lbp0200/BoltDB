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
| 5 | RESP 协议限制 | Bulk 256MB / Array 1M / Line 64MB |
| 6 | MaxClients | 默认 10000 |
| 7 | SCAN 书签淘汰 | 10000 上限 → 75% 淘汰 |

### 修复缺口（P0）

- [x] **OutputBufferLimit 默认值 0 → 32MB**（第五轮已修复）
- [x] **GOMEMLIMIT 自动设置**（第五轮已修复）
- [ ] **MaxInputBytes 默认值 0 → 1GB**（见下方新待办）

---

## 出厂默认配置基准（4C8G）

> 设计原则：默认值服务于"开箱即用"体验，配置项服务于"按需调优"。
> 以 4 核 CPU / 8GB 内存 / SSD 为基准调整出厂默认值。

| 参数 | 旧默认值 | 新默认值 | 理由 |
|------|---------|---------|------|
| `GOMEMLIMIT` | 自动计算 ~6GB | 同上（已有） | 自动检测 + 可覆盖 |
| `MaxClients` | 10000 | 10000 | 兼容 Redis |
| `OutputBufferLimit` | 32MB | 32MB | 已验证合理 |
| `MaxInputBytes` | **0（不限制）** | **1GB** | 防止异常长连接耗尽内存 |
| `MaxBulkLen` | **256MB** | **64MB** | 更符合 8GB 内存的大多数场景 |

### 未来方向：按比例自动推导

逐步让内存相关参数支持按 RAM 比例计算，同时保留手动覆盖：

- `OutputBufferLimit = min(32MB, RAM / 256)`
- `MaxInputBytes = min(1GB, RAM / 8)`

### 启动可见性

启动时打印检测到的硬件信息和生效配置摘要，让运维人员一目了然：

```
Detected: CPU 4 cores / RAM 8 GB
Active config:
  GOMEMLIMIT=6GB  max-input-bytes=1GB
  client-output-buffer-limit=32MB  max-clients=10000
```

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

### 配置文件支持（P0 — TOML 格式）

- [x] **定义 Config struct + TOML tag**：`cmd/boltDB/config.go`
- [x] **添加 BurntSushi/toml 依赖**：`v1.6.0`
- [x] **`-config` flag**：加载 TOML 配置文件
- [x] **`--dump-config` 子命令**：打印完整注释的配置模板
- [x] **优先级链**：CLI flag > 配置文件 > 自动推导 > 硬编码默认值
- [x] **deploy/boltdb.toml**：102 行全中文注释的默认配置文件

### ZRANK / ZRANGE by rank — 无跳表 O(n) 线性扫描 ⏳

**位置：** `internal/store/zrange.go:93-144`、`internal/store/zrank.go:60-82`

**决策：** P2，等 benchmark 证明是热点后再加内存 B-tree 缓存。当前 mid-point 优化已提供部分缓解。

### 规模化验证（P1 — 收购阻塞项，唯一主要缺口）

**投入：** 6 月+（分级验证）　**优先级：** P1（文档修正已完成）

README 宣称 *"Memory Redis can only store 64GB? BoltDB can handle 100TB!"*，但当前最大测试数据量仅为 MB–GB 级。

#### 第 1 级（2 周）— 10GB → 100GB 验证

- [ ] 在 bolt-remote 上部署定型负载脚本：`scripts/scale-test-tier1.sh`
- [ ] 使用 `redis-benchmark` + 自定义 Go 客户端填充 100GB 数据
- [ ] 验证指标并记录到 `docs/scaling/scale-tier1-report.md`：

| 指标 | 通过标准 |
|------|---------|
| SET 吞吐量退化（相对空库） | < 20% |
| GET 延迟 P99 | < 10ms |
| L0 峰值 | < 15 |
| 重启时间 | < 30s |
| FULLRESYNC 速率 | > 100MB/s |
| 磁盘空间放大 | < 2.5x |

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
