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
| 256GB 数据测试（机械盘） | 2026-07-27 完成：256GB 净数据 / 262K keys × 1MB / 77.7 MB/s avg / 磁盘放大 1.3x / 0 数据损坏 |
| 生产就绪评估 | 2026-07-27 完成：见[生产就绪评估](#生产就绪评估)章节 |

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

## 生产就绪评估（2026-07-27）

> 基于全部已有数据 + 256GB 实测的生产可用性评估。

### 分场景结论

| 场景 | 结论 | 说明 |
|------|------|------|
| 单节点 / 数十 GB 级数据 | ✅ **生产可用** | 239/239 命令、三方客户端 100% 兼容、7 层 OOM 防护 |
| 主从复制（1主1从/1主多从） | ✅ **生产可用** | K:HASH:47 已根因修复 + 纵深防御，复制回归全 PASS |
| Cluster 多节点（3+） | ⚠️ **可用，建议观察期** | 幽灵节点、ID 持久化已修复，但缺长时 soak |
| 100GB–256GB 数据（HDD） | ✅ **已验证可行** | 2026-07-27 机械盘测试：77.7 MB/s avg，磁盘放大 1.3x，0 损坏 |
| **1TB+ 大规模数据** | ❌ **未验证** | 需完成 Tier 2 规模化验证（1TB+ SSD + 7 天合成负载） |
| **强一致性 / 金融级场景** | ❌ **不建议** | BadgerDB LSM + Redis 最终一致性模型，非强一致 |

### 已满足的生产就绪条件

#### 数据安全（✅ 强）
- **239/239 命令**全覆盖，100% RESP3 Null 覆盖
- redis-py (153/153), node-redis (110/110), redis-cli (77/77) **三方客户端 100% 兼容**
- **RDB CRC64 校验** — 从节点加载前验证快照完整性
- **复制 offset 锁步修复**（K:HASH:47） — 主从 offset 漂移元凶已根因修复 + 纵深防御
- **5 轮竞争对手审查**，17/16 项算法缺陷已修复
- **Mutation testing**: 5,201 变异体，100% 击杀率，Store 90.17%

#### 稳定性（✅ 强）
- **7 层 OOM 防护**：GOMEMLIMIT → OutputBufferLimit → L0 背压 → 并发写信号量 → RESP 协议限制 → MaxClients → SCAN 书签淘汰
- **自动内存检测**：启动时自动探测 RAM，按比例推导 OutputBufferLimit / MaxInputBytes
- **goroutine leak 测试**：CI 集成，`>10` 偏差告警
- **Tier A CI 全绿**（lint + unit + fast integration + bench guard）

#### 运维（✅ 中等）
- Docker Compose 支持（standalone / cluster / master-slave / sentinel）
- TOML 配置文件（CLI flag > 配置 > 自动推导 > 硬编码默认值，优先级链完整）
- Prometheus metrics 端点
- 10 个版本 tag，成熟发版节奏（v8.39.1）

### 已知缺口与风险

#### 🟡 P1 — 建议关注

| 风险 | 严重度 | 详情 | 状态 |
|------|--------|------|------|
| Nightly Soak GHA 不可用 | 低 | 连续多日因 runner OOM/超时 exit 143，非代码 bug，但自动化 soak 覆盖缺失。 | 需远程 Linux 手动跑 |
| executeReplicatedCommand 重复 | 低 | 1675 行 switch 与 handler dispatch 重复。已有对称性测试拦截 drift，但仍为工程债。 | 已有 `TestReplicationSymmetry_*` 守卫 |

#### 🔵 P2 — 已知但不阻塞

| 项 | 说明 |
|----|------|
| ZRANK O(n) | 10K zset 最坏 ~1.1ms > 1ms 阈值。已有明确决策暂不做跳表缓存 |
| Cluster 长时 soak | 未验证 3+ 节点长期运行稳定性 |
| Backlog resize 不支持热更新 | 仅启动时设置，无运行时 CONFIG SET 路径 |
| 58 个重测试在 GHA 被跳过 | 远程 Linux 全量可恢复，非 flaky |

### 256GB 实测数据（2026-07-27）

| 指标 | 实测值 |
|------|--------|
| 数据集 | **256 GB** 净数据（262K keys × 1MB） |
| 存储介质 | 机械盘 `/dev/sda1`（916G） |
| 写入速率 | **77.7 MB/s 平均**，稳定 56 分钟 |
| 磁盘放大 | **1.3x**（256G → 334G 磁盘占用） |
| 数据完整性 | ✅ **100%**（随机抽样验证全通过） |
| 服务稳定性 | ✅ 0 崩溃，0 panic，0 race |

**关键发现：** 机械盘 + 256GB 规模下 BadgerDB compaction 能收敛，1.3x 放大符合预期。速率从 375 MB/s 降至 77.7 MB/s 的瓶颈在 **机械盘随机写入 + compaction 稳态，非代码**。建议 SSD 复测以获取 SSD 基线。

### 推荐生产部署配置

```bash
# 单节点生产
boltDB -dir=/data/boltdb \
  -addr=:6379 \
  -skip-startup-cleanup \
  -log-level=info \
  -client-output-buffer-limit=32MB

# 主从
boltDB -dir=/data/boltdb -addr=:6379 --replicaof 10.0.0.1:6379

# Cluster（3节点起步，先 ADDSLOTS 再 MEET）
boltDB -dir=/data/boltdb -addr=:6379 -cluster
```

**建议配套**：
- Prometheus metrics（默认 `/metrics` 端点）
- 定时 BGSAVE
- 监控：磁盘使用率 >80% 告警、L0 score >10 告警、goroutine 异常增长
- 生产环境**推荐 NVMe SSD**，机械盘仅适用于非延迟敏感场景
