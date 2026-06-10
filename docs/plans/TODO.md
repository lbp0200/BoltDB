# BoltDB 待办列表

## P0：Longitudinal Observation & Stability Verification

**Status:** COMPLETE / FROZEN

```text
PASS
Window: 2026-06-01 ~ 2026-06-09
```

- [x] Nightly soak pipeline (crontab, 2:00 daily, remote 2.16)
- [x] Evolution gate (cross-run trend analysis)
- [x] Basin analysis (phase space classification)
- [x] Drift analysis (delta > 0.05 detection)
- [x] Anomaly detection (trajectory regime shifts)
- [x] 7+ day observation window
- [x] Stable attractor verified (basin: healthy, health: 1.00, L0: 0.0, oscillation: none)

> P0 closed. Further monitoring work requires a new justification and is not part of the current roadmap.

---

## P1：Technical Debt Reduction

### P1.1 Failover Oscillation (#2)

**目标：**

- 消灭 known flaky
- Sentinel failover 收敛性验证
- Nightly failover trajectory 稳定

**跟踪：**

- [x] 识别 oscillation test 未覆盖的边界条件（连续掉线 + 大量写入）
- [x] 新增 Scenario C/D
- [x] sentinel: `selectNewMaster` 添加 TCP 存活探活（跳过死 slave，不标记 offline）
- [x] sentinel: 添加 failover cooldown（5s 冷却，防止 gossip 重触发风暴）
- [x] sentinel: `SendReplicaOf` 添加读超时（防止阻塞 failover goroutine）
- [x] 测试验证：Scenario A/B + Harden + C + D 全部通过（lint clean）

---

### P1.2 Duplicate Window Architecture

**目标：**

- 记录当前 envelope
- 评估 commit-seq ↔ repl-offset 映射方案
- 不承诺立即实现

**跟踪：**

- [ ] 设计 commit-seq ↔ repl-offset 映射方案
- [ ] 评估波及面（写路径、Badger MVCC、backlog、PSYNC、RDB 生成）
- [ ] 决策：做 vs 保持当前 bounded window + 文档化

---

### P1.3 Replication Correctness Documentation

**目标：**

- 收敛 AGENTS.md 中的 replication 内容
- 收敛 `docs/failures/`
- 收敛 correctness envelope 为独立文档

**跟踪：**

- [x] 提取 AGENTS.md 中所有 replication 内容至 `docs/replication/`
  - `architecture.md` — PSYNC, backlog, FULLRESYNC, offset, RDB snapshot, shutdown lifecycle
  - `correctness.md` — correctness envelope, deterministic replay, canonicalization, command gap list
  - `failure-modes.md` — TOCTOU, offset ordering, write-deadline, backlog exhaustion, offset drift, duplicate window
  - `verification.md` — three-tier soak, regression, degradation invariants, nightly pipeline
  - `historical-fixes.md` — commit history: 24e19c2, 6299525, 8b05096, c2dd4c7, df46325
- [x] AGENTS.md 从 452 行精简至 308 行，保留 cross-reference
- [x] 交叉验证与代码行为一致
- [x] 补充已知数据丢失命令的规范（BLPOP 等在 `correctness.md` 中文档化，多数已在 2026-06-10 实现）
- [ ] Review signoff

---

### P1.4 Sentinel Regression Coverage

**目标：**

- 补齐剩余边界条件
- 减少 taxonomy 中的 known-flaky

**跟踪：**

- [ ] 审计现有 sentinel 测试覆盖
- [ ] 补齐关键路径：sentinel 起停、master 失联判定、replica 选举、quorum 边界
- [ ] 纳入 nightly（可选，与 failover oscillation 联动）

---

## 功能缺口（v0.3+，待定）

- **Cluster 模块**
  - [ ] CLUSTER MEET 真实 TCP 握手（当前为本地创建）
  - [ ] Auto-failover（需要选举协议，复杂度高）
- **Sentinel 模块**
  - [ ] Sentinel 自身集群互联（gossip 多 sentinel 通信，已有基础框架）

## 复制功能缺口（已知数据丢失）

以下命令已通过 `isWriteCommand` → `handler.go:528-531` 传播到复制流，但在 `executeReplicatedCommand`（`internal/replication/psync.go:158`）中没有对应 case，被静默跳过。

| 命令 | 优先级 | 说明 |
|------|--------|------|
| BLPOP, BRPOP, BLMOVE, BRPOPLPUSH | ✅ | 2026-06-10 已修复 |
| HSETNX | ✅ | 2026-06-10 已修复 |
| MSETNX | ✅ | 已有实现（与 MSET 共用 case） |
| XACK, XCLAIM, XGROUP\* | ✅ | 2026-06-10 已修复 |
| PFADD, PFMERGE | ✅ | 2026-06-10 已修复 |
| SETBIT, BITOP, BITFIELD | P2 | 位图操作，store 层未实现 |
| COPY | P2 | 键复制，store 层未实现 |
| ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE | P2 | *STORE 聚合，参数解析复杂 |
| ZRANGESTORE, GEOSEARCHSTORE | P2 | 范围 *STORE，参数解析复杂 |
| JSON.*, TS.* | P3 | 模块命令，store 层未实现 |

## 近期已完成的工作

| 工作 | 说明 | 日期 |
|------|------|------|
| Split-brain convergence + hardening | `split_brain_harden_test.go`, `split_brain_convergence_test.go` | May 2026 |
| Failover oscillation regression test | `failover_oscillation_test.go` | May 2026 |
| SPOP canonicalization | `handler.go:3391-3431` — SPOP → SREM 确定性传播 | May 2026 |
| XADD `*` 修复 | `handler.go:5912` — 自动ID 替换 | May 2026 |
| EXPIRE/PEXPIRE 修复 | `handler.go:527` → PEXPIREAT 绝对时间戳 | May 2026 |
| TOCTOU fix (FULLRESYNC lost-write window) | snapshotOffset 在 db.View() 前捕获 | May 2026 |
| TOCTOU fix (CONTINUE gap-fill) | AddSlave 后补发丢失命令 | May 2026 |
| writeMu 序列化 | slave write I/O 与 Close() 死锁修复 | May 2026 |
| 三阶 Soak 语义 | short strict / long non-strict / duplicate-window regression | May 2026 |
| Duplicate-window 回归测试 | `regressions/duplicate_window_regression_test.go` | May 2026 |
| Short strict soak | `TestSoakReplicationShortStrict` (10m, strict equality) | May 2026 |
| FULLRESYNC 语义文档 | no-lost-writes + bounded duplicate window | May 2026 |
| P0 收口 | Longitudinal observation & stability verification | Jun 2026 |
| P1.1 Failover Oscillation #2 | selectNewMaster 存活探活 + failover cooldown + Scenario C/D | Jun 2026 |
| HSETNX replication fix | `psync.go` — added missing HSETNX case | Jun 2026 |
| BLPOP/BRPOP/BLMOVE/BRPOPLPUSH replication fix | `psync.go` — non-blocking replica equivalents | Jun 2026 |
| CanFailover/RecordFailover unit tests | `master_test.go` — 3 new test functions | Jun 2026 |
| selectNewMaster TCP liveness test fix | `failover_test.go` — real listener in unit test | Jun 2026 |
| XACK/XCLAIM/XGROUP replication fix | `psync.go` — Stream 消费者组 replica 实现 | Jun 2026 |
| PFADD/PFMERGE replication fix | `psync.go` — HyperLogLog replica 实现 | Jun 2026 |
| P1.3 Replication Correctness Documentation | AGENTS.md (452→308行) → `docs/replication/` 五份独立文档 | Jun 2026 |

## 已知架构边界（不会做，需文档化）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造，牵动写路径/Badger MVCC/backlog/PSYNC 语义。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏、bounded duplicate window |

## 已决策：不做

| 项 | 原因 |
|----|------|
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL / FUNCTION / MIGRATE | P2，按需补充 |
| Redis-cli 8.x 格式差异 | 24 个 FAIL，均为输出格式差异，非 bug |

---

## 附录 A：Nightly Soak 运行参考

**位置：** `ssh -i ~/.ssh/google_compute_engine elex-gm0135@10.1.2.16` → `/usr/home/elex/boltdb/`
**调度：** crontab 每早 2:00
**命令：**

```bash
# 拉回最新报告
scp -i ~/.ssh/google_compute_engine -r \
  elex-gm0135@10.1.2.16:/tmp/bolt-nightly/ /tmp/soak-results/

# SSH 查看 evolution
ssh -i ~/.ssh/google_compute_engine elex-gm0135@10.1.2.16 \
  "ls -t /tmp/bolt-nightly/20*/report/replication-evolution.md 2>/dev/null | head -1 | xargs cat"
```

## 附录 B：Monitoring Infrastructure (Completed / Frozen)

以下组件作为 P0 观测系统的一部分已完成建设，全部进入维护模式：

| 组件 | 状态 | 说明 |
|------|------|------|
| Health decomposition | COMPLETE | S/R/C 三维分解，11 因子评分 |
| Temporal analysis | COMPLETE | 窗口 slope → trajectory 分类 |
| Basin analysis | COMPLETE | phase space → stable/stressed/degraded |
| Evolution gate | COMPLETE | cross-run trend + drift + anomaly |
| Anomaly engine | COMPLETE | regime shift / escalation 检测 |
| Nightly visualization | COMPLETE | JSONL → trajectory 5-panel chart |
| Evolution report | COMPLETE | cross-run drift + basin + oscillation |
| Nightly summary | COMPLETE | markdown + JSON 双格式 |

> No further development planned. Component freeze effective 2026-06-09.

## 归档索引

已完成的计划文档在 `docs/plans/archive/`：

| 文件 | 说明 |
|------|------|
| `2026-05-15.md` | P0/P1 工程护城河（fuzz/chaos/compat/benchmark） |
| `2026-05-17.md` | 系统正式化（invariants.md、soak test、state fuzz） |
| `2026-03-10-test-coverage-improvement.md` | 测试覆盖率提升计划 |
| `test-audit-report.md` | 测试体系审计 |
| `test-effectiveness-audit.md` | 测试有效性审计（Phase 1） |
