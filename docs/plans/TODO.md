# BoltDB 待办列表

## 项目阶段

```text
Phase 1: 功能实现（Redis 兼容、复制、Sentinel、PubSub、RDB...）      ✅
Phase 2: 正确性危机（L0、PSYNC、FULLRESYNC、goroutine、shutdown...） ✅
Phase 3: 观测与收敛（health、basin、evolution、nightly、docs...）    ✅ closed at eedd2a1
Phase 4: 技术债收敛（当前阶段）
```

**当前重点：**
- 观察 nightly 数据，维持 correctness envelope
- 继续隔离/缩减 known flaky
- 避免把已经收敛的系统重新搞复杂

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

**Status:** Active — Phase 3 (Observation & Convergence) closed.

### P1.1 Failover Oscillation (#2)

**Status:** COMPLETE (committed in `eedd2a1`)

- [x] 识别 oscillation test 未覆盖的边界条件（连续掉线 + 大量写入）
- [x] 新增 Scenario C/D
- [x] sentinel: `selectNewMaster` 添加 TCP 存活探活（跳过死 slave，不标记 offline）
- [x] sentinel: 添加 failover cooldown（5s 冷却，防止 gossip 重触发风暴）
- [x] sentinel: `SendReplicaOf` 添加读超时（防止阻塞 failover goroutine）
- [x] 修复 oscillationTracker data race（RWMutex）
- [x] 测试验证：Scenario A/B + Harden + C + D 全部通过（3/3 race clean）

---

### P1.2 Duplicate Window Architecture

**Status:** DEFERRED — not a bug, an architectural tradeoff.

| 条件 | 状态 |
|------|------|
| 已发现 | ✅ |
| 已测量 | ✅ INCR ≤ 2, LPUSH ≤ 70% |
| 已回归 | ✅ TestRegressionDuplicateWindowMeasurement |
| 已文档化 | ✅ failure-modes.md, verification.md |
| 已隔离 | ✅ 三阶 soak 将其与稳定性分析分离 |
| 已影响生产 | ❌ |

当前 bounded duplicate window (µs) 是可接受的设计选择。除非 strict equality
需求升级或 divergence 持续扩大，否则不需要 commit-seq ↔ repl-offset 映射。

---

### P1.3 Replication Correctness Documentation

**Status:** COMPLETE (committed in `eedd2a1`)

- [x] AGENTS.md (452→308行) → `docs/replication/` 五份独立文档
- [x] architecture.md / correctness.md / failure-modes.md / verification.md / historical-fixes.md
- [x] 交叉验证与代码行为一致

---

### P1.4 Sentinel Regression Coverage

**Status:** COMPLETE (committed in `eedd2a1`, merged with P1.1)

- [x] CanFailover/RecordFailover 单元测试（3 个新测试函数）
- [x] selectNewMaster TCP liveness 测试修复（使用真实 listener）
- [x] Scenario C/D 集成测试
- [x] 与 P1.1 同一次提交落地

---

## 功能缺口（v0.3+，待定）

- **Cluster 模块**
  - [ ] CLUSTER MEET 真实 TCP 握手（当前为本地创建）
  - [ ] Auto-failover（需要选举协议，复杂度高）
- **Sentinel 模块**
  - [ ] Sentinel 自身集群互联（gossip 多 sentinel 通信，已有基础框架）

## Replication Gap Fix (GETDEL/GETEX)

**Date:** 2026-06-10

Fixed `GETDEL` and `GETEX` not being propagated during replication — both are mutating commands (GETDEL deletes, GETEX modifies TTL) but were absent from `isWriteCommand`, causing silent data divergence.

- `GETDEL` → added `isWriteCommand` + `executeReplicatedCommand` case (Get + Del)
- `GETEX` → added `isWriteCommand` + `executeReplicatedCommand` case (Get + Expire/PExpire/Persist)
- 5 new test functions

## Protocol Fix (LPUSH/RPUSH WRONGTYPE)

**Date:** 2026-06-10

`LPUSH`/`RPUSH` wrapped `ErrWrongType` in `"ERR %v"` instead of returning the correct `"WRONGTYPE Operation against a key holding the wrong kind of value"` response, breaking Redis protocol compatibility. Added explicit `errors.Is(err, store.ErrWrongType)` check before the generic error path.

## 复制功能缺口（已知数据丢失）

以下命令已通过 `isWriteCommand` → `handler.go:528-531` 传播到复制流，但在 `executeReplicatedCommand`（`internal/replication/psync.go:158`）中没有对应 case，被静默跳过。

| 命令 | 优先级 | 说明 |
|------|--------|------|
| BLPOP, BRPOP, BLMOVE, BRPOPLPUSH | ✅ | 2026-06-10 已修复 |
| HSETNX | ✅ | 2026-06-10 已修复 |
| MSETNX | ✅ | 已有实现（与 MSET 共用 case） |
| XACK, XCLAIM, XGROUP\* | ✅ | 2026-06-10 已修复 |
| PFADD, PFMERGE | ✅ | 2026-06-10 已修复 |
| SETBIT, BITOP, BITFIELD | ✅ | 2026-06-10 已修复：executeReplicatedCommand 添加 case |
| COPY | ✅ | 2026-06-10 已修复：executeReplicatedCommand 添加 case（含所有类型）|
| ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE | ✅ | 2026-06-10 已修复：executeReplicatedCommand 添加 case（含 WEIGHTS/AGGREGATE）|
| ZRANGESTORE, GEOSEARCHSTORE | ✅ | 2026-06-10 已修复：executeReplicatedCommand 添加 case |
| JSON.*, TS.* | ✅ | 2026-06-11 已修复：executeReplicatedCommand 添加 9 个 case（JSON: SET/DEL/ARRAPPEND/NUMINCRBY/NUMMULTBY/CLEAR, TS: CREATE/ADD/DEL） + 13 个测试 |

## ZMPOP Implementation

**Date:** 2026-06-10

Implemented `ZMPOP` — the only remaining production "not implemented" stub. Redis 7.0+ multi-key sorted set pop.

| Layer | Details |
|-------|---------|
| Store | `ZMPop(keys, modifier, count)` — iterates keys, returns first non-empty using ZPopMax/ZPopMin |
| Handler | Full arg parsing: `numkeys`, `keys`, `MIN\|MAX`, `COUNT count` — returns array with key + members+scores |
| Replication | `isWriteCommand` + `executeReplicatedCommand` case with full arg parsing |
| Coverage | Updated `TestExecuteCommand_ZMPOP_Coverage` — removed "not implemented" fallback, tests MIN/MAX/COUNT/nil |

## P2 Replication Gap Fix

**Date:** 2026-06-10

Fixed 9 P2 commands silently skipped by `executeReplicatedCommand`, causing data loss during replication:

| Command | Implementation |
|---------|---------------|
| SETBIT | Direct store call (`s.SetBit`) |
| BITOP | Direct store call (`s.BitOp`) |
| BITFIELD | Direct store call (`s.BitField`) |
| COPY | Type-aware copy via store primitives (string/list/hash/set/zset) with REPLACE support |
| ZUNIONSTORE | Full arg parsing (numKeys/keys/weights/aggregate) → `s.ZUnionStore` |
| ZINTERSTORE | Full arg parsing (numKeys/keys/weights/aggregate) → `s.ZInterStore` |
| ZDIFFSTORE | numKeys/keys parsing → `s.ZDiffStore` |
| ZRANGESTORE | Full arg parsing (BYSCORE/BYLEX/REV/LIMIT) → store primitives |
| GEOSEARCHSTORE | Full arg parsing (FROMMEMBER/FROMLONLAT/BYRADIUS/COUNT/STOREDIST) → `s.GeoSearchStore` |

Tests: 29 new test functions in `replication_coverage_test.go` covering normal ops, edge cases, invalid args.

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
| P2 Replication gap fix (9 commands) | SETBIT/BITOP/BITFIELD/COPY/ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE/ZRANGESTORE/GEOSEARCHSTORE + 29 tests | Jun 2026 |
| GETDEL/GETEX replication fix | `isWriteCommand` + `executeReplicatedCommand` + 5 tests | Jun 2026 |
| LPUSH/RPUSH WRONGTYPE fix | `handler.go` — add ErrWrongType check before ERR wrapper | Jun 2026 |
| HRANDMEMBER implementation | store + handler + coverage test — random hash field selection | Jun 2026 |
| ZMPOP implementation | store (`ZMPop`) + handler + replication + coverage test — multi-key sorted set pop | Jun 2026 |
| BZPOPMAX/BZPOPMIN blocking fix | replaced non-blocking stubs with real `BZPopMaxBlocking`/`BZPopMinBlocking` using channel-based wait + `registerAndRecheckZ` + `ZAdd` notification | Jun 2026 |
| COMMAND implementation | `internal/server/command_info.go` — 223 commands with metadata, subcommands: `COMMAND`, `COMMAND COUNT`, `COMMAND INFO <cmd...>` | Jun 2026 |
| WRONGTYPE error handling fix | added `wrapStoreError` helper + fixed ~40 locations where ErrWrongType was wrapped as `"ERR %v"` instead of proper `"WRONGTYPE ..."` | Jun 2026 |
| JSON/TS replication fix (9 commands) | `executeReplicatedCommand` — JSON.SET/DEL/ARRAPPEND/NUMINCRBY/NUMMULTBY/CLEAR + TS.CREATE/ADD/DEL + 13 tests | Jun 2026 |

## 待办缺口（已知未完成）

### P1.5 isWriteCommand 缺失写入命令

**Status:** COMPLETE (committed in `de7f628`)

| 命令 | 行号 (handler.go) | 影响 | 优先级 |
|------|-------------------|------|--------|
| RESTORE | 2294 | 创建/覆盖键 | 高 |
| FLUSHDB | 4967 | 清除所有键 | 高 |
| FLUSHALL | 4974 | 清除所有键 | 高 |
| XAUTOCLAIM | 6578 | 声明流消息（变更状态） | 高 |
| SORT … STORE | 6926/7057 | SORT 使用 STORE 选项时写入 | 高 |

修复内容：
- `replication_helper.go`: 5 个命令添加到 `isWriteCommand` 映射
- `psync.go`: 5 个命令在 `executeReplicatedCommand` 中添加 case
  - RESTORE: 解析 key/ttl/serializedData/REPLACE → `s.Restore()`
  - FLUSHDB/FLUSHALL: `s.FlushDB()` + `s.ClearCaches()`
  - XAUTOCLAIM: 解析 key/group/consumer/minIdleTime/start/COUNT/JUSTID → `s.XAutoClaim()`
  - SORT…STORE: 完整排序逻辑（list/set/string/zset 源类型），支持 BY/ASC/DESC/ALPHA/LIMIT
- 14 个新测试函数，涵盖正常路径、选项参数、非法参数、只读 SORT 无操作

### P1.6 WRONGTYPE 错误包装不完整

**Status:** COMPLETE (committed in `2d0af2a`)

约 35 个命令使用 `fmt.Sprintf("ERR %v", err)` 包装 `store.ErrWrongType`，应返回 `"WRONGTYPE Operation against a key holding the wrong kind of value"`。

修复内容：
- 使用 Python 脚本自动扫描 `executeCommand` 中所有 `h.Db.*` 返回的 `fmt.Sprintf("ERR %v", err)` 并替换为 `wrapStoreError(err)`
- 排除已有 WRONGTYPE 检查的 fallback 路径、非 store 错误路径
- 共修复 49 处，覆盖所有写命令和读命令

**已修复的命令（写命令，高优先级）：**
`MSET`, `MSETNX`, `SETBIT`, `BITOP`, `BITFIELD`, `SETRANGE`, `PFADD`, `PFMERGE`, `RESTORE`, `EXPIRE`, `EXPIREAT`, `PEXPIRE`, `PEXPIREAT`, `PERSIST`, `RENAME`, `RENAMENX`, `SMOVE`, `ZINCRBY`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `FLUSHDB`, `FLUSHALL`

**已修复的命令（读命令，中优先级）：**
`GETBIT`, `BITCOUNT`, `BITPOS`, `BITLEN`, `PFCOUNT`, `PFINFO`, `OBJECT REFCOUNT`, `OBJECT ENCODING`, `OBJECT IDLETIME`, `KEYS`, `SMISMEMBER`, `SINTERCARD`, `ZRANK`, `ZREVRANK`, `ZCOUNT`, `MEMORY USAGE`, `SCAN`, `SSCAN`, `ZSCAN`, `DBSIZE`, `TIME`

注意：对于已有完整错误路径（WRONGTYPE → ErrKeyNotFound → ERR fallback）的命令保持原样，只替换了未做区分直接返回 `"ERR %v"` 的错误路径。

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
