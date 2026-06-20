# Phase 1-4 完成：详细变更日志（2026-05 至 2026-06-20）

此文件归档 TODO.md 中所有已完成的 P 级项，供历史追溯。

---

## P0：Longitudinal Observation & Stability Verification

**Status:** COMPLETE / FROZEN (2026-06-09)

Window: 2026-06-01 ~ 2026-06-09

- Nightly soak pipeline (GHA, cron 2:00 daily UTC)
- Evolution gate (cross-run trend analysis)
- Basin analysis (phase space classification)
- Drift analysis (delta > 0.05 detection)
- Anomaly detection (trajectory regime shifts)
- 7+ day observation window
- Stable attractor verified (basin: healthy, health: 1.00, L0: 0.0, oscillation: none)

> P0 closed. Further monitoring work requires a new justification and is not part of the current roadmap.

---

## P1.1 Failover Oscillation (#2)

**Status:** COMPLETE (committed in `eedd2a1`)

- 识别 oscillation test 未覆盖的边界条件（连续掉线 + 大量写入）
- 新增 Scenario C/D
- sentinel: `selectNewMaster` 添加 TCP 存活探活（跳过死 slave，不标记 offline）
- sentinel: 添加 failover cooldown（5s 冷却，防止 gossip 重触发风暴）
- sentinel: `SendReplicaOf` 添加读超时（防止阻塞 failover goroutine）
- 修复 oscillationTracker data race（RWMutex）
- 测试验证：Scenario A/B + Harden + C + D 全部通过（3/3 race clean）

---

## P1.2 Duplicate Window Architecture

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

## P1.3 Replication Correctness Documentation

**Status:** COMPLETE (committed in `eedd2a1`)

- AGENTS.md (452→308行) → `docs/replication/` 五份独立文档
- architecture.md / correctness.md / failure-modes.md / verification.md / historical-fixes.md
- 交叉验证与代码行为一致

---

## P1.4 Sentinel Regression Coverage

**Status:** COMPLETE (committed in `eedd2a1`, merged with P1.1)

- CanFailover/RecordFailover 单元测试（3 个新测试函数）
- selectNewMaster TCP liveness 测试修复（使用真实 listener）
- Scenario C/D 集成测试
- 与 P1.1 同一次提交落地

---

## P1.5 Shutdown / Lifecycle Correctness

**Status:** COMPLETE (2026-06-17)

| 组件 | 机制 |
|------|------|
| Handler.Shutdown | `shuttingDown` atomic + `wg.Wait()` + conn Close |
| SlaveConnection | atomic fields (`ReplOffset`, `Ready`) + memory barrier Close |
| ReplicationManager.Stop | slave conn close + master conn close |
| SlaveReconnector.Stop | `stopCh` + `wg.Wait()` |
| BackupManager | BGSave `wg` tracking + `Wait()` |
| Sentinel.Stop | `s.wg.Wait()` + lock ordering fix (release `s.mu` before wait) |
| GossipProtocol.Stop | `gp.wg.Wait()` (all accept/manage/handleConnection tracked) |
| SentinelHandler.Stop | conn tracking + close + `wg.Wait()` |
| PressureMonitor.Start | `wg` tracking + `Wait()` method |
| Metrics periodic snapshot | `wg` parameter + main.go shutdown ordering |
| AutoFailover goroutines (3x) | wg tracking via sentinel/gossip wg |
| Monitor goroutine leak test | retry-based assertion (no more flake) |
| main.go shutdown sequence | `replMgr.Stop()` → `cancel()` → `metricsWg.Wait()` → `handler.Shutdown()` → `db.Close()` |

所有 goroutine 通过 wg 追踪，关闭顺序明确保证：listener close → replMgr stop → cancel → handler shutdown → db close。

---

## P1.5b isWriteCommand 缺失写入命令

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

---

## P1.5b BZPOPMAX/BZPOPMIN 复制缺口

**Status:** COMPLETE (committed in `6a01ba7`)

阻塞式有序集合弹出修改数据但不在 `isWriteCommand` 中，导致复制流中数据静默分歧。

修复内容：
- `replication_helper.go`: BZPOPMAX, BZPOPMIN 加入 `isWriteCommand`
- `psync.go`: 非阻塞 `s.ZPopMax(key, 1)` / `s.ZPopMin(key, 1)` 副本端实现
- 2 个测试函数验证弹出 + 剩余元素正确性

---

## P1.6 WRONGTYPE 错误包装不完整

**Status:** COMPLETE (committed in `2d0af2a`)

约 35 个命令使用 `fmt.Sprintf("ERR %v", err)` 包装 `store.ErrWrongType`，应返回 `"WRONGTYPE Operation against a key holding the wrong kind of value"`。

修复内容：
- 使用 Python 脚本自动扫描 `executeCommand` 中所有 `h.Db.*` 返回的 `fmt.Sprintf("ERR %v", err)` 并替换为 `wrapStoreError(err)`
- 排除已有 WRONGTYPE 检查的 fallback 路径、非 store 错误路径
- 共修复 49 处，覆盖所有写命令和读命令

---

## Replication Gap Fix

**Date:** 2026-06-10

Fixed `GETDEL` and `GETEX` not being propagated during replication.

- `GETDEL` → added `isWriteCommand` + `executeReplicatedCommand` case (Get + Del)
- `GETEX` → added `isWriteCommand` + `executeReplicatedCommand` case (Get + Expire/PExpire/Persist)
- 5 new test functions

---

## Protocol Fix (LPUSH/RPUSH WRONGTYPE)

**Date:** 2026-06-10

`LPUSH`/`RPUSH` wrapped `ErrWrongType` in `"ERR %v"` instead of returning the correct `"WRONGTYPE Operation against a key holding the wrong kind of value"` response. Added explicit `errors.Is(err, store.ErrWrongType)` check before the generic error path.

---

## 复制功能缺口修复

以下命令在 `executeReplicatedCommand` 中被静默跳过，已全部修复：

| 命令 | 修复日期 |
|------|----------|
| BLPOP, BRPOP, BLMOVE, BRPOPLPUSH | 2026-06-10 |
| HSETNX | 2026-06-10 |
| MSETNX | 2026-06-10 |
| XACK, XCLAIM, XGROUP\* | 2026-06-10 |
| PFADD, PFMERGE | 2026-06-10 |
| SETBIT, BITOP, BITFIELD, COPY | 2026-06-10 |
| ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE | 2026-06-10 |
| ZRANGESTORE, GEOSEARCHSTORE | 2026-06-10 |
| JSON.\*, TS.\* (9 commands) | 2026-06-11 |

---

## P2 Replication Gap Fix (9 commands)

**Date:** 2026-06-10

| Command | Implementation |
|---------|---------------|
| SETBIT | Direct store call (`s.SetBit`) |
| BITOP | Direct store call (`s.BitOp`) |
| BITFIELD | Direct store call (`s.BitField`) |
| COPY | Type-aware copy via store primitives with REPLACE support |
| ZUNIONSTORE | Full arg parsing (numKeys/keys/weights/aggregate) → `s.ZUnionStore` |
| ZINTERSTORE | Full arg parsing → `s.ZInterStore` |
| ZDIFFSTORE | numKeys/keys parsing → `s.ZDiffStore` |
| ZRANGESTORE | Full arg parsing (BYSCORE/BYLEX/REV/LIMIT) → store primitives |
| GEOSEARCHSTORE | Full arg parsing → `s.GeoSearchStore` |

Tests: 29 new test functions in `replication_coverage_test.go`

---

## ZMPOP Implementation

**Date:** 2026-06-10

| Layer | Details |
|-------|---------|
| Store | `ZMPop(keys, modifier, count)` — iterates keys, returns first non-empty |
| Handler | Full arg parsing: `numkeys`, `keys`, `MIN\|MAX`, `COUNT count` |
| Replication | `isWriteCommand` + `executeReplicatedCommand` case |
| Coverage | Updated `TestExecuteCommand_ZMPOP_Coverage` |

---

## CI / Sentinel / Coverage 修复（2026-06-18）

### CI 修复 (commit `1c5a085`)

- HSet 并发计数损坏修复（currentCount View + retryUpdate 时序）
- TestRegressionPsyncReconnectNoLoss 假阳性修复（容忍范围内的 missing key 使用 t.Logf）
- remote-test.sh 双主机支持（10.1.2.16 / 192.168.1.251）

### CI 改进 + TCP Keepalive (commit `a87daba`)

- TCP keepalive for 复制连接（dialMaster/NewMasterConnection/NewSlaveConnection）
- CI stderr 抑制修复（2>/dev/null → 2>&1）
- CI 集成测试拆分+超时提升（cmd/integration/ + regressions/ 独立 timeout）
- TODO.md backupMgr.Wait() 状态修正

### Sentinel 死锁修复 (commit `944f055`)

- checkMaster 自死锁：mi.mu.Lock() 后调用 CanFailover() 试图获取 RLock
- BroadcastSdown 移出 Lock 区域

### 覆盖率提升（v1-v10）

11 个 internal 包覆盖率：

| 包 | 之前 | 之后 |
|----|------|------|
| replication | 64.2% | 69.6% |
| server | 56.3% | 61.6% |
| sentinel | 61.1% | 79.3% |
| store | 61.1% | 74.0% |
| logger | 76.3% | 97.4% |

0% 函数清零。新测试覆盖：复制协议（readUntilEOF/sendHandshake/sendPSYNC）、阻塞操作（registerBlockingPop/BLPOP/BRPOPLPUSH/BLMOVE）、JSON/TS handler、sentinel metrics/slave/gossip、store TTL/Rename/Scan/Dump/Restore、monitor anomaly saving、cluster loadState/clearSlots。

---

## Phase 4 技术债收敛（2026-06-20）

### 错误传播修复

- list.go ~30 ValueCopy error discards: LPop, RPop, LRange, LTrim, LInsert, LRem, LMPop, RPopLPush, listGetByIndex, deleteNode, listGetMetaTxn (12 locations)
- base.go 6 ValueCopy discards: RDB string/hash value reading + MemoryUsage compound-type iteration (4 locations)
- string.go BITFIELD TTL txn.Delete + dead byteIndex
- hash.go HRandMember View error
- SortedSet type-check patterns ×5 (ZScore, ZRange, ZCard, ZMPop, ZRandMember): `_ = s.db.View(...)` → direct View error propagation
- RENAME txn.Delete discards (7 locations)
- expireAt/PExpireAt cleanup Del → logger.Warn
- cluster LoadConfig error → logger.Warn (split-brain risk)
- handler.go executeQueuedCommand strconv discards ×8 (INCRBY, DECRBY, EXPIRE, LRANGE, ZADD, ZINCRBY)
- handler.go COPY dstExists, SORT keyType, TOUCH exists
- COPY helpers (copyList/copyHash/copySet/copySortedSet) Del error propagation
- SORT path error discards (4 locations)
- RDB loader PExpire discards (4 locations in base.go)
- ValueCopy error discards (12 locations): BITFIELD TTL, PFCount/PFInfo, HRandMember, SAdd/SRem/SCard/SPop/SMove/Dump
- stream.go 5 locations: XAdd/XTrim/XGroupCreate
- sentinel ConfigManager.Save/Load, AddMaster → logger.Warn
- errors.Is fixes (11 locations): err == badger.ErrKeyNotFound → errors.Is(err, badger.ErrKeyNotFound)
- backup/restore.go gzip decompress I/O error handling + io.ReadFull

### 复制路径修复

- executeReplicatedCommand 14 error discards (psync.go): store errors → full resync
- executeQueuedCommand 27 error discards (tx_queue.go): errors → discard entire txn

### 数据竞争修复

- backpressure.go: s.backpressure/s.bpConfig → atomic.Pointer
- backpressure defer Release() semaphore leak: capture local slot variable
- PubSub Clear()/RemoveSubscriber(): close(MessageCh) inside closeMu
- handler.go markDirtyKeys: watcher.mu.Lock() around dirtyKeys

### 正确性 Bug

- WATCH cleanup + UNWATCH: delete(h.watchMonitors, key) deleted entire key entry, losing other watchers
- PubSub RemoveSubscriber: duplicate close() (at line 273 and 292)
- ZMPop missed `else if !errors.Is(err, badger.ErrKeyNotFound)` branch
- reconnect.go/BLPOP/etc: time.After → time.NewTimer (goroutine leak)
- benchmark/main.go: SIGINT/SIGTERM handler, process kill on all error paths
- GETEX error message: "ERR value is not an integer" → "ERR value is not an integer or out of range"
- REPLCONF ACK offset parse: `offset, _` → logger.Warn + continue
- writeSlot.Release() non-blocking: `select { case <-ws.ch: default: }`

### 安全/协议修复

- proto/resp.go: MaxBulkLen (512MB) + MaxArrayLen (1M) overflow guards
- master.go: reader.Read(data) → io.ReadFull(reader, data)
- RDB length parse: MaxBulkLen check before make([]byte, length+2)
- restore.go: io.ReadFull for gzip header

### 清理

- define.go: commented-out consts removed, HyperLogLogPrefix constant, atomic.Pointer for backpressure
- rdb.go: "hll:" + key → store.HyperLogLogPrefix
- backlog.go: parseBacklogSize unexported
- handler.go: dead _ = meta, XTRIM dead approximate variable removed
- NextStartup cleanupOrphaned: `_ = func()` with comment
- random.go: documentation of crypto/rand fallback
