# BoltDB 待办列表

## 项目阶段

```text
Phase 1: Redis 兼容、复制、Sentinel、PubSub、RDB                      ✅
Phase 2: L0、PSYNC、FULLRESYNC、goroutine、shutdown                     ✅
Phase 3: health、basin、evolution、nightly、docs                         ✅
Phase 4: 技术债收敛                                                      ✅
P0–P2 新功能：RESP3 / CLUSTER MEET / Sentinel gossip                    ✅
2026-06-21 代码审计 + 修复                                               ✅
```

---

## 当前状态

| 指标 | 值 |
|------|----|
| RESP3 Null 覆盖 | 34/34 命令（100%） |
| redis-py compat | 153/153 (100%) |
| node-redis compat | 110/110 (100%) |
| redis-cli compat | 77/77 (100%) |
| timer 泄漏 | 8/8 已修复 |
| isWriteCommand | 91/91 完整 |
| goroutine leak test | 通过 |

---

## P1：剩余 RESP3 空值（2个 → 已全部修复 ✅）

| 命令 | 行号 | 原因 | 修复难度 | 状态 |
|------|------|------|---------|------|
| CLIENT GETNAME | handler.go:1504 | 客户端名未设置时返回 nil；edge case，无数据语义 | 易（加 `respVersion==3` guard） | ✅ 已修复 |
| GEOPOS per-element | handler.go:5954 | 数组内 nil 元素；需改 RESP3 Array 中嵌入 Null | 中（需改数组元素类型） | ✅ 已修复 |

**RESP3 Null 覆盖：34/34 命令（100%）**

---

## P2：代码质量（6项 → 已全部修复 ✅）

| 项 | 位置 | 问题 | 修复 | 状态 |
|----|------|------|-----|------|
| MasterConnection.Reader 未锁定使用 | `replication/master.go:78-80` | 先 RLock 取 reader 指针，解锁后用 | 全程持 RLock；Close() 先关连接再锁做 barrier | ✅ |
| SlaveConnection.Reader 同模式 | `replication/slave.go:167-170` | 同上 | 全程持 RLock（slave Close 已正确先关连接） | ✅ |
| gossip test 不尊重 `-short` | `cluster/gossip_test.go:140` | `TestGossip_CheckFailures_MarksPFAIL` 硬编码 120s 超时 | 加 `t.Skip` when `testing.Short()` | ✅ |
| gossip.go `context.Background()` | `cluster/gossip.go:31` | 应继承应用生命周期 | 从 server root context 派生 | ✅ |
| `readUntilEOF` 无大小上限 | `replication/master.go:246-248` | 仅 warn，不限 buffer | 加硬上限（256MB）后报错断开 | ✅ |
| BGSAVE 无取消机制 | `backup/backup.go:49-59` | `db.View()` 可能被 shutdown 阻塞 | BGSave 接收 ctx，shutdown 时监控 goroutine 退出让 Wait() 返回 | ✅ |

---

## P3：架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL / FUNCTION / MIGRATE | P2，按需补充 |

---

## P4：CI / 发布

| 项 | 状态 |
|----|------|
| v8.26.0 发布 | 🔴 未推（待 commit） |
| v8.27.0 发布 | 🔴 未推（待 commit） |
| v8.28.0 发布 | 🔴 未推（待 commit） |
| 工作树有 22 文件未提交改动 | 🔴 需 commit |
| github actions CI 通过 | 🟡 待验证 |

---

## 审计结果（仅存档，不需修复）

| 审计 | 结果 |
|------|------|
| isWriteCommand | 91 条完整，0 缺失 |
| timer 模式 | 8 处 production `time.After()` 全部修复 |
| TODO/FIXME/BUG 注释 | 代码库中 0 处 |
| SlaveInstance data race | 已修复（sync.RWMutex + getter） |
| 3× double close(stopCh) | 已修复（sync.Once） |
| CLUSTER ADDSLOTS/DELSLOTS | 已修复 |
| RDB write error 吞没 | 已修复（9 处加日志） |
| ReplAckOffset data race | 已修复（atomic.Int64） |
| SaveConfig RLock deadlock | 已修复（分拆 saveConfigLocked） |

---

## 附录：Verification Commands

```bash
# All unit tests
bash scripts/remote-test.sh -race -short ./internal/...

# RESP shape suite
bash scripts/remote-test.sh -race -timeout 30s -run TestRESPShape ./internal/server/...

# Goroutine leak
bash scripts/remote-test.sh -race -timeout 120s -run TestGoroutineLeak ./cmd/integration/...

# Compatibility
python3 scripts/redis_py_compat.py
node scripts/redis_node_compat.mjs
bash scripts/redis_cli_compat.sh

# Linter
golangci-lint run --timeout 5m
```
