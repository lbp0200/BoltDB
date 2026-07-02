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
| Redis 8 命令补齐 | 5/5 批次完成（UNLINK/ZINTERCARD/BITFIELD_RO/BZMPOP + Stream + TimeSeries + Admin + DEBUG） |
| 全命令准确性测试 | 239/239 命令已覆盖（`TestCommandCompleteness`，16 个数据类型组） |
| Mutation Testing | 5,201 变异体，100% efficacy，82.3% mcover → Store 90.17%（target 90% ✅） |
| 新增 mutation kill 测试 | ~390 个（2026-07-02），远程全部通过 |

---

## 已完成的里程碑

| Phase | 内容 | 完成日期 |
|-------|------|---------|
| 1 | 加固 Coverage 测试（317 个测试审查 + 弱断言替换） | — |
| 2 | Soak 数据一致性（SET→GET/INCR 写后读验证） | — |
| 3 | 并发数据完整性（INCR 精确计数 + SET 最终一致性） | — |
| 4 | 边界和异常（RDB roundtrip/大载荷/连接中断/参数边界） | — |
| 5 | 测试质量度量（mutation testing 引入） | — |
| 6 | 消灭伪覆盖（fire-and-forget/零断言/弱断言/表驱动负面用例） | — |
| 8 | Mutation Testing 重跑（5,201 变异体，100% efficacy） | 2026-06-29 |
| 9 | Mutation Test NOT COVERED 修复（GEO/RESTORE/SORT/Admin） | — |
| 10 | Store 层核心算法优化（List 双向遍历/ZRank 双向扫描/HRandField 蓄水池/APPEND 合并事务） | — |
| 11 | 生产事故回归测试（振荡/写入风暴/脑裂/磁盘压力/BGSAVE 并发/PubSub 扇出） | — |
| 12 | Redis 8 命令补齐（5 批次，239/239 命令） | — |

---

## 下一步（按收益排序）

### 1. 修复 FLUSHDB Stream 内部 key 清理 ✅

**投入**：S（半天）　**收益**：高——生产环境真实 bug

- [x] `Del()` 缺少 `KeyTypeStream`/`KeyTypeHyperLogLog`/`KeyTypeGeo` 分支 → 已补全
- [x] `NextStartup()` 缺少 Stream/HLL/Geo 孤儿清理 → 已补全 3 个 cleanup 函数
- [x] `checkDataExists()` 缺少 Stream/HLL/Geo → 已补全
- [x] 远程验证通过：Stream teardown 0 orphan data keys

### 2. 主从写命令传播测试 ✅

**投入**：M（1 天）　**收益**：高——复制模式下写命令正确性零覆盖

- [x] 新建 `cmd/integration/replication_completeness_test.go`
- [x] 9 个数据类型组：String/Key/List/Hash/Set/SortedSet/Stream/JSON/HLL/Geo
- [x] 覆盖 ~70 个写命令的 master→slave 传播验证
- [x] 发现并修复 XTRIM panic（`stream.go:1018` 空切片越界）
- [x] 远程验证通过：9/9 组 PASS

### 3. mcover 提升至 90% ✅

**投入**：已完成两轮　**收益**：中——防御性提升

**第一轮已完成：**
- [x] 新建 `internal/server/handler_mutation_kill2_test.go`（35 个测试）— handler 层变异体
- [x] 新建 `internal/store/base_mutation_kill_test.go`（15 个测试）— store 层 Del/NextStartup/FlushDB 变异体
- [x] 共 50 个新测试，覆盖 ~120 个 NOT COVERED 变异体
- [x] 远程测试通过：50/50 PASS，无回归
- [x] 新建 `internal/server/info_handler_core_mutation_kill_test.go`（48 个测试）— info.go + handler_core.go 变异体
- [x] 新建 `internal/store/base_mutation_kill3_test.go`（66 个测试）— Del JSON/TimeSeries、Rename 全类型覆盖
- [x] 新建 `internal/server/zset_mutation_kill_test.go`（~80 个测试）— ZSET 全参数验证分支
- [x] 新建 `internal/server/stream_ts_mutation_kill_test.go`（~60 个测试）— Stream + TimeSeries 全参数验证分支

**第二轮已完成（2026-07-02）：**
- [x] 新建 `internal/store/mutation_kill_mcover90_test.go`（~80 个测试）— sorted_set decodeDataValue/memberInLexRange、list.go LPos、define.go extractRawKey、string.go/Expire/PExpire/RenameNX/Scan/ObjectEncoding 等
- [x] 修复 `internal/store/mutation_kill_phase9_test.go` 编译错误（TSExists 不存在 + uint64 类型不匹配）
- [x] 远程测试通过：`-race -short` 全 PASS

**Mutation Testing 最终结果（2026-07-02）：**
- Store 包：Runnable 3,110 / Not Covered 339 / **mcover 90.17%**（目标 90% ✅）
- Efficacy: 100%（所有 runnable 变异体均已杀死）
- 新增 ~80 个测试（1 个新测试文件 + 1 个修复文件）

**当前状态**：mcover 达标 90%。如需继续提升：
- 针对 store 包剩余 339 NOT COVERED（stream.go 93、sorted_set.go 59、define.go 38、base.go 37、string.go 32）补充测试
- 剩余 NOT COVERED 多为 corruption paths、blocking operations、dead code，测试难度较高

### 4. 长时间 Soak 稳定性 ✅

**投入**：S（命令已就绪）　**收益**：中——验证 24h+ 运行稳定性

基础设施已完备，直接在 2.16 后台运行：

```bash
# 单机 Soak（默认 1h，可调）
SOAK_DURATION=24h go test -race -timeout 25h -run TestSoak -count=1 ./cmd/integration/ &

# 主从 Soak
SOAK_REPL_DURATION=24h go test -race -timeout 25h -run TestSoakReplication -count=1 ./cmd/integration/ &

# 或用 nightly 脚本（含 standalone + replication + evolution gate）
SOAK_DURATION=1h SOAK_REPL_DURATION=1h bash scripts/run_nightly_soak.sh
```

- [x] `TestSoak`：单机随机操作 + 并发客户端 + 压力监控
- [x] `TestSoakReplication`：主从复制 + 生命周期断连重连
- [x] `TestClusterSoak`：集群多节点 + 槽位混沌
- [x] 报告输出到 `/tmp/soak-artifacts/`（Markdown + JSON）
- [x] 2.16 上运行超过 24h（PID 2057040，自 2026-06-30 起，CPU 102%，内存稳定）

---

## 已知问题

_暂无已知问题。_

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

## 附录：Verification Commands

```bash
# 全命令准确性测试（单机，~1s）
bash scripts/remote-test.sh -race -timeout 30s -run TestCommandCompleteness ./cmd/integration/...

# 全部单元测试
bash scripts/remote-test.sh -race -short ./internal/...

# RESP shape suite
bash scripts/remote-test.sh -race -timeout 30s -run TestRESPShape ./internal/server/...

# Goroutine leak
bash scripts/remote-test.sh -race -timeout 120s -run TestGoroutineLeak ./cmd/integration/...

# Compatibility
python3 scripts/redis_py_compat.py
node scripts/redis_node_compat.mjs
bash scripts/redis_cli_compat.sh

# 后台全量测试（2.16 服务器）
bash scripts/test-all-commands-remote.sh

# Linter
golangci-lint run --timeout 5m

# Mutation testing
bash scripts/mutation-test.sh --run
```
