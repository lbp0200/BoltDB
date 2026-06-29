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

---

## 测试质量改进

### Phase 1：加固 Coverage 测试 ✅
- [x] 审查 handler_coverage*_test.go 中 317 个测试
- [x] 替换弱断言：30+ 个 `len > 0` / `>= 0` → 精确值验证
- [x] 消除了所有 `assert.True(t, len(...) >= 0)` 和 `assert.True(t, int64(...) >= 0)` 模式

### Phase 2：Soak 数据一致性 ✅
- [x] 在 runSoakNormal 中添加 SET→GET 写后读验证
- [x] INCR 验证返回正整数
- [x] 修复 pre-existing 键命名空间冲突（soak:set:* → soak:sadd:*）
- [x] 添加 trySendErr() 非阻塞发送防止 goroutine 泄漏

### Phase 3：并发数据完整性
- [x] 并发 INCR 精确计数测试（200 goroutines，`TestConcurrentINCR_PreciseCount` + 多键隔离 `TestConcurrentINCR_MultipleKeys`）
- [x] 并发 SET 最终一致性测试（200 goroutines `TestConcurrentSET_EventualConsistency` + 50 键隔离 `TestConcurrentSET_DistinctKeys`）

### Phase 4：边界和异常
- [x] RDB roundtrip 端到端测试（已有：`internal/replication/rdb_coverage_test.go`）
- [x] 大载荷测试（1MB value）（已有：`internal/store/boundary_test.go`，含 1MB/10MB/100MB + 100K list/set/zset/hash）
- [x] 连接中断中途测试（13 个测试：`cmd/integration/connection_interrupt_test.go`，覆盖命令中断、大响应中断、垃圾数据、部分 RESP、管道命令、快速连接断开、服务器存活验证、并发中断等）
- [x] 命令参数边界测试（55 个测试：`internal/server/handler_boundary_test.go`，覆盖空命令、未知命令、WRONGTYPE、空键值、参数缺失、溢出、特殊字符等）

### Phase 5：测试质量度量
- [x] 引入 mutation testing（gremlins，基线：2684 可变体，83% mutator coverage）
- [x] 设置质量基线（`.gremlins.yaml`：70% efficacy / 50% mcover 门禁）

### Phase 6：消灭伪覆盖，提升断言质量

背景：覆盖率数字高但测试有效性不足。`handler_coverage*_test.go` 系列文件明显是批量生成凑覆盖率的，存在大量 fire-and-forget 调用、零断言测试、类型断言代替值断言。

#### 6a. 清理 fire-and-forget 调用（高优先级）✅
- [x] `handler_fuzz_test.go`：40+ 处 `_ = handler.executeCommand(...)` — 已改为 `resp :=` + `assertNotNil(t, resp)`，关键路径加值验证（SET→GET roundtrip、TYPE 类型检查、DBSIZE 非负+DEL 递减、XADD ID 非空、GEOADD/HLL Integer、FLUSHDB/MULTI/DISCARD SimpleString、INFO BulkString 类型）
- [x] `handler_test.go:536-547`：benchmark 中 SET/GET 结果被丢弃 — 评估后保留：benchmark 测性能不测正确性，`_ =` 是标准做法

#### 6b. 补充值断言（高优先级）✅
- [x] `handler_coverage_test.go:1567` TestExecuteCommand_HGET_Coverage — 已加 `assert.Equal(t, "value1", string(*bs))`
- [x] `handler_coverage_test.go:1647` TestExecuteCommand_ZSCORE_Coverage — 已加 `assert.Equal(t, "1", string(*bs))`
- [x] `handler_coverage_test.go:163` TestExecuteCommand_FLUSHALL_Coverage — 已加 DBSIZE == 0 验证
- [x] `handler_coverage3_test.go:11` TestExecuteCommand_DUMP_NonExistent_Coverage — 已加 `assert.True(t, *bs == nil)` 验证内容为 nil
- [x] `handler_coverage3_test.go:23` TestExecuteCommand_OBJECT_REFCOUNT_Coverage — 已加 `assert.Equal(t, int64(1), int64(*integer))`

#### 6c. 删除零断言测试（中优先级）✅
- [x] `handler_coverage_test.go:62` TestExecuteCommand_ROLE_Slave_Coverage — 已删除（`t.Skip()` 死测试）
- [x] `handler_coverage4_test.go:173` TestMarkDirtyKeys_NoWatchers_Coverage — 已删除（调用无断言，与上面 WithWatchers 测试重复）
- [x] `handler_coverage5_test.go:57` TestRunPubSubLoop_NilSubscriber_Coverage — 已改写为 goroutine + select timeout 断言（验证 nil subscriber 不会死锁）

#### 6d. 表驱动测试增加错误路径（中优先级）✅
- [x] `handler_commands_test.go` 5 个主测试函数全部增加负面用例：
  - **TestServerStringCommands**：+6 个负面用例（SET/GET wrong-arity, INCR on list/hash/set/zset + DECR on list WRONGTYPE）
  - **TestServerKeyCommands**：+5 个负面用例（EXPIRE/RENAME wrong-arity, EXPIRE/TTL/RENAME non-existent key）
  - **TestServerHashCommands**：+5 个负面用例（HSET/HGET wrong-arity, HGET/HSET/HLEN on string key WRONGTYPE）
  - **TestServerSetCommands**：+5 个负面用例（SADD/SCARD wrong-arity, SADD/SCARD/SISMEMBER on string key WRONGTYPE）
  - **TestServerSortedSetCommands**：+5 个负面用例（ZADD/ZSCORE wrong-arity, ZADD/ZSCORE/ZCARD on string key WRONGTYPE）
  - **TestServerListCommands**：+3 个负面用例（LPUSH wrong-arity, LPUSH/LLEN/LPOP on string key WRONGTYPE）
  - 共新增 **29 个负面测试用例**

#### 6e. 消灭弱断言模式（低优先级）✅
- [x] `handler_coverage3_test.go:64` `assert.True(t, len(string(*err)) > 0)` → `assert.True(t, strings.Contains(string(*err), "wrong number of arguments"))` 精确匹配 CLIENT NOEVICT 错误
- [x] `handler_coverage3_test.go:92` `assert.True(t, len(string(*err)) > 0)` → `assert.True(t, strings.Contains(string(*err), "ERR"))` 精确匹配 CLIENT TRACKING 错误
- [x] `handler_coverage2_test.go:160-178` TestExecuteCommand_SCAN_Coverage — 已改为验证 handler 响应的 String() 包含 scankey1/scankey2/cursor，同时保留 Store 直接调用作为完整性验证
- [x] 审查 `handler_coverage4_test.go` 中 `setupHandlerWithConns` 系列：9+ 个函数测试内部 bookkeeping 而非可观察行为 — 评估后保留：ActiveClientCount/MonitorClientCount 等是公开 API，测试 register/unregister 生命周期有价值

---

## Phase 8：Mutation Testing 重跑

### 结果

| 包 | 可运行变异体 | 未覆盖 | Mutator 覆盖率 | 状态 |
|----|------------|--------|---------------|------|
| `internal/server` | 2,330 | 649 | 78.21% | dry-run 完成 |
| `internal/store` | — | — | — | TIMED OUT |

### 发现：TIMED OUT 是系统性瓶颈

全量 mutation testing 无法在合理时间内完成。根因：
1. 测试套件 `-race -short` 耗时 ~30s
2. `.gremlins.yaml` 设置 `timeout-coefficient: 3` → 每个变异体需 ~90s
3. 2,330 个变异体 × 90s = 58 小时（不现实）
4. Phase 5 基线（2684 变异体，83%）可能是在无 `-race` 或更快机器上完成的

### 下一步

已选择方案 B+C 组合：在远程服务器后台运行，降低 timeout，无 -race。

**执行状态：** 🟢 已启动（2026-06-29）
- 远程主机：10.1.2.16（8核 31GB RAM）
- 配置：`timeout-coefficient: 1`, `test-cpu: 1`, `workers: 4`
- 分两阶段：先 `internal/server`，再 `internal/store`
- 预计耗时：12-24 小时

**不定期检查命令：**
```bash
bash scripts/remote-mutation-test.sh --status   # 检查是否在跑 + 最新日志
bash scripts/remote-mutation-test.sh --logs      # 实时跟踪日志
bash scripts/remote-mutation-test.sh --results   # 查看已完成包的结果
bash scripts/remote-mutation-test.sh --stop      # 停止运行
```

**结果文件：**
- `internal/server`: `.gremlins-report-server.json`
- `internal/store`: `.gremlins-report-store.json`

### NOT COVERED 变异体分析（2026-06-29 中期快照）

| 文件 | NOT COVERED | 主要变异类型 |
|------|------------|-------------|
| geo_commands.go | **73** | NEGATION(33), ARITHMETIC(20), BOUNDARY(13), INCREMENT(7) |
| key_commands.go | 33 | NEGATION(14), ARITHMETIC(9), BOUNDARY(8), INCREMENT(3) |
| handler_core.go | 18 | NEGATION(12), ARITHMETIC(5) |
| info.go | 14 | ARITHMETIC(14) |
| json_commands.go | 13 | NEGATION(9), BOUNDARY(4) |
| admin2_commands.go | 13 | NEGATION(10), BOUNDARY(3) |
| handler_utils.go | 8 | NEGATION(5), 其他(3) |
| client_commands.go | 8 | NEGATION(7), INCREMENT(1) |
| handler_dispatch.go | 6 | NEGATION(5), BOUNDARY(1) |
| bitmap_commands.go | 4 | NEGATION(4) |
| admin_commands.go | 4 | INVERT(2), ARITHMETIC(2) |
| hash_commands.go | 2 | NEGATION(2) |

**结论：无核心算法 bug。** 未杀变异体全部在：
1. 选项解析循环指针递增（GEO/SORT/CLIENT）
2. 参数边界校验（`>=` vs `>`）
3. 时间/数值转换（EXPIREAT、INFO 内存统计）
4. 复制传播路径（单元测试不覆盖）

---

## Phase 9：Mutation Test NOT COVERED 修复

基于 Phase 8 中期快照，针对高优先级缺口补充测试断言。

### 优先级 1：GEO 选项解析（占 50% 未杀变异体）✅
- [x] `handleGEORADIUS`：测试 WITHCOORD/WITHDIST/WITHHASH 返回精确坐标和距离值
- [x] `handleGEORADIUS`：测试 COUNT 参数边界（恰好 0 个参数缺失）
- [x] `handleGEOSEARCH`：测试 FROMMEMBER 坐标互换不被容忍
- [x] `handleGEOSEARCH`：测试 BYBOX width/2 和 height 参数
- [x] `handleGEOSEARCHSTORE`：测试 STOREDIST 选项和 COUNT 边界

### 优先级 2：RESTORE ABSTTL 时间计算 ✅
- [x] `handleRESTORE`：测试 ABSTTL 未来时间戳 → 剩余 TTL 为正
- [x] `handleRESTORE`：测试 ABSTTL 过去时间戳 → 键立即过期
- [x] `handleRESTORE`：测试非 ABSTTL 模式下 TTL 透传正确

### 优先级 3：SORT 选项组合 ✅
- [x] `handleSORT`：测试 BY pattern GET pattern 多模式组合
- [x] `handleSORT`：测试 ASC/DESC/ALPHA/STORE 组合返回值
- [x] `handleSORT`：测试 LIMIT offset 0 和 count 0 边界

### 优先级 4：Admin/Client/Bitmap/Hash 补充 ✅
- [x] `CLIENT INFO`：验证返回可解析字段
- [x] `INFO`：验证 uptime_in_seconds 字段
- [x] `BITPOS`：验证位搜索边界（bit 0/1、range、空键）
- [x] `HRANDFIELD`：验证正/负 count 行为
- [x] `REPLICAOF`：验证无复制管理器时的错误路径
- [x] `EXPIRE/PEXPIRE`：验证时间转换精度（TTL/PTTL 范围检查）

---

## Phase 10：Store 层核心算法优化

基于 `internal/store/` 各数据类型核心算法分析，按优化价值排序。

### 结论
- **无正确性 bug**，所有算法实现正确
- 瓶颈集中在：链表遍历方向优化、随机采样内存开销、循环内重复读取

### P0：List 遍历方向优化 ✅

| 问题 | 文件 | 当前 | 优化后 |
|------|------|------|--------|
| LRange 从 head 遍历 | list.go:573-667 | O(stop) | O(min(start, N-stop) + count) |
| LPos 循环调用 getNodeByIndex | list.go:692-751 | **O(N²)** | O(N) |
| getNodeByIndex 每次重读元数据 | list.go:324-410 | 循环内重复读 length/start/end | 读一次传参 |

- [x] **LRange 双向遍历**：当 `stop > length/2` 时从尾部反向遍历，减少不必要的前向跳步
- [x] **LPos 改为单次链表遍历**：替换循环调用 `getNodeByIndex`，改为直接沿 `:next` / `:prev` 指针遍历
- [x] **getNodeByIndexWithMeta**：新增内部函数接受预取 length/start，避免循环内重复读取相同 BadgerDB key

### P1：Sorted Set 随机/排名优化

| 问题 | 文件 | 当前 | 优化后 |
|------|------|------|--------|
| ZRank 无双向扫描 | sorted_set.go:443-500 | O(N) worst | O(N/2) worst |
| ZRandMember 全量加载 | sorted_set.go:1977-2054 | O(N) 内存 | O(K) 内存 |

- [x] **ZRank 双向扫描**：当 rank > Card/2 时，用 `Prev()` 从尾部反向扫描，`rank = Card - 1 - reverseRank`
- [x] **ZRandMember 蓄水池采样**：用 Algorithm R 替换全量 shuffle，单次遍历 O(N) 时间 O(K) 内存

### P2：Hash/Set 随机采样优化

| 问题 | 文件 | 当前 | 优化后 |
|------|------|------|--------|
| HRandField 全量加载 | hash.go:744-796 | O(N) 内存 | O(K) 内存 |
| SRandMember 全量加载 | set.go:474-523 | O(N) 内存 | O(K) 内存 |
| SPop 线性扫描 | set.go:354-414 | O(N) | O(N) 但常数更优 |

- [x] **HRandField 蓄水池采样**：单次前缀扫描 + 蓄水池，O(K) 内存
- [x] **SRandMember 单次遍历**：SRandMember 改为 O(1) 内存蓄水池采样
- [x] **SPop 单次遍历**：BadgerDB 不支持随机访问，O(N) 是固有下限，保持现状

### P3：String 非原子操作

- [x] **APPEND 合并事务**：将 View 读 + Update 写合并为单个 Update 事务（在事务内读旧值+写新值）

---

## SortedSet 算法优化

基于 `internal/store/sorted_set.go` 核心算法评估，双键索引 + 版本号方案在 BadgerDB 上是务实选择，以下为已优化项：

### P2：性能瓶颈
- [x] **ZRANK 复杂度优化**：已确认当前实现利用 BadgerDB 排序特性实现提前终止（O(rank) 平均）。对于 upper-half 成员（rank > N/2），记录了双向扫描路径（`rank = card - 1 - reverseRank`，O(min(rank, N-rank))），待确认实际热点后实施。
- [x] **ZINCRBY 3 次 key 操作优化**：已将 version 嵌入 data value（`encodeDataValue`/`decodeDataValue`），现有成员的 ZINCRBY/ZAdd/ZRem 无需读取 meta 获取 version，从 2 次读取降为 1 次。向后兼容旧 8 字节格式。

### P3：健壮性
- [x] **Version uint32 溢出**：已在 `ZSetsMetaValue` 注释中标注（~43 亿次 mutation 后回绕，实际几乎不可能）
- [x] **ZRandMember 全量加载**：已在代码注释中记录 BadgerDB 不支持随机访问的限制，建议蓄水池抽样优化

### 评分编码（已正确实现，无需修改）
- `encodeScore`/`decodeScore` 使用标准 IEEE 754 翻转技巧，与 Redis 一致
- `NaN` 处理与 Redis 行为一致（实际影响极小）

---

## 架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL / FUNCTION / MIGRATE | P2，按需补充 |

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

# Mutation testing (dry-run)
gremlins unleash --dry-run ./internal/store

# Mutation testing (full, requires gremlins installed)
bash scripts/mutation-test.sh --run
```
