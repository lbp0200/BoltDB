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
