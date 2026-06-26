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
- [ ] 并发 INCR 精确计数测试
- [ ] 并发 SET 最终一致性测试

### Phase 4：边界和异常
- [ ] RDB roundtrip 端到端测试
- [ ] 大载荷测试（1MB value）
- [ ] 连接中断中途测试
- [ ] 命令参数边界测试

### Phase 5：测试质量度量
- [ ] 引入 mutation testing
- [ ] 设置质量基线

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
```
