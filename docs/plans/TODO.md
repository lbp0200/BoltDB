# BoltDB 待办列表

> 从已归档计划中提取的未完成项。按优先级排列。

---

## P1

- **大规模边界测试** — 新增 100MB/512MB 字符串、100K 集合（List/Set/ZSet/Hash）元素的写入/读取测试，验证 BadgerDB value log 分割和 B+ 树深度行为（来源：test-effectiveness-audit.md Phase 2）
- **重试逻辑单元测试** — 新建 `internal/store/retry_update_test.go`，覆盖 maxRetries 边界、退避范围、抖动随机性、非冲突错误透传（来源：test-effectiveness-audit.md Phase 4）
- **WRONGTYPE 集成层覆盖** — stream / JSON / TimeSeries / Geo 的 WRONGTYPE 测试（来源：test-audit-report.md）

## P2

- **LSM 压缩/重平衡测试** — 大量写入后触发的 SSTable 合并、大量删除后的孤儿键清理、压缩期间并发读写（来源：test-effectiveness-audit.md Phase 2）
- **确定性冲突测试** — 手动制造 txn 冲突，断言重试次数和 fallback 行为（来源：test-effectiveness-audit.md Phase 3）
- **`t.Parallel()`** — 全仓库零使用，所有测试串行执行。添加后可加速 CI（来源：test-effectiveness-audit.md）
- **并发规模提升** — 现有并发测试仅 10 goroutines，升级到 100/1000；添加读写比例变体（来源：test-effectiveness-audit.md Phase 3）

## 功能缺口（v0.3 已标注）

- **Cluster 模块** — CLUSTER MEET/FORGET/REPLICATE 真实实现；gossip 协议；auto-failover；配置持久化
- **Sentinel 模块** — run-id 格式标准化（40-char hex）；slave 完整实现；配置持久化

## 已决策：不做

| 项 | 原因 |
|----|------|
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL / FUNCTION / MIGRATE | P2，按需补充 |
| Redis-cli 8.x 格式差异 | 24 个 FAIL，均为输出格式差异，非 bug |

---

## 归档索引

已完成的计划文档在 `docs/plans/archive/`：

| 文件 | 说明 |
|------|------|
| `2026-05-15.md` | P0/P1 工程护城河（fuzz/chaos/compat/benchmark）+ 后续会话 |
| `2026-05-17.md` | 系统正式化（invariants.md、soak test、state fuzz） |
| `2026-03-10-test-coverage-improvement.md` | 测试覆盖率提升计划 |
| `test-audit-report.md` | 测试体系审计（严重/高风险全修复） |
| `test-effectiveness-audit.md` | 测试有效性审计（Phase 1 已完成） |
| `建议glm.md` | 外部建议汇总 |
