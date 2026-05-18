# BoltDB 待办列表

> 从已归档计划中提取的未完成项。按优先级排列。

---

## P1 — 已完成

- [x] **大规模边界测试** — `internal/store/boundary_test.go`（100MB/10MB/1MB 字符串, 100K List/Set/ZSet/Hash）
- [x] **重试逻辑单元测试** — `internal/store/retry_update_test.go`（11 tests, maxRetries/backoff/jitter/error wrap）
- [x] **WRONGTYPE 集成层覆盖** — 全部 9 种数据类型（stream/JSON/TimeSeries/Geo 覆盖完成）

## P2

- **LSM 压缩/重平衡测试** — 大量写入后触发的 SSTable 合并、大量删除后的孤儿键清理、压缩期间并发读写（来源：test-effectiveness-audit.md Phase 2）
- [x] **确定性冲突测试** — `internal/store/conflict_test.go`（10/100/20 goroutines, 多个冲突场景）
- [x] **`t.Parallel()`** — **Phase 1+2 完成**。55 文件，约 900 测试函数已并行化。
  - Phase 1: backup/cluster/helper/proto/replication/sentinel + store-boundary+pubsub + integration-replication+soak（41 文件，~500 tests）
  - Phase 2: store 全部（移除 sharedStore 全局，per-test BadgerDB）+ server 全部（移除 testState 全局，per-test connState）。14 文件，~400 tests
  - 不启用：logger（全局 level 有并发写竞争，1.3s 边际收益低），integration（共享服务器），cmd/boltDB（全局 flag）
  - 效果：server 包 33s→25s（-24%），store 包维持接近
- **并发规模提升** — 现有并发测试仅 10 goroutines，升级到 100/1000；添加读写比例变体（来源：test-effectiveness-audit.md Phase 3。注：conflict_test 已有 100 goroutines 测试）

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
