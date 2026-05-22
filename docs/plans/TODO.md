# BoltDB 待办列表

> 从已归档计划中提取的未完成项。按优先级排列。

---

## P1 — 已完成

- [x] **大规模边界测试** — `internal/store/boundary_test.go`（100MB/10MB/1MB 字符串, 100K List/Set/ZSet/Hash）
- [x] **重试逻辑单元测试** — `internal/store/retry_update_test.go`（11 tests, maxRetries/backoff/jitter/error wrap）
- [x] **WRONGTYPE 集成层覆盖** — 全部 9 种数据类型（stream/JSON/TimeSeries/Geo 覆盖完成）

## P2 — 已完成

- [x] **LSM 压缩/重平衡测试** — `internal/store/compaction_test.go`（heavy write + compaction、mass delete + verify、concurrent read/write、store.Check after heavy RW）
- [x] **确定性冲突测试** — `internal/store/conflict_test.go`（10/100/20 goroutines, 多个冲突场景）
- [x] **`t.Parallel()`** — **Phase 1+2 完成**。55 文件，约 900 测试函数已并行化。
  - Phase 1: backup/cluster/helper/proto/replication/sentinel + store-boundary+pubsub + integration-replication+soak（41 文件，~500 tests）
  - Phase 2: store 全部（移除 sharedStore 全局，per-test BadgerDB）+ server 全部（移除 testState 全局，per-test connState）。14 文件，~400 tests
  - 不启用：logger（全局 level 有并发写竞争，1.3s 边际收益低），integration（共享服务器），cmd/boltDB（全局 flag）
  - 效果：server 包 33s→25s（-24%），store 包维持接近
- [x] **并发规模提升** — 新增 `cmd/integration/depth_test.go` 高并发测试（100 goroutines: INCR、Mixed R/W、List Push/Pop、Hash HSET、Set SADD/SREM）

## 功能缺口（v0.3，部分完成）

- **Cluster 模块** — MEET/FORGET/REPLICATE 命令已实现（简化版）；**配置持久化**已完成（`cluster:config` BadgerDB key）；**gossip 协议**已完成基础框架（`internal/cluster/gossip.go`；周期性 PING/PFAIL 检测、过期节点清理）
  - [x] 配置持久化（`SaveConfig`, `LoadConfig`, auto-save on slot/node change）
  - [x] Gossip 基础框架（周期性 PING、PFAIL 检测、过期节点清理）
  - [ ] CLUSTER MEET 真实 TCP 握手（当前为本地创建）
  - [ ] Auto-failover（需要选举协议，复杂度高）
- **Sentinel 模块** — **run-id 标准化**已完成（40-char hex）；**slave 健康追踪**已完成（heartbeat、online/offline、reconnect count）；**配置持久化**已完成（JSON 文件）
  - [x] run-id 标准化（`crypto/rand` 20 bytes）
  - [x] 配置持久化（`sentinel.conf.json`）
  - [x] Slave 健康追踪（heartbeat、state machine、reconnect tracking）
  - [ ] Sentinel 自身集群互联（gossip 多 sentinel 通信，已有基础框架）

## 未完成（高复杂度，需后续规划）

- **Cluster auto-failover** — 需要完整选举协议 + 共识机制
- **Sentinel full gossip mesh** — 多 sentinel 之间的完整通信协议
- **CLUSTER MEET 真实握手** — 当前为本地假节点创建，需 TCP 连接交换 ID

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
