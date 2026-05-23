# BoltDB 待办列表

> 从已归档计划中提取的未完成项。按优先级排列。

---

## 功能缺口（v0.3，待完成）

- **Cluster 模块** — MEET/FORGET/REPLICATE 命令已实现（简化版）
  - [ ] CLUSTER MEET 真实 TCP 握手（当前为本地创建）
  - [ ] Auto-failover（需要选举协议，复杂度高）
- **Sentinel 模块**
  - [ ] Sentinel 自身集群互联（gossip 多 sentinel 通信，已有基础框架）

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
