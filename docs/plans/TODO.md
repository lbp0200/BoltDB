# BoltDB 待办列表

## 项目阶段

```text
Phase 1: Redis 兼容、复制、Sentinel、PubSub、RDB                      ✅
Phase 2: L0、PSYNC、FULLRESYNC、goroutine、shutdown                     ✅
Phase 3: health、basin、evolution、nightly、docs                         ✅
Phase 4: 技术债收敛                                                      ✅
P0–P2 新功能：RESP3 / CLUSTER MEET / Sentinel gossip                    ✅
```

---

## 待定缺口

### 已知架构边界（不会做，需文档化）— 已文档化于 `docs/arch-boundaries.md`

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造，牵动写路径/Badger MVCC/backlog/PSYNC 语义。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏、bounded duplicate window |

### 已决策：不做

| 项 | 原因 |
|----|------|
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL / FUNCTION / MIGRATE | P2，按需补充 |

---

## 附录 A：Nightly Soak 运行参考

Nightly soak 在 GitHub Actions 上运行（`.github/workflows/nightly-soak.yml`），UTC 每日 02:00 触发。

**调度：** cron `0 2 * * *`（UTC）
**工作流：** `Nightly Soak` → Standalone Soak（1h） + Replication Soak（1h）并行执行
**观测指标：** evolution gate（health/basin/drift）通过 `go run cmd/evolution/main.go` 检查
**数据持久化：** evolution history 通过 `actions/cache@v4` 存储，跨 run 累计趋势数据
**历史日志：** JSONL 轨迹数据上传至 Actions Artifacts（保留 30 天）
**回查命令：**

```bash
gh run list --workflow "Nightly Soak" -L 1 --json databaseId \
  | jq -r '.[0].databaseId' \
  | xargs gh run view --log
gh run download <run-id> -n soak-standalone-<run-id> -D /tmp/soak-data
```

## 附录 B：Monitoring Infrastructure（Completed / Frozen）

| 组件 | 状态 | 说明 |
|------|------|------|
| Health decomposition | COMPLETE | S/R/C 三维分解，11 因子评分 |
| Temporal analysis | COMPLETE | 窗口 slope → trajectory 分类 |
| Basin analysis | COMPLETE | phase space → stable/stressed/degraded |
| Evolution gate | COMPLETE | cross-run trend + drift + anomaly |
| Anomaly engine | COMPLETE | regime shift / escalation 检测 |
| Nightly visualization | COMPLETE | JSONL → trajectory 5-panel chart |
| Evolution report | COMPLETE | cross-run drift + basin + oscillation |
| Nightly summary | COMPLETE | markdown + JSON 双格式 |

> No further development planned. Component freeze effective 2026-06-09.

## 归档索引

已完成的计划文档在 `docs/plans/archive/`：

| 文件 | 说明 |
|------|------|
| `2026-05-15.md` | P0/P1 工程护城河（fuzz/chaos/compat/benchmark） |
| `2026-05-17.md` | 系统正式化（invariants.md、soak test、state fuzz） |
| `2026-03-10-test-coverage-improvement.md` | 测试覆盖率提升计划 |
| `test-audit-report.md` | 测试体系审计 |
| `test-effectiveness-audit.md` | 测试有效性审计（Phase 1） |
| `phase-1-4-complete.md` | 所有已完成的 P 级项详细日志（2026-05 至 2026-06-20） |
