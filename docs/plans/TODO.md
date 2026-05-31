# BoltDB 待办列表

> 从已归档计划中提取的未完成项 + 近期完成项。按优先级排列。

---

## P0：Replication 功能冻结 — 纵向实证数据收集

**起止：** 2026-05-28 起，连续跑 7 天
**目标：** 收集 longitudinal empirical data，不做任何 replication 代码改动
**暂停：** 所有 replication feature work（Cluster/Sentinel/复制功能缺口等）

**运行位置：** `ssh -i ~/.ssh/google_compute_engine elex-gm0135@10.1.2.16` → `/usr/home/elex/boltdb/`

**状态：** ✅ 2026-05-28 已完成远程部署，crontab 已安装（每早 2:00）

**注意：** `short-strict-soak` 因已知 LIST duplicate-window 边界会预期失败（strict equality on + LPUSH 非幂等），需观察的是 divergence 量级是否增长，而非是否通过。

**环境准备（首次）：**

```bash
# 1. 本地编译 linux amd64 test binary
GOOS=linux GOARCH=amd64 go test -race -c -o /tmp/boltdb.test \
  ./cmd/integration/

# 2. 复制到远程
scp -i ~/.ssh/google_compute_engine /tmp/boltdb.test \
  elex-gm0135@10.1.2.16:/usr/home/elex/boltdb/

# 3. 复制脚本和工具
scp -i ~/.ssh/google_compute_engine -r scripts/ \
  elex-gm0135@10.1.2.16:/usr/home/elex/boltdb/
scp -i ~/.ssh/google_compute_engine -r cmd/evolution/ \
  elex-gm0135@10.1.2.16:/usr/home/elex/boltdb/

# 4. 远程执行 nightly 全量（standalone 1h + repl 30m + short strict + failover + evolution）
ssh -i ~/.ssh/google_compute_engine elex-gm0135@10.1.2.16 \
  "cd /usr/home/elex/boltdb && \
   mkdir -p /tmp/bolt-nightly/{data,report/history,jsonl} && \
   CI_NIGHTLY_SOAK=1 \
   SOAK_REPORT_DIR=/tmp/bolt-nightly/report \
   SOAK_JSONL_DIR=/tmp/bolt-nightly/jsonl \
   SOAK_DATA_DIR=/tmp/bolt-nightly/data \
   ./boltdb.test -test.v -test.run 'TestSoakReplicationShortStrict|TestSoakReplication|TestSoak' \
   -test.timeout 8h 2>&1 | tee /tmp/bolt-nightly/run.log"

# 5. 长期 replication soak（6h+，单独长时间窗口）
ssh -i ~/.ssh/google_compute_engine elex-gm0135@10.1.2.16 \
  "cd /usr/home/elex/boltdb && \
   SOAK_REPL_DURATION=6h SOAK_REPL_WRITERS=4 \
   SOAK_REPORT_DIR=/tmp/bolt-nightly/report \
   SOAK_JSONL_DIR=/tmp/bolt-nightly/jsonl \
   ./boltdb.test -test.v -test.run 'TestSoakReplication$' \
   -test.timeout 7h 2>&1 | tee /tmp/bolt-nightly/run-long.log"

# 6. 拉回报告
scp -i ~/.ssh/google_compute_engine -r \
  elex-gm0135@10.1.2.16:/tmp/bolt-nightly/ /tmp/soak-results/
```

**crond 自动化（远程）：**
```bash
# 每早 2:00 跑 nightly，结果追加到日期目录
0 2 * * * cd /usr/home/elex/boltdb && \
  mkdir -p /tmp/bolt-nightly/$(date +\%Y\%m\%d) && \
  CI_NIGHTLY_SOAK=1 \
  SOAK_REPORT_DIR=/tmp/bolt-nightly/$(date +\%Y\%m\%d) \
  SOAK_JSONL_DIR=/tmp/bolt-nightly/$(date +\%Y\%m\%d)/jsonl \
  SOAK_DATA_DIR=/tmp/bolt-nightly/$(date +\%Y\%m\%d)/data \
  ./boltdb.test -test.v \
    -test.run 'TestSoakReplicationShortStrict|TestSoakReplication|TestSoak' \
    -test.timeout 8h \
    >> /tmp/bolt-nightly/$(date +\%Y\%m\%d)/run.log 2>&1
```

**观察清单：**

| 信号 | 关注点 | 判定 |
|------|--------|------|
| duplicate-window regression | INCR gap / LPUSH dup ratio 是否在阈值内稳定 | ≥5 次运行无超阈值 |
| oscillation trend | 是否 >50% 运行显示 limit cycle | 趋势 degrading → 需要干预 |
| basin drift | healthy→stressed→degraded regime shift | 永久 basin 类型变化 → 系统退化 |
| evolution anomaly | anomaly detection 是否真实触发 | 哪些 commit 改变 trajectory |
| health score | 是否随运行次数漂移 | drift delta > 0.05 异常 |
| retry semaphore | ActiveRetries 是否保持 ≤ 100 | 峰值触顶分析 |
| goroutine plateau | delta 是否稳定 | 单调增长 = leak |

**每个早起检查上一晚的报告（远程）：**

```bash
# SSH 到远程查看
ssh -i ~/.ssh/google_compute_engine elex-gm0135@10.1.2.16 \
  "ls -t /tmp/bolt-nightly/20*/report/nightly-*-summary.md | head -1 | xargs cat"

ssh -i ~/.ssh/google_compute_engine elex-gm0135@10.1.2.16 \
  "ls -t /tmp/bolt-nightly/20*/report/replication-evolution.md 2>/dev/null | head -1 | xargs cat"

# 或拉回本地
scp -i ~/.ssh/google_compute_engine -r \
  elex-gm0135@10.1.2.16:/tmp/bolt-nightly/ /tmp/soak-results/
```

**决策点（7 天后）：**
- 所有信号稳定 → replication correctness envelope 确认，可以推进其他功能
- 任一项退化 → 分析 trail，决定是否开 replication 修复窗口

---

## 功能缺口（v0.3+，待完成）

- **Cluster 模块**
  - [ ] CLUSTER MEET 真实 TCP 握手（当前为本地创建）
  - [ ] Auto-failover（需要选举协议，复杂度高）
- **Sentinel 模块**
  - [ ] Sentinel 自身集群互联（gossip 多 sentinel 通信，已有基础框架）

## 复制功能缺口（已知数据丢失）

以下命令已通过 `isWriteCommand` → `handler.go:528-531` 传播到复制流，但在 `executeReplicatedCommand`（`internal/replication/psync.go:158`）中没有对应 case，被静默跳过。需要逐个实现：

| 命令 | 优先级 | 说明 |
|------|--------|------|
| BLPOP, BRPOP, BLMOVE, BRPOPLPUSH | P1 | 阻塞弹出，replica 端需要等效非阻塞版本 |
| MSETNX, HSETNX | P1 | 条件写入，需 replica 端等价语义 |
| SETBIT, BITOP, BITFIELD | P2 | 位图操作，使用量较低 |
| COPY | P2 | 键复制 |
| ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE | P2 | *STORE 聚合 |
| ZRANGESTORE, GEOSEARCHSTORE | P2 | 范围 *STORE |
| XACK, XCLAIM, XGROUP\* | P2 | Stream 消费者组 |
| PFADD, PFMERGE | P2 | HyperLogLog |
| JSON.* (all writes), TS.* (all writes) | P3 | 模块命令 |

## 近期已完成的工作

| 工作 | 文件/说明 | 日期 |
|------|----------|------|
| Split-brain convergence + hardening | `cmd/integration/split_brain_harden_test.go`, `split_brain_convergence_test.go` | May 2026 |
| Failover oscillation regression test | `cmd/integration/failover_oscillation_test.go` | May 2026 |
| SPOP canonicalization | `handler.go:3391-3431` — SPOP → SREM 确定性传播 | May 2026 |
| XADD `*` 修复 | `handler.go:5912` — 自动ID 替换 | May 2026 |
| EXPIRE/PEXPIRE 修复 | `handler.go:527` — 转换为 PEXPIREAT 绝对时间戳 | May 2026 |
| TOCTOU fix (FULLRESYNC lost-write window) | `handlePSyncWithRDB` — snapshotOffset 在 db.View() 前捕获 | May 2026 |
| TOCTOU fix (CONTINUE gap-fill) | AddSlave 后补发丢失命令 | May 2026 |
| writeMu 序列化 | 避免 slave write I/O 与 Close() 的死锁 | May 2026 |
| 三阶 Soak 语义 | short strict / long non-strict / duplicate-window regression | May 2026 |
| Duplicate-window 回归测试 | `regressions/duplicate_window_regression_test.go` | May 2026 |
| Short strict soak | `TestSoakReplicationShortStrict` (10m, strict equality) | May 2026 |
| 复制正确性 envelope | AGENTS.md 文档化 | May 2026 |
| FULLRESYNC 语义文档 | 明确的 no-lost-writes 保证 + bounded duplicate window 说明 | May 2026 |

## 已知架构边界（不会做，需文档化）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造，牵动写路径/Badger MVCC/backlog/PSYNC 语义。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏、bounded duplicate window |

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

