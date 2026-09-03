# A4 立项：复制记账引擎序列化（managed-mode ts 迁移）

> 状态：**立项记录**（2026-09-03）——实施前的文档先行——用户已确认启动立项。
> 关联：`1c-complete-fix-design.md` §11（kvrocks 架构对照）、§12（可行性探针结论，
> commit `dd0207c`）——`docs/plans/TODO.md` §1c-残留。

## 0. 立项摘要

- **目标**：把复制记账从"命令字节长度"迁移到"引擎写序列"（kvrocks 模式——RocksDB seqno
  记账）——从结构上消除 §1c 发散悖论（offset 落命令中间）——使误判重连退化为自愈
  CONTINUE（无降级 FULLRESYNC#2）——**断开丢失链**（误判可留——丢失才是 bug）。
- **为什么现在**：§1c 快速修复空间已穷尽（阈值调参/B1/A1b 全实证关闭）；可行性探针确认
  badger 正常写路径**无提交序引擎序列**（`db.Sequence` 非提交有序、`CommitAt` 有 oracle
  一致性风险、oracle 真序列未暴露）——迁移需 **managed mode 全面改造（A4 级）**；
  kvrocks（成熟引擎级 Redis 兼容实现）证明该目标架构大规模可行。
- **证据链**：设计文档 §11（kvrocks 对照——引擎级 WAL 批复制 + seqno 记账 + checkpoint
  全量）§12（探针结论——badger API 事实 + 三条候选 + 否决理由）。

## 1. 背景（引用，不重复全文）

§1c 残留链：store 读争用 → 排水间隙（无上界）→ 冻结误判 → 强制重连 → PSYNC 撞非命令
边界（发散悖论——代码级七环节精确 + 运行期三维度 0 异常——从侧视角不可见）→ 降级
FULLRESYNC#2 → 数据丢失。30s 阈值（`63b5c8c`）为已知最佳部分缓解（A/B 2/15）。
完整调查与裁决：`1c-complete-fix-design.md` §1-§12 + TODO §1c-残留。

## 2. 目标与设计要点

| # | 设计点 | 落点 | 效果 |
|---|--------|------|------|
| D1 | 存储层显式 ts 源（managed mode——提交序即 ts 序） | `retryUpdate` 写路径 + 全部写命令入口 | 每命令提交得原子 ts |
| D2 | backlog 按 ts 段记录（替代字节段） | `replication.go` backlog/Append/GetRange | 排水按 ts 段直发——ts 天生边界 |
| D3 | PSYNC offset = ts——边界检查 = 整数比较（恒真） | `psync.go` HandlePSync/StartsAtCommandBoundary | 误判重连 → 自愈 CONTINUE——无降级 |
| D4 | 从侧重放按 ts 确认 | `reconnect.go` readCommandLoop/ACK | 重续边界由引擎保证——无半命令语义 |
| D5 | FULLRESYNC = 引擎快照（backup at ts）+ 排水 [ts, now) | `replication_handler.go` snapshotMu 路径 | 免应用层锁绑定快照视图与 offset |

**核心不变式**：从侧重放序 == 主侧提交序（ts 序）；每已提交写必有其 ts 段；
从侧 ACK 的 ts 单调不跳。

## 3. 范围（改动面清单）

- **存储层（internal/store）**：managed-mode 迁移——显式 ts 源（读/写/快照/恢复全路径
  显式 ts——badger 文档级模式）；`retryUpdate` 的 ts 获取 + 冲突重试语义复核；批量写
  （SetStringBatch 等）的 ts 段；读路径（db.View）的快照 ts 适配。
- **复制层（internal/replication）**：backlog 结构（字节段 → ts 段——`backlog.go`）；
  排水/GetRange 按 ts；`HandlePSync`/`StartsAtCommandBoundary`（ts 整数比较）；
  `reconnect.go` 的 offset/ACK 语义（ts）；FULLRESYNC（RDB → 引擎快照——snapshotMu
  绑定降级为 ts 快照）。
- **服务器层（internal/server）**：复制相关命令（PSYNC/REPLCONF ACK 的 ts 语义——
  `replication_helper.go`/`handler_dispatch.go`）。
- **测试层**：现有守卫重写/新增（边界/重放/全量/ts 单调）；A/B 协议沿用（§7）。

## 4. 分阶段实施计划

> 每阶段独立提交 + 可回滚点；阶段完成后跑对应验证门槛再进下一阶段。

| 阶段 | 交付物（具体可验证） | 验证门槛 | 回滚点 |
|------|---------------------|----------|--------|
| **S0 引擎研究** | badger managed-mode 语义定案：显式 ts 的 oracle 交互（读 ts vs 提交 ts）、备份快照的一致性、冲突重试在 managed 下的行为、坏点清单 | 研究记录 + 最小可编译 demo（managed 打开下 读写/快照/恢复 往返测试） | 纯研究——无代码风险 |
| **S1 双轨记账** | 存储层 ts 源 + 写路径并行记录 ts（字节记账不变——复制不受影响）——ts 与提交原子性验证 | store 全包测试绿 + 慢写直方图无退化（阶段 0 遥测对照） | 开关关闭即回字节记账 |
| **S2 复制切换** | backlog/排水/PSYNC/ACK 迁移 ts 记账；`StartsAtCommandBoundary` → ts 整数比较；从侧按 ts 确认 | 现有 4 守卫重写绿 + 新增 ts 单调/重放守卫 + **dw A/B ≤1/15**（§7 协议） | 开关回字节记账（双轨并存） |
| **S3 快照全量** | FULLRESYNC = 引擎快照（at ts）+ 排水 [ts, now)——snapshotMu 锁绑定退役 | FULLRESYNC 守卫 + 兼容三套 + tier-A + `--full` 1 次 | 保留 RDB 路径（开关） |

## 5. 风险与回滚

- **badger managed-mode 深度**：显式 ts 下 oracle/MVCC 语义（读 ts 分配、提交 ts 顺序、
  版本回收、备份一致性）为文档级模式——S0 必须定案后才动代码；坏点即停（回滚点：S0 后）。
- **迁移期数据完整性**：S1-S2 双轨并存——需一致性校验（ts 段 vs 字节段的映射核验——
  每命令 ts 段长 == serializeCommand 字节长的换算表）——防双轨漂移。
- **性能**：ts 源（managed 显式分配）与批量写的 ts 段开销——S1 用阶段 0 遥测
  （store_write_slow_*）对照——退化即停。
- **兼容**：PSYNC/CONTINUE 的 ts 语义改变——兼容套件（py/node/cli）+ RESP 形状守卫覆盖。
- **回滚总则**：每阶段独立开关（配置化）——S2/S3 均有字节记账回退路径——最大回滚损失
  为单阶段。

## 6. 验证协议

- **A/B**：沿用 `1c-complete-fix-design.md` §7（从侧 LLen 探针 + dw `-count=15` × 2 批——
  门槛 ≤1/15 vs 30s 基线 2/15；纯对照不回归）。
- **守卫**：现有停滞/冻结 4 守卫（ts 语义适配）+ 新增（ts 单调、重放无重叠、全量快照
  一致性、RDB-vs-ts 双轨校验）。
- **回归套件**：`internal/replication` + `internal/store` 远程 `-race -short` 全包 +
  兼容三套 + tier-A PR-gate。
- **命令**：`bash scripts/remote-test.sh -race -short ./internal/store/... ./internal/replication/...`；
  dw A/B：`bash scripts/remote-test.sh -race -timeout 1800s -run TestRegressionDuplicateWindowMeasurement ./cmd/integration/regressions/ -count=15`。

## 7. 状态与决策点

- **状态**：立项文档（本文件）——实施未开始。
- **决策点**：① S0（引擎研究）是否立即启动（无代码风险——唯一前置）；② 每阶段完成后的
  门槛未过时的处置（回滚 vs 追加调参——按 §5 回滚总则）。
- **预期结局**：S2 完成后误判重连自愈化——丢失链断（即使读干扰误判仍在）；S3 后
  snapshotMu 锁绑定退役——FULLRESYNC 无应用层锁。若 S0 定案 managed mode 不可行
  （坏点不可绕）——A4 关闭并回写设计文档（候选仅剩"换引擎/维持现状"）。

