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

## 8. S0 引擎研究结论（2026-09-03——go doc + 最小 demo 实证）

**语义定案**：
- **managed = DB 级模式**——`badger.OpenManaged(opts)` 专用构造函数（**非 Options 字段**）；
  at-ts API（`NewTransactionAt`/`CommitAt`/`SetEntryAt`）仅 managed DB 可用——非 managed 上
  直接 panic（`managedDB=false`——`managed_db.go`）。`DB.Update` 不可用于 managed、
  `DB.View` 假定读 ts（go doc 契约）。
- **实证（demo——用后即清）**：managed 写往返 OK（`NewTransactionAt(rs)`+`Set`+`CommitAt(cs)`）；
  **read-at-ts 版本视图精确**（@3 全空/@4 见 ts4/@6 全见——点对点视图成立）；重开恢复 OK
  （ts 写持久化完好）。

**坏点清单**：
1. **`DB.Backup`/`NewStream` 在 managed 模式 panic**（"This API can not be called in managed
   mode"——backup.go→NewStream）——**D5 的 backup 快照路径被 managed 封死**。
2. **乱序 CommitAt 被接受**（A@ts5 后 B@ts4 均 err=nil——oracle 不拒、无 API 保证）——
   提交 ts 序**必须由调用方保证**——D1 的 ts 源须与提交序严格一致（写路径串行分配或
   批内预分配），否则从侧重放乱序（非交换命令发散——A4 自身引入新发散源）。
3. 冲突重试在 managed 下的行为**未实证**（`DetectConflicts` 字段存在——S1 需补测）。
4. **managed 模式无自动冲突检测（S1 设计探针 2026-09-03 实证）**：同 readTs 重叠写
   （同 key）双提交均 err=nil——last-wins——无 ErrConflict——badger 乐观冲突检测在
   手动 commit-ts 路径被绕过（即便 `DetectConflicts=true` 默认开启）。**推论**：
   retryUpdate 的乐观并发模式（read-modify-write 命令 INCR/GETSET/... 依赖冲突重试
   防 lost-update）在 managed 下**失去保护**——迁移需自带并发控制（见 §9 S1 阻断）。

**可行性裁决：部分可行——D1-D4 基石成立，D5 需替代路径**
- managed 写 + read-at-ts + 恢复**全部实证可用**——引擎序列记账的存储层基础**可行**（S1-S2 路线成立）。
- **D5 降级**：FULLRESYNC 快照**不用 backup**——改用现有 RDB-at-ts（GenerateRDB 机制在
  managed 视图下按 ts 扫描）或维持 snapshotMu+RDB 组合（D5 从"必需"降为"可选优化"——
  不影响 S1-S2 的丢失链断裂主目标）。
- **D1 设计约束**：ts 源须串行分配（提交序 == ts 序）——写路径在现有 snapshotMu 临界区
  内取 ts（该临界区已覆盖 commit——见 §1c 调查链）——天然满足序约束。
- S0 未发现不可绕坏点 → **A4 继续（S1 立项可启动）**。

## 9. S1 设计阻断：managed 无冲突检测 vs read-modify-write（2026-09-03）

**S1 设计阶段探针**（写/读路径清单 + managed 行为实证）发现 D1（引擎 managed 迁移）的
**根本阻断**：

- **事实**：① managed 模式（手动 commit-ts）**无自动冲突检测**（§8 坏点 #4——同 readTs
  重叠写双提交 last-wins）；② 服务器无全局写串行器（executeCommand 走各连接 goroutine）；
  ③ store 写路径为乐观并发（`retryUpdate`——ErrConflict → 退避重跑——read-modify-write
  命令 INCR/GETSET/SETNX/... 靠冲突重试防 lost-update）。
- **推论**：直接切 OpenManaged + retryUpdate CommitAt 化后，read-modify-write 的乐观保护
  **失效**——重叠写静默 last-wins（确定性丢更新——比 §1c 残留更严重的正确性回归）。
- **候选出路**（需用户裁决——成本/风险差异大）：
  - **A. 自带并发控制层**：store 级写互斥或读集-版本校验（mini-MVCC）——保留并发写——
    设计量最大（S1 扩展——触碰每条 read-modify-write 语义）。
  - **B. 写尝试全串行化**：ts 分配 + fn + CommitAt 置于 store 级写互斥——read-modify-write
    确定性有序——**不同 key 的并发写也串行**——吞吐形态变化（需压测确认可接受）。
  - **C. S1 暂不切引擎**：非 managed 下用户态 ts 源 + 并行记录（双轨——字节记账 + 冲突
    检测原样保留——复制零影响）——引擎 ts 切换推迟（managed 并发控制另行设计）。

**用户裁决（2026-09-03）：选 A——应用层 key 锁先行（kvrocks 式）**。kvrocks 对照查证
（`src/common/lock_manager.h`）确认：kvrocks 从不依赖引擎冲突检测——RocksDB 默认盲写——
读-改-写命令在**应用层按 key 加锁**（哈希分片互斥锁池 `LockManager` + RAII `LockGuard` +
多 key 排序取锁防死锁）——引擎层无需关心并发。BoltDB 切 managed 前须补付同样的账。
C（暂不切引擎）降为后备（若 A 实施中发现不可绕障碍）。详见 §10 S1-A 设计。

## 10. S1-A 设计：应用层 key 锁层（kvrocks 式——managed 切换前置）

**目标**：切 OpenManaged 前，为读-改-写（RMW）命令提供应用层互斥——managed 模式禁用
badger 冲突检测后，RMW 命令不丢更新（§9 阻断的解除）。

**参考**：kvrocks `LockManager`（src/common/lock_manager.h——实证）——哈希分片互斥锁池
（2^N 把锁）+ RAII `LockGuard` + 多 key `MultiGet` **排序取锁防死锁**（同 key 不去重不
重复锁——不同顺序的获取者按哈希序排队）——引擎层盲写、并发全在应用层。

**BoltDB 适配设计**：
1. **Store 级 KeyLockManager**（Go）：`sync.Mutex` 分片池（N=2^8？——可配置）——按 key
   哈希取锁——`Lock(keys...)`/`Unlock`——排序获取（防死锁——同 kvrocks）。
2. **锁覆盖范围 = RMW 命令的读写 key 集**：仅"写值依赖读值"的命令需锁（INCR/DECR/INCRBY
   族、GETSET/GETDEL、APPEND、SETRANGE、LPUSH/RPOP 等 list 族、SADD 依赖成员集的去重
   语义、ZADD/ZINCRBY、HSET 的 hash、流/XADD、LINSERT...——实施时按分类枚举定稿）；
   **盲写命令（SET 类——写值与读无关）不需要锁**——并发 last-wins 即正确 Redis 语义。
3. **集成点**：RMW 命令在 `retryUpdate` 调用前取 key 锁、提交后释放（命令层显式取锁——
   retryUpdate 保持透明）——过渡期与 badger 乐观重试**双保险并存**（字节记账不动——
   现有测试全绿 = 无行为回归的证明）。
4. **阶段切分**：S1-A1 锁层落地（零行为变化——纯并发控制补充——store 全绿验证）→
   S1-A2 切 OpenManaged + ts 源（§8 坏点 #2 约束：临界区串行分配）+ 写路径 CommitAt 化
   （key 锁接管 RMW 并发——冲突检测退役）→ S1-A3 读路径核对（View 假定读 ts——121 处
   不动——P2 实证）→ 验证（§6 协议 + 慢写遥测对照）。
5. **回滚**：S1-A1 独立提交（纯增量——可随时撤）；S1-A2 前保留 git 基线（回字节记账 =
   撤 OpenManaged 单点）。

