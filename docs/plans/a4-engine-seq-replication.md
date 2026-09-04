# A4 立项：复制记账引擎序列化（managed-mode ts 迁移）

> 状态：**实施中**（2026-09-03）——S0 ✅ → S1-A1 ✅ → **S1-A2 ✅**（引擎切换全绿——§10
> 附 5）→ S2 复制切换（蓝图已入册——§10 附 6——实施未开始）→ S3。
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

> **实际实施路径修正（2026-09-03——§9 裁决后 A 方向）**：上表为原始计划——§9 阻断
> （managed 无冲突检测 vs RMW）后 S1 改为 **S1-A1 应用层 key 锁层**（补差 + 覆盖审计——
> §10 附 3——key 锁接管 RMW 原子性）+ **S1-A2 引擎切换**（§10 附 5——tsSource + commitTS +
> discard-ts 三层——已验证全绿）——原 S1 的"双轨并行字节记账"未实施（A 方向：直接切换、
> 字节记账由开关回退）——S2/S3 行范围维持。

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

- **状态**：S0 引擎研究 ✅（§8）→ S1-A1 应用层 key 锁层 ✅（§10 附 3——补差 + 覆盖审计
  双里程碑）→ **S1-A2 引擎切换 ✅（§10 附 5——OpenManaged + tsSource + commitTS——store/
  replication/server 全绿 + §1c 守卫三件套 + lint 0）** → **S2 复制切换**（落点调查 + 蓝图
  已入册 §10 附 6——实施未开始）→ S3 快照全量（未启动）。
- **决策点**：① S2 启动前的小步前置——commit-ts 回传打通（§10 附 6 观察 1——commitTS
  暴露本次提交 ts → PropagateCommand 标注——需单独验证）；② 每阶段完成后的门槛未过时
  的处置（回滚 vs 追加调参——按 §5 回滚总则）；③ S2 的开关配置化（字节记账回退路径）。
- **预期结局**：S2 完成后误判重连自愈化——丢失链断（即使读干扰误判仍在）；S3 后
  snapshotMu 锁绑定退役——FULLRESYNC 无应用层锁。S1-A2 已实证 managed mode 可行
  （坏点全部绕行——discard-ts 三层 + key 锁接管）——A4 路线成立继续推进。

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

### §10 附：RMW 覆盖清单（2026-09-03——store 279 导出方法分类）

**A. 值 RMW（锁必需——写值依赖读值）**：INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT、GETSET、
APPEND、SETRANGE、SETBIT/BITFIELD/BITOP；list 族 LPUSH/RPUSH/LPUSHX/RPUSHX/LPOP/RPOP/
LREM/LSET/LTRIM/LINSERT/LMOVE/LMPop/BRPOPLPUSH/BLMOVE + 阻塞 B* 变体；set 族 SADD/SREM/
SPOP/SPOPN/SMOVE/SDIFFSTORE/SINTERSTORE/SUNIONSTORE；zset 族 ZADD/ZINCRBY/ZREM/
ZREMRANGEBY*/ZPOPMIN/ZPOPMAX/ZMPOP/ZDIFFSTORE/ZINTERSTORE/ZUNIONSTORE/ZSetDel；
hash 族 HSET/HMSET/HSETNX/HINCRBY/HINCRBYFLOAT/HDEL；流 XADD/XDEL/XTRIM/XSETID/XACK/
XNACK/XCLAIM/XAUTOCLAIM/XGROUP*；Geo GEOADD/GEODEL/GEOSEARCHSTORE；TS*（TSAdd/
TSIncrBy/TSDel/TSRule）；JSON*（JSONSet/Del/ArrAppend/NumIncrBy/NumMultBy/Clear）；
PFADD/PFMERGE；MSETNX；RENAME/RENAMENX。

**B. key 生命周期/类型检查（锁必需——存在性/类型依赖）**：DEL/DelString、EXPIRE/
EXPIREAT/PEXPIRE/PEXPIREAT/PERSIST；SET 族（SET/SETNX/GETSET/SETEX/PSETEX）的类型检查
（写前读 type-key——并发类型变更竞态需锁——SETNX 的存在性检查同理）。

**C. 盲写（无需锁——并发 last-wins 即正确 Redis 语义）**：SET/SetWithTTL/SETEX/PSETEX/
SetStringBatch/MSet/Restore——仅当 B 类类型检查的竞态风险经裁决可接受（建议过渡期
SET 类也取锁——与 badger 冲突检测双保险——代价小、S1-A2 后统一按清单收窄）。

**D. 只读/工具（不在覆盖内）**：GET/GETRANGE/MGET/EXISTS/TYPE/STRLEN/LLEN/LRANGE/LINDEX/
SCARD/SMEMBERS/SISMEMBER/ZSCORE/ZRANGE/ZCARD/HGETALL/HLEN/XRANGE/XLEN/KEYS/SCAN/
OBJECT*/DUMP + 内部（SNAPSHOTMU*/SAVE*/LOAD*/CLEARALLDATA/FLUSHDB/CLOSE/CHECK/...）。
FLUSHDB 全库清与写并发的互斥为独立问题（需全库写锁——不在 key 锁层——S1-A2 前裁决）。

**实现落点**：清单驱动锁注册表（命令名 → key 参数位映射——server 分发层按命令元数据
取锁——避免逐个命令方法埋锁——见 §10 设计点 3 的修订）。

### §10 附 2：既有锁层审计（2026-09-03——§10 设计假设的纠正）

**重大发现**：store **已有** `KeyLockManager`（`keylock.go`——`sync.RWMutex` 256 分片池——
`NewKeyLockManager(256)` + `keyLockMgr` 字段 define.go:85——"Key-level locking for atomic
operations"）——**非绿地**。§10 的绿地假设错误——设计改为**既有层审计 + 补差**。

**既有覆盖（6 文件——36 处引用）**：string.go（INCR 族/INCRBYFLOAT/APPEND/GETSET...）、
hash.go、linsert.go/list_types.go/lpush_rpop.go/ltrim.go（list 族 RMW）——模式 =
方法级 `s.keyLockMgr.Lock(key)` + `defer Unlock` 包住 retryUpdate。

**覆盖 gap（审计确认——A/B 清单命令在以下文件均无锁）**：set.go（SADD/SREM/SPOP/SMOVE/
*STORE）、zadd_zrem.go/zrange.go/zinter_store.go（ZADD/ZINCRBY/ZREM/ZPOP*/Z*STORE）、
rename.go（RENAME/RENAMENX）、del.go（DEL/EXPIRE/PERSIST 族）、hyperloglog.go
（PFADD/PFMERGE）、xtrim.go + stream（XADD/XDEL/XTRIM/XCLAIM/XACK/XGROUP*）、geo/TS/JSON
 变体、BITFIELD/SETBIT、MSETNX——（按 §10 附清单 A+B 全量补差）。

**修订后的 S1-A1 路线**：① 补差实施 = 按既有模式给 gap 命令方法加锁（方法级 wrap——
逐方法编辑——~10+ 文件）→ ② store 全包绿（零行为回归证明）→ ③ S1-A2 切引擎（key 锁
接管并发——冲突检测退役——§8 坏点 #4 解除验证：并发 INCR 原子性新测试）。

### §10 附 3：S1-A1 实施状态（2026-09-03——里程碑验证）

**补差完成**：审计 gap 清单全部命令组已加锁（14 个增量提交——~55 个 RMW/生命周期方法
按既有模式 wrap——单 key `Lock`/多 key `LockMulti` 排序获取）——覆盖：DEL/DelString、
EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT/PERSIST（纯委托的 ExpireAt/PExpireAt 不重锁）、
SET 族（SetWithTTL 为 SetEX/PSETEX 委托落点）、SetNX、MSetNX、set 族（SAdd/SRem/SPop/
SPopN/SMove + *STORE）、zset 族（ZAdd/ZAddWithOptions/ZRem/ZSetDel/ZRemRange* +
Z*STORE）、hll（PFAdd/PFMerge）、RENAME/RENAMENX、流（XAdd/XDel/XTrim/XSetID/XAck/
XClaim/XAutoClaim/XGroup*）、geo（GeoAddWithOptions/GeoRemove/GeoSearchStore）、TS
（TSAdd/TSDel/TSAddRule——TSIncrBy 委托 TSAdd 不重锁）、JSON（JSONSet/JSONDel/
JSONArrAppend/JSONNumIncrBy/JSONNumMultBy/JSONClear）、bit（SetBit/BitField/BitOp）。

**两处自死锁发现即修（委托模式教训）**：RenameNX→Rename、TSIncrBy→TSAdd——方法内委托
另一已加锁方法时不得自持锁（RWMutex 不可重入）——修复为不重锁 + 注释注明（读段竞态由
过渡期 badger 冲突检测覆盖）。

**里程碑验证**：store 全包远程 -race 绿（多轮 ~51-54s）+ replication 跨包绿（23.8s——
FULLRESYNC 恢复路径调已加锁的 XGroupRestore/XSetID 无回归）+ golangci-lint 0 issues +
写路径 bench ok。**慢写直方图对照**：key 锁为每次 RMW 单次互斥操作（~ns 级——分片池——
仅同 key 并发写有争用——即意图的串行化）——不同 key 写负载无退化预期；正式前后基准
A/B 列为 S1-A2 前置可选项。**推送**：18 提交待推（github SSH 网络故障期间本地积累——
恢复后 `git push origin main`）。

### §10 附 4：S1-A2 实施蓝图（2026-09-03——代码级落点调查 + 设计定案）

**范围边界**：S1-A2 = **引擎地基切换**（store 走 managed-mode + 全写路径 ts 标记提交）——
复制层字节 offset/backlog **不改**（命令流语义不变——现有套件必须全绿）；ts↔offset 映射
为 S2 的后续范围（§4）。

**落点调查结论**（代码锚点）：
- Open 站点：`define.go:490`（`badger.Open(opts)` → managed 专用构造 + ts 源初始化）。
- 写路径 chokepoint：`retryUpdate`（set.go:23——背压预检 + 信号量 + 重试循环 + 内部
  `s.db.Update(fn)` 于 set.go:59——29 文件 ~100+ 调用者——全部命令方法已由 S1-A1 加 key 锁）。
- 直接 `db.Update` 调用者：define.go:429/572/610/659/707/959（复制状态保存——
  SaveReplIDLocked/SaveMasterReplOffsetLocked/SaveBacklogLocked 族——注释明示"在 snapshotMu
  读锁保护下执行"）。
- snapshotMu：store 暴露包装（define.go:948-970——`SnapshotMuRLock` 等）——server
  processRequest 持读锁跨 commit→PropagateCommand（Issue #3 线性绑定）——**retryUpdate
  刻意不嵌套 RLock**（嵌套在 FULLRESYNC 写者排队时会死锁——set.go:23 注释）。
- **坏点 #1 实际不适用**：全库无 badger 原生 `Backup(`/`NewStream`/`WriteBatch` 调用
  （备份走自有 dump/RDB 格式——dump_restore.go + backup manager）——D5 快照路径无需降级。

**设计定案**：
1. **ts 源**：互斥保护的单调 uint64 计数器（`tsSource`——S0 坏点 #2 的乱序 CommitAt 由
   源的串行唯一分配杜绝）——重启水位：启动时扫描当前最大 ts + 1（或持久化水位键——
   与 S0 demo 的重开恢复实证一致）。分配点 = 每次提交尝试内（retryUpdate 的 CommitAt
   前 + 直接 Update 调用者的各自临界区）——因全部写路径均在 processRequest 的 snapshotMu
   读锁临界区/恢复期写锁内执行，满足"临界区内串行分配"；ts 空洞（失败尝试消耗）容忍
   （读-at-ts 语义只依赖 ≤N 前缀完整，非连续）。
2. **CommitAt 转化**：retryUpdate（set.go:59）`s.db.Update(fn)` →
   `s.db.CommitAt(tsSource.Next(), fn)`——**零签名 churn**（ts 自分配——~100 调用者不改）；
   直接 Update 调用者（define.go:429-959）同样 CommitAt 化。冲突重试分支（ErrConflict）
   在 managed 下退役（key 锁已接管同 key 串行化——循环保留以覆盖 L0/瞬态错误）。
3. **OpenManaged 切换**：define.go:490 换 managed 构造 + ts 源初始化（S0 demo 实证写往返/
   读-at-ts 视图/重开恢复 OK）。
4. **读侧**：121 处 `db.View` 不动（S0 实证 managed 下 View 照常工作）。
5. **验证协议**：store 全包远程 -race + replication 跨包（字节 offset 语义不变——
   FULLRESYNC/PSYNC/RDB 套件全绿）+ 回归守卫（§1c 三件套）+ **新增并发 INCR 原子性测试**
   （坏点 #4 解除验证——仅 key 锁保护下的原子性）+ dw A/B 门槛 ≤1/15 + lint。
6. **回滚**：切换提交与 S1-A1 可分——git revert 单提交可退。
7. **风险注记**：① ts 空洞/重启水位的边界用例需补测；② managed 下写吞吐/压实行为与
   Open 模式的差异（INFO l0_*/slow_* 遥测对照——阶段 0 字段可用）；③ CommitAt 失败重试
   的 ts 消耗语义（空洞）须文档化。

### §10 附 5：S1-A2 实施状态（2026-09-03——切换落地 + 验证全绿 ✅）

**切换落地**：OpenManaged（define.go:490——managed 为 options 开关：`OpenManaged` =
`managedTxns=true` + `Open`）+ tsSource 接线（struct 字段 + 构造器 `MaxVersion()+1`
水位 Init）+ `commitTS` chokepoint（`NewTransactionAt(MaxUint64, true)` 最新快照读 +
fn 写体 + `CommitAt(ts, nil)` 同步提交——nil 回调 = 同步 Commit 错误直返）——全库
db.Update 零残留（retryUpdate set.go:59 + define.go 6 直写站点 + cluster 3 站点经
`RunWriteLocked` 转 commitTS——穷尽复检）。测试代码同步（8 处裸 Update + gc_test
helper 的 OpenManaged 接线）。

**discard-ts 必需（发现 + 三层修复）**：managed 模式无自动版本回收——不推进则 MVCC
版本累积 → 写删量级压实停滞（60000 写 + 48000 删规模挂起实证——gc_test 同因）。
三层推进设计（badger 内部栈捕获逐步钉死——值级实证）：
1. **基础推进**：commitTS 提交后 `SetDiscardTs(ts)`——scale 探针 16.7s ✓ +
   TestRunValueLogGC 21.1s ✓（原挂起测试已过）。
2. **有序完成水位**（tsSource Begin/End/AdvanceDiscard——仅推进连续完成前缀——防止
   discard-ts 越过 in-flight 提交的 ts）——专项测试绿。
3. **pair 原子对**（discardMu 串行化 AdvanceDiscard+SetDiscardTs——单调守卫与 oracle
   实际推进间窗口的并发低值后落回退实证：CLEANUP-ASSERT discardTs=1408<lastCleanupTs
   =1413 / 1154<1158——badger 补丁副本打栈 + 值级插桩定位——txn.go:211 `discardTs >=
   lastCleanupTs`）——**根因修复点**。

**冲突测试适配**：managed 下 badger 冲突检测退役——deterministicConflictWrite（raw
retryUpdate 无 key 锁）并发写丢更新（got 1/15 vs want 10/50——机制过时）——helper 加
key 锁（S1-A1 语义：RMW 原子性由 key 锁接管）→ 确定性无丢失——注释更新。

**验证全绿（本提交前）**：vet ✓ + store 全包 -race 47.7s ✓ + replication 跨包 22.1s ✓
+ §1c 回归守卫三件套 ✓（DuplicateWindow 36.4s + SnapshotFullresyncOffset/PsyncReconnect
NoLoss 66.5s）+ golangci-lint 0 issues。调试插桩（badger 补丁副本 + go.mod replace +
探针）已用后即清。

### §10 附 6：S2 复制切换蓝图（2026-09-03——落点调查 + 设计定案方向）

> S1-A2（引擎 ts 记账）完成后，S2 = 复制层从字节 offset 记账切换到 ts 记账。本附记
> 录落点调查（代码锚点）与设计定案方向——实施前的前置调查（S2 为复制层最大改动面——
> 复制语义改变——兼容面广）。

**落点调查（代码锚点）**：
- **主侧写路径**：`processRequest`（snapshotMu RLock——commit→PropagateCommand 线性
  绑定）→ `ReplicationManager.PropagateCommand`（replication.go:371）→
  `serializeCommand(cmd)` + `backlog.Append(cmdBytes)`（replication.go:383-388——返回
  byte-offset——backlog 唯一入点）。
- **backlog**：`ReplicationBacklog`（backlog.go:12——Append/GetRange/GetCurrentOffset/
  SetOffset + `StartsAtCommandBoundary` backlog.go:152）；`BacklogWAL`
  （backlog_wal.go:34——WAL 持久化——Append(offset, cmdBytes)/Replay/Truncate——重开
  恢复的 offset 锚点）。
- **排水**：`GetRange(startOffset, endOffset)`（backlog.go:65）+ `SendBacklogData`
  （psync.go:112——主→从字节流切片）。
- **PSYNC**：`HandlePSync(replId, offset)`（psync.go:21——offset 定 FULLRESYNC vs
  CONTINUE；psync.go:52-56 的 `StartsAtCommandBoundary` 边界校验——纵深防御）。
- **从侧确认**：`SlaveReconnector`（reconnect.go——周期 GETACK/ACK——reconnect.go:
  358-418——masterWater/offset 语义——从侧按 offset 确认）。

**关键观察（蓝图定案输入）**：
1. **commit-ts 未回传**：store 提交（commitTS）与 `PropagateCommand` 同处 snapshotMu
   RLock 临界区——但提交 ts 当前不暴露给调用方——S2 的 ts 标注需打通：commitTS 返回
   本次提交 ts（或 processRequest 捕获 tsSource 水位）→ PropagateCommand(ts, cmd)。
2. **B2 排水进度判据被 S2 自然覆盖**：从侧按 ts 确认后——停滞检测的"排水进度"直接以
   ts 差度量（主侧已传播 ts vs 从侧确认 ts）——1c-complete-fix-design 的 B2 候选
   （停滞检测改测应用进度）不再需要独立事件级探针。
3. **边界天然化**：backlog 条目从"字节段 + 边界扫描"（StartsAtCommandBoundary）变为
   "ts 键条目"——ts 整数比较即边界——psync.go:52-56 的纵深防御简化为整数比较。
4. **双轨一致性**：迁移期需 ts↔byte 映射核验（每命令一条传播 = 一个 ts——ts 段内
   命令字节长 == serializeCommand 输出——换算表核验防漂移——§5 风险注记）。

**设计定案方向**：backlog entry = (ts, cmdBytes)——Append 返回 ts；GetRange(tsStart)
  排水；ACK/GETACK 载 ts；FULLRESYNC 参数 offset→ts（RDB 快照 at ts + 排水 [ts, now) 属
  S3——S2 保持 RDB + ts-排水混合过渡）；配置化开关保留字节记账回退（双轨并存——最大回滚
  损失单阶段）。

**范围/风险/验证**：改动面 = replication.go/backlog.go/backlog_wal.go/psync.go/
  reconnect.go + server 层复制命令 ts 语义（replication_helper.go/handler_dispatch.go）；
  验证门槛 = 现有 4 守卫重写绿 + 新增 ts 单调/重放守卫 + dw A/B ≤1/15（§7 协议）+ 兼容
  三套 + RESP 形状（§6）；风险最高项 = 复制语义改变面（兼容套件全覆盖）+ 双轨一致性
  （换算表核验）。**实施须先补 commit-ts 回传（观察 1）的小步改造并单独验证**。

**观察 1 深化（2026-09-03——commit-ts 回传的设计定案分析）**：链路已实证：processRequest
（handler_core.go:710——RLock 区 738-739 跨 execute→propagate）→ executeCommand
（handler_dispatch.go:50 分发）→ 命令方法 → store 方法（~100 方法无 ctx 参数）→
retryUpdate → commitTS（ts 深埋于此——同 goroutine）。每命令 1:1:1（命令↔提交↔传播）。
**每命令 ts 关联机制评估**：
1. **store 方法返回 ts**——写方法族签名全改（~50+）——NO。
2. **单字段 last-commit 捕获**——并发 RLock 持有者下竞态实证（A-commit-6 → B-commit-7 →
   A-propagate 读到 7——错误/重复 ts——backlog ts 键碰撞）——NO。
3. **预分配 + ts 流穿执行链**（processRequest 预分配 → executeCommand → 命令方法 →
   store 方法——需 ctx/签名穿透全链）——API 面大——待裁。
4. **goroutine 局部捕获**——Go 无原生 goroutine-local；runtime-gid hack 的写路径成本
   （C5 敏感）——NO。
**推荐**：ctx 流穿（Go 标准请求上下文——processRequest 建每请求 ts 槽 → ctx 携带穿透
  分发/命令/store 方法链 → commitTS 消费）——代价 = 写方法族 ctx 参数机械性增补（面大
  但机械——可分文件增量）——或备选：提交-传播对串行化（写吞吐成本——C5 敏感）。实施
  前需裁 ctx-流穿 vs 串行化的吞吐权衡。

**D-定案（2026-09-03——用户裁决 = kvrocks 式 log-in-commit）**：kvrocks 调研实证：其记账
单位 = RocksDB SequenceNumber（非字节 offset）；每命令 log-data（WriteBatchLogData——
命令参数）与数据变更**同一 WriteBatch** 写入（redis_db.cc——单一批 = 单一 seq——命令与
提交天然同序——**零分发侧打标**）；传播 = `GetWALIter(next_repl_seq)` WAL 回读重放
（FeedSlaveThread::loop——replication.cc:178——`batch.sequence != curr_seq` 离散检测 +
`max_replication_lag_` 守卫）；从侧 ACK 带最大 seq；log-data 构造点 = **76 处**
（storage/types 的 per-命令函数级——等价 bolt 的 store 方法 fn 体级——无签名破坏）。
**bolt 落点定案**：传播日志键与数据变更同 commitTS 事务写入（同 ts = 天然绑定——
无竞态、无 ctx 流穿、无串行化吞吐代价）——复制层按日志键 ts 排水。待定设计件：
① 日志键方案（ts 序键——如前缀 + ts 大端——读侧前缀扫描）；② 日志值 = 传播规范形式
（对齐现有 processRequest 的 normalize——EXPIRE→PEXPIREAT 族）；③ 成功条件日志
（失败回复不入日志——防 slave apply 错误/FULLRESYNC thrash——754 注释语义）；④ 保留/
回收（日志键增长——backlog 保留等价）；⑤ 实施顺序：commitTS 级日志支持 + 单命令族
试点 → 扩展 → 复制层读侧接 backlog/排水（S2 主体）。**实施进度（2026-09-03）**：
commitTS 级支持 + 单命令族试点 ✅（`bd0c854`——REPLLOG_+tsBE 同批日志键 + 同 ts 绑定
测试）→ string 族扩展 ✅（`6fd372e`——12 写站点标识性负载——wrapper 委托覆盖）——
**C5 核验（2026-09-03——推翻 6fd372e 初测 +30-37%）：同条件紧邻 A/B 无检测差异
（D-on 417µs vs pre-D 438µs——~5% 噪声内——初测为跨时机器漂移伪影——~2.7x 批次差）——
D 写路径开销未可观测**。


### §10 附 7：S2 读侧架构定案（2026-09-03——kvrocks WAL 回读式读侧的设计深化）

> 范围：S2 复制切换的**读侧**（backlog/PSYNC/ACK 的 ts 化——D 日志键的消费者侧）。
> 写侧（D log-in-commit）已落地（附 6 实施进度——`bd0c854`/`6fd372e`）。本文为架构
> 定案（设计文档——实施待启动授权）。

**当前读侧机制映射（字节记账——实证）**：
- 传播：processRequest（RLock 区）→ PropagateCommand（replication.go:371——RLock 复制
  slaves → `serializeCommand(cmd)` → `backlog.Append(cmdBytes)` 返回**字节 offset** →
  WAL.Append(cmdOffset, cmdBytes) 持久化 backlog → maybeTruncateBacklogWAL）。
- PSYNC：HandlePSync(replId, offset)（psync.go:21——offset 请求 → 边界校验
  `offset ∈ [backlogStart, currentOffset]` + `StartsAtCommandBoundary`（backlog.go:152——
  字节↔命令边界映射）→ 命中 = SendBacklogData 增量续传；未命中/重启空 backlog = 降级
  FULLRESYNC）。
- ACK：REPLCONF GETACK * / ACK（reconnect.go:431——从侧上报 masterOffset →
  `masterWater`——主侧排水进度判据的字节确认）。
- FULLRESYNC：写锁跨 snapshotOffset 捕获 + GenerateRDB + 发送（replication_handler.go
  40-60——RDB 快照 + 1MB backlog 排水）。

**S2 目标架构（ts 记账——kvrocks WAL 回读式）**：
- D 日志键（REPLLOG_+tsBE——应用级 WAL——ts 序！）= 命令流的**天然 ts 序源**——读侧的
  增量续传源 = 日志键前缀扫描（kvrocks 的 GetWALIter(next_repl_seq) 模拟——从 last-fed-ts
  起扫）。
- 记账单位迁移：字节 offset → **ts**——backlog 条目（或等效）的推进水位 = ts 水位；
  ACK 确认 = 从侧已应用的最大 ts；PSYNC 请求 = (replId, ts)（边界校验变 ts 整数比较——
  StartsAtCommandBoundary 的字节映射退役）。
- 保留/回收（④）：日志键按"主侧已全部确认水位"推进删除（badger 的 vlog 压实自然回收）——
  或过渡期配置化上限。

**架构决策点（定案候选 + 推荐）**：
- **D1 读侧源形态**：(a) backlog 保留 + ts 侧信道（backlog 条目 N ↔ ts 映射——并发 RLock
  下同步打标竞态（附 6 观察 1——单字段捕获证伪）——需 per-条目 ts 携带——backlog 条目
  结构改 (offset, cmd) → (offset, ts, cmd)——ts 源仍困于竞态——**否决**）；
  (b) **日志键为源（推荐——kvrocks 式）**：读侧增量续传直接前缀扫描日志键（ts 序——
  天然单调——零同步打标）——backlog/WAL 字节记账退役或降为兼容影子；
  (c) 混合（影子双写——过渡期字节记账保留 + 日志键并行——兼容性最稳——成本 = 双写）。
- **D2 从侧确认单位**：字节 masterWater → **ts 水位**（从侧按日志键 ts 确认——GETACK/
  ACK 的语义改为 ts——排水进度判据（B2——附 6 观察 2 自然覆盖）改测 ts 推进）。
- **D3 PSYNC 语义**：(replId, ts) 请求——边界校验 = ts ∈ [logStart-ts, current-ts] 整数
  比较——StartsAtCommandBoundary 字节映射退役（附 6 观察 3——边界天然化）。
- **D4 日志值形式（②——规格随本定案冻结）**：全重放 RESP 数组（含值参数）——SET 族
  TTL 绝对化（PXAT 绝对毫秒——对齐 processRequest 的 EXPIRE→PEXPIREAT 规范化原则——
  消除相对 TTL 的传播滞后漂移）——成功写才记（附 6 待定件③——fn 成功条件已实现）。
  部署与读侧切换同步（全值入日志 = 2x vlog 写放大——消费者落地前不部署）。
- **D5 保留/回收（④）**：日志键删除水位 = min(全部从侧确认 ts)——删除旧日志键释放
  vlog——过渡期配置上限兜底（max-log-ts-age）——backlog 保留等价语义。

**实施分级（读侧切换——每级可回滚）**：
1. **影子双写 + 读侧观测**（D1c——最稳起步）：PropagateCommand 旁路日志键双写（string
   族已具备——其余族补 D 覆盖——候选 1）+ 读侧探针（按 ts 扫描日志键回放验证与 backlog
   等价——事件级比对）。
2. **PSYNC/ACK ts 语义**（D2/D3）：从侧确认改 ts——主侧排水进度判据改 ts 推进——字节
   记账保留为影子（换算表核验双轨一致——附 6 双轨一致性）。
3. **backlog 退役**（D1b——kvrocks 式）：增量续传源切日志键——backlog/WAL 字节记账删除
   （配置化开关保留回滚——字节记账+ts 记账双轨切换）。
4. **D4 部署**（全值重放形式 + 2x vlog 写放大——与读侧切换同步）+ **D5 保留**上线。

**验证门槛**：现有 4 守卫重写绿（ts 语义）+ 新增 ts 单调守卫（backlog/日志键 ts 序——
并发无倒挂）+ ts 重放守卫（日志键回放 == 字节 backlog 回放事件级等价）+ dw A/B ≤1/15
（§7 协议）+ 兼容三套 + RESP 形状。回滚 = 配置化开关切回字节记账（双轨影子已就位——
无数据迁移）。风险最高项 = 复制语义改变面（兼容套件全覆盖）+ 双轨一致性（换算表核验）。

### 分级-2 落点深化（2026-09-04——代码级映射——D2/D3 具体改造面）

- **PSYNC 侧（psync.go HandlePSync + reconnect.go sendPSYNC）**——现 (replId, 字节
  offset) 三决策点：① CONTINUE 范围 = offset ∈ [backlogStart, currentOffset]（含空
  backlog 重启特例）→ ts 化 = ts ∈ [logStartTS, currentTS]（REPLLOG_ 前缀扫描键界——
  空 = 无日志键，特例自然消解）；② StartsAtCommandBoundary(offset) 字节边界校验（防
  K:HASH:47 类误帧降级 FULLRESYNC）→ ts 化退役（每 ts 即命令边界——整数比较）；
  ③ FULLRESYNC 应答 Offset = currentOffset → currentTS。slave 侧 resume = lastOffset
  （sendPSYNC 发送）→ lastAppliedTS（CONTIUE 后维持 + 落盘持久化）。
- **ACK 侧（reconnect.go——一字节水三用途）**：① GETACK 应答 = slave lastOffset（防
  主超时 keepalive）→ 语义与 ts 无关（保留字节或改 lastAppliedTS 均可——推荐后者统
  一）；② masterWater = 主侧 ACK 回复通告（backlog 水位）→ 主侧 currentTS；③ 停滞判据
  = masterWater > slaveOffset 且 idle > stall（armed/收敛窗 + drainStall 双阈值——
  §1c 相关）→ masterTS > slaveTS + 同 idle 判据（判据单位换 ts——语义不变——B2 排水
  判据自然覆盖）。字段拆：masterWater 三用途共字段——ts 化需按语义分流（keepalive
  可字节可 ts——判据水位必须 ts）。
- **排水判据（主侧——B2）**：字节 masterWater 推进 → min(全从侧确认 ts) 推进（D5 保留
  水位同源——附 6 观察 2）。
- **换算表（过渡期双轨核验）**：offset↔ts 双向映射 = 每 backlog 条目起始字节偏移 ↔ 其
  命令日志键 ts（backlog 条目与日志键值同 RESP 格式——解析对齐即可建表）——守卫重写
  （4 守卫 + ts 单调/重放）以表核验双轨一致（分级-3 backlog 退役前的验证锚）。
  **已落地（2026-09-05）**：`ReplConversionTable`（internal/replication/
  conversion_table.go——事件对齐构建 + 双向换算 OffsetToTS/TSToOffset + AlignCheck
  双轨核验 + 空表边界）——`TestConversionTableDualTrack`/`TestConversionTableEmpty`
  + **`TestConversionTableDetectsDivergence`**（反转序构造分叉——锚检测语义实证——
  missing=2499 根因家族的早期检测面）+ **`TestConversionTableConcurrentWriters`**
  （8×25 并发——锚要么一致要么报对齐错——绝不静默错表）覆盖（replication 全包远程
  -race 绿）。**守卫接入**：`TestTSReplayEquivalence`（ts 重放守卫）末尾追加换算表
  构建 + AlignCheck 断言——事件级等价的强形式（offset↔ts 双向映射建表核验）。
- **顺序建议**：先 ACK 判据 ts 化（从侧自算 lastAppliedTS——主侧 ACK 回复带 currentTS
  ——换算表核验双轨）→ 后 PSYNC (replId, ts)（slave 需 lastAppliedTS 持久化）→ 排水
  判据 ts 推进 → 分级-3 backlog 退役。

### ts 域深化（2026-09-04——ACK/PSYNC ts 的域语义定案）

- **域结论：ACK/PSYNC 的 ts 一律是"直接主侧 ts 域"**。从侧本地提交 ts（从侧 apply 经
  store 写路径（D 覆盖方法）产生的自有 log 键 ts）是**另一域**——与主侧 ts 不可比
  （两域各自从 1 起步独立推进——slave 本地 ts=50 与 master ts=100 无意义）——
  "从侧自算 lastAppliedTS" 的正解 = 从侧跟踪**已应用的直接主侧 ts 水位**（流中携带的
  主侧 ts——非本地 commit ts）。
- **流须携带主侧 ts**（从侧获知已应用主侧 ts 的前提）：两形态——(a) backlog 条目
  (offset, ts, cmd)（D1a 条目结构——主侧需 per-命令提交 ts——**commit-ts 回传**：store
  提交 ts 暴露给 PropagateCommand——cf21964 机制矩阵的"store 返回 ts"为可行项——
  ctx 流穿/单字段捕获已否决）；(b) log-key 增量流（分级-3——键 ts 天然携带——终态）。
  分级-2 的 ACK-ts 若先于分级-3 落地，走 (a) + 回传；否则 ACK-ts 顺延至分级-3 的
  天然携带（回传面 = 服务器两调用点 + backlog 条目格式——比 ctx 流穿小一个量级）。
- **从侧自有 log 键的域归属**：从侧 apply 的本地提交 log 键（本地域）服务于从侧的
  **下游**（其 sub-slave 的 ACK/PSYNC——链式复制的 hop-local 域）——不参与上游 ACK
  判据（同直接主侧的域对齐仅在相邻 hop 内）。
- **换算表角色（定案）**：非 ACK 判据的运行期依赖——是**过渡期验证锚 + 升级桥**：
  (i) 守卫重写（ts 单调/重放 + 4 守卫）用表核验双轨一致（分级-3 退役前）；(ii) 半升级
  窗口（部分从侧仍字节 offset）的 PSYNC 续传桥（offset ↔ ts 换算）——运行期判据全
  ts 后表降为测试工具。
- **顺序修正**：原"先 ACK 判据 ts 化"的隐含前置 = 主侧 per-命令 ts 可用（回传或分级-3
  流）——实际首步 = **commit-ts 回传打通**（store 提交 ts → PropagateCommand → backlog
  条目 ts）——ACK 判据 ts 化随其后（从侧以条目 ts 更新 lastAppliedTS——主侧 ACK 回复
  带 currentTS）。

### 回传代码面勘察（2026-09-04——commit-ts 回传的可行面修正）

- **现状**：commitTS（ts_source.go:101）与 retryUpdate（set.go:23）均 **error-only 返回**
  （ts 无逃逸通道）——store 写方法多返回（error/int64/string）但均无 ts；821/346
  PropagateCommand 调用点仅传命令参数（ts 到达需经执行链携带）。
- **回传可行面（修正"两调用点"乐观低估）**：ts 到达 821 需执行链携带——store 方法
  （~99 retryUpdate 点签名加 ts 返回）+ handler 层（277 dispatch 的处理器签名）——
  量级 ≈ ctx 流穿（数千签名）——非小步；共享捕获（单字段/最近 log 键查）均竞态
  （cf21964 实证——A-commit-6→B-commit-7→A-propagate 读 7）。
- **推荐修正（分级 2/3 重排）**：ACK/PSYNC ts 语义改走 **分级-3 log-key 增量流**（键 ts
  天然携带——**零回传**）——分级-2 并入分级-3（log-key 增量流先行 = 含 ACK/PSYNC ts
  语义 + 从侧 lastAppliedTS 跟踪——backlog 影子同流并行 + 换算表验证——后退役）。
  回传仅在 backlog 影子需**内联条目 ts**（(offset, ts, cmd)）时才必要——且换算表可用
  探针式事件对齐（log-key ↔ backlog 条目）构建——回传或可全免（实施时复核）。

### log-key 增量流传输设计（2026-09-04——分级-3 主体的 wire/相位/ACK 定案）

- **wire 形态（每条目 = (ts, 全命令)）**：master 按从侧请求 ts 增量扫描日志键
  （`ReplLogEntriesFrom`——6785b82——首个 ts ≥ 请求 ts 起）——每条目发送 = 显式 ts
  （日志键 ts）+ 全命令 RESP（值源见相位——初始为 backlog 影子事件对齐的既有全命令
  ——**非日志键当前标识性值**）——从侧按条目 ts 推进 lastAppliedTS。传输载体复用
  既有复制流（新命令类型或 ts 注记形态——对齐 kvrocks FeedSlaveThread 的 WAL 回读
  重放——实施细定）。
- **与 ②/D4 的子相位**：(i) **协议相位**——(ts, 全命令) 流 + 从侧 lastAppliedTS +
  ACK-ts（值 = backlog 影子对齐的全命令——**无需 ② 部署**——换算表事件对齐即值源
  ——backlog 保持权威）；(ii) **终态**——backlog 退役后值源切日志键自身（②/D4 全重放
  形式——2x vlog 写放大与部署同步）。**②/D4 非 feed 前置——feed 协议相位先行**。
- **lastAppliedTS 跟踪**：从侧对每**已应用**条目以 wire ts 更新（直接主侧 ts 域——
  应用后推进——与现字节 lastOffset 同位点同步维护——CONTINUE 落盘持久化）。
- **ACK received-vs-applied（定案 = applied）**：ACK-ts 报已应用最大 ts（与现字节 ACK
  的应用语义一致——B2 排水判据测应用推进）——received-only 水位仅协议测试用（不进
  判据——从侧"收到但未应用"窗口不触发排水误判）。
- **②/D4 实施勘察（2026-09-04——`528c236` 对齐硬化后的退役前置结论）**：
  - **调用面全量**：`encodePropagateCommand` 89 调用点（~40 种命令——15+ 文件——
    形态统一：`retryUpdate(fn, 30, encodePropagateCommand(命令, 键))`——logValue
    可变参数经 `commitTS`（ts_source.go:101）写 `replLogKey(ts)`（:112-113）——
    标识符值 = 命令名+键（RESP 编码））。
  - **D4 升级范围**：89 站点 × logValue 标识符 → 全重放值（命令+全部参数+PXAT
    绝对 TTL——每站点本地变量拼装——机械但量大——2x vlog 写放大已知成本——
    D 定案 §10 附6 已入册）。
  - **零对齐 feed 切换落点**：D4 后 `FeedEntriesFrom` 值源 = log 键自身值
    （`parseCommandEvents(logValue)` → 命令参数——直接组帧 REPLLOG——**无 backlog
    事件对齐**——对齐硬化（`verifyFeedAlignment`——528c236）退役——并发 commit 序
    vs append 序分叉问题消失（每 log 键携带自身完整命令——与 backlog 无关联）。
  - **从侧侧零改动**：REPLLOG 分支（feedEntryParse 拆 ts+全命令 → apply）已支持
    全命令——D4 部署仅 master 侧值源 + 站点 logValue 升级。
  - **部署依赖**：2x vlog 写放大与读侧切换同步（(ii) 终态）——退役决策的前置。

### §10 附8：backlog 内存环删除设计（offset 水位改 ts 源——2026-09-04）

> 前置已齐（2026-09-04 前序提交）：PSYNC-ts（`26403e1`）、ACK-ts（`15d5f7b`）、
> feed-loop 增量流 + 接线（`982057b`/`07fd694`）、重连 ts 域 catch-up（`2c8ecd0`）、
> FeedEntriesFrom 增量 seek（`ea6704a`）、feed-loop 跳过 WAL 字节记账（`2e1e68b`）、
> feed-mode 复跑零丢失守卫（`1f1ef74`）。本设计为 backlog 退役**最后一步**——删除
> backlog 内存环 + WAL 字节记账，offset 水位改 ts 源。

**退役条件（gate——全部满足才可实施）**：
1. **字节从侧（PSYNC 3 参/ts=0）完全退役**——部署内从侧全量 feed-mode（master+slave
   双侧 `--feed-loop`），无 ts=0 请求进入 PSYNC；
2. **换算表双轨核验持续通过**（过渡期验证锚——§10 附7：每 backlog 条目起始字节偏移
   ↔ 其日志键 ts 事件对齐建表——守卫重写以表核验双轨一致）；
3. **feed-mode 规模验证零丢失持续**（`TestRegressionPsyncReconnectNoLossFeed`
   ——`1f1ef74`——修复后 PASS 已确认——退役前再复跑）。

**offset 水位改 ts 源——消费者迁移表**（现状 17 处消费点 → ts 域）：

| 消费者 | 现状（字节域） | 迁移后（ts 域） |
|---|---|---|
| `GetMasterReplOffset()`（replication.go:240） | `backlog.GetCurrentOffset()` | `store.ReplLogCurrentTS()`（log 键最大 ts）——或换算表映射的兼容字节（半升级窗口） |
| RDB `snapshotOffset`（handler:74） | 字节快照点 | snapshot 时 `currentTS`（线性化不变式：快照点 ts = catch-up 起点 ts+1） |
| CONTINUE 字节补发（handler:158） | `SendBacklogData(backlog, offset, cur)` | 删除——统一 `CatchUpAndEnableSlaveTS`（resumeTS+1 起 FeedSlave） |
| `CatchUpAndEnableSlave` 字节循环（replication.go:526） | backlog `[start,end)` | 删除——仅留 `CatchUpAndEnableSlaveTS` |
| PSYNC 字节判定（psync.go:77-90） | backlog range/boundary | 删除（ts=0 无请求）——仅留 ts 域 CONTINUE + FULLRESYNC |
| GETACK 回复（handler:321） | 字节 offset（双轨带 ts） | ts 为主（ACK-ts 已 4 参落地 `15d5f7b`——字节字段保留兼容或移除） |
| INFO `master_repl_offset`（info.go:66,96） | 字节水位 | ts 水位（或换算表兼容字节——监控兼容性注记） |
| WAIT（admin2_commands.go:26,45,231） | 字节等待目标 | ts 等待目标（slave ReplAckTS ≥ 目标 ts） |
| `FeedSlave` SendCommand offset（feed_source.go:115） | 字节 track（feed 允许漂移） | ts（feedSinceTS 已 ts 化——字节仅双轨兼容） |
| monitor/pressure.go:217 | 字节 offset | ts（或换算表兼容） |
| backlog WAL 重建校验（replication.go:118） | backlog.offset | 删除（WAL 已跳过 `2e1e68b`——环删除后 WAL 一并删除） |

**实施步骤（两阶段，阶段 1 可回滚）**：
- **阶段 1——offset 改 ts 源（双轨并存——backlog 环降为影子）**：`GetMasterReplOffset`
  改读 ts 源；RDB snapshot 线性化点改 ts；WAIT/INFO/GETACK 迁移；backlog 环仍 Append
  但无消费者（影子——换算表核验双轨一致）——此阶段**可回滚**（配置开关切回字节源）。
- **阶段 2——删除环（gate 通过后）**：删 `ReplicationBacklog`/`BacklogWAL`/
  `SendBacklogData`/`CatchUpAndEnableSlave` 字节循环/psync 字节分支——配置化开关
  （`--feed-loop`）保留为启动要求（回滚 = 阶段 1 状态需代码还原——阶段 2 不可在线回滚，
  故 gate 必须严格）。

**风险**：① INFO master_repl_offset 语义变化（字节→ts）——客户端/监控兼容注记；
② 半升级窗口（部分从侧字节）——换算表桥（§10 附7 (ii) 升级桥）；③ RDB 线性化点迁移
——保持"快照点 ts = catch-up 起点 ts+1"不变式（与 FULLRESYNC 守卫同构——4 守卫重写
以表核验）。
