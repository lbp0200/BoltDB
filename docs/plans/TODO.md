# BoltDB 待办列表

> 本文件只列**未完成**的工作。已完成阶段的过程记录归入对应设计文档（`a4-engine-seq-replication.md`、
> `1c-complete-fix-design.md`）或下方「已收口」索引；逐提交细节以 `git log` 为权威。
> 2026-09-05 整理：删除 296 行中已完成的堆积记录，迁入上述文档。

## 待办

### 1. A4 阶段 1——offset 水位改 ts 源（双轨并存——可回滚）

`GetMasterReplOffset` 改读 ts 源（`store.ReplLogCurrentTS()`）；RDB snapshot 线性化点改 ts；
WAIT / INFO `master_repl_offset` / GETACK / monitor 迁移。**backlog 环仍 Append 但无消费者**
（降为影子——换算表核验双轨一致）。回滚 = 配置开关切回字节源。

- 消费者迁移表（17 处）+ 两阶段步骤 → `a4-engine-seq-replication.md` §10 附8
- 前置已齐：PSYNC-ts / ACK-ts / feed-loop 接线 / 重连 ts 域 / 增量 seek / WAL 双轨 / 换算表
- 风险注记：INFO `master_repl_offset` 语义变化（字节→ts）需监控兼容性说明
- **实施状态（2026-09-05）**：`GetMasterReplOffset` 改 ts 源 + `GetBacklogCurrentOffset`
  字节直读面（内部字节路径全迁移）+ WAIT ts 判据（`GetReplAckTS`）已落地——远程 -race
  全绿（replication 49.3s + server）+ 复制守卫复跑通过；**语义守卫**
  `TestRegressionFeedModeTSSemantics`（feed 模式 INFO master_repl_offset == currentTS /
  ROLE offset == ts / WAIT 1 返回 ≥1——ts 判据）已补——**并暴露并修复既有缺口**：
  从侧从不主动上报 REPLCONF ACK（只在主侧 GETACK 请求时回复——而主侧从不发 GETACK）
  → 主侧 UpdateSlaveAckTS 永不触发 → WAIT 恒 0——reconnect.go 周期 goroutine 改为
  每周期先主动上报 `REPLCONF ACK <lastOffset> <lastAppliedTS>`（真实 Redis 从侧语义）。
  **剩余**：`framework.WaitForReplicaSync` 的 feed 模式字节/ts 错域比较——**已修
  （2026-09-05）**：新增 `GetSlaveLastAppliedTS` + WaitForReplicaSync 的 feed 模式
  ts 判据分支（slave lastAppliedTS >= master currentTS——applied 语义）——远程
  feed 守卫复跑无回归；监控兼容注记（INFO/ROLE/monitor/collector 的 feed 模式 ts
  语义）已入 a4 附8 风险①；**半升级窗口实测已闭合（2026-09-05）**：
  `TestRegressionHalfUpgradeByteSlave`——feed 主侧 + 字节从侧（ts=0）混合——字节
  从侧 FULLRESYNC + 字节 catch-up + 断连重连自愈全通过（数据面无丢失）——阶段 1
  双轨并存兼容承诺验证；**剩余**：阶段 2 gate 1（部署内字节从侧退役）
- **全量回归确认（2026-09-05——阶段 1 全部落地后）**：regressions 全量守卫 9 批次
  全绿（核心复制 7 守卫 + DuplicateWindow/BacklogExhaustion/WriteDeadlineStorm/
  FullresyncKeyLoss/FullresyncBacklogWrap/ReplicationThrash + RetryStorm/DiskPressure/
  L0Collapse/SplitBrain/Failover/Pubsub/RdbConcurrent/ShutdownRace/DeterministicReplay/
  ExpireCondition/ClientBuffer）+ cmd/integration 关键批次（复制核心 8 + MasterSlave 族
  6 + 传播族/shutdown/断连 42.2s）全绿——字节模式与 feed 模式双轨零回归

### 2. A4 阶段 2——删除 backlog 内存环（gate 严格——不可在线回滚）

删 `ReplicationBacklog` / `BacklogWAL` / `SendBacklogData` / `CatchUpAndEnableSlave` 字节循环
/ psync 字节分支。`--feed-loop` 保留为启动要求（回滚需代码还原）。

**退役三 gate（全部满足才可实施）**：
1. 字节从侧（PSYNC 3 参 / ts=0）完全退役——部署内从侧全量 feed-mode，无 ts=0 请求进入 PSYNC；
2. 换算表双轨核验持续通过（`ReplConversionTable.AlignCheck`——过渡期验证锚）；
3. feed-mode 规模验证零丢失持续（`TestRegressionPsyncReconnectNoLossFeed` 退役前再复跑）。

**删除面审计与实施蓝图（2026-09-05——代码面已核对——gate 满足后的执行顺序）**：

| 删除点 | 生产调用点（现状） | gate 相关性 |
|---|---|---|
| `CatchUpAndEnableSlave` 字节循环（replication.go:575） | handler:125（FULLRESYNC 后）、handler:198（字节 CONTINUE） | gate 1（ts==0 从侧退役）+ gate 3（改造后规模验证） |
| `SendBacklogData`（psync.go:171） | handler:188（字节 CONTINUE 补发）、replication.go:580（CatchUp 内部） | gate 1 |
| psync.go 字节分支（ts==0——:79-118） | HandlePSync 自身 | gate 1 |
| `BacklogWAL`（backlog_wal.go） | replication.go:421（仅 feedLoop 关时 Append） | gate 1（feed 部署不记 WAL） |
| `ReplicationBacklog` 环 + `GetBacklogCurrentOffset`（阶段 1 字节直读面——10 处调用） | handler:74/131/187/332 + replconf:55 + replication.go:578/589 + feed_source:125 | gate 1 + gate 2（换算表依赖环——退役前最后一次 AlignCheck 核验） |
| `GetMasterReplOffset` 字节回退分支（feedLoop 关） | 阶段 2 后 feedLoop 为启动要求——恒 ts 源 | gate 1 |

**关键设计点**：FULLRESYNC 后 handler:125 的字节 catch-up 改为**直接 ts 域激活**
（`FeedSetEnabled(snapshotTS+1)`——RDB 快照点 == snapshotTS——补发从 snapshotTS+1
起——无需字节 catch-up——`CatchUpAndEnableSlaveTS` 等价路径——阶段 2 唯一行为
改造）；GETACK 字节字段（handler:332 第 3 参）保留兼容（旧主/旧从——EncodeReplconfAck
4 参形态不变——字节值恒 0 或随环删移除——实施时定）。
**实施顺序**：① 部署全 feed（gate 1 确认）→ ② 换算表最后一次 AlignCheck + 规模守卫
复跑（gate 2/3）→ ③ FULLRESYNC ts 激活改造（CatchUpAndEnableSlave 删除）→ ④
字节分支/环/WAL 删除 → ⑤ 守卫更新（FULLRESYNC ts 激活守卫 + 4 守卫 ts 化核对）→
⑥ 全量回归（regressions + cmd/integration）。

### 3. dw A/B ≤1/15 正式验收（gate 于阶段 2 之后）

§7 协议（`1c-complete-fix-design.md`）——双轨下重复窗口度量。**基线数据已测**（纯对照
14/15+1 flake、探针开 15/15——见 a4 §10 附9），正式验收须在复制切换（阶段 2）后重跑。

```bash
bash scripts/remote-test.sh -race -timeout 180s -v ./cmd/integration/regressions/ \
  -run TestRegressionDuplicateWindowMeasurement          # 加 -count=15（5 批 × 3 次）
DW_READ_PROBE=1 ...                                      # 探针开 = §7 完整形态
```

### 4. SSD 写入基线复测（v8.52.0 遗留——疑写路径塌陷）

**未解现象**：2026-09-03 基准测到 GC 前置健康（rewritten=0）后，1MB SET 负载启动 ~166 ops/s
（≈166 MB/s）→ **崩塌至 ~5 ops/s**（21 分钟仅 9.5%——中止）。持续负载下写路径塌陷
（疑 L0/vlog 压实风暴）——**根因未定位**，SSD 基线仍缺。vlog 6.3G 残留为已知 badger 机制
（tombstone 卡空 L0）。

- **下一步**：`scale-data-filler`（按 CLUSTER SHARDS/SLOTS 分组 pipeline）或分段小批量重测
- 前置三查：① `DEBUG GC` 已完成（GC 期间 1MB SET 减速 1350×）；② 无残留 redis-benchmark；
  ③ `-r` 必带（否则覆盖写同一 key）；测完 FLUSHDB

### 5. ✅ 发散悖论（C4）——**feed 模式结构性消失（2026-09-05 分析定论）——仅字节路径残留**

原定义：主侧发送字节 vs 从侧接收字节的**抓包级**直接比对（层 D——外部工具）——「恢复
路径重设计（层 C：降级无损化）」与 A4 S3 的前提——§1c 失败链最后一环（历史：七环节
代码级精确 + 逐命令/ACK/应用历史三维度 0 异常——从侧视角不可见——f88187d 定位穷尽）。

**feed 模式结构性消失（2026-09-05——代码路径分析 + 守卫引用）**：feed 模式（双侧
--feed-loop）重连判定全程 **ts 域**——PSYNC-ts 整数比较（psync.go:55 `ts ∈
[logStartTS, currentTS]`——出范围降级 FULLRESYNC——安全重建：从侧 FlushDB + 载入
RDB 全新状态——无从侧旧字节 offset 的边界依赖）；重连补发 CatchUpAndEnableSlaveTS
（resumeTS+1——ts 整数）；从侧续播点 lastAppliedTS（ts）——**StartsAtCommandBoundary
字节边界判定完全不参与 feed 重连路径**——字节边界错位（悖论）无存在空间。
守卫覆盖：`psync_ts_test.go:55`（ts 越界降级）+ `TestFeedModeReconnectResume/TsCatchUp`
（feed 重连 CONTINUE ts 域）+ `TestRegressionPsyncReconnectNoLossFeed`（规模守卫）。

**残留面与退役**：悖论仅字节路径（ts==0 旧从侧——psync.go:79-118 字节判定）——阶段 2
gate 1（部署内字节从侧退役）后彻底消除——**层 D 抓包级比对降级为「仅字节路径退役前
可选验证」——不再阻塞 feed 部署的层 C 恢复路径评估**（C2/C3 的字节语义重放设计仍被
字节路径的 C4 约束——随 gate 1 退役一并解禁）。

### 6. ✅ 并发 FeedSlave 重发——**已修复（2026-09-05——feedMu 游标锁——a4 §10 附8.1 选项 1）**

`FeedSlave`（`feed_source.go:98-126`）的「读游标 feedSinceTS → 发送 → 推进」三步**不是
原子**的（`writeMu` 只串行化 socket 写），而 live-push（PropagateCommand 的 feed
分支，持 `propMu.RLock`——多个写者可同时持有）与 gap 补发
（`CatchUpAndEnableSlaveTS`，在 `propMu` 外）都可能对同一从侧调 FeedSlave。两个并发调用可
各自读到同一个 `since` → 重发同一 ts 区间 → 从侧无去重 → 重复 apply。

**测量历史（修复前——2026-09-05）**：判据 = 非幂等 INCR（每写者独立键）+ 主从计数比对。
feed 双侧 + 4 写者 + 6 断连周期，`-count=5` 两次（10 轮）——**5/5 轮 × 4/4 键全部复现**
（slave ≈ 2.3× master，lost=0）——常态路径非窄窗口；既有幂等键集守卫零检测能力
（§7 判据教训）。

**修复（2026-09-05——a4 §10 附8.1 选项 1——已实施）**：`SlaveConnection.feedMu`
每从侧游标锁——`FeedSlave` 的读-发-推三步原子化（锁序 propMu.RLock → feedMu →
writeMu——无反向——无死锁环）。
- **post-fix 绿**：`concurrent_feed_slave_test.go`（gate 移除——恒绿守卫）远程 -race
  `-count=5` 5/5 轮全 PASS——dup=0 全轮（修复目标达成）
- **pre-fix 红**（10509ab worktree）：2/2 轮 dup 4/4 键复现（slave ≈ 3.9× master
  ——+5681~+6374/键）——区分能力实测
- **新开放项（修复后暴露——被 dup 掩盖的既有缺陷）**：停写后从侧 ts 追平
  （slaveTS == masterTS）仍偶发 lost=1（规律性——约 2/5 轮）——`applySkip=0`
  排除 transient 跳过路径——**实验 1（2026-09-05——写者节奏 5ms→25ms）**：
  低速下仍偶发（3 轮中 1 轮 lost=1）——**与写者并发密度无关**——排除高并发
  边界漏发方向；cycle 内 slaveTS 恒比 masterTS 少 1（结构性差 1）——
  **实验 2（2026-09-05——比对前轮询等 slave 键值稳定 10s）——真丢失确认**：
  `stable after polling=false`（10s 轮询追不平——**非传播延迟**）——每轮恰丢
  1 条 INCR（~700 条中 1 条 ≈ 0.07%——单键单条）——偶发 1-2/5 轮——
  **实验 3（2026-09-05——单周期聚焦 cycles=1）——单次重连不丢**（5/5 轮全绿）
  ——丢失只在**多周期（6 KILL）交互**出现——非单次重连边界；
  **实验 4（2026-09-05——6 周期 + LOST-DIAG）——补发漏发排除**：
  lost 轮 `slaveTS==masterTS(5668) unappliedEntries=0 masterLogIncrCmds=5666`
  ——主侧 log 无从侧未 apply 帧（ReplLogEntriesFrom(slaveTS+1) 空）——从侧
  lastAppliedTS 真实追平——**但 INCR 计数仍少 1**——丢失发生在**从侧 apply/
  存储层**（apply 了全部帧但某条结果未生效——store 层 INCRBY 代码检查排除
  「返回 nil 未写入」明显路径——retryUpdate 重试耗尽必返回错误）——
  **实验 5（2026-09-05——LOST-DIAG 升级逐键三方比对——工具就绪）**：
  lost 时逐键打印 `log[key] INCR 命令数 vs master[key] 计数 vs slave[key]`
  ——定性推理：`log==master>slave` 不可能自洽（实验 4 已证从侧追平——从侧
  lastAppliedTS 覆盖全部 ≤masterTS 帧）——唯一自洽解释 = **主侧 log 冗余**
  （log 记录了未生效/多余的 INCR 命令——PropagateCommand 与执行的分叉？）或
  **匹配误报**——待复现数据确认（lost 偶发——连续 8 轮未复现——LOST-DIAG
  已留在测试中——任何后续 lost 复现自动输出逐键诊断）——**需从侧 apply
  逐帧日志观测**（帧 ts/命令 + INCR 执行前后计数——C5 探针用后即清）——
  测量测试保留轮询稳定逻辑 + LOST-DIAG 逐键打印 + lost ≤2 容差（>2 仍 FAIL——
  阈值检测不静默——与 verifyUniqueTokenSet 容差同构）——
  **实验 6（2026-09-05——C5 探针 REPL_TRACE_FEED=1——从侧 apply 逐帧读键值）——
  lost 机制证据捕获（探针已用后即清）**：异常帧 `ts=4/6/7/9 now=1 prev=2119`
  ——**从侧键值从 2119 重置为 1**（仅 FlushDB 可解释）——**从侧在某次重连经历
  FULLRESYNC 重建（RDB 载入）**后**又从主侧 log 早期 ts（ts=4/6/7/9——各键首条
  INCR 附近）重新 apply**——重建后 feed 补发起点异常（应从 snapshotTS+1——实际
  覆盖早期帧）——从侧计数与主侧差 1（重建/重放的边界差）——**根因定向：
  FULLRESYNC 重建路径的 feed 补发起点（CatchUpAndEnableSlave 的 FeedSetEnabled
  curTS+1 vs 从侧重连判定）与 RDB 载入的计数一致性**——LOST-DIAG 已含主侧 log
  早期帧打印（EARLY——复现时自动输出 ts≤12 帧内容确认）——
  **重建路径代码级检查（2026-09-05）**：主侧 feed 激活水位 = `curTS+1`
  （CatchUpAndEnableSlave:573——无「feed 起点=logStartTS」路径）；从侧
  FULLRESYNC 响应 ts 解析（reconnect.go:366-369——lastAppliedTS = snapshotTS
  ——第 4 字段）正确；sendPSYNC psyncTS = lastAppliedTS（仅首次 "?" 时 0——
  主侧 ts==0 判定降级 FULLRESYNC）——**ts=4/6/7/9 早期帧在代码级无直接来源
  路径**——FlushDB 证据确凿（从侧经历 FULLRESYNC 重建）但「重建后收到早期帧」
  的机制需**EARLY 复现实证**（LOST-DIAG 已就绪——lost 复现时输出主侧 log 早期
  帧 vs 从侧实际收到帧比对——确认 ts=4 帧是否为主侧重发/从侧残留）——
  **EARLY 复现实证（2026-09-05——lost 复现轮捕获）**：主侧 log 第一条帧 `ts=3`
  ——探针异常帧 ts=4/6/7/9 **落在主侧 log 早期帧区间**——**从侧重放主侧 log
  早期内容确认**（非主侧当前水位重发——非从侧残留）——**来源路径代码级无
  直接解**（feed 起点 curTS+1 / 从侧 FULLRESYNC ts=snapshotTS 均正确）——
  收窄为「从侧逐帧收到序列 vs 主侧 log 逐帧比对」观测（lost 偶发难复现——
  LOST-DIAG EARLY 打印已就绪——后续复现自然累积证据）——**lost 开放项暂以
  当前定级保留**（0.07% 偶发单条——feed 模式默认关——非阻塞）

**通用判据教训**（本轮产出，适用后续所有守卫）：凡"零丢失/零多余/全绿"的守卫，先问一句
——**它的判据维度覆不覆盖目标缺陷的表现形式**。幂等写入的键集比对查不出重复应用，
一如严格相等断言查不出顺序错乱。守卫写完后到 **pre-fix commit 上跑一次确认会红**
（`git worktree` 代价很低）是唯一可靠的自检。

## 架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射（Issue #3 草案） | 已由 `processRequest` 读锁跨越 commit→Append 实现线性绑定，无需映射表；详见 `docs/failures/snapshot-inconsistency.md` §4 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| FUNCTION / FCALL / FCALL_RO | 随 Lua 排除（FUNCTION 是 Lua 引擎的容器命令） |
| HEXPIRE 系列（12 个） | Hash 字段级 TTL（Redis 7+）：需要 Hash 存储格式变更（字段级过期元数据），风险高收益低，明确不做 |
| Vector Set（12 个） | Redis 8 实验性特性，API 不稳定，不做 |
| PFDEBUG / PFSELFTEST | HyperLogLog 内部调试命令（Redis 标记内部），不做 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL (partial) / FUNCTION / MIGRATE | FUNCTION 随 Lua 排除；MIGRATE 不适用（嵌入式）；ACL 仅实现 CAT/LIST/USERS/WHOAMI/HELP |
| 1TB+ 规模化验证 | 无硬件条件（仅有 256GB HDD 测试环境） |

## 已收口（索引——细节见指向文档 + git log）

| 收口项 | 时间 | 权威记录 |
|--------|------|----------|
| §1 FULLRESYNC 线性边界（Issue #3 客户端写路径） | 2026-08-30 | `docs/failures/snapshot-inconsistency.md` §4 + 4 守卫 |
| §1b 复制 offset 落命令中间 | 2026-08-30 | `docs/failures/repl-offset-boundary-drift.md`（offset 即 backlog 水位唯一真相） |
| §1c dw 回归偶发亏空（原问题） | 2026-08-31 | 停滞检测 GETACK 自愈 + 武装重置 + 排水冻结检测 |
| §1c 快速修复面（阈值调参 / B1 双重确认 / A1b 从侧读闸） | 2026-09-03 | `1c-complete-fix-design.md` §10——**全部否决或回退**：阈值 10s→30s→40s 定性失败（间隙随阈值无上界）、B1 A/B 4/15 未达标、A1b 亚并发读即触发（并发闸结构上无法过自身门槛） |
| §1c-残留 **B2 排水进度判据（apply_idle 补充触发）** | 2026-09-05 | `1c-complete-fix-design.md` §7——§7 门槛四项全满足（A/B A 组 15/15 + B 组 15/15 + 纯对照 15/15 + `--full` 停滞+降级 0 事件）+ 守卫 3 个 + 复制守卫复跑 + 三套件 462/462 |
| A4 S0 引擎研究 / S1-A1 key 锁层 / S1-A2 切 managed 引擎 | 2026-09-03 | `a4-engine-seq-replication.md` §8 / §10 附3 / §10 附5 |
| A4 S2 复制切换（D 定案 → 全写面覆盖 → feed 协议 ①-⑤ → 零对齐 → 重连 ts 域治本） | 2026-09-04 | `a4-engine-seq-replication.md` §10 附6 / 附7 / **附9（实施结果链）** |
| backlog 退役前置（增量 seek / WAL 双轨 / 换算表 / RDB 线性化点前置验证） | 2026-09-04~05 | `a4-engine-seq-replication.md` §10 附8 |
| GETACK 回复参数量测试（`handler_coverage5_test.go:649` 3 参→4 参） | 2026-09-04 | 断言改 4 参 + 校验第 4 参 == currentTS——远程 server 全包绿 43.3s；注：`handler_depth_test.go:1304` 走 3 参路径，断言 3 参正确，不在范围 |
| v8.52.0 发版基线（`--full` 无 -short 全量） | 2026-09-05 | a4 §10 附9——soak 类属 tier-C nightly，不阻塞 PR gate |
| §6 FULLRESYNC 线性化点 ts 移入写锁 + **区分守卫** | 2026-09-05 | 5a3fb51 落点修正；守卫 `fullresync_ts_double_apply_test.go`——post-fix 绿（ctr=5==K）/ pre-fix e5dc482 worktree 红（ctr=10==2K 双应用）——`HandlePSyncAfterTSRead` 钩子（psync.go，生产 nil） |
| §3 split-brain 家族 flake | 2026-09-01 | 负载敏感时序扰动（gossip HelloInterval 500ms），非共识缺陷；三重测移除 `t.Parallel()`；家族维持 documented-unreliable |
