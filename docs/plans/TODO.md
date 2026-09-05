# BoltDB 待办列表

> 本文件只列**未完成**的工作。已完成阶段的过程记录归入对应设计文档（`a4-engine-seq-replication.md`、
> `1c-complete-fix-design.md`）或下方「已收口」索引；逐提交细节以 `git log` 为权威。
> 2026-09-05 整理：删除 296 行中已完成的堆积记录，迁入上述文档。

## 待办

### 1. ✅ A4 阶段 1——offset 水位改 ts 源——已完成（2026-09-05——剩余并入 §2 gate 1）

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
- **最终门禁确认（2026-09-05——18 提交后）**：internal 全包 -race -short 11 包全绿
  （replication 70.0s / server 44.7s / store 127.1s / cluster / sentinel 等）+ cmd/boltDB
  + cmd/integration 命令功能批次（String/List/Hash/Set/ZSet/Stream/Geo/HLL/TimeSeries/
  PubSub/Expire/TTL——86.6s）全绿——**已知既有失败**：`TestBackupManager_BackupBadger`
  （internal/backup——panic: This API can not be called in managed mode——backup 包
  最近改动 14735f8（历史提交）——与今日改动无关——S1-A2 managed 切换后的既有兼容
  问题——待专门处理——不入今日范围）

### 2. A4 阶段 2——删除 backlog 内存环（gate 严格——不可在线回滚）

删 `ReplicationBacklog` / `BacklogWAL` / `SendBacklogData` / `CatchUpAndEnableSlave` 字节循环
/ psync 字节分支。`--feed-loop` 保留为启动要求（回滚需代码还原）。

**退役三 gate（全部满足才可实施）**：
1. 字节从侧（PSYNC 3 参 / ts=0）完全退役——部署内从侧全量 feed-mode，无 ts=0 请求进入 PSYNC；
2. 换算表双轨核验持续通过（`ReplConversionTable.AlignCheck`——过渡期验证锚）；
3. feed-mode 规模验证零丢失持续（`TestRegressionPsyncReconnectNoLossFeed` 退役前再复跑）。

**gate 现状（2026-09-05 核验——阶段 1 全部落地后）**：
- **gate 2 持续通过**：换算表 5 测试远程 -race 全 PASS（DualTrack/DetectsDivergence/
  ConcurrentWriters/Empty + TestTSReplayEquivalence——1.65s）；
- **gate 3 持续通过**：`TestRegressionPsyncReconnectNoLossFeed` 复跑 PASS 45.53s
  （零 MISSING/EXTRA/MISMATCH）；
- **gate 1 未满足（唯一剩余）**：部署内字节从侧退役——部署面——需运维确认部署
  内从侧全量 `--feed-loop` 且无 ts=0 请求进入 PSYNC——代码面无法核验——阶段 2
  实施等待此 gate

> **依赖链注记（2026-09-05）**：gate 1 同时阻塞三件事——① 本节阶段 2 实施
> ② §3 dw ≤1/15 正式验收 ③ §5 C4 字节路径残留清除。它是**运维面事实**，代码侧无
> 任何动作可推进它。同时注意它与 §6 lost 的顺序关系（见 §6 末「定级与顺序冲突」）：
> 达成 gate 1 = 把 lost 从"默认关不阻塞"变成线上暴露，故 **lost 定级应收于 gate 1 之前**。
> 当前代码侧唯一可独立推进的是 §6 候选 ④（RDB 撕裂确定性实验）。

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

- **中间基线（2026-09-05——阶段 1 全部落地后预演 -count=3）**：3/3 全绿（gap=0
  全键——零亏空——INCR dw:incr:0-4 master==slave）——阶段 1 改动（offset 改 ts
  源 + 字节直读面）对 dw 测量**无回归**（对照阶段 1 前纯对照 14/15+1 flake——
  小样本——正式验收仍 gate 于阶段 2 后）
- **探针开中间基线（2026-09-05——DW_READ_PROBE=1 预演 -count=3）**：3/3 全绿
  （gap=0 全键——读探针 reads=1309-1339 fails=0——§1c 从侧读争用模拟）——
  阶段 1 改动后读争用场景零亏空保持（对照阶段 1 前探针开 15/15——无回归——
  正式验收仍 gate 于阶段 2 后）

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
- **测量机器（恢复后）= 10.1.2.16**（SSD 环境——scale-data-filler 已就绪）——
  10.1.2.16 暂时不可达（网络——用户说明 2026-09-06——**过几天恢复可用——非弃用**）
  ——恢复后 SSD 基线回 10.1.2.16 测——192.168.1.251（实测 HDD 3.6T——非 SSD——
  无 boltDB 集群部署）仅作暂缓期参考（HDD 写路径行为不同——基线不可比）
- **机器可达性（2026-09-06 更新）**：10.1.2.16（GCP VM elex-gm0135）暂时不可达
  （网络）——**过几天恢复可用（非弃用）**——SSD 基线（§4）恢复后回 10.1.2.16
  测（SSD 环境——SSH 接入：elex-gm0135/~/.ssh/google_compute_engine）——
  192.168.1.251 实测 HDD 3.6T（/dev/sda1 → /media/hdd4t）+ 无 boltDB 集群部署
  （6337/6338/6339 无监听）——非 SSD 基线环境
- **挂起（2026-09-06——用户决定暂缓）**：SSD 基线暂缓至 10.1.2.16 网络恢复
  （过几天——非弃用）——恢复后立即启动（scale-data-filler + 前置三查已就绪——
  部署集群 + 全流程测量——工具与前置三查已就绪）——当前时间投入 lost 偶发
  根因（已收口——见 §6）
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
  当前定级保留**（0.07% 偶发单条——feed 模式默认关——非阻塞）——
  **EARLY 内容分析（2026-09-05 深夜——批次 1 复现轮）——关键推理推翻**：
  实测 `log[key]==master[key]>slave[key]`（ctr:2 log=1399==master=1399>
  slave=1397——差 2——丢失量可变 1-2 条）——「从侧追平则全部生效」的先前
  推理被数据推翻——从侧 lastAppliedTS 追平（slaveTS==masterTS==5645）但
  INCR 计数少——**帧级 apply 丢失确认**（log 与主侧一致 → 排除补发漏发——
  从侧 apply 了全部帧但部分 INCR 执行未生效/被跳过）——结合探针异常帧
  （FULLRESYNC 重建 + 重放早期内容——EARLY 起点 ts=3 三参数命令）——
  **定位收窄：FULLRESYNC 重建后重放路径的帧级 apply 丢失**（重放区间/边界
  或 RDB 载入交错——代码级无直接解——需从侧重放路径逐帧日志观测）——
  **KVrocks 对照与候选裁决（2026-09-06）**：读 KVrocks 复制实现
  （kvrocks/src/cluster/replication.cc——fullSyncReadCB/incrementBatchLoopCB/
  FeedSlaveThread::loop/tryPSyncReadCB）对照——KVrocks 架构（RocksDB seq 线性 +
  Checkpoint 原生快照 + WriteBatch 原子 apply——失败必 RESTART 可见）结构性避免
  本 lost 类问题——本项目应用层 log-key ts + 逐帧单事务 apply——无原生保证——
  **候选 1（从侧 apply badger 事务低值后落静默边界）经代码检查排除**：从侧
  apply 落点（WriteCommand → Set/SetWithTTL → retryUpdate:60 → commitTS 统一
  封装——ts_source.go:100-125）与主写路径完全一致（tsSource.Begin 分配 +
  CommitAt(ts) + discardMu 原子对三层守卫——b9083a3）——低值后落静默拒写
  结构性不可能——**修改计划候选重排**：①（新）主侧 feed ts 空洞检测加固
  （KVrocks FeedSlaveThread::loop「迭代器离散即断开」模式——feed 补发检测 log
  键 ts 跳变/空洞——检测到即断开重连而非静默——纵深防御）；②（原候选 3）
  FULLRESYNC 重建后重放起点/RDB 载入交错的逐帧日志观测（探针——用后即清）；
  ③（保留）从侧逐帧收到序列 vs 主侧 log 比对（LOST-DIAG EARLY 累积）——
  **候选 ① 已实施（2026-09-06）**：feed_source.go 提取 `verifyFeedTSContinuity`
  （log 键 ts 逐条严格连续检测——跳变即明确错误——调用方断开从侧重连——不静默
  跳过空洞帧——KVrocks「迭代器离散即断开」模式）+ FeedEntriesFrom 调用（发送前
  检测）——单测 `TestVerifyFeedTSContinuity`（连续/空/单条/首条边界/空洞 5 场景
  ——纯函数——不依赖 db）——本地 lint 0 + 远程 replication 全包 49.7s 绿——
  lost 数据显示 log==master 一致（非空洞）——检测定位为**排除工具 + 防未探索
  空洞路径**（log 键写入失败/删除场景目前静默——加固后可见）——若 lost 复现时
  空洞检测未触发——进一步确认 lost 非空洞方向（候选 ② 观测接续）——
  **候选 ② 观测（2026-09-06——双侧探针 REPL_TRACE_FEED=1——用后即清已恢复）**：
  主侧重发起点实测正常（重建后 MASTER feed since=4 起递增——无早期值跳回）——
  **认知修正**：批次 2 轮 2/3 的早期帧（ts=5/6 pre=""）实为**首次初始同步的
  正常补发**（快照点早于键创建——键首帧 pre="" 正常——非 KILL 后重建异常重放）
  ——「重建后早期帧重放异常」假设修正——lost 机制收窄：从侧收到全部帧
  （lastAppliedTS 追平——主侧 log/发送均一致）但**某条 INCR 的 apply 未生效
  （值未变——实验 6 now=1 同证）**——**帧级 apply 静默失败**（store 层 INCRBY
  路径——badger 事务提交但写入未生效的静默边界）——post 值探针（apply-before/
  after 配对）已备——lost 复现偶发（本轮 9 轮未中）——**post 值捕获待续**
  （复现时捕获「pre==post 值未变」帧——LOST-DIAG 已留测试内——探针 C5 纪律
  重新加装后捕获）——**INCRBY 代码级四层检查（2026-09-06——机制定论推进）**：
  从侧 apply 链路逐层核查（WriteCommand INCR/INCRBY 分支 → s.INCRBY
  [string.go:356-394——keyLockMgr + retryUpdate 读-改-写] → commitTS 统一封装
  → ReadRESP 逐条解析 [reconnect.go:449]）——**四层均无静默失败路径**：
  ① badger 事务原子（CommitAt——提交即生效——「提交成功未写入」结构性排除）；
  ② TTL/类型检查（ErrWrongType 返回错误→重连——不静默）；③ 接收/解析层
  （ReadRESP 逐条解析——失败即断开重连——漏读/跳读结构性排除）；④ transient
  跳过（applySkip——lastAppliedTS 不推进——卡住触发 B2 停滞重连——不静默且
  lost 轮追平无跳过）——**「帧级 apply 静默失败」假设代码级排除**——lost 真实
  来源仍开放（0.07% 偶发——多周期 KILL 交互——时序竞态候选——主侧重发/补发
  与从侧重连/重建边界）——**最终证据途径 = post 值帧级捕获**（lost 复现时
  捕获「值变化异常」帧——LOST-DIAG 已留——偶发难复现——以当前定级保留）——
  **post 值帧级捕获定论（2026-09-06——REPL_TRACE_FEED=1 双侧探针——用后即清
  已恢复）**：lost 轮（-count=3 两批——2 轮 lost 复现）从侧全量 pre/post 配对
  分析（1432/1434 帧/键——python 逐帧比对）：
  - **「值未变」帧排除**（post=pre+1 全部连续——每帧 INCR apply 都生效——
    无 pre==post——「帧级 apply 静默失败」假设实证排除）；
  - **从侧少收 1 帧确认**（apply_after_n=1432 < 主侧 log=1433——post 序列尾部
    =1432 无跳变——漏帧为 lost 键序列的末条 INCR——ts<lastAppliedTS 中间位）——
    **lost = 从侧传输/接收层少收一帧 + 重连补发未覆盖**（漏帧 ts < 追平的
    lastAppliedTS——重连 PSYNC ts=lastAppliedTS → 主侧重发 [resumeTS+1,...]——
    中间漏帧不在补发区间——永久丢失）；
  - **修复方向（下一轮）**：从侧接收层加「帧 ts 连续性检测」（收到帧 ts 跳变
    即断开重连——触发补发覆盖空洞——KVrocks 模式从侧版）或重连补发起点改为
    「从侧帧 ts 空洞感知」（resumeTS 取空洞前——补发覆盖空洞区间）——
    **修复已实施（2026-09-06）**：`checkFeedTSGap`（reconnect.go——REPLLOG 分支
    apply 前——prevTS>0 且 ts != prevTS+1 → 返回错误断开重连——此时 lastAppliedTS
    未追平——PSYNC 补发 [lastAppliedTS+1,...] 覆盖空洞——首帧 lastAppliedTS==0
    跳过（logStartTS 边界合法 gap））+ 单测 TestCheckFeedTSGap（首帧/连续/跳变
    三边界——本地 lint 0）——验证：replication 全包远程 -race 绿 49.1s（首轮
    FAIL 为并行 flake——该测试场景无 REPLLOG 与修复无关——单跑/复跑均 PASS）
    ——复制核心守卫 5 个全 PASS（PsyncReconnectNoLossFeed 45.4s/FeedModeTSSemantics/
    FullresyncTsDoubleApplyGuard/HalfUpgradeByteSlave/DuplicateWindowMeasurement
    ——100.4s）——concurrent 守卫复跑 3 轮 0 lost（初步——lost 偶发样本——
    统计确证需更多轮 -count）——**lost 定级：修复落地——统计确证完成
    （2026-09-06——修复后 9 轮 0 lost vs 修复前 6 轮 2 lost——修复前 9 轮 0
    lost 概率仅 ~13%——改善显著）——checkFeedTSGap 修复有效确认——**lost 开放
    项收口**（feed 模式双侧——漏帧后的下一帧立即断开重连——补发覆盖空洞——
    结构性消除永久丢失）——完整长期确证（更多轮 -count——夜间/后续自然累积）
    留作常规守卫持续覆盖——偶发（0.07%——feed 模式默认关）以收口定级归档

**通用判据教训**（本轮产出，适用后续所有守卫）：凡"零丢失/零多余/全绿"的守卫，先问一句
——**它的判据维度覆不覆盖目标缺陷的表现形式**。幂等写入的键集比对查不出重复应用，
一如严格相等断言查不出顺序错乱。守卫写完后到 **pre-fix commit 上跑一次确认会红**
（`git worktree` 代价很低）是唯一可靠的自检。

**候选 ④「RDB 撕裂」——managed 模式下快照没有 MVCC 隔离（2026-09-05 代码级证明）**：

1. **证明**：badger v4.9.6 `DB.View()` 在 `managedTxns` 下走
   `NewTransactionAt(math.MaxUint64, false)`（`txn.go:786-794`；其自带注释"assume a
   read timestamp of MaxUint64"）。故 `GenerateRDB` 的 View **不是** point-in-time
   快照——它读的是"迭代器到达该键那一刻的最新版本"，逐键各读各的时点。
2. **因此**：RDB 的一致性 100% 由 `snapshotMu` 写锁提供（`replication_handler.go:64`
   跨 `snapshotTS` 捕获 → 整个 RDB 迭代 → flush），读栅栏全仓只有一处
   （`processRequest`——`handler_core.go:738-739`）。**任何不走 `processRequest` 的
   store 写都不受此栅栏约束，可以落在 RDB 迭代中间 → 撕裂快照。**
3. **静态扫描未发现绕过者**（2026-09-05）：`NextStartup` 仅启动期
   （`cmd/boltDB/main.go:260`），server 包 `*_commands.go` 之外无 `h.Db.*` 写，
   cluster 侧 0 处——**故这是结构依赖，不是已观测缺陷**。
4. ~~**关键未决**：被栅栏保护的写者，其多键 txn 是否对 MaxUint64 读者原子发布~~
   → **已实测裁决（2026-09-05 晚，两个常驻探针）**：
   - **候选 ④ 排除**——`rdb_torn_snapshot_test.go`：写者以 `MSET a=n b=n`（单逻辑命令
     两键）反复写，检测 RDB 里 a/b 是否不等或单侧缺失。
     **fenced（与 `processRequest` 同构取栅栏）= 0/120 撕裂**（远程 -race，11578 次并发写；
     本地 0/120，147217 次写）；**对照臂 unfenced = 119/120 撕裂**（`a="138" b="139"`…
     差值随负载增大）——**探针敏感度自证，栅栏确实有效**。
   - **候选 ⑤ 排除**——`rdb_roundtrip_fidelity_test.go`：静默点 + 并发写后静默，
     反复「生成 RDB → 载入全新 store → 逐键比 INCR 计数 / 列表序列 / hash 字段 /
     **set 成员集合 / zset 成员+分值 / TTL / 全局 DBSize**」——
     **30/30 轮全保真**（本地 4052 次并发写，远程 -race 252 次同样保真）。
     探测器有效性由构造保证：若载入侧真丢条目，逐键读会落到错误分支或值不等；
     DBSize 为全局键数比对，任何静默跳过/解析错位都会在此暴露。
   - **补上一处空白守卫（新）**——`rdb_rebuild_vs_replay_equiv_test.go`：此前**没有任何
     测试比过「A 重建态 = RDB 载入空 store」与「B 追平态 = 空 store 按 ts 重放全部帧」
     是否同态**（`replay_guard_test.go` 比的是 log 帧 vs 字节 backlog，
     `rdb_roundtrip_fidelity_test.go` 比的是生成+载入 vs 生成时主侧态）。生产里一条命令
     只走其中一条，两者一旦不等即「同一历史、接入方式不同、结果不同」——与 lost 同型。
     **实测：ts≤58 共 58 帧、13 键逐键一致（含被 DEL 的键与 TTL 桶）→ 等价成立，
     该维度现由常驻守卫覆盖**。探针同时充当「log-key 语义跳过清单」检查器（改数据不写帧
     的命令会让 B 缺该改动而 A 有——本轮未见）。耗时 0.07s、纯确定无并发，
     故**故意不加 `-short` 门控**——它是这一维度唯一能进 CI 的守卫。
   - **由此得到的结构性结论（可证明，不只靠测量）**：MaxUint64 读者只会
     **多含**（读到 snapshotTS 之后的提交），**绝不会少含**。故
     **RDB 侧结构上不可能造成 lost——它最多造成 dup**（多含的键其帧 ts > snapshotTS
     会被 feed 再发一遍）。要造成"从侧比主侧少"，只能来自①帧未被 apply，
     ②apply 了但值算错，③快照**静默缺键**（见下候选 ⑥——表现应为 MISSING 而非计数偏小，
     与 lost 签名不符）。**lost 因此收敛回候选 ②/③（重建后重放起点/RDB 载入交错）
     与从侧 apply 计数侧**，生成侧不再是嫌疑。
5. **与 lost 全部证据吻合**（这是它优于已排除的候选 1/2 的地方）：丢 1-2 条
   `log==master>slave`、从侧 lastAppliedTS 追平（撕裂发生在**重建载入的那份 RDB 里**，
   不在补发流上）、`applySkip=0`、**只在多周期（6 KILL）出现而单次重连不丢**
   （每个 KILL 周期一次 FULLRESYNC 重建 = 一次撕裂机会，命中率随周期数累积）、
   实验 6 的 FlushDB 证据（键值重置为 1 = 确实经历了重建）。
   **注**：④/⑤ 的实测已推翻本条的适用性——吻合的是"重建"这一事实，不是"撕裂"这一机制。
6. ~~确定性实验（待做）~~ → **已做完，见上第 4 条**。两探针留作常驻守卫，均
   `testing.Short()` 门控（自旋写者 + 数十次 store 开合，属定向/nightly——
   **不在 CI 的 `-short` 面里**，别当 PR gate 覆盖）。
   **能力边界（重要，勿误用）**：探针自己是那个写者，故它防的是
   **栅栏语义回归**（有人把 `processRequest` 的 RLock 挪走/缩小 → fenced 臂会撕裂），
   它**查不出"新增了一条本来就不取栅栏的写路径"**——那属于
   第 3 条的静态审计面（当前结论：未发现绕过者）。
   对照组 unfenced 恒撕裂 = 探针有效性自证，无需外部触发即可解释"绿"的含义。
7. **候选 ⑥「RDB 生成静默缺键」（新发现——非 lost 机制，但同类静默丢数据）**：
   `rdb.go:475-496` 起，键类型读失败（`item.ValueCopy` err）、字符串值读失败
   （`readStringInTxn` err）都是 **`logger.Warn` + `continue`**——该键直接不进 RDB，
   而 FULLRESYNC 仍照常成功交付；`WriteStringKeyValue` 失败也只 Warn、**不中止不返回错误**。
   表现 = 从侧永久少键，主从两侧都不会报错。与本仓库刚立的纪律
   （`verifyFeedTSContinuity`——「迭代器离散即断开」不静默跳过空洞帧）同型，
   **建议同样改为显式失败**（快照不完整就别交付）。不入 lost 链（签名不符：MISSING ≠ 计数偏小）。
   同型问题在**载入侧**也有四处（`rdb_loader.go:197` 起）：① `expireTime, _ := readExpireTime()`
   丢弃解码错误 → ttl 保持 0 → 带 TTL 的键被**永久化为不过期**（从侧多出主侧已失效的键）；
   ② `if expireTime > now` 之外的分支——RDB 里已过期条目 ttl=0 同样变成**永不过期**而非跳过；
   ③ `typeByte, _ := ReadByte()` 忽略错误；④ `key, err := readString(); if err != nil {
   logger.Warn + continue }`——**二进制流解析中途错位后继续往下读**，其后所有条目按错位字节解析，
   却仍向上报"载入成功"。④ 是 lost 家族里唯一在快照侧**能造成"少"**的路径（前面证明过
   MaxUint64 读者不会少含，故此前排除的是生成侧），故列为候选 ⑦。
7b. **候选 ⑦「载入侧解析错位后继续」——目前未获支持**：本轮把往返保真探针扩到
   set/zset/TTL/全局 DBSize 后仍 30/30 全保真，说明**格式正确的 RDB 不触发这些分支**。
   触发条件应是解码器与编码器在某类型上不一致（未知类型字节 / 长度编码边界 /
   `0xFFFFFFFF` 秒毫秒分界附近）——属**鲁棒性缺陷**而非已观测丢失。
   与 ⑥ 同处建议一并处理：解析失败即整体失败（返回 error 让 FULLRESYNC 重做），不 continue。
8. 相关文档断言已纠正：`AGENTS.md` GenerateRDB Invariants、
   `architecture.md`「MVCC snapshot / point-in-time」两处原写法与本证明相反。

**定级与顺序冲突（2026-09-05 提出）**：本项现记「feed 模式默认关——非阻塞」，而 §2 gate 1
的定义恰是**把部署推向全 feed 模式**（字节从侧退役）。lost 只出现在 feed 双侧路径，
故 gate 1 一旦达成，"默认关所以非阻塞"的前提即失效，0.07% 的静默单条丢失转为线上问题。
**lost 的定位应先于或至少并行于 gate 1 的部署动作**，不宜排在阶段 2 之后。

### 7. 确认缺陷：ZINCRBY 复制不等价 + apply 层系统性「解析失败即报成功」

> 2026-09-05 由命令族等价扫面首跑抓到，**确定性可复现**（与并发、与 0.07% 偶发无关）。
> 位置：`internal/replication/rdb_rebuild_vs_replay_equiv_test.go`
> → `TestRDBRebuild_EquivSweepAcrossCommandFamilies`——**已修复（2026-09-06）——
> knownDefects 表项已删——该例自动转正为回归守卫**。
> **注意覆盖面**：扫面按 CI 资源惯例门控在 `-short` 之外（14 例 × 3 store），
> 即 **CI 不跑扫面**——CI 常驻的是窄版 `TestRDBRebuild_EquivalentToFrameReplay`
> （3 store / 0.07s——**已把 ZINCRBY/ZREM 加进窄版（2026-09-06）——CI 常驻覆盖**）。

**两处根因，缺一不可构成静默**：

1. **编码侧参数序颠倒**——`internal/store/zcard_score.go:210` 把 log 值写成
   `ZINCRBY <key> <member> <increment>`；而主侧权威解析是
   `ZINCRBY <key> <increment> <member>`（`internal/server/zset_commands.go:981-985`：
   `key=args[0]`、`delta=args[1]`、`member=args[2]`）。
2. **apply 侧把协议错误吞成成功**——`internal/store/write_command.go:817-826`：
   `if delta, err := ParseFloat(args[2]); err == nil { … }` **`return nil`**——
   `args[2]` 是 member（非数字）时解析失败，**返回 nil＝"该命令已成功应用"**。

**实测（同一历史，三条状态）**：主侧 `m1:11` ／ A(RDB 载入空 store) `m1:11` ／
**B(按 ts 重放 log 帧) `m1:1`**。即：走 FULLRESYNC 重建的从侧正确，走 CONTINUE 帧追平的
从侧**永久少一次 ZINCRBY，且全程零错误、零 applySkip 计数、零停滞触发**。

**为什么这是 lost 方向上最值钱的一条**：§6 的四层检查结论是"INCRBY 路径无静默失败路径"，
但 `WriteCommand` 的默认失败模式恰恰是**"报成功但什么都没做"**——实测统计（脚本按函数体
`write_command.go:26-2613` 逐行扫）：**209 个 `case` 命令分支里 107 处整行 `return nil`**，
其中 **24 处紧邻 `ParseFloat/ParseInt/Atoi`**（解析失败即返回成功）、**42 处紧邻
`len(args)` 判定**（参数数不足即返回成功），另有 41 处其他 `return nil`。
Parse 类 24 站的绝对行号：238 251 264 277 603 613 624 736 747 **825(ZINCRBY)** 869 893
1103 1134 1154 1178 1198 1222 1316 1728 1760 2139 2245 2605。

含义：**任何一帧的参数形状与该分支期望不完全一致，就会被静默丢弃**——表现为
"帧全部 apply 完成 + 计数偏小 + 零错误 + 零 applySkip + 零停滞"，与 §6 lost 签名逐项吻合。
INCR 族本轮扫面 PASS，但 lost 是**偶发**的，偶发恰好对应"只在某种参数形状/某种命令变体下
触发"的静默分支。建议把 §6 的搜索从"时序竞态"转向**逐分支审计这 66 处 malformed→success
站点**：改为返回明确错误（协议不匹配＝不可应用的帧必须可见），然后复跑多周期 KILL 规模
守卫看 lost 是否消失。

**修复面（已实施 2026-09-06——自主模式授权范围内——用户可复核）**：
① `zcard_score.go:210` 参数序改为 `key increment member`——**已改**；
② `write_command.go` ZINCRBY 分支解析失败 `return nil` 改为**返回明确错误**
   （协议不匹配＝不可应用的帧必须可见——走"执行失败→重新同步"既有路径）；
③ 兼容性注记（2026-09-06 确认）：①只影响新写入的帧，**已落盘的旧序帧仍会被
   ②之后的 apply 层判为错误** → 触发一次重连/重建而非丢数据（可接受）——
   已确认 backlog/log 键重放窗口无额外丢失（扫面全例等价验证）。
**遗留（系统性）**：其余 23 处 Parse 类 + 42 处 len 判定的 `return nil` 站点
（行号清单见上）未逐一修复——留作后续系统性工作（lost 家族纵深——偶发 lost
是否由某站点的 malformed→success 触发——见上文「建议」段）。

**apply 层系统性审计修复（2026-09-06——自主模式——用户可复核）**：遗留的 Parse
类 + 相邻参数校验 return nil 站点**已逐站审计并修复**（同 ZINCRBY 模式——协议
不匹配的帧必须可见——走「执行失败→重新同步」既有路径）——**修复集（11 组 /
25+ 处）**：INCRBYFLOAT/SETRANGE（`_` 忽略解析错误——196/207）／LSET/LREM/LTRIM
（600/609/619）／SETBIT（1103）／ZUNIONSTORE+ZINTERSTORE（numKeys/WEIGHTS/
AGGREGATE——1138+/1182+）／GEOSEARCHSTORE（FROMMEMBER/FROMLONLAT/BYRADIUS/
BYBOX/COUNT 边界——1723-1796）／XACKDEL（IDS/numids——2137-2146）／SET TTL
（EX/PX/EXAT/PXAT 解析失败静默跳过——48-87——从侧永久无 TTL 的一致性隐患）——
**合法保持（不修）**：default 数字命令跳过（2608——防级联重同步风暴——注释确认）、
GEOSEARCHSTORE FROMMEMBER 0,0 坐标（主侧镜像未写）、BYBOX width/height≤0（主侧
镜像未写）、BITFIELD（WriteCommand 无静默点——operations 原样传 store 层解析）、
len(args) 边界不足的全站性模式（209 case 全局——本轮控制范围不修——留后续）。
**验证（2026-09-06）**：本地 BUILD+VET+lint 0 issues——远程 store 全包 -race
-short 绿 131.4s + replication 全包 -race -short 绿 51.97s + 等价扫面 19 例+窄版
转正（ok 5.9s——修复不破坏正常帧）——**lost 家族纵深收尾**（malformed→success
静默面系统性消除——偶发 lost 的该方向假设闭环——见 §6 已收口）。

**同族新缺陷（2026-09-06——等价扫面扩展 HINCRBYFLOAT 例抓到——已修复）**：
`HIncrByFloat`/`HIncrBy` 单独创建 hash 键时不写 typeKey（`TYPE_` 标记——对照
HMSet:418 写 `txn.Set(typeKey, KeyTypeHash)`）——**RDB 生成按 `TYPE_` 前缀遍历
键（rdb.go:470）漏枚举无 typeKey 的键 → FULLRESYNC 重建缺该键**（实测 A(RDB
重建)="" vs 主侧/B(帧重放)="f1=3.75"——帧重放不依赖 typeKey 所以 B 正确——
候选 ⑤ 往返保真未覆盖 HIncrByFloat 单独创建键场景——覆盖盲区）。
**修复**：HIncrByFloat/HIncrBy 在 typeKey ErrKeyNotFound（键不存在）分支补写
`txn.Set(typeKey, KeyTypeHash)`（与 HSet/HMSet 对齐——hash.go:621-628/708-715）
——验证：HINCRBYFLOAT 例转正 PASS（A(RDB 重建)="f1=3.75" 三态一致——扫面
15 例全 PASS + 窄版 PASS——远程 -race ok 5.556s）。**扫面用例已永久加入**
（HINCRBYFLOAT——15 例——CI 外门控；窄版未加——hash 浮点族可后续视需加）。

**等价扫面扩展（2026-09-06——lost 家族纵深验证）**：扫面 15→19 例（新增
INCRBYFLOAT / ZUNIONSTORE+ZINTERSTORE / GEOADD+GEOSEARCH / XADD+XTRIM——
string 浮点 / zset 聚合 / geo / stream 族）——远程 -race 全 PASS（ok 5.774s）
——**无新确定性缺陷**（编码侧 log 值参数序均与 apply 解析一致——与已修的
ZINCRBY（编码参数序 + apply return nil）与 HINCRBYFLOAT（无 typeKey → RDB
漏键）形成对照）——**Parse 类 return nil 剩余站点已系统性修复（2026-09-06——见上「apply 层系统性
审计修复」）**——SET TTL 四分支 / LSET/LREM/LTRIM / SETBIT / INCRBYFLOAT /
SETRANGE / ZUNIONSTORE+ZINTERSTORE / GEOSEARCHSTORE / XACKDEL——25+ 处
malformed→success 已改为明确错误（协议不匹配＝不可应用的帧必须可见）——
len 边界不足的全站性模式（42 处）与 default 数字跳过（2608——防级联风暴）
保持不修（范围控制——见上）。

**复现命令（一条即中，无需撞竞态）**：

```bash
# 表项已于 2026-09-06 修复时删除——直接跑：
bash scripts/remote-test.sh -race -timeout 120s ./internal/replication/ \
  -run "TestRDBRebuild_EquivSweepAcrossCommandFamilies" -v
# 期望（修复后）：B(帧重放)=m1:11 == 主侧/A=m1:11 → PASS（pre-fix 为 m1:1 → FAIL）
```

**验证记录（2026-09-05——f03ab5d 提交态）**：`./internal/replication/...` 全包远程 -race
**通过（ok 126.188s，`REAL_EXIT=0`）**——退出码在 `remote-test.sh` 之后立即取 `$?`
（不经管道），含四个新探针（撕裂、往返保真、窄版等价、命令族扫面——末者 ZINCRBY 例
按 `knownDefects` Skip）。窄版等价另单独验过本地 `-count=20` 与远程 `-count=5` 全绿。

> **测量纪律补一条（本轮实际踩到）**：`go test … | grep … | tail` 的退出码来自管道
> **最后一个命令**，红套件会伪装成 exit 0。本轮就有一次远程运行输出里明明写着
> `FAIL …/internal/replication`，而任务通知报 exit 0，差点被当成"绿"。判绿必须看
> `go test` 自身的退出码（`set -o pipefail`，或先重定向到文件再取 `$?`）。
> 与 §6「判据维度必须覆盖缺陷表现形式」同一类：**测量通道的可信度要先于测量结论。**

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
| **A4 阶段 1（offset 水位改 ts 源）** | 2026-09-05 | a4 §10 附9（实施结果链）+ TODO §1——7c273e4 主体 + 2003494 语义守卫/WAIT 缺口修复 + 82aa601 同步判据 + 7b0253c 半升级窗口 + 9aa1c94 全量回归 + c9722ec 最终门禁——剩余（阶段 2 gate 1）见 TODO §2 |
| **§6 并发 FeedSlave 重发（feedMu 游标锁）** | 2026-09-05 | a4 §10 附8.1 选项 1——e304a07 实施——post-fix `-count=5` 全绿（dup=0）/ pre-fix 10509ab worktree 红（2/2 轮 dup 4/4 键）——恒绿守卫 `concurrent_feed_slave_test.go`——新开放项（lost=1 偶发）见 TODO §6 |
| **C4 发散悖论（feed 模式结构性消失）** | 2026-09-05 | TODO §5——e1fd352——重连判定全程 ts 域（PSYNC-ts 整数比较 + 降级 FULLRESYNC + resumeTS+1）——字节边界不参与——仅字节路径残留（gate 1 退役后彻底消除）——层 D 降级可选验证 |
| §3 split-brain 家族 flake | 2026-09-01 | 负载敏感时序扰动（gossip HelloInterval 500ms），非共识缺陷；三重测移除 `t.Parallel()`；家族维持 documented-unreliable |
