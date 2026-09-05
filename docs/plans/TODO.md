# BoltDB 待办列表

## 待办 / 延期中

### 1c-残留. 复制读争用残留的完整修复（架构级——2026-09-03 调查链收口）

> 背景（简）：dw 回归偶发亏空的**原问题已修复**（2026-08-31 停滞检测 GETACK 自愈 +
> 武装重置 + 排水冻结检测）；但并发负载下**残留仍偶现**——机制链：store 读争用（复制期间
> 外部读）→ 排水数据间隙（无上界）→ 冻结误判 → 强制重连 → PSYNC 撞非命令边界（发散
> 悖论——从侧视角不可见）→ 降级 FULLRESYNC#2 → 数据丢失。`63b5c8c`（排水冻结阈值 30s）
> 为已知最佳部分缓解（A/B 2/15）。
>
> **快速修复空间已穷尽**：阈值调参 10s→30s→40s 定性失败（间隙随阈值增长无上界）；
> B1 双重确认（连续 2 ACK 周期）A/B 4/15 未达标——已回退；A1b 从侧读闸机制否决
> （亚并发读即触发——并发闸结构上无法过自身门槛）。阶段 0 观测已落地（`3840d9e`——
> INFO `# Stats` store_write_conflicts/l0_*/slow_* 零开销遥测）。
>
> **完整路线图 + 候选裁决 + A/B 协议：`docs/plans/1c-complete-fix-design.md`**。
> 剩余候选：
> 1. B2 排水进度判据——停滞检测改测应用进度（需事件级慢写/freeze 时序数据——
>    INFO 累计计数分辨率不足，需临时探针采集）。**探针采集面已落地（2026-09-05）**：
>    `SlaveReconnector.lastApplyTime`（最近一次成功应用复制命令的时间——feed 条目 +
>    数据命令两处应用点更新）+ INFO `repl_apply_idle_ms`（从侧应用空闲毫秒——B2
>    数据源）+ 停滞检查点日志 `apply_idle` 维度（事件级 slow-apply/freeze 时序）
>    ——**判据未翻转**（现判据仍为数据到达空闲 idle——B2 判据翻转属推阈风险，按
>    §7 A/B gate 收口——探针数据收集若干轮后定判据）。
>    **行为测试已落地（2026-09-05）**：`TestSlaveReconnector_readCommandLoop_
>    ApplyIdleTracking`（应用 SET 后 GetApplyIdle 推进断言——"收到数据且已应用"
>    与"收到数据但应用卡住"可区分）+ INFO `repl_apply_idle_ms` 字段存在断言
>    （TestBuildInfoResponse_ReplicationSection 补 Replication 初始化）——
>    本地 vet + 远程 -race 定向/全包（replication 48.2s + server 42.1s）+
>    cli 兼容套件 93/93 全绿（新增 INFO 键无解析回归）。
>    **区分能力实证（2026-09-05）**：`TestSlaveReconnector_readCommandLoop_
>    ApplyIdleDistinguishesStall`——transient skip（JSON.CLEAR missing key——
>    isTransientReplicationError）后 lastDataTime 刷新但 lastApplyTime **不更新**
>    ——apply_idle 显著大于 data idle（单元级同构 §1c 冻结链"收到数据但应用
>    卡住"）——现有判据（data idle）在此场景不触发——B2 判据翻转的数据依据。
>    **判据翻转方案已入册（2026-09-05——1c §7）**：补充触发 `applyIdle >
>    replDrainStallTimeout(30s)` 且水位未收敛——不替换现判据（盲区补充）——
>    独立常量 `replApplyStallTimeout` 可一行回滚——A/B 验证计划（A=探针+翻转 /
>    B=探针+现判据——门槛 ≤1/15）。
>    **判据翻转已实施（2026-09-05）**：`replApplyStallTimeout`（30s）+ 停滞
>    检查点 else 分支补充触发 `masterOffset > slaveOffset && applyIdle >
>    applyStall`——本地 vet + 无 -race（停滞守卫组 4 个 + 探针组 2 个全过——
>    补充触发未误触发现有守卫）+ 远程 -race 定向/全包（replication 49.9s +
>    server 41.7s）全绿。**A/B 验证（A 组 = 翻转+探针开）dw -count=15
>    15/15 全绿**（0 失败——门槛 ≤1/15 显著通过；B 组基线 = 探针开 15/15
>    全绿——无回归）。**补充触发正向路径守卫已加（2026-09-05）**：
>    `TestSlaveReconnector_readCommandLoop_ApplyStallForcesReconnect`——transient
>    skip 后数据到达但应用停滞超阈值 + 主节点水位高于已应用 offset → 补充触发
>    强制重连（现判据 data idle 两条均不触发——盲区闭合的直接证据）——本地
>    无 -race + 远程定向（4.7s）+ replication 全包（49.5s）全绿。
>    **纯对照不回归确认（2026-09-05——判据翻转后重测 §7 门槛项）**：无探针无
>    翻转 dw -count=15（5 批 × 3 次）**15/15 全绿 0 失败 0 停滞**——判据翻转
>    未引入误触发（纯对照路径翻转前后均 15/15——翻转前基线 2026-09-04/05
>    亦 15/15）——§7 门槛"纯对照不回归"满足。
>    **--full 停滞+降级确认（2026-09-05——§7 门槛项收口）**：判据翻转后无
>    -short 全量（replication 49.4s + server 40.0s + store 270.5s + integration
>    复制 8.0s + cluster 13.1s 重测组）全绿——停滞+降级事件合计 = **0 < 2**
>    ——§7 门槛"`--full` 停滞+降级合计 < 2"满足——判据翻转验证闭环完整
>    （A/B A 组 15/15 + B 组 15/15 + 纯对照 15/15 + 守卫全绿 + --full 0 事件）。
> 2. 架构级：**A4 复制记账引擎序列化——已立项（2026-09-03）**——managed-mode ts 迁移
>    （S0 引擎研究 ✅ → **S1-A1 应用层 key 锁层 ✅（2026-09-03——补差 + 覆盖完备性复核
>    双里程碑）** → **S1-A2 切引擎 ✅（2026-09-03——§10 附5——全量验证绿）** → S2 复制
>    切换 → S3 快照全量——每阶段可回滚——dw A/B 门槛 ≤1/15）——
>    计划与设计见 [a4-engine-seq-replication.md](a4-engine-seq-replication.md)（§8 S0 结论/
>    §9 S1 阻断裁决/§10 S1-A 设计 + §10 附 3 S1-A1 实施状态 + **§10 附 5 S1-A2 实施状态**）；
>    A2 复制应用批量合并（随 A4 的写批单位自然覆盖——不单独立项）。
>    **S1-A1 收口（2026-09-03）**：审计 gap 清单全部命令组加锁（~55 方法——14 增量——
>    每增量远程 store -race 绿）——两处自死锁发现即修（RenameNX→Rename、TSIncrBy→TSAdd
>    委托模式）+ **写路径覆盖完备性复核**（retryUpdate 全量审计 25 候选——22 空洞修复
>    （`45be299`——含 XReadGroup 作用域锁——阻塞等待必须在锁外）——LMove/RPopLPush 假阳性
>    （列表族 lockListKeysOrdered 已覆盖）——NextStartup/RenameNX 例外核实）——store +
>    replication 跨包远程 -race 绿 + lint 0——S1-A2 前置（key 锁覆盖完备）已证。
>    **S1-A2 收口（2026-09-03——`b9083a3`）**：引擎切换 OpenManaged + tsSource（MaxVersion
>    水位）+ commitTS chokepoint（全库 db.Update 零残留——含 cluster 3 站点）——managed
>    模式 discard-ts 必需（三层：基础推进 → 有序完成水位 → discardMu pair 原子对——值级
>    实证 `discardTs=1154<lastCleanupTs=1158` 回退链）——冲突测试适配（raw helper 加 key
>    锁——managed 退役冲突重试）——验证全绿（store 47.7s + replication 22.1s + §1c 守卫
>    三件套 + lint 0）。**S1-A2 完工后 §1c-残留进入 S2/S3 阶段——dw A/B ≤1/15 验收在
>    复制切换后执行**。
>    **S2 实施中（2026-09-03——D 定案 = kvrocks 式 log-in-commit）**：用户裁决 D
>    （kvrocks 调研实证：RocksDB SequenceNumber 记账 + log-data 与数据同批（单一批 =
>    单一 seq——零分发侧打标）+ WAL 回读传播——见 a4 §10 附6 D-定案）。已提交：D 地基
>    （`bd0c854`——commitTS 同批传播日志键 REPLLOG_+tsBE——日志键 ts 天然 = 数据 commit
>    ts——无竞态/无 ctx 流穿/无提交串行化）+ string 族扩展（`6fd372e`——12 写站点标识性
>    日志值——SetEX/PSETEX/INCR/DECR 经 wrapper 委托天然覆盖）。C5 基准（`6fd372e` 初测
>    SET 路径 -race 158-165µs/op vs S1-A2 基线 120 = +30-37%）——**归因核验（2026-09-03）
>    推翻初测：同条件紧邻 A/B 无检测差异（D-on 417µs vs pre-D 438µs——~5% 噪声内）——
>    初测为跨时机器漂移伪影（~20 分钟内两批次 158-165 vs 417-438 = ~2.7x 漂移）——D 的
>    每写 +1 日志键开销在当前噪声下未可观测——C5 无碍（bench 跨时对比不可靠教训：同条件
>    A/B 必需）**。
>    **D 覆盖全写面完成（2026-09-04——~90 写站点）**：hash（`ffb91af`）+ list（`5233ab2`）
>    + set（`f1efb8f`）+ zset（`b0c7e57`）+ stream（`e369cd0`）+ geo（`f1a9c77`）+ 收尾
>    （`5a5a56a`——json/hll/ts/rename/linsert/expire + Del/LMove/LTrim/LSet/泛型 + 语义跳过
>    清单）——各族远程 -race 全绿 + lint 0。
>    **读侧分级-1 闭环（2026-09-04）**：读侧探针（`11f1deb`——store.ReplLogEntries 公共读
>    helper + log 键 vs backlog 事件级比对）+ 并发 ts 单调探针（`2253a47`——8×25 并发 SET
>    下 log 键 ts 严格升序无碰撞——远程 -race 无 DATA RACE）+ 兼容三套回归全绿（cli 93 +
>    py 247 + node 122——D 覆盖后命令面零回归）。
>    **S2 分级 2/3 设计定案（2026-09-04——a4 §10 附7）**：落点深化（`0ab02c3`——PSYNC 三
>    决策点 ts 化 + ACK 三用途字段分流 + 换算表设计）+ ts 域深化（`1f303db`——ACK/PSYNC
>    ts = 直接主侧 ts 域——从侧本地 commit ts 域不同域（仅服务下游 hop）——流须携带主侧
>    ts）+ 回传勘察（`ff6a200`——commitTS/retryUpdate error-only——回传面 ≈ ctx 流穿量级
>    ——共享捕获竞态实证——**推荐分级 2/3 重排：log-key 增量流先行（键 ts 天然携带——零
>    回传）——含 ACK/PSYNC ts 语义 + 从侧 lastAppliedTS——backlog 影子并行 + 换算表验证
>    后退役**）。
>    **S2 feed 协议 ①-⑤ + feed-loop + 生产接线全落地（2026-09-04）**：
>    ① wire（`e322be9`——REPLLOG (ts, 全命令) flattened——嵌套 bulk 无收益否决）；
>    ② sender + 从侧分支（`298c87d`——master 增量 sender + 从侧 REPLLOG 分支 apply +
>    lastAppliedTS 推进 + 字节 lastOffset 双轨）；③ ACK-ts（`15d5f7b`——ACK 带
>    lastAppliedTS + GETACK 带 currentTS——停滞判据 ts 形式——masterTS==0 字节兜底）；
>    ④ PSYNC-ts（`26403e1`——4 参整数边界 [logStartTS, currentTS]——ts==0 字节模式保留）；
>    ⑤ 验证门槛（`a3b0cb2` ts 重放守卫——日志键回放 == 字节 backlog 回放事件级等价 +
>    `b043c63`/`cc3ca39` 4 守卫 ts 双轨重写——含 GETACK 4 参测试修复 `1263d50`）。
>    feed-loop 增量流（`982057b`——FeedEntriesFrom nolint 转正 + feed-state since-ts 游标 +
>    PropagateCommand 分支双轨并存（feed 从侧走 REPLLOG——非 feed 从侧字节路径不变——
>    默认关零成本回滚））——E2E 闭环（`6838470`——master feed 流 → 从侧 apply → 收敛 +
>    lastAppliedTS == master 水位）——生产接线（`07fd694`——`--feed-loop` 启动标志默认关 +
>    config `[replication] feedloop` + 三套件 feed-mode 零回归：cli 93 + py 247 + node 122）。
>    **剩余方向（backlog 退役尾）**：1. **feed-mode 规模验证 → 退役决策**（**2026-09-04
>    实测阻断：psync-reconnect-no-loss 在 feed-mode 下 missing=2499/3666——无丢失不变量
>    FAIL——根因 = FeedEntriesFrom 值源对齐（log 键序 ↔ backlog 事件序绝对位置 1:1）在
>    并发写者下分叉（processRequest 的 snapshotMu.RLock 共享——commit-A/commit-B/
>    append-B/append-A 交错——log 序 (A,B) vs backlog 序 (B,A)——错误关联→从侧数据
>    错位/缺失）——**退役被阻断——backlog 保持权威——修复 = ②/D4 值源切换（log 键
>    全值——零对齐）前置，或对齐硬化（log 键标识符值 vs 事件命令+键校验——错位检测
>    回退 FULLRESYNC）**——双轨核验锚（replay 守卫 + E2E + 重连 + 三套件）在串行/顺序
>    写场景全绿——并发场景为残留缺口——`--feed-loop` 默认关不受影响）；2. **dw A/B
>    ≤1/15**（§7 协议——双轨下重复窗口度量——复制切换后验收）；3. **②/D4 全重放形式
>    部署**（**并发对齐分叉实测——由"仍延后"提级为退役前置**：值源切日志键全值（零
>    对齐）——`ReplLogEntriesFrom` 增量 seek 转正——2x vlog 写放大与读侧切换同步）；
>    4. §2 SSD 崩塌复测（疑 discard-ts/压实同源——S1-A2 后新代码复验）+ C4 抓包级比对
>    （发散悖论根因——S3 前提）。
>    **对齐硬化已落（2026-09-04——安全层）**：FeedEntriesFrom 每条目校验（log 键标识符
>    值 vs 事件命令+键——错位检测报错——从侧回退字节路径）——并发验证实测：失败模式
>    从"missing=2499 数据丢失"变为"convergence 停滞（lag=5184）"——**错误关联数据已防
>    （安全层 ✓）但可用性仍缺（feed 报错→从侧停滞——治本 = ②/D4 值源切换——硬化为
>    过渡安全网——退役仍被阻断）**。
>    **D4 值源切换全族收官（2026-09-04——退役真修复前置齐）**：12 族 ~90 写站点
>    logValue 标识符→全重放值（`7035868` string（SetWithTTL 用 PXAT 绝对 TTL——gc-test
>    比例 0.5→0.25 适配——2x vlog 写放大实证）→ `08dd4d5` hash（encodeHashValue helper）→
>    `f94d050` list → `700a9dc` set → `e398ee1` zset（ZADD pairs+flags + Z\*STORE helper——
>    ZDIFFSTORE 无 weights）→ `e2253ed` stream（XCLAIM/XAUTOCLAIM options 构造 + XGROUP
>    四子命令）→ `58243f5` geo（BYRADIUS/BYBOX——lon-lat-member 序）→ `eb41fb1` json →
>    `b6efdae` hll → `7e61b23` rename → `1bea7ee` base（相对 TTL 标准形态——PERSIST 免改）→
>    `c40eecf` ts——D 覆盖收尾）——每族经定向 + 远程 -race store 全包绿（57-60s）。
>    **零对齐 feed 值源已切（2026-09-04——`b9a7fa4`——退役真修复落地）**：FeedEntriesFrom
>    值源 = log 键自身全重放命令（parseReplLogValue——无 backlog 事件读取/对齐）——
>    **并发 commit/append 分叉结构性消除（missing=2499 根因真修复——非对齐硬化式报错
>    兜底）**——对齐硬化 verifyFeedAlignment 退役（528c236 过渡安全网完成使命）+
>    feed_alignment_test 重写为 TestParseReplLogValue——FeedSlave/wire/从侧处理不变——
>    replication 全包远程 -race 23.2s 绿。
>    **feed-mode 规模验证复跑已做（2026-09-04——零对齐后仍 missing=2473/3818 ≈65%）**：
>    TestFeedModeScaleNoLoss（3 writers×5ms + 5 kill cycles——复刻 psync-reconnect-no-loss
>    结构——master+slave 双侧 EnableFeedLoop）实测 missing key 跨 writer/seq **分散**（非尾部
>    连续）+ slave offset 超前 ~93k 字节（D4 全重放值更大——双轨设计测量伪影）+ 日志
>    `feed 增量发送到从节点失败 ... use of closed network connection`——**零对齐未消除数据
>    丢失（与最初 missing=2499 同量级）**。**根因锁定投递/补发层（非值源对齐层）**：重连
>    CatchUpAndEnableSlave 走 backlog **字节** catch-up（按从侧漂移的字节 offset）后跳
>    feedSinceTS=curTS+1——字节域与 ts 域 REPLLOG 流坐标不兼容→间隙命令既不在字节补发也
>    不在增量流→丢失——**治本 = backlog 影子退役本身（重连改 ts 域）——非 D4/零对齐可解**。
>    探针已回滚。
>    **feed-mode 复跑验证 PASS（2026-09-04——治本确认）**：新增
>    `TestRegressionPsyncReconnectNoLossFeed`（psync_reconnect_noloss_feed_test.go——
>    master+slave 双侧 EnableFeedLoop——复刻 psync-reconnect-no-loss 结构）远程 -race
>    PASS（45.8s：tokens=5636 recon=6 goroutine-delta=4——**零 MISSING/EXTRA/VALUE
>    MISMATCH**——missing=2473 原始复现 → 修复后 ≤2 容差内全绿；slave offset 超前
>    712074 为 feed 模式已知双轨测量伪影——D4 全重放值更大——测试按非关键处理）——
>    **重连改 ts 域（2c8ecd0）为 feed-mode 数据丢失治本确认**。
>    **剩余方向（backlog 退役尾——D4 前置已清）**：1. **backlog 影子退役**（分级-3 尾——
>    **feed-mode 规模验证已确认为数据丢失治本（重连改 ts 域——字节 catch-up 与 REPLLOG
>    ts 流坐标不兼容是 missing=2473 根因）**——增量续传源切 log-key——`ReplLogEntriesFrom`
>    增量 seek 转正——backlog/WAL 字节记账删除——配置化开关保留回滚——字节+ts 双轨切换）。
>    **重连改 ts 域已落地（2026-09-04——治本替换过渡安全网 9435523）**：psync.go ts 域
>    CONTINUE（仅 feed-loop 开启时——feed-loop 关字节模式 ts>0 保持 FULLRESYNC）+ handler
>    CONTINUE 分支按 result.TS>0 分流 + CatchUpAndEnableSlaveTS（propMu 内原子激活 +
>    propMu 外 FeedSlave 补发 gap [resumeTS+1, curTS]——双发窗口由从侧 lastAppliedTS
>    去重兜底）——定向重连三测 + replication/server 全包 + 三守卫 + PsyncReconnectNoLoss
>    远程 -race 全绿（TestPSyncTSRange 补 SetFeedLoop(true)——ts 域边界判定属 feed-loop
>    场景语义）。剩余：~~`ReplLogEntriesFrom` 增量 seek 转正~~（**已落地（2026-09-04）**：
>    FeedEntriesFrom 改经 store.ReplLogEntriesFrom(since) 增量 seek——replLogKey(since)
>    O(log N) seek + 顺序迭代——消除 O(n) 全量扫描 + Go 层过滤——语义等价（首个 ts>=since
>    键起——与旧全量扫描过滤一致）——定向增量 seek 六测 + replication/store 全包远程
>    -race 全绿）+ ~~backlog/WAL 字节记账删除~~（**WAL 字节记账双轨切换已落地（2026-09-04）**：
>    PropagateCommand 在 feed-loop 开启时跳过 wal.Append + maybeTruncateBacklogWAL——
>    log-key 为权威持久化源（commit 即写日志键）——backlog 内存环保留（offset 水位 =
>    PSYNC 判定/FULLRESYNC offset/字节从侧 ts=0 兼容的基础）——重启后 backlog 空 →
>    PSYNC 安全降级 FULLRESYNC（ts 域重建——已治本）——--feed-loop 关闭即完全恢复字节
>    WAL 记账（回滚开关）——replication/server 全包 + 三守卫 + feed 变体远程 -race 全绿；
>    剩余：backlog 内存环删除（需字节从侧 ts=0 完全退役后——offset 水位改 ts 源——
>    **设计方案已入册：a4 §10 附8**——消费者迁移表 11 项 + 两阶段实施（阶段 1 offset
>    改 ts 源可回滚 / 阶段 2 删环 gate 严格）+ 退役三 gate（字节从侧退役/换算表核验/
>    feed 零丢失复跑）。**换算表已落地（2026-09-05——gate 2 前置完成）**：
>    `ReplConversionTable`（conversion_table.go——OffsetToTS/TSToOffset/AlignCheck +
>    空表边界）——测试覆盖（含分叉检测 + 8×25 并发鲁棒——锚检测语义实证）+
>    replication 全包远程 -race 绿——**守卫重写已接入**（`TestTSReplayEquivalence`
>    ts 重放守卫末尾换算表构建 + AlignCheck 断言——事件级等价的强形式）——
>    阶段 1 实施时其余守卫（4 守卫）同步以表核验。
>    **RDB 线性化点 ts 化前置验证（2026-09-05——阶段 1 先行）**：勘察确认不变式
>    "快照点 ts = catch-up 起点 ts+1"当前实现下成立（snapshotOffset 锁内捕获 +
>    feed 激活 feedSinceTS=currentTS+1 propMu 内读）——**阶段 1 落点修正**：psync.go:122
>    currentTS（FULLRESYNC 响应第 4 字段）锁外读可能早于快照水位——须移入写锁内
>    与 snapshotOffset 同位——验证测试 TestFullresyncTsDomainInvariant 远程 -race 绿
>    （详见 a4 §10 附8）。
>    **--full 验证（2026-09-05——无 -short 全量基线）**：replication 48.3s +
>    server 40.1s + store 253.6s + cmd/integration 复制重测组 8.0s + cluster 重测组
>    13.2s（MultiNode/Gossip/SlotSync/MigrateSlotUnderLoad/Failover/BlockingFuzz
>    全 PASS）全绿——soak 类（TestClusterSoak/TestSoak*）属 tier-C nightly
>    （SOAK_DURATION=1h）不阻塞 PR gate——当前 HEAD 发版基线确认。
>    2. ~~feed-mode 规模验证复跑~~（**已完成——零对齐后仍 missing=2473——见上**）；3. **dw A/B
>    ≤1/15**（§7 协议——双轨下重复窗口度量——复制切换后验收）+ ②/D4 部署同步读侧切换
>    （2x vlog 写放大已知成本——消费者落地后）；4. §2 SSD 崩塌复测 + C4 抓包级比对
>    （S3 前提——开放项）。
>    **dw A/B 纯对照基线已测（2026-09-04）**：`TestRegressionDuplicateWindowMeasurement`
>    -count=15（5 批 × 3 次，远程 -race）——批次 1/2/4/5 全绿 + 批次 3 内 1 次 FAIL
>    （单跑复跑 PASS 39.7s：INCR gap=0、LIST extra/missing=0、HSET/ZSET match、
>    send_drop=0 apply_skip=0 lag=0——非数据丢失——为已知 §1c flake 面——该测试走
>    字节模式 FULLRESYNC，feed-loop 改动无因果链——远程宿主为 192.168.1.251 非
>    GCP 10.1.2.16——跨机对比不可靠教训同源）。门槛判定：纯对照 14/15 绿 + 1 flake
>    （复跑 0 失败 0 停滞）——不构成回归；正式 ≤1/15 验收仍 gate 于复制切换后
>    （backlog 内存环删除）——读探针 harness（§7 协议完整形态）随候选验证回退未内置。
>    **dw A/B 探针开已测（2026-09-05——§7 协议完整形态落地后首测）**：dw 测试内置
>    从侧 LLen 读探针（DW_READ_PROBE=1 / `-args DW_READ_PROBE` 启用——每 ~10ms 一轮
>    ≈ 每 10 ACK 节奏——模拟 §1c 从侧读争用；默认关保持纯对照基线不变）——**探针开
>    -count=15（5 批 × 3 次）15/15 全绿 + 纯对照同窗口 15/15 全绿**——两侧均 0 失败
>    0 停滞——门槛（≤1/15）显著通过——读探针 harness 恢复内置（§7 协议完整形态）。
>    正式 ≤1/15 验收仍 gate 于复制切换后（backlog 内存环删除）——本组为完整形态
>    基线数据点（含读争用注入）。
> 3. 发散悖论（C4）根因定位：主侧发送字节 vs 从侧接收字节的抓包级直接比对（层 D——
>    外部工具）——恢复路径重设计（层 C：降级无损化）的前提。
>
> 一键验证：`bash scripts/remote-test.sh -race -timeout 180s -v ./cmd/integration/regressions/ -run TestRegressionDuplicateWindowMeasurement`

### 2. v8.52.0 发布遗留（非阻塞）

| 项 | 现象 | 下一步 |
|----|------|--------|
| **SSD 基线复测（2026-09-03 已执行——爬行中止）** | 目标：可信 SSD 写入基线（对比机械盘 28 MB/s）。**正确姿势**（2026-08-06 调查结论）：① 先确认 `DEBUG GC` 已完成（GC 与写入严重互斥，GC 期间 1MB SET 减速 1350×）；② `ps aux \| grep redis-benchmark` 确认无残留进程；③ 单进程 `redis-benchmark --cluster -h 10.1.2.16 -p 6337 -t set -n 65534 -r 20000000 -d 1048576 -c 50`（必须带 `-r`，否则覆盖写同一 key）；④ 记录吞吐 + DBSIZE 分布 + 磁盘占用；⑤ 测完 FLUSHDB 清理。备选：用已修复的 `scale-data-filler`（按 CLUSTER SHARDS/SLOTS 分组 pipeline）替代 benchmark | ⚠️ **实测（2026-09-03 11:17-11:40）**：GC 前置 rewritten=0 健康 → 基准启动 ~166 ops/s（≈166 MB/s）后**崩塌至 ~5 ops/s**（21 分钟仅 9.5%——全量需 ~4 小时——非 ~5 分钟）——**中止**——持续负载下写路径塌陷（疑 L0/vlog 压实风暴）为实测发现；FLUSHDB 清理 ✓（DBSIZE 归零）；vlog 6.3G 残留为已知 badger 机制（tombstone 卡空 L0——自然压实回收——同 §2 vlog 单元）。**备选**：scale-data-filler（分组 pipeline）或分段小批量重测 |

### 3. GETACK 回复参数量测试未随 S2 ACK-ts 更新（既有失败——非阻塞）

`TestHandleSlaveReplicationConnection_RepliesToGetAck`
（`internal/server/handler_coverage5_test.go:649`）断言主节点对 `REPLCONF GETACK *`
的回复为 **3 参**（`REPLCONF ACK <offset>`），但 S2 ACK-ts（`15d5f7b`——③ ACK-ts）已把
GETACK 回复切到 **4 参** `REPLCONF ACK <offset> <ts>`（经 `EncodeReplconfAck`，
`internal/replication/feed_wire.go:38`——主侧带 currentTS、从侧应答带 lastAppliedTS）。

- **现象**：`GETACK reply has 4 args, want 3`（远程 `-race -short ./internal/server/...` FAIL）。
- **既有确认（2026-09-04）**：`git stash` 干净树复跑该测试 → 同样失败——与 CONTINUE→FULLRESYNC
  修复无因果链（后者仅改 `psync.go` + `feed_reconnect_test.go`）。§1c-残留 提到的
  `1263d50`「GETACK 4 参测试修复」覆盖了 replication 侧守卫，**漏了这条 server 侧 coverage 测试**。
- **下一步**：~~断言改 4 参（`REPLCONF`/`ACK`/`<offset>`/`<ts>`）~~ —— **已修复（2026-09-04）**：
  断言改 4 参 + 校验第 4 参 == currentTS（fresh 无写路径 = 0）——远程 `-race -short
  ./internal/server/...` 全绿（43.3s）。注：`handler_depth_test.go:1304`
  （`TestSentinelBoundary_ReplconfGetack`）走 `executeCommand` → replconf_commands.go:54-60
  的 3 参路径——断言 3 参正确，不在本次修改范围。

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

---

## 已收口（历史归档——2026-08-30 ~ 2026-09-01——完整记录见 git 历史 + 指向文档）

- **§1 FULLRESYNC 线性边界（Issue #3 客户端写路径——2026-08-30 收口）**：
  `processRequest` 持 snapshotMu.RLock 跨越 commit→PropagateCommand 线性绑定；
  守卫 4 个 + 风暴回归全 PASS。详见 `docs/failures/snapshot-inconsistency.md` §4。
- **§1b 复制 offset 落命令中间（2026-08-30 修复）**：offset 即 backlog 水位唯一真相
  （删 IncrementReplOffset）；修复后 0/~4900 不可服务 offset 通告。详见
  `docs/failures/repl-offset-boundary-drift.md`。
- **§1c dw 回归偶发亏空（2026-08-31 修复 + 残留收口）**：原问题 = 尾巴静默投递缺口 →
  停滞检测 GETACK 自愈 + 武装重置 + 排水冻结检测；残留（读争用触发链）定论链
  `f013bdb`→`2a35170`→`8a6ac84`→`71cab80`→`85e2f14`→`f88187d`→`4e9e56b`→`e5a1bb7`→
  `d7af179`——路线图见 `docs/plans/1c-complete-fix-design.md`。
- **§3 split-brain 家族 flake（2026-09-01 已定位）**：负载敏感时序扰动（gossip
  HelloInterval 500ms + 收敛断言在 -p=2 争用下间歇越界），非共识逻辑缺陷；三个重测试
  移除 `t.Parallel()`；家族维持 documented-unreliable（tier-A 跳过模式已有处理），
  发版验证以定向包（regressions/守卫/dw）为准。
