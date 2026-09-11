# BoltDB 待办列表

> 本文件只列**未完成**的工作。已完成阶段的过程记录归入对应设计文档（`a4-engine-seq-replication.md`、
> `1c-complete-fix-design.md`）或下方「已收口」索引；逐提交细节以 `git log` 为权威。
> 2026-09-05 整理：删除 296 行中已完成的堆积记录，迁入上述文档。
> 2026-09-06 整理：删除已收口大节（§1 阶段 1 / §5 C4 / §6 lost 过程记录 / §7 扫面批次与审计——
> v8.58.0 发版内容，见 CHANGELOG.md + 下方已收口索引）。

## 待办

> **待办现状（2026-09-11）**：§2 + §3 + §8 + §9 均已收口/通过（正文留痕如下）。
> 唯一 known-open = §6 lost=1 偶发——非阻塞 flake，定性见下方
> 「已知 known-open」小节。

### 2. A4 阶段 2——删除 backlog 内存环（gate 严格——不可在线回滚）

> **✅ 已收口（2026-09-08）**：删环主体 + 测试适配全部完成并验证——
> `476d6d9`（删环收口 + FLUSHDB 传播帧修复）+ `f48c449`（ts 域测试适配）。
> 远程 -race 全绿（internal 全 10 包 + cmd/integration replication 相关多批次 +
> regressions 守卫组四件套）+ lint 0 issues + gofmt 干净。注：dup 已 feedMu 修复；
> §6 lost=1 偶发 = 非阻塞 known-open flake，定性见「已知 known-open」小节（原误标 §7）。

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
> ② §3 dw ≤1/15 正式验收 ③ 字节路径残留清除（原 §5 C4 残留面——gate 1 退役后
> 彻底消除）。它是**运维面事实**，代码侧无任何动作可推进它。

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
4 参形态不变——字节值恒 0（2026-09-06 预研定论——见下「预研结论」）。
**实施顺序**：① 部署全 feed（gate 1 确认）→ ② 换算表最后一次 AlignCheck + 规模守卫
复跑（gate 2/3）→ ③ FULLRESYNC ts 激活改造（CatchUpAndEnableSlave 删除）→ ④
字节分支/环/WAL 删除 → ⑤ 守卫更新（FULLRESYNC ts 激活守卫 + 4 守卫 ts 化核对）→
⑥ 全量回归（regressions + cmd/integration）。

**预研结论（2026-09-06——gate 1 等待期设计核对——实施细节已定——少未知）**：
- **FULLRESYNC ts 激活（蓝图第 ③ 步）——设计可行**：snapshotTS 已在 handler:85
  写锁内读取（与 snapshotOffset 同位——FULLRESYNC 响应已含 `+FULLRESYNC %s %d %d`
  ——从侧存为 lastAppliedTS）——`FeedSetEnabled(snapshotTS+1)` 与现有激活等价
  （CatchUpAndEnableSlave:595-606 追平后也是 `FeedSetEnabled(true, curTS+1)`——
  直接激活时 curTS=snapshotTS——FeedSlave 从 snapshotTS+1 读 log 增量——覆盖
  RDB 快照点到当前水位的所有帧——无需字节 catch-up）——propMu 竞态保护模式已有
  （replication.go:588-608——propMu 内激活——写路径 RLock 阻塞——无丢失无重复）——
  **实施**：新增 `CatchUpAndEnableSlaveTS(slave, snapshotTS)`（跳过字节 catch-up——
  直接 propMu 内 FeedSetEnabled(true, snapshotTS+1) + SetReady(true)）——
  handler:125 改调；handler:198（字节 CONTINUE——ts==0 从侧）随 gate 1 退役同删；
- **GETACK/ACK 字节字段——定论：恒 0（保持 4 参形态）**：EncodeReplconfAck
  4 参形态（`REPLCONF ACK <offset> <ts>`——feed_wire.go:38）保持——offset 字段
  恒 0（gate 1 后无字节从侧——无实际语义——ts 为唯一主字段）——旧 3 参主/从按
  len 判定忽略第 4 参——向后兼容——handler:332 的 `GetBacklogCurrentOffset()`
  → 0；handler:74（FULLRESYNC 响应 offset 字段恒 0——snapshotTS 已有）；
  131/187（字节 catch-up 起点——随 ts 激活删除）；
- **gate 1 达成后实施清单（更新——少未知）**：① 部署全 feed → ② 换算表最后
  AlignCheck + 规模守卫 → ③ CatchUpAndEnableSlaveTS 新增 + handler:125/198 改
  （ts 激活替代字节 catch-up）→ ④ 删字节分支/环/WAL（BacklogWAL/ReplicationBacklog/
  GetBacklogCurrentOffset 的 10 处调用点——74/131/187/332 按上定论处理）→
  ⑤ 守卫更新 → ⑥ 全量回归。

### 3. dw A/B ≤1/15 正式验收（✅ 已通过 2026-09-08——全 5 批 -count=3 PASS）

> **gate 解除（2026-09-08）**：阶段 2 删环已收口（TODO §2）——复制切换完成——
> 正式验收 gate 满足，可重跑下方命令。
>
> **✅ 验收通过（2026-09-08）**：`TestRegressionDuplicateWindowMeasurement` `-count=15`
> （5 批 × 3 次）全 PASS——远程 -race 每批 ~60s，gap=0 零亏空（dw ≤1/15 达标）。

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

### 8. EXPIRE/PEXPIRE 相对 TTL 的 store 层规范化（✅ 已收口 2026-09-11）

> **收口**：`Expire`/`PExpire` 改 `retryUpdateLazy` 记规范绝对 `PEXPIREAT` 帧
> （闭包捕获提交内算出的绝对过期点；未命中记 NOOP 占 ts——SPOP `0931b6b` 同模式）。
> PExpire 帧取秒级 `ExpiresAt*1000` 对齐（非 `nowMs+ms`——否则从侧重算 ceil 因起始余量
> 不同可差 1 秒）。条件变体 NX/XX/GT/LT（`key_commands.go:handleEXPIRE`）+
> `isPositiveIntegerResp` 门（`e322b7c`）原样保留；PERSIST/EXPIREAT 未动。
> 新守卫 `TestRegressionCanonicalExpireAbsolutePoint`（帧为 PEXPIREAT 断言 +
> EXPIRETIME/PEXPIRETIME 主从精确相等）+ 既有全族 + strict soak 绿。
>
> **背景（2026-09-11——`0931b6b` 调查副产品）**：`handler_core.go` 的
> EXPIRE→PEXPIREAT 规范化（`propagateArgs`）在 feed-only 下已死——
> `PropagateCommand` 忽略参数只当排水触发；store 记 raw `EXPIRE key seconds`
> （`base.go:190`）/ `PEXPIRE`（`base.go:275`）→ 从侧滞后 lag 才 apply →
> 绝对过期点漂移 lag 量级。`e322b7c` 只修了条件拒绝误传播，相对 TTL 本体未动。
> 当前 workload 全长 TTL（EXPIRE ≥1s、SETEX ≥10s）+ strict soak 只比 key/值/类型
> 不比 TTL 绝对点 → 无爆点 → 本单（`0931b6b`）范围外，记入 TODO。
>
> **修法（SPOP 同模式 `0931b6b`）**：`Expire`/`PExpire` 改 `retryUpdateLazy`——
> 闭包捕获提交内算出的绝对过期点，记规范 `PEXPIREAT key absoluteMS` 帧；
> 未命中（success=false）记 NOOP（占 ts）。注意面：① 条件变体 NX/XX/GT/LT +
> `isPositiveIntegerResp` 门（`e322b7c` 语义——拒绝不传播）必须保留；② 秒/毫秒
> 精度对齐（store 内 ExpiresAt 秒级 vs PEXPIREAT 毫秒）；③ PERSIST 不动。
>
> **验证**：远程 -race `TestRegressionCanonicalExpire*` 全族 +
> strict soak（`TestSoakReplicationShortStrict`）+ 本文件 §2 级回归面。

### 9. 并发 drain 扫描瞬时空洞 flake（✅ 已收口 2026-09-11——done-前缀读）

> **收口**：drain 发送面改 done-前缀读——`FeedEntriesFrom` 先读 `tsSource.done`
> 连续完成水位，再以该水位为 readTs 上界扫 `[since, done]`（`ReplLogEntriesRange` +
> `ReplLogDoneTS` + `tsSource.doneWater`）。一切 ≤ done 的 ts 在水位读到时早已提交可见
> （commitTS 同步提交 + End 恒在返回后），读集按构造稠密——in-flight ts 恒大于 done，
> 根本不可见。`done < since` 时返回空等水位（不报错、不记 drop）。CI `test` 的隔离
> skip 已摘除；无界读（`ReplLogEntriesFrom`）诊断/测试/探针保留。
> 新守卫 `TestDrainDoneBoundedNoGap`（真实 `FeedEntriesFrom` 路径：零 gap + 精确
> 320 帧 + 游标收敛——修前构造 10 轮挂约 5 轮）10/10 绿；原 flake
> `TestCatchUpAndEnableSlaveTS_ConcurrentPropagateNoDupNoHole` -count=10 全绿。
> 验证：flake×10 + strict soak + regressions CI 同款 + internal 全包 + cmd/boltDB +
> lint 0 issues（远程 -race）。
>
> **现象（2026-09-11）**：`TestCatchUpAndEnableSlaveTS_ConcurrentPropagateNoDupNoHole`
> 远程 `-race -count=10` 挂 3-5 轮、两种签名：① `got 319 want 320`（send_drop=0，无
> writer 报错）；② `send_drop=1`（`feed log ts gap at ts=25/27`）。**pre-fix worktree
>（`102b819`）同样复现——非 `0931b6b` 引入**（此前 main 靠单轮运气绿）。
>
> **机制（已定位到扫描层）**：8 writer 并发提交时 drain 侧前缀扫描与提交竞态——
> 扫描快照错过某 ts（in-flight 未提交 或 迭代器离散——09-06 lost 家族同类）：
> miss 落在 range 中部 → gap 报错 → drop（签名②）；miss 落在 tail（最后已提交键）→
> 连续前缀无错 → 游标越过 → 静默少一帧（签名①）。生产侧均自愈（② 下轮 drain 重试；
> ① 后续 push/停滞检测触发重连补发——strict soak 精确收敛为证），故测试的零容忍
> 断言（`send_drop==0` + 精确计数）与 timing artifact 不兼容。
>
> **隔离**：CI `test` job `-skip` 加本测试（`[flaky]` 标签——与既有 `[GHA: resource]`
> 区分：本项与机器无关）；`remote --full` 仍覆盖。
>
> **实证结论（2026-09-11——机制已定罪，修复待选型）**：
> 探针 `TestDrainGapForensic`（`internal/replication/drain_gap_forensic_test.go`——
> 只读 drain 模拟：游标 + `ReplLogEntriesFrom(since)` + `verifyFeedTSContinuity`，
> 缺失 ts 反查逐键提交墙钟三分类）10/10 轮复现 gap（2–6 个/轮，共 45 个）：
> `DURING`（提交于扫描窗口内）43 + `AFTER_END`（扫描结束仍未提交）2 +
> `BEFORE_START`（扫描开始前已提交）**0** + `NEVER_COMMITTED`（静置仍无）**0**，
> 且 10/10 轮 `finalSince == maxTS+1` 自愈收敛。另有 600 次全量扫描（stable/fresh
> 双探针，已删，结论留档）对已提交键零缺席；badger 源码确认 `CommitAt(nil)` 同步
> 落盘（`txn.go:Commit→req.Wait`），`View` 在 managed 下确为 MaxUint64 无快照读。
>
> **定罪**：签名② = snapshot-race / commit 乱序（in-flight），transient，恒自愈；
> 迭代器漏已提交键 = 零证据；墓碑丢失永久空洞 = 零证据。签名①（静默少一帧，本次
> 未复现）归因为同一 commit-race 在“最后一扫”边界的窄窗口（final drain 遍历中提交
> 且槽位已过）——生产侧经重连自愈（resumeTS 仍在 miss 点之前），与既有观察一致。
>
> **候选裁决**：① gap 截断发前缀 + 游标停 gap——② 自愈（等价重试），① 不彻底（保留）；
> ③ gap 重扫一次——② 即愈（forensic 证下一轮恒有），① 不可见仍漏（保留）；
> ② done-前缀读（`tsSource.done` 连续完成水位限读集）——唯一根治两侧（in-flight ts
> 根本不在读集内；final-drain 窄窗口同被水位卡掉）——**推荐**，但 `doneTs` 指定快照读
> 在 managed 下的一致性需单独实证（`NewTransactionAt(doneTs)` 读已提交前缀是否成立）。
> 另：测试零容忍断言（`send_drop==0`）与自愈重试的结构性冲突仍在——生产修好后测试判据
> 需单独立项（本单只输出证据，不改测试）。
>
> **修复方向（待设计——勿直接放宽测试）**：候选① gap 处截断发前缀 + 游标停 gap
> （mid-range 自愈；tail-miss 仍可能 short——不彻底）；候选② done-前缀读
> （`tsSource.done` 连续完成水位 + 指定 ts 快照读——`0931b6b` 后每 ts 恰一帧使前缀
> 稠密——但迭代器离散 artifact 同样作用于前缀读——需实证）；候选③ gap 重扫一次
> （transient 即愈——tail-miss 不可见仍漏）。先做候选机制实证（抓到 scan-miss 的
> 最小复现 + 区分 snapshot-race vs 迭代器离散）再定方案。

## 已知 known-open（据实定性，非阻塞 flake）

### §6 并发 FeedSlave 重发 lost=1 偶发（documented known-open flake）

- **性质**：非阻塞 flake。dup（重复 apply）已由 feedMu 游标锁 `e304a07` 修复，守卫断言
  `dup==0` 恒绿；**lost=1 = 未归因罕见竞态**——当前证据不足以点名修复点，据实定性、不盲改。
- **调查与复现（2026-09-08）**：三轮只读审计排除错误假设（原"瞬时错误 skip → ts 空洞"对本守卫
  inert——`isTransientReplicationError` 仅对 "key not found" 返回 true，而 INCR 缺失键 store
  语义 = 自动创建为 0，不报 key-not-found）；结构性差异 = BoltDB `lastAppliedTS` 应用层 atomic、
  与 store 写入解耦（对照 kvrocks RocksDB WAL sequence number 原子推进）；**高成本复现轮
  `-count=16`（4批×4，远程 -race）+ 前轮 3 = 累计 19 次串行零复现**（全 lost=0/dup=0）。
- **现状**：本项**无已知可靠触发条件**（区别于 memory 记录的 flake 家族"仅 `--full` -p=2 触发"——
  那是另一组测试的并发时序扰动）→ 维持 documented known-open flake。守卫
  `concurrent_feed_slave_test.go`（dup==0 + lost≤2）恒绿，非阻塞。
- **推进条件**：修复需待真实环境复现抓 LOST-DIAG 逐键定位具体丢失 ts 后再谈方案——当前无定向手段可烧。
- **权威细节**：dup 修复收口见下方索引表 §6（`e304a07`）+ 2026-09-08 测量记录；守卫 =
  `cmd/integration/regressions/concurrent_feed_slave_test.go`。

## 方法论（守卫写作——lost 调查产出——保留）

**通用判据教训**：凡"零丢失/零多余/全绿"的守卫，先问一句——**它的判据维度覆不覆盖目标缺陷
的表现形式**。幂等写入的键集比对查不出重复应用，一如严格相等断言查不出顺序错乱。守卫写完后到
**pre-fix commit 上跑一次确认会红**（`git worktree` 代价很低）是唯一可靠的自检。

**测量纪律**：`go test … | grep … | tail` 的退出码来自管道**最后一个命令**，红套件会伪装成
exit 0。判绿必须看 `go test` 自身的退出码（`set -o pipefail`，或先重定向到文件再取 `$?`）。
与判据教训同一类：**测量通道的可信度要先于测量结论。**

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
| **§6 并发 FeedSlave 重发（feedMu 游标锁）** | 2026-09-05 | a4 §10 附8.1 选项 1——e304a07 实施——post-fix `-count=5` 全绿（dup=0）/ pre-fix 10509ab worktree 红（2/2 轮 dup 4/4 键）——恒绿守卫 `concurrent_feed_slave_test.go`。**遗留 lost=1 偶发（dup 已修复，此项为被 dup 掩盖后暴露的既有缺陷）**：候选根因修正（2026-09-08 二轮只读审计）：原归属「从侧 apply 瞬时错误 skip → ts 空洞」**对本守卫 inert**——`isTransientReplicationError`（reconnect.go:707）**仅对 "key not found"** 返回 true，而 INCR 缺失键 store 语义 = 自动创建为 0（string.go:322-324 `return 0, nil`）不报 key-not-found → skip 路径几乎不触发；FULLRESYNC 重连窗口（replication_handler.go:74-79）文档化的结构风险是**双应用（dup）**非 lost，且 live-push/catch-up 的 dup 已 feedMu 覆盖。kvrocks 对照 = RocksDB WAL sequence number 做 apply 游标（引擎原生单调 + apply/推进原子 + checkpoint 对齐），BoltDB `lastAppliedTS` 为应用层 atomic、与 store 写入解耦。**2026-09-08 测量**：3/3 `-count=1` 串行 lost=0（偶发非必现，cycle "not converged" 均为写者活跃期 slaveTS 滞后 1-2 帧假性、最终 converge）——定性 = **未归因的罕见竞态**（documented known-open flake）。**高成本复现轮（2026-09-08）**：`-count=16`（4批×4，远程 -race）串行零复现（全 lost=0/dup=0）——本项无已知可靠触发条件（区别于 memory 记录的 flake 家族"仅 --full -p=2 触发"——那是另一组测试的并发时序扰动）→ 维持 documented known-open flake；修复需待真实复现抓 LOST-DIAG 逐键定位具体丢失 ts 后再谈方案 |
| **C4 发散悖论（feed 模式结构性消失）** | 2026-09-05 | TODO §5——e1fd352——重连判定全程 ts 域（PSYNC-ts 整数比较 + 降级 FULLRESYNC + resumeTS+1）——字节边界不参与——仅字节路径残留（gate 1 退役后彻底消除）——层 D 降级可选验证 |
| §3 split-brain 家族 flake | 2026-09-01 | 负载敏感时序扰动（gossip HelloInterval 500ms），非共识缺陷；三重测移除 `t.Parallel()`；家族维持 documented-unreliable |
| **v8.58.0 发版（56 提交——lost 定论修复 + 等价扫面 32 例 6 确定性缺陷 + apply 审计 11 组 + backup managed 兼容）** | 2026-09-06 | `CHANGELOG.md` v8.58.0——checkFeedTSGap 修复（141 轮 0 lost）——internal 全 10 包无 -short 全绿 + 复制守卫三件套 + lint 0 issues |
| **§4 SSD 写入基线根因定论关闭** | 2026-09-07 | c49967a——NVMe（Samsung 960 PRO）零塌陷 130 keys/s vs HDD（sda1）单调崩塌 17→9 keys/s A/B 决定性对照——根因 = 数据落 HDD 分区的磁盘物理极限，非存储引擎 bug |
| **§5 RDB 生成/载入侧 fail-fast + 判别守卫** | 2026-09-07 | `CHANGELOG.md` v8.58.1——b26523e（~50 处 continue→return err）+ 00e41a6（expire-time 补漏）+ 39f048d（判别守卫 pre-fix RED/post-fix GREEN）——remote -race + e2e roundtrip + code_review single-depth clean |
| **A4 阶段 2（删 backlog 内存环——gate 严格）** | 2026-09-08 | TODO §2——476d6d9（删环收口：ring/WAL/换算表/字节分支全删 + FLUSHDB 传播帧修复——ClearAllData 无 logValue 不产生 REPLLOG 帧 → 从侧收不到清库——TestReplicationCompleteness_Key FLUSHDB poll 回归修复）+ f48c449（ts 域测试适配：catchup 重写/replLogCount helper/整删结构死码）——远程 -race 全绿 + regressions 守卫四件套 + lint 0 issues；遗留开放项 = §6 并发 FeedSlave 重发 lost=1 偶发（dup 已 feedMu 修复——见下方索引表 §6 + 2026-09-08 测量） |
