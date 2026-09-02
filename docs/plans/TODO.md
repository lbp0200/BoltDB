# BoltDB 待办列表

> 2026-08-27 已清理：已完成项全部归档至 git 历史（`git log -- docs/plans/TODO.md`），本文件仅保留待办/延期/已决策不做三类。

## 待办 / 延期中

### 0. 下次继续（2026-08-30）

1. **`--full` 已绿（2026-08-30）**：远程 `-race -timeout 1800s ./internal/... ./cmd/integration/...` exit 0。
   版本已 bump 到 8.57.0。未 push / 未 tag（需授权）。
   1c LIST 亏空未单独立项关闭；本轮 regressions 包在 `--full` 中 PASS。
2. **push** —— 属对外可见动作，需明确授权后再做。
3. **reword `088ce37` 与 `14bd901` 的提交信息** —— 两条仍带着"手工摆出的交错即根因"这一
   过强表述（含 `first byte="\r"`）。文件内容已由 `c84293f`/`f4cec87` 更正，但提交信息未改；
   且这两个提交不在栈顶，reword 会改写其后提交的 hash —— 重写历史，需授权。
4. **Issue #3 补一条更正评论** —— 外部评论仍沿用过强表述；文档侧已改写为实测口径。需授权。
5. **Issue #3 客户端写路径已收口** —— 守卫不再 Skip。
   `TestSoakReplicationShortStrict`（严格相等默认开）远程 `-race` PASS：
   409 keys 全等，`mo==so=910512`，`send_drop=0 apply_skip=0`。

~~soak 收敛判据~~ → 已改，见 §1b：`replicationOffsetsConverged`（`lag <= 0`）。

### 1. FULLRESYNC 线性边界（Issue #3 — **2026-08-30 已收口客户端写路径**）

> **状态**：`processRequest` 持 `snapshotMu.RLock` 跨越 `executeCommand`（提交）与
> `PropagateCommand`（`backlog.Append` = offset）；FULLRESYNC 持写锁跨越
> `snapshotOffset` 捕获与 View。已提交未传播的写入对快照不可见。
> `retryUpdate` 不再 RLock（嵌套读锁在写者排队时会死锁）。EXEC 单独加栏。
> **Issue：** https://github.com/lbp0200/BoltDB/issues/3
>
> 守卫：`TestFullresyncBoundary_CommittedButUnpropagatedWrite`、
> `TestFullresyncBoundary_FenceBlocksSnapshotWriteLock`、
> `TestProcessRequest_WriteFenceBlocksFullresyncLock`、
> `TestProcessRequest_EXECFenceBlocksFullresyncLock`
> （后两条走真实 `processRequest`，栏从 handler 拿掉会立刻红）。
>
> **风暴回归（2026-08-30，远程 `-race`）**：
> `TestRegressionDuplicateWindowMeasurement` PASS — INCR/LIST 全等，
> marker 时 `lag=0 send_drop=0 apply_skip=0`。
> `TestRegressionSnapshotFullresyncOffset`、`TestRegressionPsyncReconnectNoLoss` PASS。
>
> 写锁仍覆盖整个 RDB 生成期 → 全量同步期间写入停摆（DB 越大越久）。
> 不经 `processRequest` 的 `store.*` 直写仍无栏（非客户端复制路径）。

### 1b. 复制 offset 落在命令中间（**已修复 2026-08-30**）

> **根因**：`masterReplOffset` 是命令长度**求和**（`IncrementReplOffset`），而 backlog 环在自己的锁下
> **连续**推进（`Append`）——两条时间线无同步耦合。FULLRESYNC 临界区只阻塞"提交"、不阻塞
> `PropagateCommand`，于是整个 RDB 生成期间两者持续漂移；主节点在 `+FULLRESYNC` 里通告"和"，
> 而字节按"环位置"切 → 从节点拿到半条命令开头的字节流（实测首字节 `'\n'`、`'3'`），`ReadRESP` 全流错位。
> 现场证据：`current_offset=1844104 offset=1843962`（差 142），与非命令边界降级日志同源。
>
> **修复**：offset 即 backlog 水位，唯一真相。删除 `masterReplOffset` 字段与 `IncrementReplOffset`；
> `GetMasterReplOffset()`→`backlog.GetCurrentOffset()`；`HandlePSync` 不再直读字段；
> `Stop()` 用同一次环读取同时持久化 offset 与 backlog。
> **有意的语义变更**：backlog 未恢复（崩溃/截断）时不再把持久化 offset 复活为水位——空环配高水位
> 会让 CONTINUE 从全零字节里"服务"请求；此时水位为 0，重连一律 FULLRESYNC。
> 干净重启仍保留 offset（新增 `TestReplicationManager_PersistedOffsetWithBacklog` 覆盖）。
>
> **实测**：修复前 ~1.1%（18/1579）的 FULLRESYNC 窗口通告了不可服务的 offset；修复后 0/~4900。
> 守卫：`TestFullresyncAdvertisedOffsetIsServable`（`internal/replication/repl_offset_boundary_test.go`）。
>
> **自我更正记录**：初版用手工摆出的交错"证明" `offset=39` 落在 `[0,59)`，但紧凑采样器 20000 次
> 读到计数器落后 2700 次、落在命令中间 **0 次**（按整条命令后缀落后仍是边界）；中间态只在真实
> FULLRESYNC 窗口形状下才显现。教训：**证明的可达性必须实测，不能靠手工摆布 API。**

详见 `docs/failures/repl-offset-boundary-drift.md`（症状、机制、数据、实现清单）。
此前报的"stable lag=142 投递缺口"已澄清为**判据误判**（非投递丢失）：判据改为 lag 必须归零后，
2/2 次精确收敛（`mo==so`）且 LIST 主从多重集完全相等。

~~**仍未收口的一条**：
1. `cmd/integration/soak_replication_test.go` 的 `waitReplicationConvergence` 仍把"stable lag"当收敛，
   与回归用例刚改掉的同一个洞（副本退避中/落后于待重传尾部时会被判为已收敛）。~~
   → **已改（2026-08-30）**：成功条件只剩 `lag <= 0`（`replicationOffsetsConverged`）；
   冻结正 lag 超时失败。守卫 `TestReplicationOffsetsConverged`。

~~2. 从节点 `SlaveReconnector` 在其测试 `Close()` 之后仍继续退避重连~~ → **已查清并修复（2026-08-30）**：
不是测试工装问题（框架 cleanup 确实按 `replMgr.Stop()` → `h.Shutdown()` → `backupMgr.Wait()` → `db.Close()` 走），
而是 `ReplicationManager.Stop()` 从未停 `slaveReconnector` —— 只有 `StopSlaveReplication`（`SLAVEOF NO ONE`）会关 `stopCh`。
`MaxRetries=0` 即无限重试，每一轮 `tryReplicate` 都会走到 `LoadRDB`/`executeReplicatedCommand`，
也就是**在 `db.Close()` 之后访问 store**，直接违反 `docs/replication/architecture.md` 已写明的关机契约
（该契约第二条此前只是文档，未实现）。既有的 `TestSlaveReconnector_GoroutineLeak`/`_StartStop`
之所以漏掉，是因为它们**显式调用 `sr.Stop()`**，测的是"能被停"而不是"关机会停"。
守卫：`TestReplicationManagerStopStopsSlaveReconnector`（修复前 Stop 后重连次数 1→3，修复后不变）。

### 1c. dw 回归偶报 1 个元素亏空（**已定位并修复 2026-08-31**）

> **现象**（远程 `-race`，`TestRegressionDuplicateWindowMeasurement`）：偶发（约 1/5~1/10 批次）
> `missing_on_slave=1`（LIST 少 1 元素）或单个 INCR `gap=1`，且 marker 可见时
> `lag=29~95`（如 `mo=1370053 so=1370024 lag=29`、`mo=1357062 so=1357018 lag=44`）。
>
> **定位（2026-08-31，fresh-offset 重读证明是真尾巴缺口）**：
> 测试原在 phase-5 打印**轮询循环里的陈旧 mo/so**（marker GET 之前捕获），把真实落后
> 掩盖成了"lag 冻结"。改为 measure 时重读后确认：**从节点 offset 确实落后主节点一条命令
> （29 字节 ≈ 一条 INCR），且 `send_drop=0 apply_skip=0`、单次 CONTINUE、读循环健康
> （无第二次 EOF）**。两条丢弃路径、offset 边界机制、catch-up 竞态全部排除。
>
> **成因**：收敛 marker（writeWg.Wait() 之后设置，理论上应是 backlog 最后一条）可见于
> 从节点，但从节点 lastOffset 仍低于主节点水位——即 **backlog 中 marker 之后还有命令**：
> CLIENT KILL 窗口内停顿的 in-flight 写入（客户端超时/错误返回后，服务端仍在处理），
> 在 marker 之后才完成 append。该尾巴命令从未投递到从节点：live push 静默丢失（无
> 写错误 → 无 send_drop；从节点根本没收到 → 无 apply_skip），读循环无错误 → 无重连，
> 且系统无任何"主节点水位 vs 从节点已应用位置"的核对机制 → 缺口永久存在。
>
> **修复（2026-08-31）**：
> 1. **从节点 → 主节点 GETACK**：`readCommandLoop` 每 1s 发 `REPLCONF GETACK *`；
>    主节点 `handleSlaveReplicationConnection` 回复 `REPLCONF ACK <masterOffset>`
>    （RESP 数组，与从节点 ACK 同构，从节点 ReadRESP 可直接解析）。
> 2. **停滞检测 + 强制重连自愈**：从节点解析该回复，若 `masterOffset > lastOffset`
>    且数据流空闲超过 `replStallTimeout`（2s）→ 返回错误强制重连，PSYNC CONTINUE
>    从 lastOffset 重放缺失尾巴（幂等安全：续传不重叠）。
> 3. **关键设计**：仅"落后且空闲"触发——正常追赶期数据持续流动不触发（避免重连风暴）；
>    主节点**不**按 ACK 落后补推（ACK 是已应用位置，补推会 double-apply 非幂等命令）。
> 4. **dw 判据收紧**（此前明确"定位前不收紧"）：收敛要求 marker 可见 **且** `so >= mo`；
>    phase-5 重读 fresh offsets。
>
> **守卫**：
> - `TestSlaveReconnector_readCommandLoop_StallForcesReconnect`（落后+空闲 → 强制重连）
> - `TestSlaveReconnector_readCommandLoop_NoStallWhenDataFlows`（数据流动 → 不误判）
> - `TestHandleSlaveReplicationConnection_RepliesToGetAck`（主节点正确回 ACK <offset>）
>
> **实测（2026-08-31，远程 `-race`）**：dw 回归 10/10 全绿（含 `send_drop=1`/`send_drop=3`
> 轮——修复前 send_drop 轮偶发亏空，修复后均收敛 `lag=0`）；`internal/replication`（44s）、
> `internal/server`（40s）全包绿；`TestRegressionPsyncReconnectNoLoss`、
> `TestRegressionSnapshotFullresyncOffset` PASS。
>
> **后续观测（2026-08-31，归因修订）**：`TestRegressionSnapshotFullresyncOffset` 偶发失败
> **并非主机相关 flake——在 10.1.2.16 上亦复现**（B 态 1/12，与 192.168.1.251 的 ~1/10 一致），
> 且与提交 `6a886f6` 的 GETACK/停滞检测机制**强相关**（旧结论"无证据表明引入"已收回）：
> - **失败轮机制链（实测日志）**：`复制流停滞` WARN（lag=242、idle=2763ms）→ 强制重连 →
>   PSYNC offset 撞 `backlog.StartsAtCommandBoundary` → **`PSYNC CONTINUE offset 非命令边界，
>   降级为全量同步`** → FULLRESYNC → 结构校验失败（INCR -1、LIST 值全异、HASH 空），
>   且 so==mo 仍收敛——**§1c 尾巴静默投递缺口在 CONTINUE 追赶/排水路径上的再现**，
>   降级 FULLRESYNC 的排水尾仍会丢，且该路径破坏整体数据。
> - **6a886f6 的实缺陷**：GETACK 发送器 RESP 头 `*2` 但携带 3 元素（REPLCONF/GETACK/*），
>   主节点 ReadRESP 只读 2 个，残留 `$1\r\n*\r\n` 被解析为未知命令 → 每秒
>   `WRN 从节点发送了未知命令 cmd=*`（日志刷屏；守卫测试用了同样的畸形字节，掩盖了问题）。
> - **A/B 同主机（10.1.2.16）**：提交前（`6a886f6^`）0/23、提交后（`6a886f6`）1/24——
>   失败仅随该提交出现。
> - **开放项**：① 为何从节点 lastOffset=3902871 未过命令边界（从节点字节记账漂移 vs
>   边界检查误报，需定向探测）；② 停滞重连的恢复路径必须避免经 FULLRESYNC 降级破坏数据
>   （恢复设计决策）；③ GETACK 畸形格式修复（`*2`→`*3`，低风险）。
>
> **武装修复（2026-08-31 晚，commit `75f6024`）**：停滞检测增加**"近期曾收敛"武装前提**
> （`replStallArmWindow`=30s：仅当 ACK 显示 `masterOffset <= lastOffset` 后、再遇
> 落后+空闲才判定停滞）——消除追赶排水期误判。效果：**数据损坏消失**（隔离 8 轮 +
> dw + 守卫全绿、无停滞 WARN），**但 --full 下 SnapshotFullresyncOffset 以新形状失败**：
> 从节点在 lag=43 处**冻结 40s 收敛超时**（无停滞 WARN——武装正确地未在追赶期触发，
> 恢复被禁用后冻结无自愈）。**结论**：§1c 类尾巴冻结仍在负载下存在（追赶期/排水末段
> 的最后 43~242 字节静默停投），根因（主节点投递洞 vs 从节点应用阻塞）仍未定——恢复
> 设计需在"防损坏（武装）"与"防冻结（及时重连）"之间平衡，建议定向探测冻结机制后
> 再定恢复方案。
>
> **冻结探测调查（2026-08-31 深夜，探测已移除）**：为定案"投递洞 vs 应用阻塞"，临时加了
> **从节点看门狗**（readCommandLoop 单 goroutine：长时间无"读完成"时，lastApply >= lastRead
> → 读卡死（主节点未投递）；否则 → 应用卡死（store 阻塞））与**主节点 catch-up 轮次日志**
> （DWDBG catchup round/ready，-v 下验证可见）。**结果**：2 次额外 --full（带探测）均未复现
> 冻结（~1/3 --full 罕见）；full_fix 冻结轮（探测前）证据指向**从节点侧停止**（主节点视角
> 健康：连接存活、ACK 回复持续、catch-up 完成）。**机制未定案**；看门狗设计如上，下次复现
> 时可快速恢复。另观测到两个无关 --full flake：`TestReplicationNewCommands`（隔离 PASS，
> 负载 flake）与 `TestRegressionSplitBrainConvergenceHarden`（tier-A 已知不可靠家族）。
> **根治修复暂缓**：待冻结捕获定案后再设计；武装修复为现行缓解。
>
> **追加（同日 run #4 --full 带探测）**：dw 回归失败暴露 **16,270 个散落缺失 LIST 值**
> （list0-4: 1581/3620/3695/3699/3675；EXTRA=0、so==mo 收敛、INCR gap=0、
> send_drop/apply_skip=0）——INCR 走批量 SetStringBatch 完整而 LIST 逐元素 RPush 缺失，
> 两者同流 ⇒ 排水必完整，**缺失在 RDB 侧**。加载侧无任何逐元素错误日志（"存储列表值失败"/
> "读取列表元素失败"均 0 次 ⇒ RPush 丢值假设排除）；读取侧原子写入 + 栏内读取不可能
> 不一致 ⇒ **收窄至 `readListInTxn` 链式跟随的静默提前终止**（visited 环检测 / `:next`
> 断链 → 短列表且无错误），与散落缺失签名吻合。**下次复现的验证方向**：FULLRESYNC 后
> 对比 RDB 长度键与实载列表长度；若确认链断，检查 LPush 的 linkNodes/节点写入。
>
> **再追加（同日 RDB 往返验证，负面结论）**：确定性 RDB 往返测试（2 列表 × 3000 值
> LPush → GenerateRDB → LoadRDBWithStore → LRANGE 对比）**本地全过**（0.62s，零缺失、
> 零错位）——RDB 内容路径（链读取/编码/加载）在无负载场景**不丢值**，确定性 RDB bug
> **排除**。结合 run #4 实证（FULLRESYNC 后缺失、offset 收敛、INCR 完整、加载零错误），
> **机制收窄到 FULLRESYNC 协调/排水侧的负载相关路径**（传输中断 / offset 交接间隙 /
> 排水应用），且与冻结同为"offset 收敛但数据不完整"类——下次复现优先抓
> FULLRESYNC 通告 offset 与 RDB 实际覆盖、排水起始点的对齐。
>
> **再再追加（同日规模验证，7000 值/列表 × 2 往返本地全过 1.98s）**：规模相关 bug
> 假设排除（真实 dw 列表 ~5500 < 7000）——RDB 内容路径在真实规模下完整，机制确证在
> 负载/时序相关的 FULLRESYNC 协调或排水侧。
>
> **再再再追加（同日 RDB 传输帧验证）**：RDB 传输为**长度头 `$<len>\r\n` + 精确读取**
> （主节点 replication_handler.go:81 发送、从节点 `ReadBulkString` 按长度读；短读 →
> io.ErrUnexpectedEOF → "read RDB failed" → 重连，可见非静默）——**传输截断理论排除**。
> 至此 RDB 侧全链路（写入/链结构/读取/编码/加载/传输帧）均已排查无 bug；机制仍指向
> 负载/时序相关的 FULLRESYNC 协调或排水侧。
>
> **再再再再追加（同日 offset 交接链审查）**：FULLRESYNC offset 交接链逐环节验证均正确——
> 主节点 [2] `snapshotOffset` 在写锁内捕获（replication_handler.go:68）、RDB 视图同锁内
> （:69）、`+FULLRESYNC` 通告偏移 = 同一 S（:75）、解锁后排水自 S 起（:107-108）；
> 从节点 `lastOffset.Store(S)`（reconnect.go:306）→ LoadRDB → 排水 [S, mo)。
> **RDB（≤ S）与排水（[S, mo)）之间结构性不存在缺口/重叠**——交接链无缺陷，机制
> 确证不在交接处；剩余候选唯有负载/时序相关的排水-应用交互（与冻结同区）。
>
> **机制定案（2026-09-01 02:05 捕获轮，已修复）**：并发负载（--full 与 dw -count=30 同机
> 争用）下 dw 回归第 14 轮失败（**5,731 缺失值、send_drop=1、EXTRA=0、so==mo 收敛**）。
> 机制链：**武装时钟跨连接泄漏**——`lastConvergedTime` 连接建立时不重置，上一连接/上一
> 测试迭代的收敛（30s 武装窗内）为当前连接排水期的瞬时停顿武装停滞检测 → 排水期停滞
> 误触发（idle=2.55s、lag=44）→ 强制重连 → PSYNC offset 撞 `StartsAtCommandBoundary`
> → "非命令边界"降级**第二次 FULLRESYNC** → 数据丢失（offset 收敛但数据不完整）。
> **修复**：连接建立时 `lastConvergedTime.Store(0)`——排水期停顿不再武装、无排水期误触发、
> 无降级链；停滞检测仍覆盖"收敛后尾巴冻结"（本连接收敛后）。守卫：
> `TestSlaveReconnector_readCommandLoop_NoStallDuringDrainAfterPreviousConvergence`。
> 验证：守卫 10/10 绿（远端 -race）+ dw 修复验证批 + --full（后续）。
>
> **扩展修复（同日）**：--full 复跑确认**冻结仍复现**（SnapshotFullresyncOffset：排水推进到
> lag=162 后尾巴停滞 40s、停滞检测全程失敏（未收敛不武装）、无恢复 → 收敛超时）——
> 即"无收敛排水停滞"形状。**扩展修复**：停滞检查增加**排水冻结检测**——未收敛
> （追赶排水期）时仅**超长空闲**（> `replDrainStallTimeout`=10s）才判定冻结并强制重连自愈
> （瞬态发送停顿 idle=2.55s 不误判、真冻结 10s+ 触发）；已收敛（武装）仍按 2s 阈值。
> 守卫：`TestSlaveReconnector_readCommandLoop_DrainFreezeForcesReconnect`（未收敛 +
> 超长空闲 → 重连）+ `NoStallDuringDrainAfterPreviousConvergence`（瞬态空闲不误判）。
> 至此 §1c 两类形状（16k 缺失 = 武装泄漏误触发降级；冻结 = 排水尾滞失敏无恢复）
> 均有明确机制与修复。
>
> **长期验证追加（2026-09-02）——残留确凿**：并发负载（--full -p=2 等价的包间争用）下
> dw 回归 **6/6 批次全部复现** 16k 缺失（失败率 2/15~5/15；--full 下 dw 亦 FAIL：9,380 缺失 +
> 5 停滞 WARN + 4 降级；同轮 SnapshotFullresyncOffset 73.7s 失败、split-brain 家族仍失败、
> TestClusterSoak 失败）——**修复（武装重置 + 排水冻结检测）对"负载间隙（10.5s→20-21s，
> 随争用增长）触发排水冻结误判 → 强制重连 → PSYNC 非命令边界 → 降级第二次 FULLRESYNC →
> 数据丢失"链不足**，阈值调优本质脆弱（间隙分布与冻结检测重叠）。
> **机制精炼（探针证据）**：①RDB 生成遍历完整（short-walk 0）②RDB 加载完整
> （loaded==rdb_len）③**排水#2 应用计数 ≈ 缺失值**（dw_apply ~1,400-2,000/列表 vs 缺失
> ~1,700/列表）——命令已到达并执行但效果从 store 丢失（应用后丢失；候选：后续
> FULLRESYNC 的 FlushDB 链 / store 侧）。**下一步**：恢复路径重设计（排水冻结恢复不
> 打断可恢复排水；降级路径 FULLRESYNC 链探针——本轮探针已清理还原）。
>
> **时间线探针追加（2026-09-02 04:05）**：排水期 LLEN 采样 + 应用计数（44 采样
> **全部 applied==llen**——物化正常、无 LLEN 骤降）——**FlushDB 链与 store 侧非物化
> 均排除**；损失维持"排水#2 应用后效果丢失"结论（缺失 ~1,838/2,000 每列表 ≈
> 应用计数）——机制未获更细定位，候选收窄至恢复路径交互（强制重连/降级时刻
> 的 in-flight 数据交接；重连后 PSYNC 重放的排水区间与已应用区间的字节级对齐）。
>
> **交接探针追加（2026-09-02 04:37）**：主侧 FULLRESYNC#2（恢复）= S2=1264944 + RDB
> 143KB（≈2,000/列表）+ 排水 [S2, so]（≈250/列表，远小于 RDB）；从侧收敛时
> **final llen == applied（~2,000/列表）**——最终列表 == 应用计数，**恢复的 RDB#2
> 内容（pre-S2 值）未出现在最终列表**；缺失 ~1,966/列表 ≈ RDB#2 内容——**缺失归属：
> 恢复 FULLRESYNC 数据未完全物化**（RDB#2 加载值在最终状态缺席；加载完整性已验
> （loaded==rdb_len）+ FlushDB 链已排除——机制候选收窄至恢复期 RDB 加载与排水应用
> 的交互/覆盖时序）。
>
> **字节级审计追加（2026-09-02 05:32）——新模式：4 批连续全绿**：审计探针（主侧
> 排水区间 [S2, so] + 从侧 FULLRESYNC S/收敛 so——仅日志、无 store 读）下 dw 回归
> **4/4 批全绿**（15/15×4、60 计数 0 失败 0 停滞——2026-09-02 02:29 起调查以来首次）；
> 而历史 6/6 失败批（含本调查全部带 store 读探针的批次——LLen/遍历续走——badger
> 读事务与写路径争用）——**store 读争用与残留触发强相关**（候选机制：读事务争用
> 加剧主/从写路径延迟 → 排水数据间隙 → 冻结误判链）。负载/时段不确定性未排除
> （同一负载 02:29-04:17 失败 6/6、04:44-05:32 全绿 4/4）。**生产等价场景评估**：复制
> 期间外部读（客户端查询）对从节点写路径的争用——若确认，需为复制路径隔离
> 读负载或复核冻结阈值。探针已清理还原。
>
> **A/B 对照确认（2026-09-02 05:48）**：同一负载/主机/时段（05:34-05:48，与全绿批
> 04:44-05:32 同期）下，唯一变量 = 从侧每 10 ACK 对 dw:list:0-4 执行 LLen 纯读
> （结果丢弃、最小日志）——**失败复现：9 PASS + 6 FAIL + 56,431 缺失 + 6 停滞
> （失败率 40%）vs 纯日志 4/4 批全绿（0 失败 0 停滞）**——**store 读争用确认是
> 残留触发根因**（slave 侧读事务与排水应用写路径的争用 → 写延迟 → 数据间隙 →
> 冻结误判链）。**修复方向**：①复制路径读隔离/降级（读不阻塞复制写）；②或复核
> 排水冻结阈值（读争用下间隙的分布）；③或从侧复制写路径与读路径的存储级
> 优先级隔离。生产等价场景（复制期间外部读）已被 A/B 直接模拟并确认。
> 探针已清理还原。
>
> **修复验证（2026-09-02 06:14）——30s 阈值部分有效**：实施 `replDrainStallTimeout`
> 10s→30s（读争用间隙 ~10-21s 放行、真冻结 40s+ 仍在收敛窗内触发）后，同一 A/B
> 设置（30s 修复 + LLen 读探针）下 dw 回归 **13 PASS + 2 FAIL + 19,973 缺失 + 2 停滞**
> （对照 10s 基线 9 PASS + 6 FAIL + 56,431 缺失）——**失败 6→2（-67%）、缺失 -65%——
> 部分有效但未完全消除**（2 例间隙 > 30s——阈值本质脆弱再确认：读争用间隙随
> 争用增长无上界）。**结论：阈值调参仅为部分缓解；完整修复需结构性方案**
> （复制写路径与读路径的存储级隔离 / 恢复路径重设计——见上）。30s 阈值作为
> 部分缓解保留；真冻结检测（40s 收敛窗内触发）不受影响。
>
> **无回归验证（2026-09-02 06:21）**：30s 阈值下守卫测试（停滞/冻结 4 守卫）远端
> `-race` **全 PASS**（3.4s）+ `TestRegressionSnapshotFullresyncOffset` 独立运行 **PASS**
> （52.3s，integrity 子测试通过）——**真冻结检测 30s 兼容确认**（40s 收敛窗内仍
> 触发自愈），30s 修复无回归。
>
> **--full 验证（2026-09-02 06:54）——30s 修复部分有效再确认**：当前 main（30s 阈值）
> 的 --full（-p=2 最强争用）下 dw 回归仍 FAIL（81.8s、9,274 缺失、1 停滞 + 2 降级——
> 对照 10s 时代 --full：9,380 缺失、5 停滞 + 4 降级）——**停滞 5→1（-80%）、降级
> 4→2——30s 修复在最强争用下仍部分有效，但残留（阈值超限间隙）仍在**；另
> TestRegressionSplitBrainConvergenceHarden FAIL（20.8s）——已知负载敏感 flake 家族
> （documented-unreliable，非 §1c）。**结论：30s 阈值缓解读争用误判的多数情况，
> 但残留间隙无上界——完整修复仍需存储级读写隔离（见上）。**
>
> **最终门禁验证（2026-09-02 07:46）**：tier-A PR-gate **全绿**（store/server 基准均在
> 10% 阈值内、单测 12 包全 ok、Fast integration 通过）+ **Redis 三套兼容 100%**
> （py 247/247、node 122/122、cli）+ lint 0 issues——当前 main（`63b5c8c` 30s 修复 +
> 文档链至 `dda7408`）门禁与兼容健康确认；工作区干净、与 origin/main 同步。
>
> **结构修复代码级穷尽调查（2026-09-03 02:00）——修复定夺的边界**：badger v4.9.6
> 代码级追查定案：store 读（`db.View` 只读事务——无 conflictKeys/pendingWrites）与写
> （`db.Update` 读写事务——冲突检测提交）**均经 oracle `readTs()` 分配 readTs**（txn.go:
> 83-93——readMark.Begin + txnMark.WaitForMark——**读事务创建等待在途写刷盘**）；写提交
> 的 `hasConflict`（txn.go:126-151——自身 reads vs 近期提交事务写入）→ `ErrConflict` →
> retryUpdate 退避（1-50ms）。**修复候选评估**：①`DetectConflicts=false`——**否决**
> （keyLockMgr 覆盖不全——set/json/geo 等大量读写改写操依赖冲突检测保读改写安全——
> 数据完整性风险）；②阈值调参——已做（30s 部分有效，逼近 40s 收敛窗上限）；
> ③恢复路径重设计——未定。**修复阻塞点**：读扰动写路径的**确切运行链需写路径埋点
> 实证**（读创建 readTs 等待 vs 提交冲突 vs oracle 锁——观测基础设施缺失：INFO 无
> 重试指标、collector.Snapshot 无服务端消费）；badger 共享 oracle/提交机制使读写隔离
> 属架构级改造。已加 retryMetrics.conflicts 计数（运行期可见性——供后续实证使用）。
>
> **实证精化（2026-09-03 02:00）——写路径的极度敏感**：写提交延迟探针（>10ms 慢写
> 日志）+ LLen 读探针的实证 A/B：**+LLen 2/15 FAIL + 415 条慢写 vs 纯对照（无 LLen、
> 仅写路径日志探针）2/15 FAIL + 463 条慢写**——对照组无读也失败、慢写同量级——
> 结合早期纯日志（复制层日志、**写路径外**）4/4 全绿：**写路径对任何额外扰动
> （读争用/写路径内日志 I/O）极度敏感 → 排水时序扰动 → 间隙 → 冻结误判**；慢写
> （>10ms 写提交）为并发争用固有（415-463 条/批）。**修复方向收窄**：写路径内任何
> 额外开销均为扰动源（生产写路径须保持零额外日志/读）；残留修复仍需存储级
> 隔离或恢复路径重设计（候选与阻塞同上——未定）。探针已清理。
>
> **武装态实证 + 40s 阈值验证（2026-09-03 02:17）——阈值调参路径定性失败**：
> 武装态验证运行（dw_armed1）捕获停滞 **2 条均 armed=false、idle≈30.4s**（30,446/
> 30,399ms——读争用间隙刚越 30s 即误判）——武装态 2s 假设否决。据此将阈值
> 30s→40s 后验证（dw_40s1）：**12 PASS + 3 FAIL + 27,137 缺失（vs 30s 基线 2/15）——
> 未消除失败、反而略差——间隙自然长度越 40s（误判在阈值处截断间隙——间隙随
> 阈值增长无上界）——阈值调参路径定性失败（10s→30s→40s 全部部分/无效）**。
> **结构修复定论**：残留的完整修复不在阈值维度——需存储级读写隔离（读事务与
> 写提交的架构级隔离——badger 共享 oracle/提交机制）或恢复路径重设计（防
> 误判重连的数据丢失）——具体设计仍未定（见上候选评估）。当前 main 保持 30s
> 阈值（63b5c8c 部分缓解——2/15 为已知最佳调参状态）。
>
> **恢复路径聚焦定案（2026-09-03 02:55）——丢失链 + 偏移发散（字节级实证）**：
> 丢失链实证定链：**误判（armed=false idle≈30-40s / armed=true idle≈2.4s——读争用
> 间隙触发）→ 强制重连 → PSYNC 于从侧 watermark → 主侧 `StartsAtCommandBoundary`
> 检查失败（watermark 落主侧流命令中间）→ 降级 FULLRESYNC#2 → 数据丢失**（降级后
> offset 收敛 mo==so 但数据仍缺——丢失在恢复的 RDB/排水内容层面）。**字节级探针
> （降级点 dump watermark±12 字节）决定性证据**：watermark 落命令中间的**参数起始**
> （如 `…hset…$9\r\n[d]w:hset:1…`、`…$31\r[\n]dw:converge:…`——恰在 `$N\r\n` 之后）——
> 从侧 offset 推进（len(serializeCommand(req.Args))）与主侧流真实边界**累积分发**。
> **代码级闭合矛盾**：主侧 backlog=serializeCommand(cmd)（replication.go:383）、
> 排水直发 backlog 原始字节（SendBacklogData）、从侧 ReadRESP 按帧精确消费
> （io.ReadFull）、从侧重序列化同函数——四者闭合应无发散——但实证发散存在
> （算术根不可定：残差假设穷尽——解析/序列化/发送路径均无不对称）。**修复候选**：
> ①从侧按实际消费字节推进（候选 A——发散消除的根修复——需解析器改造波及 12+
> 调用点且代码级分析示其为 no-op——价值不确定）；②边界回退重放（双重应用——否决）。
> **定论**：阈值调参（上）与恢复路径边界检查均非完整修复所在——从侧 offset 推进
> 与主侧流边界的**结构性不一致**（算术根待运行期逐命令长度埋点定位）为残留根因
> 的最后形态——完整修复仍未落地（入册待续）。探针已清理。
>
> **环读错位候选否决（2026-09-03 03:00）**：排水路径 `SendBacklogData` →
> `GetRange(start,end)` 逐字节 `buffer[(start+i)%size]` 读取——**回绕由取模透明
> 处理、与 Append 写入位置（offset%size）自洽——排水流与 backlog 字节一致、
> 无错位**。发散根代码级排除闭合：主侧 backlog 序列化（replication.go:383）、
> 排水直发原始字节（SendBacklogData/GetRange）、从侧按帧精确消费（ReadRESP
> io.ReadFull）、从侧重序列化同函数（reconnect.go:474/497）——**五环节全部验证
> 精确、无不对称——发散的存在性矛盾在代码级不可解**。**最终定论**：从侧 offset
> 推进与主侧流边界的发散，其算术根仅在运行期可见（逐命令记录从侧重序列化
> 长度 vs 实际消费字节——定位发散的命令类型/时机），代码级静态分析已穷尽；
> 完整修复依赖该运行期定位（入册待续——下步：从侧消费字节埋点对照）。
>
> **运行期定位定论（2026-09-03 03:20）——发散悖论**：实施消费字节计数（ReadRESP
> Array.Consumed——数组头/元素头/body 精确累计）+ 从侧逐命令对照（re-serialized
> vs 实际消费）——dw 运行（dw_mm1：11 PASS + 4 FAIL + 4 降级）**全程 0 条失配**——
> 每条数据命令的 re-serialized 长度 == 实际消费（从侧解析自洽）。叠加已验证的
> 精确链接：主侧序列化、排水环读（取模）、从侧按帧消费、从侧重序列化同函数、
> ReadBulkString 精确消费（$len 行 + length+2）、**FULLRESYNC RDB-vs-S 原子一致**
> （replication_handler.go:58-69——snapshotMu 写锁内捕获 snapshotOffset + 生成 RDB，
> fullresync_boundary_test 守卫）——**七环节全部精确——但从侧 watermark 仍在主侧
> backlog 落命令中间（字节证据）——发散悖论**：从侧累计消费 == 自身流位置，而
> 该位置在主侧流中非边界——**唯一剩余解释：从侧收到的流 ≠ 主侧 backlog 的
> [S, mo) 字节序列（某处流级错位——运行期仅见）**。完整修复仍需更深运行期
> 对照（主侧逐命令发送记录 vs 从侧逐命令消费记录——对齐定位错位点）——入册
> 待续。探针已清理还原。
>
> **流程验证追加（2026-09-02 04:41）**：从侧 FULLRESYNC 流（tryReplicate）=
> ReadBulkString → LoadRDB（内含 FlushDB，rdb_loader.go:141）→ 之后才进入
> readCommandLoop 排水——**严格顺序、无加载/排水重叠**——"恢复期交互/覆盖"候选在
> 流程层面不被支持；损失机制在代码层面仍未定位（顺序正确 + 加载完整性已验 +
> 各候选排除）——下一步收窄至**主侧排水发送路径的字节级审计**（FULLRESYNC#2 的
> [S2, so] 区间内容 vs 从侧实际应用的命令集逐字节比对）。
>
> 另记（同日 tier-A 门禁）：`guard_bench.sh --server` 在本地 Mac M 系列上偶发误报——
> `BenchmarkParseScore` 噪声达 125~212ns/op（±40%），`+12%` 级"回归"为测量噪声而非
> 真实回归（server 基线 `testdata/bench_baseline_server.txt` 系 2026-07-18 生成，仅 5
> 样本）。tier-A 的 -race 阶段需经 remote-test.sh 执行（本机禁跑）。
>
> **一键验证**：
> `bash scripts/remote-test.sh -race -timeout 180s -v ./cmd/integration/regressions/ -run TestRegressionDuplicateWindowMeasurement`
> `bash scripts/remote-test.sh -race -short -timeout 600s ./internal/replication/... ./internal/server/...`

### 2. v8.52.0 发布遗留（非阻塞，2 项待观察）

| 项 | 现象 | 下一步 |
|----|------|--------|
| **node3 vlog 35GB 未回收** | `DEBUG GC` 在 node1/node2 完整回收（36G/32G → 1.1M），node3 返回 0 次重写：FLUSHDB 的 tombstone 卡在空 L0 层（3.6KB 表，score 0.00），无法下沉到 L5/L6 旧数据触发 discard 统计；`Flatten` 按 score 跳过空层。属 badger 机制限制，非命令缺陷 | ✅ **已解决（2026-08-31）**：自然 compaction 已触发（写流量→L0 堆积），node3 vlog 35GB → 1.1M+2GB 活跃（稀疏）；三节点 `DEBUG GC 0.5` `rewritten` 均=0（健康形态：vlog 已小、无 discard 可回收）；`/usr/local/boltdb_data` 实测 ~12M，磁盘 774G 空闲 |
| **SSD 基线复测（下次执行）** | 目标：拿到可信 SSD 写入基线（对比机械盘 28 MB/s）。**正确姿势**（2026-08-06 调查结论）：① 先确认 `DEBUG GC` 已完成（GC 与写入严重互斥，GC 期间 1MB SET 减速 1350×）；② `ps aux \| grep redis-benchmark` 确认无残留进程；③ 单进程 `redis-benchmark --cluster -h 10.1.2.16 -p 6337 -t set -n 65534 -r 20000000 -d 1048576 -c 50`（必须带 `-r`，否则覆盖写同一 key）；④ 记录吞吐 + DBSIZE 分布 + 磁盘占用；⑤ 测完 FLUSHDB 清理。备选：用已修复的 `scale-data-filler`（按 CLUSTER SHARDS/SLOTS 分组 pipeline）替代 benchmark | ⏳ 下次（预计 ~5 分钟/64GB）<br>**一键验证**：`./scripts/cluster-ops.sh gc --all` 后 `ps aux \| grep -v grep \| grep redis-benchmark \|\| echo ok`；再跑单进程 benchmark，`redis-cli --cluster -h 10.1.2.16 -p 6337 DBSIZE` 核对 65534 |

---

### 3. split-brain 家族 flake（**已定位：负载敏感时序，非真 bug** — 2026-09-01）

**现象**：--full（-p=2 包并行）下间歇失败（6 次/3 次 --full：TestSplitBrainConvergenceReplay、
TestRegressionSplitBrainConvergence(Harden)、TestRegressionFailoverOscillationScenarioD）。

**定位（A/B 证据）**：独立运行 4/4 PASS；与 regressions 同机并发（-p=2 等价的包间争用）
复现间歇失败（首轮 2/3 FAIL、次轮 3/3 PASS；移除 t.Parallel() 后仍复现 2/3 FAIL）——
**负载敏感时序扰动**（gossip HelloInterval 500ms + 收敛断言（MinAgreedFraction=1.0、
HEALTH>0.50、重连数≤5）在争用下间歇越界），非共识逻辑缺陷。

**处置**：①三个重测试移除 `t.Parallel()`（包内串行，降低包内争用；独立验证 4/4 PASS）；
②包间争用（-p=2 记忆守卫的刻意并行，改 -p=1 将拉长门禁至 ~55-60min）残留——家族
维持 documented-unreliable 状态（tier-A 跳过模式已有处理），发版验证以定向包
（regressions/守卫/dw）为准。

---

## 架构边界（已决策：不做）
| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射（Issue #3 草案） | 已由 `processRequest` 读锁跨越 commit→Append 实现线性绑定，无需映射表；详见 `docs/failures/snapshot-inconsistency.md` §4 |
| 完全线性化 FULLRESYNC | ✅ 已实现（2026-08-30）；远程 dw / overlap / PSYNC / strict soak PASS；`./cmd/integration/ -short` 全绿 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| FUNCTION / FCALL / FCALL_RO | 随 Lua 排除（FUNCTION 是 Lua 引擎的容器命令） |
| HEXPIRE 系列（12 个：HEXPIRE/HEXPIREAT/HEXPIRETIME/HPEXPIRE/HPEXPIREAT/HPEXPIRETIME/HPERSIST/HPTTL/HTTL/HSETEX/HGETEX/HGETDEL） | Hash 字段级 TTL（Redis 7+）：需要 Hash 存储格式变更（字段级过期元数据），风险高收益低，明确不做 |
| Vector Set（12 个：VADD/VCARD/VDIM/VEMB/VGETATTR/VINFO/VISMEMBER/VLINKS/VRANDMEMBER/VREM/VSETATTR/VSIM） | Redis 8 实验性特性，API 不稳定，不做 |
| PFDEBUG / PFSELFTEST | HyperLogLog 内部调试命令（Redis 标记内部），不做 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL (partial) / FUNCTION / MIGRATE | FUNCTION 随 Lua 排除；MIGRATE 不适用（嵌入式）；ACL 仅实现 CAT/LIST/USERS/WHOAMI/HELP，SETUSER/DELUSER 未实现 |
| 1TB+ 规模化验证 | 无硬件条件（仅有 256GB HDD 测试环境） |
