# BoltDB 待办列表

> 2026-08-27 已清理：已完成项全部归档至 git 历史（`git log -- docs/plans/TODO.md`），本文件仅保留待办/延期/已决策不做三类。

## 待办 / 延期中

### 0. 下次继续（2026-08-30）

1. **1c LIST 亏空仍待 `missing>0` 那次** —— 计数器已落地。2 次远程 `-race -v`：
   `send_drop=0` 与 `send_drop=1`，`apply_skip=0`，LIST 都全等。`send_drop=1`
   被 backlog/FULLRESYNC 收回。下次亏空时读这两个计数。不要收紧 dw 的 lag==0。
2. **push** —— 属对外可见动作，需明确授权后再做。
3. **reword `088ce37` 与 `14bd901` 的提交信息** —— 两条仍带着"手工摆出的交错即根因"这一
   过强表述（含 `first byte="\r"`）。文件内容已由 `c84293f`/`f4cec87` 更正，但提交信息未改；
   且这两个提交不在栈顶，reword 会改写其后提交的 hash —— 重写历史，需授权。
4. **Issue #3 补一条更正评论** —— 外部评论仍沿用过强表述；文档侧已改写为实测口径。需授权。
5. **Issue #3 客户端写路径已收口** —— 守卫不再 Skip。`SOAK_REPL_STRICT_EQUALITY=1`
   可以开来做 soak，默认仍关。

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

### 1c. dw 回归偶报 1 个 LIST 元素亏空（**计数器已落地；待 dw 实测归因**）

> **现象**（远程 `-race`，`TestRegressionDuplicateWindowMeasurement`）：5 个 INCR key 全部 `gap=0`，
> 但 `dw:list:1 master_len=2019 slave_len=2018 → missing_on_slave=1`，且此时
> `converged at iter 13 (mo=1341949 so=1341905)` —— **从节点 offset 仍差 44 字节**。
> 本会话修复前也出现过同形状（`dw:list:3 master_len=2114 slave_len=2113`），非新引入回归。
>
> **已排除**：
> 1. 收敛判据误判（overlap 用例那条已证为判据问题，但 dw 用的是 marker 判据，marker 已到、
>    而亏空在中间位置，无法用"尾部未送达"解释——流是有序的，中间挖不掉洞而不破坏帧）。
> 2. 解析→再序列化不等长：`TestReplicatedCommandRoundTripsByteIdentically` 覆盖 14 种形态
>    （空值、空 key、二进制、内嵌 CRLF、UTF-8、多参数、marker 形状、8KB 长值…）全部字节等同。
>
> **剩余怀疑对象**：`PropagateCommand` 里"发送失败只 warn 不重投"（`slave.SendCommand` 返回错误即丢弃，
> 依赖对端读侧发现断连）与 `readCommandLoop` 的 `isTransientReplicationError` 分支
> （**推进 offset 但不 apply**，为保字节锁步而刻意丢数据）。后者按设计就会造成"offset 对得上、数据少了"。
>
> **已落地（2026-08-30）**：两条丢弃路径现为原子计数器，经 `INFO replication` /
> Prometheus / dw 用例可读：
> - 主：`repl_send_drop_count` / `GetReplSendDropCount()` — live `SendCommand` 失败
> - 从：`repl_apply_skip_count` / `GetReplApplySkipCount()` — skip apply 仍推进 offset
>
> dw 断言 `apply_skip == 0`；LIST 亏空时把两个计数打进失败信息，一次跑即可判明
> 发送侧 / 应用侧 / 两条都不是。`send_drop` 不单独断言为 0（CLIENT KILL 期间 live
> push 失败在设计上可能发生，命令仍在 backlog）。
>
> **实测（2026-08-30，远程 `-race -v`）**：
> 1. 未复现亏空。`send_drop=0 apply_skip=0`，INCR/LIST/HSET 全等，`mo==so=1358224`。
>    CLIENT KILL 没打出 send_drop。
> 2. （北京时间 07:43 再跑）仍未复现亏空。`send_drop=1 apply_skip=0`，LIST 五把 key
>    全等（`missing_on_slave=0`），INCR 全等，`mo==so=1378327`。CLIENT KILL 窗口
>    打出 1 次 live `SendCommand` flush 失败（`use of closed network connection`），
>    数据仍对齐——send_drop 被 backlog/FULLRESYNC 收回。亏空仍待下次 `missing>0`
>    时读这两个计数。
>
> 在定位前不要收紧 dw 的 lag==0 判据（会让用例常红且无法区分成因）。
>
> **一键验证**：
> `bash scripts/remote-test.sh -race -timeout 180s -v ./cmd/integration/regressions/ -run TestRegressionDuplicateWindowMeasurement`

### 2. v8.52.0 发布遗留（非阻塞，2 项待观察）

| 项 | 现象 | 下一步 |
|----|------|--------|
| **node3 vlog 35GB 未回收** | `DEBUG GC` 在 node1/node2 完整回收（36G/32G → 1.1M），node3 返回 0 次重写：FLUSHDB 的 tombstone 卡在空 L0 层（3.6KB 表，score 0.00），无法下沉到 L5/L6 旧数据触发 discard 统计；`Flatten` 按 score 跳过空层。属 badger 机制限制，非命令缺陷 | ⏳ 待自然 compaction 后重跑 `DEBUG GC`（有写流量触发 L0 堆积后即可）<br>**一键验证**：`redis-cli -h 10.1.2.16 -p 6337 DEBUG GC 0.5`（三节点分别执行，对比 `rewritten`；node3 为 0 则待下次写流量后再试） |
| **SSD 基线复测（下次执行）** | 目标：拿到可信 SSD 写入基线（对比机械盘 28 MB/s）。**正确姿势**（2026-08-06 调查结论）：① 先确认 `DEBUG GC` 已完成（GC 与写入严重互斥，GC 期间 1MB SET 减速 1350×）；② `ps aux \| grep redis-benchmark` 确认无残留进程；③ 单进程 `redis-benchmark --cluster -h 10.1.2.16 -p 6337 -t set -n 65534 -r 20000000 -d 1048576 -c 50`（必须带 `-r`，否则覆盖写同一 key）；④ 记录吞吐 + DBSIZE 分布 + 磁盘占用；⑤ 测完 FLUSHDB 清理。备选：用已修复的 `scale-data-filler`（按 CLUSTER SHARDS/SLOTS 分组 pipeline）替代 benchmark | ⏳ 下次（预计 ~5 分钟/64GB）<br>**一键验证**：`./scripts/cluster-ops.sh gc --all` 后 `ps aux \| grep -v grep \| grep redis-benchmark \|\| echo ok`；再跑单进程 benchmark，`redis-cli --cluster -h 10.1.2.16 -p 6337 DBSIZE` 核对 65534 |

---

## 架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射（Issue #3 草案） | 已由 `snapshotMu` 临界区实现线性绑定，无需额外映射表；详见 `docs/failures/snapshot-inconsistency.md` §4 |
| 完全线性化 FULLRESYNC | ✅ 已实现（2026-08-27 `snapshotMu`）；严格校验待远程验证 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| FUNCTION / FCALL / FCALL_RO | 随 Lua 排除（FUNCTION 是 Lua 引擎的容器命令） |
| HEXPIRE 系列（12 个：HEXPIRE/HEXPIREAT/HEXPIRETIME/HPEXPIRE/HPEXPIREAT/HPEXPIRETIME/HPERSIST/HPTTL/HTTL/HSETEX/HGETEX/HGETDEL） | Hash 字段级 TTL（Redis 7+）：需要 Hash 存储格式变更（字段级过期元数据），风险高收益低，明确不做 |
| Vector Set（12 个：VADD/VCARD/VDIM/VEMB/VGETATTR/VINFO/VISMEMBER/VLINKS/VRANDMEMBER/VREM/VSETATTR/VSIM） | Redis 8 实验性特性，API 不稳定，不做 |
| PFDEBUG / PFSELFTEST | HyperLogLog 内部调试命令（Redis 标记内部），不做 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL (partial) / FUNCTION / MIGRATE | FUNCTION 随 Lua 排除；MIGRATE 不适用（嵌入式）；ACL 仅实现 CAT/LIST/USERS/WHOAMI/HELP，SETUSER/DELUSER 未实现 |
| 1TB+ 规模化验证 | 无硬件条件（仅有 256GB HDD 测试环境） |
