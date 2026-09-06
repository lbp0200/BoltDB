# BoltDB 待办列表

> 本文件只列**未完成**的工作。已完成阶段的过程记录归入对应设计文档（`a4-engine-seq-replication.md`、
> `1c-complete-fix-design.md`）或下方「已收口」索引；逐提交细节以 `git log` 为权威。
> 2026-09-05 整理：删除 296 行中已完成的堆积记录，迁入上述文档。
> 2026-09-06 整理：删除已收口大节（§1 阶段 1 / §5 C4 / §6 lost 过程记录 / §7 扫面批次与审计——
> v8.58.0 发版内容，见 CHANGELOG.md + 下方已收口索引）。

## 待办

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
  **挂起（2026-09-06——用户决定暂缓）**：10.1.2.16（GCP VM elex-gm0135）暂时
  不可达（网络——**过几天恢复可用——非弃用**）——恢复后 SSD 基线回 10.1.2.16
  测（SSH 接入：elex-gm0135/~/.ssh/google_compute_engine——部署集群 + 全流程
  测量——工具与前置三查已就绪）——192.168.1.251（实测 HDD 3.6T——非 SSD——
  无 boltDB 集群部署——6337/6338/6339 无监听）仅作暂缓期参考（HDD 写路径行为
  不同——基线不可比）
- 前置三查：① `DEBUG GC` 已完成（GC 期间 1MB SET 减速 1350×）；② 无残留 redis-benchmark；
  ③ `-r` 必带（否则覆盖写同一 key）；测完 FLUSHDB

### 5. 遗留鲁棒性：RDB 生成/载入侧静默点（候选 ⑥/⑦——非 lost 机制——同类静默丢数据风险）

lost 调查产出（2026-09-05/06——探针实测裁决后剩余**未落地建议**——与 lost 签名不符
（MISSING ≠ 计数偏小）——鲁棒性缺陷而非已观测丢失——**建议改为显式失败**）：

- **生成侧（rdb.go:475-496 起）**：键类型读失败（`item.ValueCopy` err）、字符串值读失败
  （`readStringInTxn` err）都是 `logger.Warn` + `continue`——该键直接不进 RDB，而
  FULLRESYNC 仍照常成功交付；`WriteStringKeyValue` 失败也只 Warn、不中止不返回错误。
  表现 = 从侧永久少键，主从两侧都不报错。与本仓库纪律（`verifyFeedTSContinuity`
  ——「迭代器离散即断开」不静默跳过空洞帧）同型——**建议同样改为显式失败**
  （快照不完整就别交付）。
- **载入侧（rdb_loader.go:197 起——四处）**：① `expireTime, _ := readExpireTime()`
  丢弃解码错误 → ttl 保持 0 → 带 TTL 的键被**永久化为不过期**（从侧多出主侧已失效的键）；
  ② `if expireTime > now` 之外的分支——RDB 里已过期条目 ttl=0 同样变成**永不过期**而非跳过；
  ③ `typeByte, _ := ReadByte()` 忽略错误；④ `key, err := readString(); if err != nil {
  logger.Warn + continue }`——**二进制流解析中途错位后继续往下读**，其后所有条目按错位字节解析，
  却仍向上报"载入成功"。**建议：解析失败即整体失败**（返回 error 让 FULLRESYNC 重做），不 continue。

**当前状态**：格式正确的 RDB 不触发这些分支（往返保真探针 30/30 全保真——rdb_roundtrip_fidelity_test.go
——含 set/zset/TTL/全局 DBSize）；触发条件应是解码器与编码器在某类型上不一致（未知类型字节 /
长度编码边界 / `0xFFFFFFFF` 秒毫秒分界附近）——**未修（非紧急——正常帧不触发）**。

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
| **§6 并发 FeedSlave 重发（feedMu 游标锁）** | 2026-09-05 | a4 §10 附8.1 选项 1——e304a07 实施——post-fix `-count=5` 全绿（dup=0）/ pre-fix 10509ab worktree 红（2/2 轮 dup 4/4 键）——恒绿守卫 `concurrent_feed_slave_test.go`——新开放项（lost=1 偶发）见 TODO §6 |
| **C4 发散悖论（feed 模式结构性消失）** | 2026-09-05 | TODO §5——e1fd352——重连判定全程 ts 域（PSYNC-ts 整数比较 + 降级 FULLRESYNC + resumeTS+1）——字节边界不参与——仅字节路径残留（gate 1 退役后彻底消除）——层 D 降级可选验证 |
| §3 split-brain 家族 flake | 2026-09-01 | 负载敏感时序扰动（gossip HelloInterval 500ms），非共识缺陷；三重测移除 `t.Parallel()`；家族维持 documented-unreliable |
| **v8.58.0 发版（56 提交——lost 定论修复 + 等价扫面 32 例 6 确定性缺陷 + apply 审计 11 组 + backup managed 兼容）** | 2026-09-06 | `CHANGELOG.md` v8.58.0——checkFeedTSGap 修复（141 轮 0 lost）——internal 全 10 包无 -short 全绿 + 复制守卫三件套 + lint 0 issues |
