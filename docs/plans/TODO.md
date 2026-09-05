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

### 2. A4 阶段 2——删除 backlog 内存环（gate 严格——不可在线回滚）

删 `ReplicationBacklog` / `BacklogWAL` / `SendBacklogData` / `CatchUpAndEnableSlave` 字节循环
/ psync 字节分支。`--feed-loop` 保留为启动要求（回滚需代码还原）。

**退役三 gate（全部满足才可实施）**：
1. 字节从侧（PSYNC 3 参 / ts=0）完全退役——部署内从侧全量 feed-mode，无 ts=0 请求进入 PSYNC；
2. 换算表双轨核验持续通过（`ReplConversionTable.AlignCheck`——过渡期验证锚）；
3. feed-mode 规模验证零丢失持续（`TestRegressionPsyncReconnectNoLossFeed` 退役前再复跑）。

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

### 5. 发散悖论（C4）根因定位

主侧发送字节 vs 从侧接收字节的**抓包级**直接比对（层 D——外部工具）。是「恢复路径重设计
（层 C：降级无损化）」与 A4 S3 的前提。§1c 失败链的最后一环至今未实证。

### 6. 悬空改动：FULLRESYNC ts 移入写锁（未提交 + 缺区分守卫）

`internal/server/replication_handler.go` 工作区有未提交改动——第 4 字段由 `result.TS`
（`HandlePSync` 锁外读）改为 `snapshotTS`（`SnapshotMuLock` 写锁内读 `ReplLogCurrentTS`）。
即 a4 §10 附8「阶段 1 落点修正」。

- **已验**：本地 `go vet` 干净 + 远程 `-race`（replication 69.0s + server 50.9s）全绿
- **未完成 ①**：后果定级存疑——代码注释记「从侧去重兜底，仅无谓重传」，但若从侧 dedup 阈值
  正是这个过旧 ts，则 (staleTs, snapshotTs] 区间的命令已在 RDB 内又被重放 → **INCR/XADD/
  LPUSH 等非幂等命令双应用（发散）**。须读通 `reconnect.go` lastAppliedTS 与 dedup 路径定级。
- **未完成 ②**：`TestFullresyncTsDomainInvariant` **不具区分能力**（pre-fix 上同样通过）——
  需补一个在 pre-fix commit 上实测会红的守卫（`git worktree` 跑一次代价很低）。
- 未完成前**不得**标为「阶段 1 前置已完成」。

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
| §3 split-brain 家族 flake | 2026-09-01 | 负载敏感时序扰动（gossip HelloInterval 500ms），非共识缺陷；三重测移除 `t.Parallel()`；家族维持 documented-unreliable |
