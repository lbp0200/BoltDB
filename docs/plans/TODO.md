# BoltDB 待办列表

## 待办 / 延期中

### 0. 需授权/需手动的外部动作

1. ~~reword `088ce37` 与 `14bd901` 的提交信息~~ —— ✅ **已评估不执行（2026-09-03）**：
   两条消息的过强表述（"手工摆布直接证明"）已由紧随其后的 `c84293f`（"更正先前的过度
   声明"）与 §1b 自我更正记录纠正——历史自纠正；reword 会重写其后全部 67 个提交 hash
   （含 §1c 定论链）并需 force-push——措辞修正的收益 < 历史重写成本。保留此裁决以防
   将来再议。
2. **Issue #3 更正评论** —— ✅ **已发布（2026-09-03，gh CLI）**：更正 2026-08-29
   "Verification result" 评论的过强表述（"2 losses = delivery gap"实为收敛判据误判——
   判据收紧 lag==0 后 2/2 精确收敛；commit-seq 映射已被锁绑定收口否定——严格断言全绿）。
   评论：https://github.com/lbp0200/BoltDB/issues/3#issuecomment-5520004674

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
>    INFO 累计计数分辨率不足，需临时探针采集）。
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
>    **候选下一步**：1. ~~D 覆盖继续扩展~~ ✅（全写面完成——2026-09-04）；2. S2 ②/④ 增量
>    （日志值全重放形式——D4——部署与读侧切换同步——仍延后）；3. 复制层读侧（S2 主体——
>    **log-key 增量流先行（分级 2/3 合并——零回传）**——下一步方向（2026-09-04——feed
>    传输设计定案 `a1b1cb3`）：① wire 形态落地（(ts, 全命令) 条目——新命令类型/ts 注记
>    ——实施细定项）；② master 侧按 ts 增量发送（`ReplLogEntriesFrom`——6785b82——从侧
>    请求 ts 起扫描）+ 从侧 (ts, 全命令) 解析 + lastAppliedTS 应用侧推进（CONTINUE 落盘
>    持久化）；③ ACK-ts（**applied 语义**——received-only 仅协议测试不进判据）；
>    ④ PSYNC (replId, ts)（整数边界——StartsAtCommandBoundary 字节映射退役）；⑤ 4 守卫
>    重写 + ts 单调/重放守卫 + dw A/B ≤1/15——大设计——a4 §10 附7 定案锚）；4.
>    ~~C5 开销归因~~ ✅（2026-09-03——同条件 A/B 定论：
>    初测 +30-37% 为机器漂移伪影——D 开销未可观测）。
> 3. 发散悖论（C4）根因定位：主侧发送字节 vs 从侧接收字节的抓包级直接比对（层 D——
>    外部工具）——恢复路径重设计（层 C：降级无损化）的前提。
>
> 一键验证：`bash scripts/remote-test.sh -race -timeout 180s -v ./cmd/integration/regressions/ -run TestRegressionDuplicateWindowMeasurement`

### 2. v8.52.0 发布遗留（非阻塞）

| 项 | 现象 | 下一步 |
|----|------|--------|
| **SSD 基线复测（2026-09-03 已执行——爬行中止）** | 目标：可信 SSD 写入基线（对比机械盘 28 MB/s）。**正确姿势**（2026-08-06 调查结论）：① 先确认 `DEBUG GC` 已完成（GC 与写入严重互斥，GC 期间 1MB SET 减速 1350×）；② `ps aux \| grep redis-benchmark` 确认无残留进程；③ 单进程 `redis-benchmark --cluster -h 10.1.2.16 -p 6337 -t set -n 65534 -r 20000000 -d 1048576 -c 50`（必须带 `-r`，否则覆盖写同一 key）；④ 记录吞吐 + DBSIZE 分布 + 磁盘占用；⑤ 测完 FLUSHDB 清理。备选：用已修复的 `scale-data-filler`（按 CLUSTER SHARDS/SLOTS 分组 pipeline）替代 benchmark | ⚠️ **实测（2026-09-03 11:17-11:40）**：GC 前置 rewritten=0 健康 → 基准启动 ~166 ops/s（≈166 MB/s）后**崩塌至 ~5 ops/s**（21 分钟仅 9.5%——全量需 ~4 小时——非 ~5 分钟）——**中止**——持续负载下写路径塌陷（疑 L0/vlog 压实风暴）为实测发现；FLUSHDB 清理 ✓（DBSIZE 归零）；vlog 6.3G 残留为已知 badger 机制（tombstone 卡空 L0——自然压实回收——同 §2 vlog 单元）。**备选**：scale-data-filler（分组 pipeline）或分段小批量重测 |

> node3 vlog 35GB 未回收 —— ✅ **已解决（2026-08-31）**：自然 compaction 已触发，
> 35GB → 1.1M+2GB 活跃（稀疏）；三节点 `DEBUG GC 0.5` `rewritten` 均=0（健康形态）。

## 架构边界（已决策：不做）
| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射（Issue #3 草案） | 已由 `processRequest` 读锁跨越 commit→Append 实现线性绑定，无需映射表；详见 `docs/failures/snapshot-inconsistency.md` §4 |
| 完全线性化 FULLRESYNC | ✅ 已实现（2026-08-30）；远程 dw / overlap / PSYNC / strict soak PASS |
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
