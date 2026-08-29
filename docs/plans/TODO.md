# BoltDB 待办列表

> 2026-08-27 已清理：已完成项全部归档至 git 历史（`git log -- docs/plans/TODO.md`），本文件仅保留待办/延期/已决策不做三类。

## 待办 / 延期中

### 1. FULLRESYNC 线性边界（Issue #3 — **实现未达成目标，窗口仍在**）

> **状态（2026-08-29 复核）**：`snapshotMu`（`d5e210d`、`ecaf9df`）已落地，但**没有消除重复窗口**。
> 结论由确定性复现证明（不依赖竞态）：`internal/replication/fullresync_boundary_test.go`
> → `TestFullresyncBoundary_CommittedButUnpropagatedWrite`（当前 FAIL）。
> **Issue：** https://github.com/lbp0200/BoltDB/issues/3 — `Implement linearizable FULLRESYNC boundary`（不可关闭）

**根因**：读锁只覆盖 badger 提交，**不覆盖 offset 赋值**。`PropagateCommand()`（`backlog.Append` + `IncrementReplOffset`）
在 `handler_core.go:808` 于 `executeCommand()` 返回后调用，位于锁外。
「已提交但未传播」的写入因此同时落在 RDB 与 backlog `[snapshotOffset, current)` 两侧 —— INCR/LPUSH 在从节点翻倍。
锁把 `offset 捕获 → View 开启` 变成原子，但需要原子的其实是 `commit → offset 赋值`。

**副作用**：写锁覆盖整个 RDB 生成期 → 全量同步期间**所有写入停摆**（DB 越大停摆越久），且暴露窗口比 §3 更长。

**待决策的修法**（详见 `docs/failures/snapshot-inconsistency.md` §4）：
1. 让同一把读锁跨越 `commit → offset 赋值`（须先去掉 `retryUpdate` 内的读锁，否则嵌套 RLock 遇等待中的写者必死锁；需覆盖 janitor/EXEC/SPOP 等全部写路径）—— 漏一处释放即永久冻结写入，风险最高
2. commitTs ↔ repl-offset 映射表 + 按 View `readTs` 裁剪 gap（正是此前被"锁已足够"这个假设否掉的方案）
3. 恢复 `d5e210d` 之前有界容忍

**在 1 或 2 落地前必须保持**：
- `TestRegressionSnapshotFullresyncOffset` / `TestRegressionDuplicateWindowMeasurement` 的严格相等断言（`sv==mv`、零去重）**不成立**，会 fail/flake
- `SOAK_REPL_STRICT_EQUALITY=1` 不要开启

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
