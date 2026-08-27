# BoltDB 待办列表

> 2026-08-27 已清理：已完成项全部归档至 git 历史（`git log -- docs/plans/TODO.md`），本文件仅保留待办/延期/已决策不做三类。

## 待办 / 延期中

### 1. FULLRESYNC 线性边界（Issue #3 — 已实现，待验证）

> **状态：已实现（2026-08-27）** — `store.snapshotMu` RWMutex 将 `snapshotOffset` 捕获与 MVCC View 原子绑定，消除微秒级重复窗口。
> **Issue：** https://github.com/lbp0200/BoltDB/issues/3 — `Implement linearizable FULLRESYNC boundary`

**实现**：`BotreonStore.snapshotMu`（写锁覆盖 `GetMasterReplOffset()→GenerateRDBWithSnapshotLock(View)`，`retryUpdate` 持读锁）；`replication_handler.go` 在 FULLRESYNC 快照阶段持写锁，RDB 生成完即释放；回归校验收紧为严格相等。

**待验证**：
1. `TestRegressionSnapshotFullresyncOffset` 严格相等（已由本改动收紧 veirfyIncr/verifyList 为 `sv==mv`，零重复）
2. `TestRegressionDuplicateWindowMeasurement` 零重复（INCR/LPUSH 零容忍）
3. `TestSoakReplication` 严格模式 `SOAK_REPL_STRICT_EQUALITY=1` 恢复硬校验

详见 `docs/failures/snapshot-inconsistency.md` §4。

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
