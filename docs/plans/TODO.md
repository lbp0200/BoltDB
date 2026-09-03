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
>    （S0 引擎研究 ✅ → S1-A 应用层 key 锁先行（kvrocks 式——A 方向已裁决——S1-A1 锁层 →
>    S1-A2 切引擎）→ S2 复制切换 → S3 快照全量——每阶段可回滚——dw A/B 门槛 ≤1/15）——
>    计划与设计见 [a4-engine-seq-replication.md](a4-engine-seq-replication.md)（§8 S0 结论/
>    §9 S1 阻断裁决/§10 S1-A 设计）；A2 复制应用批量合并（随 A4 的写批单位自然覆盖——
>    不单独立项）。
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
