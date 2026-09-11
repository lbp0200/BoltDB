# 生产就绪评估与下一步路线（v8.60.0 后，2026-09-11）

> 状态：TODO 待办区已清空（§2/§3/§8/§9 收口；§6 记录移除，守卫保留）。
> 本文档回答“离生产就绪还差什么”，以及下一步的可选项。

## 一、已有证据（实验室就绪）

| 维度 | 证据 | 位置 |
|---|---|---|
| 正确性门禁 | CI test/test-fast/test-heavy/lint/bench 全绿合入 | `.github/workflows/go.yml` |
| 确定性复制 | SPOP→SREM、EXPIRE→PEXPIREAT、XADD id 固化等规范化 + 常驻守卫 | `deterministic_replay_test.go`、`expire_*_test.go` |
| 快照/重连 | FULLRESYNC 线性化、PSYNC 重连零丢失、done-前缀读（§9） | 回归守卫 + `drain_done_bounded_test.go` |
| 浸泡收敛 | strict soak 精确收敛 + Nightly Soak（漂移/退化不变量） | `soak_replication_test.go`、nightly pipeline |
| 混沌/降级 | kill/oom/disk-pressure/failover/停滞自愈（`--full`） | `cmd/integration/*` |
| 故障复盘纪律 | 每个历史缺陷有根因记录 + pre-fix 会红的守卫 | `docs/replication/historical-fixes.md` |

## 二、缺口（按优先级）

1. **零线上部署**：无真实流量/磁盘/时钟漂移/运维动作证据。再全的 CI 也只是实验室证据。
2. **容量天花板未知**：仅 256GB HDD 实测（NVMe 对照差 7 倍）；1TB+ 明确未验证。生产盘型直接决定生死。
3. **可用性故事不完整**：哨兵 ScenarioD 全票判据 flaky（时序敏感，非共识 bug，见 §三）；RPO/RTO 从未定义。
4. **可运维性未演练**：备份恢复、监控告警、升级回滚（删环后回滚需代码还原）无生产演练记录。
5. **安全纵深**：ACL 仅 CAT/LIST/USERS/WHOAMI/HELP；无认证体系。

## 三、哨兵 ScenarioD 说明（2026-09-11 定性）

3 哨兵 + 500ms gossip 心跳下断言 `MinAgreedFraction=1.0`（全票一致）：独立跑 4/4 过，
并发/`--full -p=2` 或慢 runner 下 gossip 多轮传播延迟即 2/3 → 判红。共识逻辑无 bug
实锤；判据比生产（Redis Sentinel 多数派 2/3）更严。若修，方向是判据放宽（多数派/
延长窗口/重试确认），测试-only，零生产风险。CI 现状：`test` 全跳过、`test-heavy`
（split brain 步骤）skip ScenarioD，其余场景覆盖中。

## 四、下一步可选项

| 选项 | 内容 | 风险/成本 |
|---|---|---|
| A. ScenarioD 判据放宽 | 改多数派一致，重跑远程验证，摘 skip | 低（测试-only） |
| B. 生产试部署 | 单实例 + 真实盘 + 监控，先影子流量 | 中（需运维投入） |
| C. 容量基线 | 目标盘型 + 目标数据量级的写入/压实基线 | 中（需硬件） |
| D. 可运维手册 | 备份恢复演练、升级回滚步骤、告警阈值 | 低（文档+演练） |
| E. 按兵不动 | 当前 v8.60.0 封存，等真实需求再动 | 无 |

## 五、决策记录

- 2026-09-11：§2 gate 1 关闭（无外部署，feed-only 唯一）；§6 known-open 记录移除；
  发 v8.60.0。下一步待定（本文档起草）。
