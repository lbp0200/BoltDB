# Nightly Soak 运行提示词

当你收到"晚上运行"或"运行 nightly soak"的指令时，执行以下操作：

## 步骤 1：运行 standalone soak

```bash
cd /Users/lbp/Projects/BoltDB

CI_NIGHTLY_SOAK=1 \
SOAK_DURATION=1h \
SOAK_JSONL_DIR=/tmp/soak-data/jsonl \
SOAK_REPORT_DIR=/tmp/soak-data/report \
  go test -race -timeout 90m \
    ./cmd/integration/ -run TestSoak -v 2>&1 | tee /tmp/soak-data/test-output.txt
```

输出的信息（都在 `t.Log` 里）：
- `PressureMonitor` 采样（每 30s）
- `LogSummary` — 统计摘要
- `CheckDegradation` — 退化不变性断言
- `HealthScore` — 健康评分报告
- `TemporalAnalysis` — trajectory/oscillation/persistence/recovery
- `BasinAnalysis` — 吸引子盆地分析

保存的文件：
- `/tmp/soak-data/report/standalone-summary.json` — 结构化摘要，可用于跨 run 对比
- `/tmp/soak-data/report/standalone-report.md` — Markdown 报告
- `/tmp/soak-data/jsonl/soak-{timestamp}.jsonl` — 完整时间线

## 步骤 2：查看结果

```bash
# 查看汇总
cat /tmp/soak-data/report/standalone-report.md

# 对比两次 run 的 summary
cat /tmp/soak-data/report/standalone-summary.json

# 查看跨 run 演化趋势（需 ≥2 个历史 run）
cat /tmp/soak-data/report/standalone-evolution.md

# 查看历史存档
ls -la /tmp/soak-data/report/history/

# 查看原始输出（含 degradation 断言）
grep -E 'DEGRADATION|soak:|HEALTH|trajectory|basin' /tmp/soak-data/test-output.txt
```

## 步骤 4：关注的关键指标

| 指标 | 健康 | 警告 | 失败 |
|------|------|------|------|
| trajectory | stable | degrading | stuck/oscillating |
| basin | healthy | stressed | degraded/collapsed |
| HealthScore | >0.85 | 0.70-0.85 | <0.70 |
| L0 velocity | ~0 或负 | 正 | 持续正 + 加速 |
| escapable | true | — | false |

## 步骤 3：Evolution Analysis（跨 run 长期演化）*

每次 nightly soak 运行后，系统自动执行演化趋势分析：

1. 当前 run 的 summary 存档到 `{SOAK_REPORT_DIR}/history/{name}-{timestamp}.json`
2. 分析所有历史存档，生成 `{name}-evolution.md`
3. 关键判断：

| 信号 | 含义 |
|------|------|
| Health 持续下降 (>0.005/run) | 存储/复制/集群维度正在退化 |
| Basin depth 持续加深 | 系统在吸引子盆地中越陷越深 |
| Oscillation 比例 >50% | 系统不稳定，可能在两个 basin 间振荡 |
| Regime shift 检测 | basin type 永久性转变（如 healthy→stressed） |
| Escalating degradation | 恢复力持续下降 |

*注意：history 目录在首次运行后为空。Evolution Analysis 从第 2 次 nightly run 开始才产生有意义的结果。

关注 `/tmp/soak-data/report/standalone-evolution.md` 中的 Warnings 区域。如果出现 Regime Shift 或 Escalating Degradation 告警，需深入分析 `docs/failures/` 中的失效模式。

## 参考

- `internal/monitor/evolution.go` — Evolution analysis 源码
- `internal/monitor/basin.go` — Basin analysis 源码
- `internal/monitor/temporal.go` — Temporal analysis 源码
- `docs/failures/` — 已知失败的退化模式
