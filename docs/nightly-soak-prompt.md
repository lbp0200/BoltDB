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

# 查看原始输出（含 degradation 断言）
grep -E 'DEGRADATION|soak:|HEALTH|trajectory|basin' /tmp/soak-data/test-output.txt
```

## 步骤 3：关注的关键指标

| 指标 | 健康 | 警告 | 失败 |
|------|------|------|------|
| trajectory | stable | degrading | stuck/oscillating |
| basin | healthy | stressed | degraded/collapsed |
| HealthScore | >0.85 | 0.70-0.85 | <0.70 |
| L0 velocity | ~0 或负 | 正 | 持续正 + 加速 |
| escapable | true | — | false |

## 参考

- `docs/failures/` — 已知失败的退化模式
- `internal/monitor/basin.go` — Basin analysis 源码
- `internal/monitor/temporal.go` — Temporal analysis 源码
