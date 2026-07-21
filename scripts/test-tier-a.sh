#!/usr/bin/env bash
# Tier A — PR Gate (< 8 min)
# Lint, unit tests, fast integration tests.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== Tier A: Test quality guards ==="
bash scripts/guard_test_quality.sh

echo "=== Tier A: Lint ==="
golangci-lint run --timeout 5m

echo "=== Tier A: Benchmark regression guards ==="
bash scripts/guard_bench.sh proto
bash scripts/guard_bench.sh --store
bash scripts/guard_bench.sh --server

echo "=== Tier A: Unit tests ==="
go test -race -timeout 120s -short -count=1 ./internal/... ./cmd/boltDB/...

echo "=== Tier A: Fast integration ==="
# 分类标签: [GHA: resource] = GHA runner 资源不足，远程 Linux 可通过
SKIP_PATTERN='TestCluster(MultiNode|Gossip|SlotSync|SetSlotNodePropagation|MovedRedirect|AskRedirect|Failover|MigrateKey|Soak|BlockingFuzz)'  # [GHA: resource] 多节点集群
SKIP_PATTERN+='|TestGoroutineLeak'                                                                                                          # [GHA: resource] 完整的 goroutine 跟踪
SKIP_PATTERN+='|TestSoak|TestSoakReplication|TestSoakReplicationShortStrict'                                                                 # [GHA: resource] 耗时 > 5min
SKIP_PATTERN+='|TestFuzzServer'                                                                                                             # [GHA: resource] 随机 fuzz
SKIP_PATTERN+='|TestChaos'                                                                                                                  # [GHA: resource] 混沌测试
SKIP_PATTERN+='|TestMixedCluster'                                                                                                           # [GHA: resource] 需外部 Redis
SKIP_PATTERN+='|TestSentinel'                                                                                                               # [GHA: resource] 需 sentinel 集群
SKIP_PATTERN+='|TestRegression'                                                                                                             # [GHA: resource] 回归套件 ~10min
SKIP_PATTERN+='|TestShutdownWithReplication'                                                                                                # [GHA: resource] 主从关闭顺序
SKIP_PATTERN+='|TestGracefulShutdown'                                                                                                       # [GHA: resource] 优雅关闭
SKIP_PATTERN+='|TestRegressionFailoverOscillation'                                                                                          # [GHA: resource] GHA 慢 runner 超时
SKIP_PATTERN+='|TestSplitBrain'                                                                                                             # [GHA: resource] GHA 上不可靠

go test -race -timeout 300s -skip "${SKIP_PATTERN}" -count=1 ./cmd/integration/
