#!/usr/bin/env bash
# Tier A — PR Gate (< 8 min)
# Lint, unit tests, fast integration tests.
set -euo pipefail

echo "=== Tier A: Lint ==="
golangci-lint run --timeout 5m

echo "=== Tier A: Unit tests ==="
go test -race -timeout 120s -short -count=1 ./internal/... ./cmd/boltDB/...

echo "=== Tier A: Fast integration ==="
SKIP_PATTERN='TestCluster(MultiNode|Gossip|SlotSync|SetSlotNodePropagation|MovedRedirect|AskRedirect|Failover|MigrateKey|Soak|BlockingFuzz)'
SKIP_PATTERN+='|TestGoroutineLeak'
SKIP_PATTERN+='|TestSoak|TestSoakReplication|TestSoakReplicationShortStrict'
SKIP_PATTERN+='|TestFuzzServer'
SKIP_PATTERN+='|TestChaos'
SKIP_PATTERN+='|TestMixedCluster'
SKIP_PATTERN+='|TestSentinel'
SKIP_PATTERN+='|TestRegression'
SKIP_PATTERN+='|TestShutdownWithReplication'
SKIP_PATTERN+='|TestGracefulShutdown'
SKIP_PATTERN+='|TestRegressionFailoverOscillation'
SKIP_PATTERN+='|TestSplitBrain'

go test -race -timeout 300s -skip "${SKIP_PATTERN}" -count=1 ./cmd/integration/
