#!/usr/bin/env bash
# Tier B — Post-Merge (< 20 min)
# Multi-node cluster, goroutine leak, chaos, replication, sentinel, regressions.
set -euo pipefail

echo "=== Tier B: Multi-node cluster ==="
CLUSTER_MULTI='TestCluster(MultiNode|GossipPropagation|SlotSync|SetSlotNodePropagation|MovedRedirect|AskRedirect|Failover|MigrateKey|BlockingFuzz)'
go test -race -timeout 300s -run "${CLUSTER_MULTI}" -count=1 ./cmd/integration/

echo "=== Tier B: Goroutine leak ==="
go test -race -timeout 120s -run 'TestGoroutineLeak' -count=1 ./cmd/integration/

echo "=== Tier B: Chaos / Fuzz ==="
# Exclude FuzzServerCommandSequence (heavy — belongs in Tier C)
go test -race -timeout 120s -run 'Test(FuzzServer|Chaos)' -count=1 ./cmd/integration/

echo "=== Tier B: Replication ==="
# Skip heavy replication stress tests (Tier C)
go test -race -timeout 300s -run 'TestReplication' -skip 'TestReplication(Stress|Chaos)' -count=1 ./cmd/integration/

echo "=== Tier B: Sentinel ==="
go test -race -timeout 120s -run 'TestSentinel' -count=1 ./cmd/integration/

echo "=== Tier B: Regression (fast) ==="
# Exclude heavy regressions (Tier C)
go test -race -timeout 300s \
  -skip 'TestRegression(L0Collapse|ReplicationThrash|ReplicationThrashFullresync|PsyncReconnectNoLoss|DuplicateWindowMeasurement|SnapshotFullresyncOffset|SnapshotConsistency)' \
  -count=1 ./cmd/integration/regressions/

echo "=== Tier B: Mixed cluster ==="
go test -race -timeout 120s -run 'TestMixedCluster' -count=1 ./cmd/integration/

echo "=== Tier B: Split brain ==="
go test -race -timeout 120s -run 'TestSplitBrain|TestRegressionFailoverOscillation' -count=1 ./cmd/integration/

echo "=== Tier B: Shutdown ==="
go test -race -timeout 60s -run 'Test(ShutdownWithReplication|GracefulShutdown)' -count=1 ./cmd/integration/
