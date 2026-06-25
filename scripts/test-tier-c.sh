#!/usr/bin/env bash
# Tier C — Nightly (unlimited)
# Soak, heavy regression, long fuzz, evolution gate.
set -euo pipefail

DURATION="${SOAK_DURATION:-5m}"
REPL_DURATION="${SOAK_REPL_DURATION:-5m}"
CLUSTER_DURATION="${SOAK_CLUSTER_DURATION:-5m}"

echo "=== Tier C: Standalone soak (${DURATION}) ==="
SOAK_DURATION="${DURATION}" go test -race -timeout 30m -run TestSoak -count=1 ./cmd/integration/

echo "=== Tier C: Replication soak (${REPL_DURATION}) ==="
SOAK_REPL_DURATION="${REPL_DURATION}" go test -race -timeout 30m -run TestSoakReplication -count=1 ./cmd/integration/

echo "=== Tier C: Cluster soak (${CLUSTER_DURATION}) ==="
SOAK_CLUSTER_DURATION="${CLUSTER_DURATION}" go test -race -timeout 30m -run TestClusterSoak -count=1 ./cmd/integration/

echo "=== Tier C: Heavy regression ==="
go test -race -timeout 600s \
  -run 'TestRegression(L0Collapse|ReplicationThrash|ReplicationThrashFullresync|PsyncReconnectNoLoss|DuplicateWindowMeasurement|SnapshotFullresyncOffset|SnapshotConsistency)' \
  -count=1 ./cmd/integration/regressions/

echo "=== Tier C: Full regression suite ==="
go test -race -timeout 600s -count=1 ./cmd/integration/regressions/

echo "=== Tier C: Fuzz stress ==="
go test -race -timeout 120s -run 'FuzzServerCommandSequence' -count=1 ./cmd/integration/

echo "=== Tier C: Rep. stress ==="
go test -race -timeout 120s -run 'TestReplication(Stress|Chaos)' -count=1 ./cmd/integration/

echo "=== Tier C: Short strict replication soak ==="
go test -race -timeout 15m -run TestSoakReplicationShortStrict -count=1 ./cmd/integration/
