# Test Tiering Strategy

## Why

The test suite has grown past CI capacity. Running everything on every PR
creates a 25+ minute feedback loop. Tiering separates fast correctness checks
from heavy verification, letting developers get PR results in <5 minutes.

## Tiers

### Tier A — PR Gate (< 5 min)

Blocking. Every PR must pass.

| What | Command | Est. Time |
|------|---------|-----------|
| Lint | `golangci-lint run --timeout 5m` | ~1 min |
| Unit tests | `go test -race -timeout 60s -short ./internal/...` | ~2 min |
| Fast integration | `go test -race -timeout 300s -run 'TierAFast' ./cmd/integration/` | ~2 min |

Fast integration = single-node tests, compat, wire format, basic commands,
error handling. No multi-node TCP, no goroutine leak, no chaos, no soak.

### Tier B — Post-Merge (< 20 min)

Non-blocking. Runs after merge to main. Catches multi-node cluster, goroutine
leaks, replication, sentinel, and chaos bugs before they reach nightly.

| What | Command | Est. Time |
|------|---------|-----------|
| Cluster multi-node | `go test -race -timeout 300s -run 'TierBCluster' ./cmd/integration/` | ~1 min |
| Goroutine leak | `go test -race -timeout 120s -run 'TestGoroutineLeak' ./cmd/integration/` | ~2 min |
| Chaos/Fuzz | `go test -race -timeout 120s -run 'TierBChaos' ./cmd/integration/` | ~2 min |
| Replication | `go test -race -timeout 300s -run 'TestReplication' ./cmd/integration/` | ~3 min |
| Sentinel | `go test -race -timeout 120s -run 'TestSentinel' ./cmd/integration/` | ~2 min |
| Regression (fast) | `go test -race -timeout 300s -skip 'TierC|Oscillation' ./cmd/integration/regressions/` | ~3 min |
| Mixed cluster | `go test -race -timeout 120s -run 'TestMixedCluster' ./cmd/integration/` | ~1 min |

### Tier C — Nightly (unlimited)

Runs on schedule only. Covers soak, heavy regression replay, and evolution gate.

| What | Command | Timeout |
|------|---------|---------|
| Standalone soak | `go test -race -timeout 120m -run TestSoak ./cmd/integration/` | 120 min |
| Replication soak | `go test -race -timeout 120m -run TestSoakReplication ./cmd/integration/` | 120 min |
| Cluster soak | `go test -race -timeout 30m -run TestClusterSoak ./cmd/integration/` | 30 min |
| Heavy regression | `go test -race -timeout 600s -run 'TestRegressionL0Collapse|TestRegressionReplicationThrash' ./cmd/integration/regressions/` | 10 min |
| Regression soak | `go test -race -timeout 600s -run 'TestRegressionPsyncReconnectNoLoss|TestRegressionDuplicateWindowMeasurement|TestRegressionSnapshotFullresyncOffset|TestRegressionSnapshotConsistency' ./cmd/integration/regressions/` | 10 min |
| Fuzz stress | `go test -race -timeout 120s -run 'FuzzServerCommandSequence' ./cmd/integration/` | 2 min |

## Test Selection

Tests are selected by naming convention + `-run`/`-skip` patterns. No build
tags or file reorganization needed.

### Skip patterns

Tests that run in the fast integration step exclude:

- `TestCluster(MultiNode|Gossip|SlotSync|MovedRedirect|AskRedirect|Failover|MigrateKey|Soak|BlockingFuzz)`
- `TestGoroutineLeak`
- `TestSoak`
- `TestSoakReplication`
- `TestFuzzServer`
- `TestChaos`
- `TestMixedCluster`
- `TestSentinel`
- `TestRegression`
- `TestShutdownWithReplication`
- `TestGracefulShutdown`
- `TestRegressionFailoverOscillation`
- `TestSplitBrain`
- `TestReplication(Stress|Chaos)`

### Adding new tests

- **Tier A**: basic command correctness, single-node, wire format, compat
- **Tier B**: multi-node TCP, goroutine leak, chaos, replication, sentinel
- **Tier C**: soak, heavy regression, long fuzz, evolution gate

## CI Workflow Changes

### PR Gate (`go.yml`)

```
test:              unit + lint (unchanged)
test-fast:         Tier A fast integration (new, replaces old test-integration)
```

### Post-Merge

```
test-heavy:        Tier B multi-node + goroutine leak + replication (new)
```

### Nightly (`nightly-soak.yml`)

```
standalone:        Tier C soak (unchanged)
replication:       Tier C replication soak (unchanged)
stress:            Tier C fuzz + regressions (unchanged)
regressions:       Tier C heavy regression (unchanged)
```

## Verification

```bash
# Run Tier A locally
bash scripts/test-tier-a.sh

# Run Tier B locally
bash scripts/test-tier-b.sh

# Run Tier C locally (long)
bash scripts/test-tier-c.sh
```
