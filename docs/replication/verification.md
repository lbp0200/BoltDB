# Replication Verification

Replication correctness verification is split into three tiers to prevent the
known architectural duplicate-window boundary from polluting long-run stability
analysis.

## Tier 1: Short Strict Soak (5-15m)

```bash
SOAK_REPL_DURATION=10m go test -race -timeout 15m ./cmd/integration/ \
  -run TestSoakReplicationShortStrict
```

| Property | Value |
|----------|-------|
| Strict equality | ON (`t.Errorf` on any divergence) |
| Focus | Reconnect lifecycle, PSYNC CONTINUE gap-fill, deterministic command replay (SPOP, XADD, EXPIRE), offset correctness under chaos |
| Writers | 4-8 goroutines, moderate throughput |
| Lifecycle chaos | Aggressive (30% tick chance, random partitions) |
| Pass criteria | Zero structural corruption, bounded goroutine delta, strict dataset match |
| When to run | After any replication, PSYNC, or handler change |

## Tier 2: Long Non-Strict Soak (6h / overnight)

```bash
SOAK_REPL_DURATION=6h SOAK_REPL_WRITERS=4 go test -race -timeout 7h \
  ./cmd/integration/ -run TestSoakReplication
```

| Property | Value |
|----------|-------|
| Strict equality | OFF (informational comparison — expects duplicate-window drift) |
| Focus | L0 score trajectory, retry semaphore bounds, goroutine plateau, reconnect stability, health score, basin analysis, cross-run evolution |
| Writers | 4 goroutines, sustained throughput |
| Lifecycle chaos | Moderate (reduced to prevent FULLRESYNC thrash) |
| Pass criteria | Degradation invariants hold (see below), health score stable, no regime shift |
| When to run | Nightly CI soak pipeline, pre-release stability validation |

## Tier 3: Duplicate-Window Regression (30-60s)

```bash
go test -race -timeout 60s ./cmd/integration/regressions/ \
  -run TestRegressionDuplicateWindowMeasurement
```

| Property | Value |
|----------|-------|
| Purpose | Quantify INCR overcount and LPUSH duplicate ratio |
| Mechanism | High-concurrency writers + forced FULLRESYNC → post-convergence delta |
| Pass criteria | INCR gap ≤ 2, LPUSH duplicate ratio ≤ 70%, ZSET/HSET exact match |
| When to run | CI on every replication change |

### Why Three Tiers

The dual-timeline architecture (Badger MVCC snapshot ≠ replication offset)
creates a microsecond duplicate window. This window:

- **Is harmless** for idempotent commands (SET, HSET, SADD, ZADD)
- **Is bounded** for non-idempotent commands (INCR off-by-1, LPUSH ~50%)
- **Would require** commit-seq ↔ repl-offset mapping to eliminate

Long strict equality soak tests WILL show drift from this window, but that drift
says nothing about L0 stability, backpressure, goroutine management, or compaction.
The three-tier split ensures each concern has the right verification tool.

## Regression Tests

```bash
go test -race -timeout 600s ./cmd/integration/regressions/...
```

Key replication regression tests:

| Test | What it verifies |
|------|-----------------|
| `TestRegressionSnapshotFullresyncOffset` | No lost writes after FULLRESYNC; tolerates residual duplicate window |
| `TestRegressionPsyncReconnectNoLoss` | Zero data loss after PSYNC CONTINUE + FULLRESYNC cycles |
| `TestRegressionDuplicateWindowMeasurement` | INCR gap ≤ 2, LPUSH dup ratio ≤ 70% |

## Degradation Invariants

These invariants are checked by `PressureMonitor.CheckDegradation()` in soak tests.
Violation means replication entered a positive-feedback collapse:

- **FULLRESYNC frequency must not continuously increase** — Reconnect count
  must be below `MaxReconnectCount`. Escalating reconnects mean replication
  is thrashing.
- **Replication lag must converge** — After a disconnect spike,
  `master_repl_offset - slave_repl_offset` must converge to near zero.
  Persistent divergence = unrecoverable degradation.

## Nightly Pipeline (Replication)

The nightly soak pipeline includes 30 minutes of replication soak:

```bash
# Full nightly (1h standalone + 30min repl + evolution + plot)
./scripts/run_nightly_soak.sh

# Quick smoke (10min + 5min)
SOAK_DURATION=10m SOAK_REPL_DURATION=5m ./scripts/run_nightly_soak.sh
```

Replication-specific artifacts:

```
/tmp/bolt-nightly/
├── jsonl/
│   └── soak-{timestamp}.jsonl              # Raw timeline
├── report/
│   ├── history/
│   │   └── replication-{date}-{time}.json  # Cross-run evolution archive
│   ├── replication-summary.json            # Last run summary
│   ├── replication-report.md               # Markdown report
│   └── replication-evolution.md            # Cross-run trend (≥2 runs)
```

Evolution analysis:

```bash
# Full report
go run ./cmd/evolution/ -dir=/tmp/bolt-nightly/report

# Specific prefix
go run ./cmd/evolution/ -dir=/tmp/bolt-nightly/report -prefix=replication

# JSON gate result (CI consumption)
go run ./cmd/evolution/ -dir=/tmp/bolt-nightly/report -json

# Drift report (last run vs trailing 3-run avg)
go run ./cmd/evolution/ -dir=/tmp/bolt-nightly/report -drift
```

Signals to watch in evolution reports:

| Signal | What it means |
|--------|---------------|
| Drift delta > 0.05 in health or basin depth | Drift detected |
| healthy→stressed→degraded basin | Regime shift ⚠️ |
| >50% runs show limit cycle | Oscillation degrading |
| Permanent basin type change | Regime shift warning |
| Recovery capability declining | Escalating degradation |

## Failover Oscillation (Nightly)

The nightly pipeline runs `TestRegressionFailoverOscillation` as a
continue-on-error job:

```bash
INCLUDE_FAILOVER=1 ./scripts/run_nightly_soak.sh
```

- Output archived to `failover-oscillation-{timestamp}.log`
- Summary reports extract `OSCILLATION DETECTED` signal
- Scenario A: single failover + heal, Scenario B: chain failover
- Pass criteria: no post-convergence agreement drops, monotonic convergence
