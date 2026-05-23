# BoltDB Stability Specification

> Operational envelope and convergence guarantees for BoltDB.
> This document defines the boundaries within which BoltDB is validated to operate correctly.

## 1. Client Capacity

| Parameter | Limit | Notes |
|-----------|-------|-------|
| Max concurrent clients | 1000 | Higher counts degrade throughput; LSM compaction becomes bottleneck |
| Max client connections per second (burst) | 500 | Above this, TCP accept queue may overflow |
| Recommended safe zone | ≤ 500 connected clients | Sustained operation without backpressure engagement |
| Monitored metric | goroutine delta ≤ baseline + 50 | Degradation gate triggers at 3 consecutive windows (90s) above threshold |

## 2. Backlog & Replication Envelope

| Parameter | Safe | Warning | Failure |
|-----------|------|---------|---------|
| Backlog size | 1 MB (default) | ≤ 64 MB | > 128 MB (memory pressure) |
| Reconnect rate | ≤ 3 / hour | ≤ 10 / hour | > 50 / hour |
| Slave re-sync time | < 1 s | < 5 s | > 30 s |
| Master-slave offset lag | ≤ 100 commands | ≤ 1,000 commands | > 10,000 commands |
| FULLRESYNC frequency | ≤ 1 / hour | — | Escalating: prevents convergence |

### Replication Guarantees

- **PSYNC CONTINUE**: Guaranteed if slave offset is within backlog (default 1 MB circular buffer).
- **FULLRESYNC**: Guaranteed to converge eventually. However, each FULLRESYNC has a ~100-200ms View Blind Window where writes committed during RDB generation may be lost (see `docs/failures/snapshot-inconsistency.md`).
- **Eventual convergence**: After FULLRESYNC, a subsequent FULLRESYNC will repair any data from the blind window.

## 3. L0 Compaction Envelope

| Zone | L0 Score | Behavior |
|------|----------|----------|
| Normal | 0 – 5 | No backpressure. Compaction keeps up. |
| Mild pressure | 5 – 8 | Minimal pre-delay on writes. Compaction still draining. |
| Soft threshold | 8 – 10 | `preWriteCheck` introduces delay proportional to score (max 1s). |
| Active backpressure | 10 – 20 | Delays increase; writeSlot semaphore limits concurrent retries to 50. |
| Hard threshold | ≥ 20 | Writes rejected with `ErrWriteRejected`. **Ring the alarm.** |
| Recovery | ≤ 8 | After load stops, L0 must recover below 8.0 within observation window. |

### L0 Velocity & Acceleration

| Metric | Healthy | Warning | Failure |
|--------|---------|---------|---------|
| L0 velocity | ~0 or negative | Positive | Sustained positive + accelerating |
| L0 acceleration | ≤ 0 | — | Positive (second derivative > 0) |
| Peak L0 per run | < 10 | < 20 | ≥ 25 |
| Final L0 after drain | < 8 | < 15 | ≥ 25 |

Invariant: After any write load stops, L0 must converge below `L0RecoveryThreshold` (8.0) within the drain window. Failure to recover indicates compaction cannot keep up with the write pattern.

## 4. Convergence Guarantees

### Basin Attractor Convergence

The system's health state is modeled as a particle in a basin attractor landscape:

| Basin | Interpretation | Escapable | Action |
|-------|---------------|-----------|--------|
| Healthy | System operating in safe envelope | — | Normal |
| Stressed | Under load but converging | Yes | Monitor |
| Degraded | L0 elevated, backpressure active | Usually | Investigate if persistent |
| Collapsed | L0 > 25, writes rejected | Maybe | Emergency intervention |

**Convergence time**: From stressed back to healthy, typical recovery is ≤ 30s after load stops. If recovery takes > 60s, `RecoveryTimeHealth` drops below 1.0.

**Regime shift detection**: If the basin changes permanently (e.g., healthy → stressed for ≥ 2 consecutive runs), the evolution gate triggers a FAIL signal. This indicates a structural change in the system's operating regime.

### Oscillation / Limit Cycle

- Acceptable: brief oscillation (±1-2 L0 score swings)
- Warning: limit cycle detected (>3 zero-crossings with amplitude > 0.01 in health score)
- Failure: sustained oscillation with amplitude > 0.1 across runs

## 5. FULLRESYNC Guarantees

| Property | Guarantee |
|----------|-----------|
| Snapshot consistency | MVCC point-in-time via single `db.View` transaction (see `GenerateRDB`) |
| Data integrity | All TYPE_ records and their value keys (list/hash/set/zset) read from same transaction. TOCTOU eliminated. |
| BACKLOG after snapshot | Backlog is replayed after `GenerateRDB` completes; `snapshotOffset` is captured AFTER RDB generation |
| View Blind Window | ~100ms-2s of writes may be lost per FULLRESYNC cycle due to MVCC/replication offset dual-timeline problem |
| Recovery from blind window | Subsequent FULLRESYNC will repair. Eventual convergence, NOT exact linearizable recovery. |
| Concurrent writes during RDB | Safe. RDB generation uses `PrefetchValues: false` iterator to avoid memory pressure. |

### Known Limitation: View Blind Window

BoltDB has a dual-timeline problem that Redis does not:
- MVCC snapshot timeline (badger txn/version)
- Replication offset timeline (masterReplOffset)

These are not synchronously mapped. The result:
```
badger View reads state at T_view
  ↓
GenerateRDB completes, snapshotOffset captured
  ↓
Writes committed T_view < T < snapshotOffset:
  NOT in RDB  (committed after View started)
  NOT in backlog (committed before offset capture)
```

This means ~100ms-2s of writes can be permanently lost on each FULLRESYNC. A subsequent FULLRESYNC recovers them. This is a documented design tradeoff.

Full details: `docs/failures/snapshot-inconsistency.md`

## 6. Sentinel Failover Envelope

| Parameter | Safe | Warning | Failure |
|-----------|------|---------|---------|
| Sentinel agreement | 100% | < 100% | < MinAgreedFraction (default 1.0) |
| Leader churn | 0 | > 0 | > 5 changes (default) |
| Cluster fragmentation | false | — | true (split-brain) |
| Failover time | < 5 s | < 15 s | > 30 s |
| Gossip convergence | < 3 rounds | < 5 rounds | > 10 rounds |

## 7. Backpressure System Contract

```
retryUpdate → preWriteCheck(L0 score) → soft/hard gate → writeSlot semaphore (50)
```

| Component | Limit | Behavior |
|-----------|-------|----------|
| `writeSlot` semaphore | 50 | Blocks when full; goroutines wait at semaphore, not in unbounded retry loop |
| `ActiveRetries` | ≤ 100 | Semaphore ensures bound. Spikes above 100 = semaphore not working. |
| `TotalRetries` | Unbounded | Cumulative counter; not a control signal. |
| Soft delay | ≤ 1s per write | Proportional to (L0 - softThreshold) |
| Hard rejection | Immediate | Returns `ErrWriteRejected` to client |

## 8. Goroutine Lifecycle

| Component | Tracking | Shutdown Signal |
|-----------|----------|----------------|
| Connection handlers | `Handler.wg` | `conn.Close()` unblocks `ReadRESP` |
| Slave reconnector | `SlaveReconnector.wg` | `stopCh` close + master conn close |
| Slave replication | `Handler.wg` | `replMgr.Stop()` closes TCP conn |
| PubSub / Monitor | Root context | `ctx.Done()` on `cancel()` |
| PressureMonitor sampling | Background goroutine | `ctx.Done()` on `cancel()` |

**Invariant**: After `handler.Shutdown()` returns, zero goroutines access the DB.

**Shutdown ordering** (enforced by `main.go`):
```
close listener → ServeTCP returns
→ replMgr.Stop()
→ cancel() (root context)
→ handler.Shutdown()  (close client conns + WaitGroup.Wait)
→ db.Close()
```

## 9. Evolution Monitoring Gate

The evolution gate is a CI-enforced check that runs after every nightly soak:

| Signal | WARN | FAIL |
|--------|------|------|
| Health dropping for 3+ consecutive runs | 3 drops → WARN | — |
| Regime shift to stressed/degraded | — | Permanent basin change |
| Escalating degradation | — | > 50% of recent runs degraded |
| Storage + overall degrading | Both degrading → WARN | — |

The gate is checked by `cmd/evolution/main.go` reading accumulated history from `report/history/`.

## 10. Test Coverage Requirements

Every documented failure mode in `docs/failures/` must have:

1. **Replayable regression test** in `cmd/integration/regressions/`
2. **Measurable invariants** via `DegradationAssertion` thresholds
3. **Convergence verification**: after the failure scenario, the system must return to a healthy state within the specified timeout

Current regression coverage:

| Failure Mode | Replay Test | Expected JSON | Degradation Gate |
|-------------|-------------|---------------|------------------|
| L0 collapse | `regressions/l0_collapse_test.go` | — | MaxL0Score < 22, L0Recovery < 12 |
| Retry storm | `regressions/retry_storm_test.go` | `retry_storm_expected.json` | MaxActiveRetries < 50 |
| Shutdown race | `regressions/shutdown_race_test.go` | — | Goroutine delta ≤ 10 |
| Snapshot inconsistency | `regressions/snapshot_inconsistency_test.go` | `snapshot_inconsistency_expected.json` | Structural match |
| Snapshot FULLRESYNC overlap | `regressions/snapshot_fullresync_overlap_test.go` | — | CheckDegradation + data verification |
| Replication thrash | `regressions/replication_thrash_test.go` | `replication_thrash_expected.json` | MaxReconnectCount < 12 |
| Failover oscillation | `integration/failover_oscillation_test.go` | — | Trajectory monotonic + no oscillation |
| Split-brain convergence | `integration/split_brain_harden_test.go` | — | Post-heal leader stability + monotonic convergence |
| Backlog exhaustion | `regressions/backlog_exhaustion_test.go` | — | FULLRESYNC fallback + data convergence |
