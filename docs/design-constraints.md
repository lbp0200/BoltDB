# Design Constraints

## 1. Convergence: The Primary System Property

BoltDB's primary quality attribute is not throughput or latency — it is **convergence**:
the ability to return to a stable operating state after any perturbation.

### 1.1 Convergence Definition

A system is convergent if, for any bounded perturbation (write burst, partition,
memory pressure), there exists a bounded recovery time `T` after which the
system returns to steady state:

```
∀ perturbation P: ∃ T(P) such that ∀ t > T(P):
  L0_score(t) < L0_recovery_threshold
  ∧ ActiveRetries(t) < ActiveRetries_recovery
  ∧ goroutine_delta < goroutine_plateau
  ∧ slave_lag < replication_convergence_bound
```

### 1.2 Measured Invariants (Regression Gate)

| Invariant | Healthy | WARN | DEGRADED | FAIL |
|-----------|---------|------|----------|------|
| Goroutine delta | ≤ 20 | ≤ 50 | — | > 50 |
| ActiveRetries | ≤ 30 | ≤ 100 | — | > 100 |
| L0 peak | ≤ 10 | ≤ 15 | ≤ 20 | > 25 |
| L0 final recovery | ≤ 8 | — | ≤ 15 | > 20 |
| Reconnects (per run) | ≤ 10 | ≤ 25 | — | > 50 |
| Monotonic L0 rise | ≤ 50% | ≤ 70% | — | > 70% |
| Heap sawtooth | periodic GC | — | monotonic growth | — |

---

## 2. Recovery Time Budgets

After stress stops, the system must recover within these bounds:

| Metric | Max Recovery Time | Measurement Point |
|--------|------------------|-------------------|
| L0 score < 10 | 30s | From last write |
| ActiveRetries < 10 | 10s | From last write rejection |
| Goroutine delta < 10 | 5s | From shutdown signal |
| Slave offset convergence | 15s | From last partition end |
| Write latency normalization | 5s | From last backpressure event |

**Soak gate:** if any recovery exceeds 2× the budget, the run is DEGRADED.

---

## 3. Backpressure Design Constraints

### 3.1 L0 Score Thresholds

| Threshold | Value | Behavior |
|-----------|-------|----------|
| Soft (delay) | 8.0 | Pre-write delay proportional to score, max 1s |
| Hard (reject) | 20.0 | Writes rejected with `ErrWriteRejected` |
| Critical (stall) | 25.0 | Must never be reached; if hit, manual intervention needed |

### 3.2 writeSlot Semaphore

| Property | Value | Rationale |
|----------|-------|-----------|
| Max concurrent `retryUpdate` | 50 | Prevents retry goroutine explosion |
| Acquire timeout | none (blocking) | Backpressure propagates to client |
| Release guarantee | always in defer | Prevents slot leak |

### 3.3 Retry Budget

| Property | Value |
|----------|-------|
| Max retry time per write | 5s total (soft), 30s absolute (hard) |
| Retry backoff (conflict) | 1ms–50ms exponential with jitter |
| Retry backoff (blocked) | 1ms–2s exponential with jitter |
| Max concurrent retry goroutines | 50 (= writeSlot capacity) |

---

## 4. Replication Design Constraints

### 4.1 Backlog Coverage

| Property | Value | Constraint |
|----------|-------|------------|
| Default backlog size | 1 MB | Must cover `max_expected_throughput × max_expected_partition_duration` |
| Max backlog size | 512 MB | Hard limit to prevent OOM |
| Backlog eviction | ring buffer | Oldest entries dropped when full; FULLRESYNC forced if slave offset evicted |

**Design rule:** `backlog_size > write_throughput × expected_max_partition_duration`

For default settings (1 MB backlog, ~200 KB/s write throughput @ 4 writers):
- Safe partition duration: ~5s before FULLRESYNC risk
- At 8 writers (~400 KB/s): ~2.5s safe window

### 4.2 Reconnect Behavior

| Phase | Max Duration | Backoff |
|-------|-------------|---------|
| Initial reconnect | 1s | Immediate retry |
| Subsequent retries | 32s cap | Exponential: 1s, 2s, 4s, 8s, 16s, 32s, … |
| FULLRESYNC | unbounded | Depends on DB size |

### 4.3 Convergence After Partition

```
After partition ends:
  slave_repl_offset must converge to master_repl_offset within 15s
  Full resync is acceptable but must complete within 60s per GB of data
  reconnect_count must not increase after convergence
```

---

## 5. Shutdown Ordering (Enforced)

```
1. close listener       → ServeTCP returns
2. replMgr.Stop()       → close slave TCP connections → unblock reads
3. cancel()             → cancel root context → all goroutines see Done
4. handler.Shutdown()   → close all client TCP conns + wg.Wait()
5. db.Close()           → deferred — guaranteed: 0 goroutines accessing DB
```

**Invariant:** After step 4 returns, exactly zero goroutines may call any DB method.

**Verification:** Every long-lived goroutine must be tracked in `Handler.wg` or
`SlaveReconnector.wg`. Adding a new goroutine without WG tracking is a violation.

---

## 6. State Machine Constraints

From `docs/state-machine.md`:

### 6.1 Connection State Transitions

```
Normal      → Subscribed     (SUBSCRIBE/PSUBSCRIBE only)
Normal      → Monitor        (MONITOR only)
Normal      → Transaction    (MULTI only, non-exclusive with Normal)
Normal      → ReplSlave      (PSYNC only, connection takeover)
Subscribed  → Normal         (QUIT or UNSUBSCRIBE all)
Monitor     → Normal         (QUIT only)
Transaction → Normal         (EXEC or DISCARD)
```

**Illegal transitions (must error, never silently succeed):**
- SUBSCRIBE from Monitor mode
- MONITOR from Subscribed mode
- MULTI from Subscribed or Monitor mode
- PSYNC from any mode other than Normal

### 6.2 Command Legality Per State

| Command | Normal | Subscribed | Monitor | ReplSlave |
|---------|--------|------------|---------|-----------|
| Read (GET, etc.) | ✅ | ❌ | ❌ | ❌ |
| Write (SET, etc.) | ✅ | ❌ | ❌ | ❌ |
| SUBSCRIBE | →Subscribed | ✅ | ❌ | ❌ |
| MONITOR | →Monitor | ❌ | ❌ | ❌ |
| MULTI | →Transaction | ❌ | ❌ | ❌ |
| PSYNC | →ReplSlave | ❌ | ❌ | ❌ |
| PING | ✅ | ✅ | ✅ | ❌ |
| QUIT | →Closed | →Closed | →Closed | ❌ |

### 6.3 Replication Role Constraints

| Operation | Master | Slave |
|-----------|--------|-------|
| Accept client connections | ✅ | ❌ |
| Execute writes | ✅ | ❌ |
| Accept PSYNC | ✅ | ❌ |
| Propagate writes | ✅ (to slaves) | ❌ |
| Accept REPLCONF ACK | ✅ | ❌ |
| Run reconnection loop | ❌ | ✅ |

---

## 7. Feature Admission Policy v0

Any new feature must answer these questions before merging:

### 7.1 State Impact
- Does this add a new connection state? → Update `docs/state-machine.md`
- Does it change any existing state transition? → Verify no illegal transitions
- Is the new state tracked in CLIENT LIST? → Add state flag

### 7.2 Replication Impact
- Does this touch the write path? → Must propagate to slaves
- Does it change RDB format? → Version bump needed, backward compat test
- Does it change offset semantics? → Verify backlog + FULLRESYNC logic
- Are there new data types? → Must implement RDB encode/decode + Check()

### 7.3 Convergence Impact
- Could this create a positive feedback loop? → Prove boundedness
- Does it add goroutines? → Track in Handler.wg or SlaveReconnector.wg
- Does it affect the write path? → Verify backpressure interaction
- Does it allocate unbounded memory? → Add upper bound

### 7.4 Lifecycle Impact
- Does it affect shutdown ordering? → Verify against section 5
- Does it add new DB access paths? → Must stop before handler.Shutdown() returns
- Does it create new timer/ticker? → Must clean up on ctx.Done()

### 7.5 Regression Coverage
- Is there an existing regression case that covers this? → Run it
- Should a new regression test be added? → Add to `cmd/integration/regressions/`
- Is the failure mode documented? → Add to `docs/failures/`
- Is the expected convergence trajectory defined? → Add to `expected_metrics.json`

---

## 8. Regression Suite Coverage

| Failure Mode | Test | Type | Stress Dimension |
|-------------|------|------|-----------------|
| Retry storm | `TestRegressionRetryStorm` | Local pressure | Write concurrency |
| L0 collapse | Partially covered by retry-storm | Local pressure | Write throughput |
| Replication thrash | `TestRegressionReplicationThrash` | Distributed stress | Partition frequency |
| Replication FULLRESYNC | `TestRegressionReplicationThrashFullresync` | Distributed stress | Partition duration |
| Snapshot inconsistency | `TestRegressionSnapshotConsistency` | Correctness | Data type coverage |
| Snapshot (concurrent writes) | `TestRegressionSnapshotConcurrentWrites` | Correctness | Write concurrency |
| Shutdown race | `TestGracefulShutdown` (existing) | Lifecycle | Connection chaos |
| Shutdown + replication | `TestShutdownWithReplication` (existing) | Lifecycle | Slave connections |

---

## 9. JSONL Schema (Machine-Readable Timeline)

Each soak run produces a JSONL file at `SOAK_JSONL_DIR/soak{-repl}-{timestamp}.jsonl`.

```jsonl
{"ts": 1779299343673794000, "go": 71, "heap_mb": 265.0, "alloc_mb": 263.7,
 "gc": 3, "ar": 0, "tr": 0, "wb": 0, "rj": 0, "dl": 0, "l0": 0,
 "mo": 36, "so": 0, "bl": 1048576, "rc": 0, "sl": 1}
```

Fields:
| Field | Type | Description |
|-------|------|-------------|
| `ts` | int64 | Unix nanosecond timestamp |
| `go` | int | runtime.NumGoroutine |
| `heap_mb` | float | MemStats.HeapInuse in MB |
| `alloc_mb` | float | MemStats.HeapAlloc in MB |
| `gc` | int | MemStats.NumGC |
| `ar` | int64 | BotreonStore.ActiveRetries |
| `tr` | int64 | BotreonStore.TotalRetries |
| `wb` | int64 | Writes blocked by Badger |
| `rj` | int64 | Writes rejected by backpressure |
| `dl` | int64 | Writes delayed by backpressure |
| `l0` | float | Latest L0 compaction score |
| `mo` | int64 | Master replication offset |
| `so` | int64 | Slave replication offset |
| `bl` | int64 | Backlog buffer size |
| `rc` | int64 | Slave reconnect count |
| `sl` | int | Connected slave count |

Analysis patterns:

```bash
# L0 sawtooth check: score over time
cat soak-*.jsonl | jq -s '[.[] | {t: .ts, l0: .l0}]'

# Retry spike detection: ActiveRetries > 50
cat soak-*.jsonl | jq -s 'map(select(.ar > 50))'

# Goroutine leak detection: monotonic increase in tail
cat soak-*.jsonl | jq -s '
  [.[].go] | .[length/2 | floor:] |
  select(length > 1) |
  [.[1:] | [., .-1]] | flatten |
  select(any(. > 0))'
```
