# Snapshot Inconsistency → Linearizable FULLRESYNC Boundary

## Symptoms (Pre-Fix)
- After FULLRESYNC, slave permanently behind master on non-idempotent commands (INCR, LPUSH)
- Slave converges at the offset level but data is wrong
- Replicated lists shorter than master (RDB View window dropped writes)

## Root Cause History

### 1. TOCTOU (Original)
`GenerateRDB` read TYPE_ records and value data in separate transactions. A write between the two could create dangling TYPE_ with no data. **FIXED**: single `db.View`.

### 2. Replication Hole (First fix attempt)
Old flow:
```
HandlePSync captures offset O1  ← pre-snapshot
↓
GenerateRDB                      ← RDB includes O1→O2
↓
send +FULLRESYNC O1              ← announces O1 to slave
↓
AddSlave                          ← PropagateCommand sends O3+
```
**Commands O1→O2 are in the RDB but slave starts at O1.** Never replayed. Permanent data loss.

**First fix** (May 2026): capture snapshotOffset AFTER GenerateRDB, send backlog under slaveConn.mu. Eliminated structural corruption but replaced the replication hole with a View blind window: ~100ms-2s of writes between View start and offset capture were in neither RDB nor backlog.

### 3. Conservative Snapshot Offset (Intermediate fix)
**Fix** (May 2026, `replication_handler.go:65`): capture `snapshotOffset` **BEFORE** `db.View()`, not after.

```
capture snapshotOffset  ← PRE-View
↓
GenerateRDB(View)       ← MVCC snapshot guaranteed to include all writes < snapshotOffset
↓
send +FULLRESYNC snapshotOffset
↓
send backlog(snapshotOffset→currentOffset)  ← covers writes during RDB generation
↓
AddSlave (under slaveConn.mu)
```

Invariant: `store.Set()` (badger commit) → `PropagateCommand()` (offset increment). Therefore any write with `offset < snapshotOffset` committed to badger before snapshotOffset capture → visible in View → **in RDB**. ✓

**Guarantee: no lost writes.** Every write is either in RDB or in backlog.

**Residual:** writes committed between `snapshotOffset` capture and `db.View()` start are in **both** RDB and backlog — a microsecond duplicate window (typically 0 concurrent writes). Idempotent commands unaffected; INCR/LPUSH could double-apply once.

### 4. Linearizable Boundary (Current fix — Issue #3)

`store.BotreonStore.snapshotMu` (`sync.RWMutex`) atomically binds the offset capture to the MVCC View:

- FULLRESYNC holds the **write** lock across `GetMasterReplOffset()` → `GenerateRDBWithSnapshotLock()` (which is `GenerateRDB` under the same critical section). No concurrent write can land between offset capture and View start, so RDB and backlog are disjoint.
- Normal write path (`retryUpdate`) holds a **read** lock, so concurrent writers remain parallel except for the microsecond FULLRESYNC window (blocked at `RunWriteLocked` boundary).

```
SnapshotMuLock()                           ← WR
  snapshotOffset = GetMasterReplOffset()   ← atomic with View
  GenerateRDBWithSnapshotLock(View)        ← still under WR
SnapshotMuUnlock()                         ← release before network I/O
↓
send +FULLRESYNC snapshotOffset + RDB
↓
send backlog(snapshotOffset→currentOffset) + AddSlave + CatchUpAndEnableSlave
```

**Guarantees (Issue #3 closed):**
```
- no lost writes
- snapshot consistency (badger MVCC)
- no structural corruption
- ZERO duplicate window (linearizable offset ↔ MVCC binding) — strict equality now holds
- eventual convergence
```

No commit-seq ↔ repl-offset table is needed: the dual-timeline gap is closed by the critical-section ordering. The alternative "commitTs mapping + backlog trimming" design remains valid but is not implemented — the lock achieves the same invariant with lower complexity.

## Prevention
- Run `store.Check()` after every RDB load in tests
- `TestRegressionSnapshotFullresyncOffset`: validates FULLRESYNC convergence — now asserts **exact** INCR/LIST equality (tolerance removed, see duplicate_window_regression_test)
- `TestRegressionDuplicateWindowMeasurement`: validates zero duplicate window under reconnect chaos
- `TestSoakReplication` with `SOAK_REPL_STRICT_EQUALITY=1` validates long-run strict equality
- **New commits to replication code must pass `snapshot_fullresync_overlap_test`**
- Monitor RDB generation duration — barrier-to-reconnect pressure grows with backlog divergence
- For strict equality validation: `SOAK_REPL_STRICT_EQUALITY=1 go test ./cmd/integration/... -run TestSoakReplication`
