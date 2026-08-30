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

### 4. Linearizable Boundary (Issue #3 — **closed 2026-08-30**)

**Status:** `processRequest` holds `snapshotMu.RLock` across `executeCommand`
(badger commit) and `PropagateCommand` (`backlog.Append` = offset). FULLRESYNC
holds the write lock across `GetMasterReplOffset()` → View. A write cannot be
visible to the snapshot until it has also been assigned an offset.

The 2026-08-29 revision of this section documented the failure of the first
`snapshotMu` attempt (`d5e210d`, `ecaf9df`), which only locked `retryUpdate`
and therefore still allowed:

```
A: retryUpdate → commit → RUnlock
B: SnapshotMuLock → snapshotOffset → View（含 A）→ Unlock
A: PropagateCommand → offset >= snapshotOffset → 进 backlog
⇒ W ∈ RDB AND W ∈ backlog gap
```

That interleaving is now impossible on the client write path: B's write lock
waits until A has appended. Guard:
`TestFullresyncBoundary_CommittedButUnpropagatedWrite` and
`TestFullresyncBoundary_FenceBlocksSnapshotWriteLock`.

`store.BotreonStore.snapshotMu` (`sync.RWMutex`):

- FULLRESYNC holds the **write** lock across `GetMasterReplOffset()` → `GenerateRDBWithSnapshotLock()`.
- Client writes hold the **read** lock in `processRequest` across `executeCommand` and `PropagateCommand`. `retryUpdate` does **not** take the lock (nested RLock deadlocks when a writer is queued).
- `EXEC` is included in the fence even though it is not `isWriteCommand`.

```
processRequest: RLock
  executeCommand → retryUpdate → badger commit
  PropagateCommand → backlog.Append   ← offset
RUnlock

FULLRESYNC: Lock (WR)
  snapshotOffset = GetMasterReplOffset()
  GenerateRDBWithSnapshotLock(View)
Unlock
send +FULLRESYNC snapshotOffset + RDB
send backlog[snapshotOffset, current)
```

Cost: the write lock is still held for the entire RDB generation, so joining a
replica stalls concurrent writes for that duration (grows with DB size). The
read lock on the write path also covers L0 backoff inside `retryUpdate`.

**Guarantees:**
```
- no lost writes                          ✓
- snapshot consistency (badger MVCC)      ✓
- no structural corruption                ✓
- duplicate window: zero on client writes ✓
- eventual convergence                    ✓
```

Direct `store.*` calls that skip `processRequest` (tests, internal metadata
`RunWriteLocked`) are still unfenced. That is not a client replication path.

**Bigger sibling bug (fixed 2026-08-30):** `GetMasterReplOffset()` used to be a
sum of command lengths, maintained separately from the backlog's contiguous
watermark, so it could point *inside* a command — and that value is what
`+FULLRESYNC` advertises and what `GetRange` slices the ring at. The replica
then received a stream starting mid-command, mis-framing everything after it.
Measured at ~1.1% of joins pre-fix, 0 post-fix — an order of magnitude more
frequent than the commit-vs-propagate race above, and the explanation for the
`非命令边界` downgrade logs. Proof, numbers and the change list:
[repl-offset-boundary-drift.md](repl-offset-boundary-drift.md).

**What landed (2026-08-30):** candidate 1 — span `commit → backlog.Append` on
the client write path (`processRequest` + `EXEC`). `retryUpdate` no longer
RLocks. SPOP still fenced because it is `isWriteCommand` and propagates SREM
inside `executeCommand`.

## Prevention
- Run `store.Check()` after every RDB load in tests
- `TestFullresyncBoundary_CommittedButUnpropagatedWrite`: deterministic check that RDB ∩ backlog-gap is empty for a committed-but-unpropagated write
- `TestRegressionSnapshotFullresyncOffset`: validates FULLRESYNC convergence
- `TestRegressionDuplicateWindowMeasurement`: duplicate window under reconnect chaos
- `TestSoakReplication` with `SOAK_REPL_STRICT_EQUALITY=1` — safe to enable for soak once this fence is on the write path
- **New commits to replication code must pass `snapshot_fullresync_overlap_test`**
- Monitor RDB generation duration — the write lock is held for all of it, so barrier-to-reconnect pressure and write stalls grow with DB size
- For strict equality validation: `SOAK_REPL_STRICT_EQUALITY=1 go test ./cmd/integration/... -run TestSoakReplication`

