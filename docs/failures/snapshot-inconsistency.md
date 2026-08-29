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

### 4. Linearizable Boundary (Issue #3 — implemented, **invariant NOT achieved**)

**Status (2026-08-29): the duplicate window is still open.** `snapshotMu` landed
(`d5e210d`, `ecaf9df`) and binds `snapshotOffset` to the View, but that is not
the pair the invariant needs.

`store.BotreonStore.snapshotMu` (`sync.RWMutex`):

- FULLRESYNC holds the **write** lock across `GetMasterReplOffset()` → `GenerateRDBWithSnapshotLock()`.
- Write path (`retryUpdate`) holds a **read** lock.

```
SnapshotMuLock()                           ← WR
  snapshotOffset = GetMasterReplOffset()   ← atomic with View open
  GenerateRDBWithSnapshotLock(View)        ← still under WR
SnapshotMuUnlock()                         ← release before network I/O
↓
send +FULLRESYNC snapshotOffset + RDB
↓
send backlog(snapshotOffset→currentOffset) + AddSlave + CatchUpAndEnableSlave
```

**Why it does not close the window.** The lock spans `commit` only; the repl
offset is assigned in `PropagateCommand()`, which the server layer calls
*after* `executeCommand()` returns (`handler_core.go:808`) — outside the read
lock. So the ordering in §3 still permits the bad interleaving:

```
goroutine A: retryUpdate → badger commit → RUnlock   [W now visible to any View]
goroutine B: SnapshotMuLock → snapshotOffset = N → View sees W → Unlock
goroutine A: PropagateCommand(W) → backlog offset = N+k,  N+k >= snapshotOffset
⇒ W ∈ RDB AND W ∈ backlog[snapshotOffset, currentOffset)   ← duplicate
```

Nothing serialises `commit → offset assignment`, so no lock on
`offset capture → View open` can make them atomic. The bad set is unchanged in
kind from §3: it is the writes sitting between commit and propagate at the
instant `snapshotOffset` is read — which the lock does not observe at all.

What the write lock *does* cost: it is held for the entire RDB generation
(`GenerateRDBWithSnapshotLock`), so every write path blocks on `RLock` for that
duration — a full stop-the-world write stall per joining replica, growing with
DB size, in exchange for closing only the sub-window described below.

**Reproduced deterministically** (no race needed — it replays the program order
above): `internal/replication/fullresync_boundary_test.go`
→ `TestFullresyncBoundary_CommittedButUnpropagatedWrite`

```
duplicate window is not zero: W is in the RDB snapshot AND in backlog [30,74),
so the replica applies LPUSH boundary:probe twice (master len 1, slave len 2).
```

**What the lock does buy:** it removes the §3 sub-window (offset captured, then
a *new* commit slipping in before View open) and it stops writes during RDB
generation. Neither removes the commit-vs-propagate gap.

**Guarantees (current, verified):**
```
- no lost writes                          ✓ (ordering commit → propagate holds)
- snapshot consistency (badger MVCC)      ✓
- no structural corruption                ✓
- duplicate window: BOUNDED, not zero     ✗ (strict equality is unsafe — see below)
- eventual convergence                    ✓
```

Consequence: the tolerances removed from `TestRegressionSnapshotFullresyncOffset`
and `TestRegressionDuplicateWindowMeasurement` in `d5e210d` assert an invariant
the code does not provide — they will fail/flake under a write storm, and
`SOAK_REPL_STRICT_EQUALITY=1` must stay off until one of the fixes below lands.

**Candidate fixes** (open decision, none implemented):
1. **Span the critical section** — hold one `snapshotMu` read lock across
   `commit → backlog.Append/IncrementReplOffset`. Requires removing the read
   lock from `retryUpdate` (a nested `RLock` deadlocks once a writer queues) and
   wrapping every writer, including background ones (expiry janitor, `EXEC`,
   `SPOP` normalisation). Highest risk of a write freeze from a missed release.
2. **commit-ts ↔ repl-offset mapping** — record `badger commitTs` per backlog
   entry, capture the View's `readTs`, and trim gap entries with
   `commitTs <= readTs`. Exact and localised to the FULLRESYNC path; this is the
   table the earlier revision of this doc rejected — that rejection assumed the
   lock worked.
3. **Restore bounded tolerance** — keep §3's documented "microsecond duplicate
   window" language and reinstate the pre-`d5e210d` thresholds. Cheapest, leaves
   a known non-zero rate.

## Prevention
- Run `store.Check()` after every RDB load in tests
- `TestFullresyncBoundary_CommittedButUnpropagatedWrite`: deterministic check that RDB ∩ backlog-gap is empty for a committed-but-unpropagated write
- `TestRegressionSnapshotFullresyncOffset`: validates FULLRESYNC convergence
- `TestRegressionDuplicateWindowMeasurement`: duplicate window under reconnect chaos
- `TestSoakReplication` with `SOAK_REPL_STRICT_EQUALITY=1` — **do not enable** until fix 1 or 2 lands
- **New commits to replication code must pass `snapshot_fullresync_overlap_test`**
- Monitor RDB generation duration — the write lock is held for all of it, so barrier-to-reconnect pressure and write stalls grow with DB size
- For strict equality validation: `SOAK_REPL_STRICT_EQUALITY=1 go test ./cmd/integration/... -run TestSoakReplication`

