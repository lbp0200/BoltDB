# Architecture Boundaries

This document describes known architectural limitations that were evaluated
and explicitly decided **not to fix**. Each carries a correctness implication
that is bounded, documented, and accepted.

---

## 1. commit-seq ↔ repl-offset 映射

### Problem

BoltDB has two independent timelines:

| Timeline | Mechanism | Granularity |
|----------|-----------|-------------|
| MVCC snapshot | BadgerDB transaction version | Per-write commit timestamp |
| Replication offset | `masterReplOffset` counter | Per-propagated command byte count |

There is no persistent mapping from a replication offset to a specific Badger
MVCC transaction. This means:

- After a slave reconnects with `PSYNC <replid> <offset>`, the master cannot
  say "give me the MVCC snapshot that corresponds to exactly this offset."
- Instead, the snapshot is taken at `GetMasterReplOffset()` time (a
  best-effort point), and any writes that committed between the offset capture
  and the snapshot start appear in both the RDB and the backlog.

### Current Behavior (Fixed — Issue #3, 2026-08-27)

The snapshot offset capture and MVCC `View` are **atomically bound** by
`store.snapshotMu` (`RWMutex`): FULLRESYNC holds the write lock across
`GetMasterReplOffset() → GenerateRDBWithSnapshotLock(View)`, while normal
writes hold a read lock via `retryUpdate`. No write can commit between the
offset capture and the `View` start, so the two datasets are disjoint.

```
SnapshotMuLock()                            // WR
  snapshotOffset = GetMasterReplOffset()    // captured under lock
  GenerateRDBWithSnapshotLock(View)         // still under WR
SnapshotMuUnlock()                          // release before network I/O
backlog.Slice(snapshotOffset, currentOffset)
```

Writes with `offset < snapshotOffset` are in the MVCC snapshot → RDB.
Writes with `offset >= snapshotOffset` are in the backlog.
**Zero duplicate window** — linearizable offset↔MVCC binding.

Historical note: before the fix, any write that committed between
`GetMasterReplOffset()` and `db.View()` appeared in both RDB and backlog
(microsecond window, 0–1 writes, idempotent commands unaffected).

### What the Fix Required (Done)

- `store.snapshotMu` RWMutex + `retryUpdate` read-lock + FULLRESYNC write-lock
- No `(replOffset → badgerTS)` mapping is needed — the lock achieves the same
  invariant with lower complexity. The mapping remains a valid alternative but is
  not implemented.

### Decision

**Fixed.** The linearizable boundary is enforced by the critical-section
ordering. See `docs/failures/snapshot-inconsistency.md §4`.

---

## 2. 完全线性化 FULLRESYNC

### Problem

During FULLRESYNC, the slave receives:

1. RDB snapshot (bulk data at `snapshotOffset`)
2. Backlog slice from `[snapshotOffset, currentOffset)`

These two phases are sequential over the wire, but the slave processes them
in order: load RDB → replay backlog. Between the RDB load and the backlog
replay, there is a brief window where the slave has the full snapshot but is
missing the backlog commands. This is **not** linearizable with respect to the
master's write timeline.

### Current Behavior

```
Master timeline (offsets):
  [0] ... [snapshotOffset] ... [currentOffset]
       ^--- in RDB ---^   ^--- in backlog ---^

Slave timeline:
  t1: receive RDB → load RDB (has state at snapshotOffset)
  t2: receive backlog → replay backlog (catches up to currentOffset)
```

Outcome: after FULLRESYNC, the slave converges to `currentOffset` equality.
No writes are lost. The only cost is a brief window (t1–t2) where the slave is
not yet caught up — equivalent to the normal replication lag window.

### What a Fix Would Require

Linearizing FULLRESYNC is equivalent to fixing the commit-seq ↔ repl-offset
mapping (boundary #1). A perfectly linearizable FULLRESYNC would need:

- A precise MVCC timestamp corresponding to `snapshotOffset`
- The ability to take an MVCC snapshot at exactly that timestamp
- The backlog to start from precisely that point with no overlap

These are the same architectural changes that fix boundary #1.

### Decision

**Fixed — Issue #3 (2026-08-27, `store.snapshotMu`).** Current guarantees:

| Property | Status |
|----------|--------|
| No lost writes | ✅ Guaranteed — every write is in RDB or backlog |
| No structural corruption | ✅ Guaranteed — Badger MVCC provides consistent snapshots |
| Zero duplicate window | ✅ Guaranteed — linearizable offset↔MVCC binding |
| Eventual convergence | ✅ Guaranteed — slave catches up to `currentOffset` |

---

## Summary of Implications

| Scenario | Impact |
|----------|--------|
| Slave reconnects after short outage | PSYNC CONTINUE: no snapshot, no gap |
| Slave reconnects after long outage | FULLRESYNC: zero duplicate window (strict equality) |
| Heavy write load during FULLRESYNC | Same — zero duplicate window; no data loss |
| Slave promotes to master (failover) | Offset reset; no gap from prior timeline |
| Long strict soak (6h+) | Strict equality holds; no duplicate-window drift |

`TestRegressionDuplicateWindowMeasurement` now asserts **zero** duplicate window;
any non-zero gap is a regression.
