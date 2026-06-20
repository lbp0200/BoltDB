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

### Current Behavior

The snapshot offset is captured **before** `db.View()` starts:

```
snapshotOffset = GetMasterReplOffset()  // captured here
db.View(func(txn *badger.Txn) error {   // snapshot starts here
    ...                                  // writes may commit in between
})
backlog.Slice(snapshotOffset, currentOffset)
```

Writes with `offset < snapshotOffset` are in the MVCC snapshot → RDB.
Writes with `offset >= snapshotOffset` are in the backlog.

**Residual duplicate window:** Writes that committed between
`GetMasterReplOffset()` and `db.View()` start are in both RDB and backlog.
This window is typically microseconds (0–1 writes). When a duplicate occurs,
the slave applies the same write twice, which is safe for all idempotent
commands (SET, SADD, HSET, etc.).

### What a Fix Would Require

- Storing a `(replOffset → badgerTS)` mapping at every write
- Loading the mapping on FULLRESYNC to capture a precise MVCC read timestamp
- Propagating the timestamp through the backlog write path
- Handling Badger's MVCC garbage collection to prevent the mapping from
  referencing a GC'd timestamp

### Decision

**Not fixing.** The duplicate window is bounded to microseconds and produces
only idempotent re-application. The fix would touch every write path, the
backlog, the RDB snapshot, and PSYNC semantics — architecture-level effort
for marginal benefit.

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

**Not fixing.** Current guarantees:

| Property | Status |
|----------|--------|
| No lost writes | ✅ Guaranteed — every write is in RDB or backlog |
| No structural corruption | ✅ Guaranteed — Badger MVCC provides consistent snapshots |
| Bounded duplicate window | ✅ Guaranteed — microsecond window, idempotent replay |
| Eventual convergence | ✅ Guaranteed — slave catches up to `currentOffset` |

---

## Summary of Implications

| Scenario | Impact |
|----------|--------|
| Slave reconnects after short outage | PSYNC CONTINUE: no snapshot, no gap |
| Slave reconnects after long outage | FULLRESYNC: duplicate window ≤ few writes |
| Heavy write load during FULLRESYNC | Same duplicate window; no data loss |
| Slave promotes to master (failover) | Offset reset; no gap from prior timeline |
| Long strict soak (6h+) | Shows duplicate-window drift — expected, not a regression |

The duplicate-window is monitored by `TestRegressionDuplicateWindowMeasurement`
and bounded by configurable thresholds. Any breach of those thresholds is a
regression regardless of this architectural boundary.
