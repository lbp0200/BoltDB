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
| Replication offset | backlog write watermark (`GetMasterReplOffset`) | Per-propagated command bytes |

There is no persistent mapping from a replication offset to a specific Badger
MVCC transaction. This means:

- After a slave reconnects with `PSYNC <replid> <offset>`, the master cannot
  say "give me the MVCC snapshot that corresponds to exactly this offset."
- Instead, the snapshot is taken at `GetMasterReplOffset()` time (a
  best-effort point), and any writes that committed between the offset capture
  and the snapshot start appear in both the RDB and the backlog.

### Current Behavior (Issue #3 — closed on the client write path, 2026-08-30)

`processRequest` holds `snapshotMu.RLock` across `executeCommand` (commit) and
`PropagateCommand` (`backlog.Append`). FULLRESYNC holds the write lock across
`GetMasterReplOffset() → View`. A committed-but-unpropagated write cannot be
visible in the snapshot.

`retryUpdate` no longer takes the read lock (nested RLock deadlocks when a
writer is queued). `EXEC` is fenced even though it is not `isWriteCommand`.

See `docs/failures/snapshot-inconsistency.md` §4 and
`TestFullresyncBoundary_FenceBlocksSnapshotWriteLock`.

### Decision

**Fixed on the client write path.** Direct `store.*` calls that skip
`processRequest` are still unfenced (tests / internal metadata).

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

**Fixed on the client write path** (same fence as boundary #1). Current guarantees:

| Property | Status |
|----------|--------|
| No lost writes | yes — every write is in RDB or backlog |
| No structural corruption | yes — Badger MVCC consistent snapshots |
| Zero duplicate window | yes — commit → offset under snapshotMu.RLock |
| Eventual convergence | yes — slave catches up to `currentOffset` |

---

## Summary of Implications

| Scenario | Impact |
|----------|--------|
| Slave reconnects after short outage | PSYNC CONTINUE: no snapshot, no gap |
| Slave reconnects after long outage | FULLRESYNC: client writes not double-applied |
| Heavy write load during FULLRESYNC | Writes stall for RDB generation; no duplicate window |
| Slave promotes to master (failover) | Offset reset; no gap from prior timeline |
| Long strict soak (6h+) | `SOAK_REPL_STRICT_EQUALITY=1` is meaningful |

`TestFullresyncBoundary_FenceBlocksSnapshotWriteLock` is the deterministic
certificate; dw / overlap regressions remain storm probes.
