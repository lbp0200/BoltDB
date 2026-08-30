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

### Current Behavior (Issue #3 — **invariant NOT achieved**, 2026-08-29)

`store.snapshotMu` binds `snapshotOffset` capture to the MVCC `View`
(FULLRESYNC write lock across `GetMasterReplOffset() → GenerateRDBWithSnapshotLock`).
That closes the old "commit between offset capture and View open" sub-window.
It does **not** serialise `badger commit → PropagateCommand` (`backlog.Append`).
A write that has committed but not yet been appended is visible in the RDB and
then also lands in backlog `[snapshotOffset, current)` — INCR/LPUSH double-apply.
See `docs/failures/snapshot-inconsistency.md` §4 and
`TestFullresyncBoundary_CommittedButUnpropagatedWrite` (currently Skip, fails
by construction).

```
retryUpdate: commit → RUnlock
FULLRESYNC:  SnapshotMuLock → snapshotOffset → View sees W → Unlock
writer:      PropagateCommand(W) → offset >= snapshotOffset
⇒ W ∈ RDB AND W ∈ backlog gap
```

### What landed (insufficient)

- `store.snapshotMu` RWMutex + `retryUpdate` read-lock + FULLRESYNC write-lock
- The `(commitTs → repl-offset)` mapping was rejected on the false assumption
  that the lock already achieved linearizability. That mapping is the remaining
  localised candidate (TODO §1 option 2).

### Decision

**Not fixed.** Do not close Issue #3. Do not enable `SOAK_REPL_STRICT_EQUALITY=1`.
Keep `TestFullresyncBoundary_CommittedButUnpropagatedWrite` skipped until a
fix that spans `commit → offset` (or trims the gap by `readTs`) lands.

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

**Not fixed.** `snapshotMu` did not make FULLRESYNC linearizable (see boundary #1).
Current guarantees:

| Property | Status |
|----------|--------|
| No lost writes | yes — every write is in RDB or backlog |
| No structural corruption | yes — Badger MVCC consistent snapshots |
| Zero duplicate window | **no** — committed-but-unpropagated writes land in both |
| Eventual convergence | yes — slave catches up to `currentOffset` (modulo Issue #3 extras) |

---

## Summary of Implications

| Scenario | Impact |
|----------|--------|
| Slave reconnects after short outage | PSYNC CONTINUE: no snapshot, no gap |
| Slave reconnects after long outage | FULLRESYNC: bounded duplicate window still open (Issue #3) |
| Heavy write load during FULLRESYNC | Same window; INCR/LPUSH can double-apply once |
| Slave promotes to master (failover) | Offset reset; no gap from prior timeline |
| Long strict soak (6h+) | Do not enable `SOAK_REPL_STRICT_EQUALITY=1` |

`TestRegressionDuplicateWindowMeasurement` asserts exact INCR/LIST equality as a
*probe*, not a certificate of the boundary — the deterministic proof that the
window is non-zero is `TestFullresyncBoundary_CommittedButUnpropagatedWrite`.
