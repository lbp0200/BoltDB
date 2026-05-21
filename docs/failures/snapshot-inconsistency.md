# Snapshot Inconsistency

## Symptoms
- After FULLRESYNC, slave is permanently behind master on non-idempotent commands (INCR, LPUSH)
- Slave converges at the offset level but data is wrong
- `store.Check()` passes — data exists, just semantically incorrect
- Replicated lists have duplicate elements (if backlog replays commands already in RDB)
- Or: replicated lists are shorter than master (if RDB View window dropped writes)

## Root Cause (Original: TOCTOU)
- **Original pre-fix**: `GenerateRDB` read TYPE_ records and value data in separate transactions. A Write between the two reads could create dangling TYPE_ with no data. **FIXED**: single `db.View`.

## Root Cause (Real: Replication Hole)

**The critical FULLRESYNC bug** (handler.go: handlePSyncWithRDB):

Old flow:
```
HandlePSync captures offset O1  ← pre-snapshot
↓
GenerateRDB                      ← snapshot includes O1→O2
↓
send +FULLRESYNC O1              ← announces O1 to slave
↓
AddSlave                          ← PropagateCommand sends O3+
```

**Commands O1→O2 are in the RDB but slave starts at O1.** These commands are **never replayed** — there was no backlog send in the FULLRESYNC path. Result: permanent data loss.

This is a **replication hole**, not just eventual inconsistency. Commands between offset capture and snapshot completion are silently dropped.

## Fix (Applied)

New flow (`handler.go:612-700`):
```
GenerateRDB()                     ← badger View, consistent snapshot
↓
capture snapshotOffset             ← post-View → snapshot boundary
↓
send +FULLRESYNC snapshotOffset    ← announces real boundary
↓
send backlog(snapshotOffset→currentOffset)  ← covers writes during RDB send
↓
AddSlave (under slaveConn.mu)      ← PropagateCommand takes over
```

## Known Limitation: View Blind Window

Even with the fix, there's a fundamental dual-timeline problem:

| Timeline | Source |
|----------|--------|
| MVCC snapshot timeline | badger txn/version |
| Replication offset timeline | masterReplOffset |

**These two timelines are not synchronously mapped.** The badger `db.View` captures a point-in-time state at timestamp T_view. The replication offset is captured AFTER the View returns (O_snapshot). Writes committed between T_view and O_snapshot are:

- **Not in the RDB** (committed after View started)
- **Not in the backlog** (committed before snapshotOffset capture)

These ~100-200ms of writes are permanently lost for this FULLRESYNC cycle. A **subsequent FULLRESYNC** recovers them (they're included in the next RDB).

Redis doesn't have this problem because it's single-threaded — the RDB boundary and replication command boundary are naturally aligned. BoltDB's goroutine concurrency creates two independent timelines.

## FULLRESYNC Guarantees

```
Guaranteed:
- snapshot consistency (badger MVCC)
- eventual convergence (subsequent FULLRESYNC repairs)
- no structural corruption (no duplicate chain, correct zset cardinality)

NOT guaranteed:
- exact linearizable snapshot boundary
- zero-window offset binding between MVCC timeline and replication timeline
```

This is a documented design tradeoff. See `TestRegressionSnapshotFullresyncOffset` for the regression that validates convergence behavior within the known window.

## Prevention
- Run `store.Check()` after every RDB load in tests
- Integration test: write all data types, generate RDB, load into fresh DB, verify all keys match
- `TestRegressionSnapshotFullresyncOffset`: validates FULLRESYNC convergence + structural integrity
- Monitor RDB generation duration — if >5s, backlog blind window grows proportionally
- In soak tests, verify replication converges after chaos cycles
- **New commits to replication code must pass `snapshot_fullresync_overlap_test`**
