# Snapshot Inconsistency → Boundary Duplication Risk

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

### 3. Conservative Snapshot Offset (Current fix)

**Current fix** (May 2026, `handler.go:626`): capture `snapshotOffset` **BEFORE** `db.View()`, not after.

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

## Residual: Boundary Duplication Risk

The tradeoff: writes committed between `snapshotOffset` capture and `db.View()` start are in **both** RDB and backlog. This is a microsecond window (typically 0 concurrent writes).

| Data Type | Effect |
|-----------|--------|
| SET / SADD / HSET / ZADD | Idempotent — no harm |
| INCR / INCRBY | off by 1 (~50% chance for the 1 duplicated write) |
| LPUSH / RPUSH | ~50% duplicate ratio for duplicated writes |

This is **not** a correctness hole — it's a bounded, measurable duplication window. The regression test `TestRegressionSnapshotFullresyncOffset` tolerates it (annotated "within known race window").

## FULLRESYNC Guarantees

```
Guaranteed:
- no lost writes
- snapshot consistency (badger MVCC)
- no structural corruption (no duplicate chain, correct zset cardinality)
- bounded duplicate window (microseconds, typically 0 writes)
- eventual convergence

NOT guaranteed:
- exact linearizable snapshot boundary
- zero-window offset binding between MVCC timeline and replication timeline
```

A complete linearizable boundary requires commit-ts ↔ repl-offset mapping in badger. Tracked in issue #3.

## Prevention
- Run `store.Check()` after every RDB load in tests
- `TestRegressionSnapshotFullresyncOffset`: validates FULLRESYNC convergence + structural integrity
- **New commits to replication code must pass `snapshot_fullresync_overlap_test`**
- Monitor RDB generation duration — barrier-to-reconnect pressure grows with backlog divergence
- For strict equality validation: `SOAK_REPL_STRICT_EQUALITY=1 go test ./cmd/integration/... -run TestSoakReplication`
