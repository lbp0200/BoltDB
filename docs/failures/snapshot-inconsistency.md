# Snapshot Inconsistency

## Symptom
- RDB snapshot contains a key's TYPE_ record but no corresponding data record
- Or: snapshot contains data for a key that has no TYPE_ record
- After loading RDB on slave, some keys are missing or corrupted
- Orphan keys detected by `store.Check()` — "N orphan type keys, N orphan data keys"
- Slave diverges from master but reports the same replication offset

## Root Cause
- RDB generation uses a single `db.View` transaction — this is correct for ensuring point-in-time consistency
- However, the data is read with **sub-iterators** within the same transaction:
  - TYPE_ iterator iterates all keys with prefix `TYPE_`
  - For each key, value is read with `txn.Get()`, list/hash/set/zset elements are read with **additional sub-iterators** from the same `txn`
- **TOCTOU bug** (original pre-fix): In `GenerateRDB`, the TYPE_ record and value data were read in separate transactions. Fixed in current code — all reads happen in one `db.View` call.
- Remaining risk: **large snapshot generation** blocks compaction:
  - `db.View` transactions pin the LSM tree
  - If the snapshot takes too long, BadgerDB can't compact L0 → L0 score rises
  - Long-running `db.View` can block writers (BadgerDB v4 behavior)
- **Iteration with `PrefetchValues: false`** is correct (avoids memory pressure), but sub-iterator for each key creates per-key overhead

## Invariant Violated
- **All reads in single db.View**: TYPE_ record and value data read in same transaction (FIXED)
- **Snapshots are point-in-time**: no partial updates visible (FIXED)
- **No TOCTOU**: data matches TYPE_ record exactly

## Fix (Already Applied)
1. Single `db.View` transaction in `GenerateRDB` — all reads consistent
2. `PrefetchValues: false` on the TYPE_ iterator — no memory pressure
3. `readListInTxn` handles `ErrKeyNotFound` gracefully for last node's `:next` key

## Prevention
- Run `store.Check()` after every RDB load in tests
- Integration test: write random data, generate RDB, load into fresh DB, verify all keys match
- Monitor RDB generation duration — if >5s, consider streaming RDB (write-as-you-read)
- Add snapshot integrity checksum (not currently implemented)
- In soak tests, verify `store.Check()` passes after full replication cycle
