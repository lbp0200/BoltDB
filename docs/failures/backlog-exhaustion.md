# Backlog Exhaustion

## Symptom

Under heavy write load during slave disconnection:

- Backlog ring buffer (default 1 MB) overflows
- Slave reconnects but cannot PSYNC CONTINUE (offset no longer in backlog)
- Forced into FULLRESYNC — expensive RDB snapshot + transfer
- During FULLRESYNC, View Blind Window causes ~100ms-2s of writes to be lost
- If FULLRESYNC happens repeatedly, replication lag never closes
- In extreme cases, memory pressure from backlog + RDB generation simultaneously

## Root Cause

BoltDB uses a fixed-size circular buffer for the replication backlog (1 MB default). When a slave disconnects:

1. Write commands continue to accumulate in the backlog
2. When the buffer wraps, the oldest entries are overwritten
3. If the slave reconnects with an offset that has been overwritten, PSYNC fails
4. The slave falls back to FULLRESYNC
5. FULLRESYNC requires a badger `db.View()` transaction and RDB generation — expensive for large databases

The key invariant is: **backlog size must be larger than the maximum writes during any expected slave disconnect window**. If the disconnect window exceeds what the backlog can hold, FULLRESYNC is inevitable.

## Invariant Violated

- **PSYNC should succeed if slave reconnects within the backlog window**: If the slave's offset is within `[currentOffset - backlogSize, currentOffset]`, PSYNC must succeed
- **FULLRESYNC must be correct even if forced**: Data convergence after FULLRESYNC is guaranteed (eventually), but the View Blind Window means recent writes may be lost in a single cycle

## Known Limitation

BoltDB's backlog is 1 MB by default (not configurable at runtime). This means:

- At 1 MB/s write throughput, a slave disconnection of >1 second forces FULLRESYNC
- At 100 KB/s, a disconnect of >10 seconds forces FULLRESYNC
- Each FULLRESYNC has a ~100-200ms View Blind Window (see `docs/failures/snapshot-inconsistency.md`)

## Prevention

- Monitor `BacklogSize` metric in production — if the backlog is consistently near capacity, increase it or identify write bursts
- Track slave reconnect time — if reconnects consistently exceed the backlog window, the slave is too slow to reconnect
- In soak tests, measure `writesDuringPartition / backlogSize` ratio to verify FULLRESYNC boundary behavior
- After a FULLRESYNC, verify data integrity with known keys that existed before and during the partition
