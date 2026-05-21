# L0 Collapse

## Symptom
- L0 score climbs above 20 (hard threshold)
- All writes are rejected with `ErrWriteRejected`
- `L0Rejected` counter spikes
- Retry goroutines accumulate, `ActiveRetries` stays high
- Compaction cannot keep up because writes keep coming
- Positive feedback: L0 score rises → writes rejected → clients retry → more L0 pressure
- Eventually all write operations fail, database enters read-only-hell state

## Root Cause
- Write throughput exceeds BadgerDB compaction throughput
- L0 tables accumulate faster than compaction can merge them into L1
- Common triggers:
  - Bulk load (massive SET/MSET)
  - Too many small writes with no batching
  - Under-provisioned compaction goroutines (`NumGoroutines` too low)
  - Disk I/O contention (same disk as other processes)
  - Too many concurrent write transactions (BadgerDB's `doWrites` saturates)

## Invariant Violated
- **L0 score must stay below hard threshold**: `l0Score < L0HardThreshold` (default 20)
- **Compaction must not fall behind indefinitely**: L0 score should sawtooth, not monotonically increase

## Fix
1. Backpressure system (P1-B) — already implemented in `preWriteCheck`:
   - Soft threshold (8.0): delay proportional to L0 score, max 1s
   - Hard threshold (20.0): reject writes with backpressure error
2. Verify `writeSlot` semaphore is correctly sized — `DefaultMaxConcurrentWrites=50`
3. Add monitoring on compaction: if L0 score stays above 10 for >30s, trigger manual compaction
4. Consider increasing `NumLevelZeroTables` and `NumLevelZeroTablesStall` in BadgerDB options
5. Add write-coalescing batching before `retryUpdate` for small writes

## Prevention
- Watch `L0Score` in soak dashboard (it should sawtooth between 0 and ~5)
- Alert if L0 score > 15 for more than 10 seconds
- In soak tests, ensure L0 score drops below soft threshold within 30s of write stop
- Tune throughput: if L0 score > 10 under normal load, reduce write concurrency or improve compaction
- Use SSD with high IOPS for BadgerDB data directory
