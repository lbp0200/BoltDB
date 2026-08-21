# Historical Replication Fixes

Chronological log of replication correctness fixes, with commit hashes, root
cause, and the problem each one solved.

---

## 24e19c2 — FULLRESYNC Replay Hole

**Date:** May 2026

**Summary:** FULLRESYNC sent `+FULLRESYNC` with pre-snapshot offset and NO
backlog. Commands during RDB generation were silently dropped on the replica.

**Root cause:** The original implementation captured the offset before the RDB
snapshot and sent no backlog — assuming that no writes could happen during
RDB generation. Under concurrent writes, this assumption was false.

**Symptoms:**
- Slave had a subset of master data after FULLRESYNC
- No errors logged — commands silently dropped
- Dataset divergence grew with write throughput

**Fix:** Created the foundation for the current snapshot offset protocol
(backlog window `[snapshotOffset, currentOffset)`), though early versions
still had a lost-write window.

**Related:** [failure-modes.md](failure-modes.md#snapshot-offset-ordering)

---

## 6299525 — Eliminate Lost-Write Window (Current Approach)

**Date:** May 2026

**Summary:** Captured `snapshotOffset` BEFORE `db.View()` instead of after,
closing the ~100ms–2s lost-write window.

**Root cause:** When `snapshotOffset` was captured after `GenerateRDB` completed,
writes that committed between the RDB snapshot and the offset capture were in
neither RDB nor backlog — permanently lost.

**Key insight:** The invariant `store.Set() → PropagateCommand()` means any
write with `offset < snapshotOffset` committed to Badger before
`GetMasterReplOffset()` → guaranteed visible in the MVCC snapshot. Writes with
`offset >= snapshotOffset` are covered by the backlog.

**Residual:** A microsecond duplicate window now exists (writes in both RDB and
backlog), bounded and tested.

**Regression guards:**
- `TestRegressionSnapshotFullresyncOffset`
- `TestRegressionDuplicateWindowMeasurement`

---

## 8b05096 — TOCTOU, Deadlock, Offset Drift, Shutdown Race

**Date:** May 2026

**Summary:** A comprehensive fix pack addressing four distinct bugs found during
replication correctness audit:

### 1. TOCTOU: Offset Capture vs Slave Registration

`currentOffset` was captured BEFORE `AddSlave` in both FULLRESYNC and CONTINUE
paths. Writes between capture and registration were permanently lost.

**Fix:** FULLRESYNC: `AddSlave` before capturing offset under `writeMu`.
CONTINUE: Gap-fill `[capturedOffset, postAddOffset)` after `AddSlave`.

### 2. writeMu Deadlock

Shared `mu` on `SlaveConnection` caused deadlock between `Close()` and
`handlePSyncWithRDB`.

**Fix:** Added `SlaveConnection.writeMu` to serialize write I/O. `Close()` closes
TCP connection first (unblocks pending I/O), then drains `writeMu`.

### 3. Slave Offset Drift

`readCommandLoop` counted PING, REPLCONF GETACK, and SELECT in `lastOffset`.
Master's `PropagateCommand` does NOT count these. Slave's offset drifted ahead,
forcing spurious FULLRESYNCs.

**Fix:** PING → PONG (no count), REPLCONF GETACK → ACK offset (no count),
SELECT → ignored (no count).

### 4. Shutdown Race

`handleSlaveReplicationConnection` blocked on `ReadRESP` without TCP connection
close during shutdown. DB closed while goroutines still running → panic.

**Fix:** Strict ordering: `replMgr.Stop()` → close TCP → `ReadRESP` unblocks →
`cancel()` → goroutines exit → `handler.Shutdown()` → `db.Close()`.

---

## c2dd4c7 — Deflake SlaveReconnector GoroutineLeak Test

**Date:** May 2026

**Summary:** `TestSlaveReconnector_GoroutineLeak` was flaky due to timing —
goroutines weren't always collected by GC before the leak check.

**Fix:** Added grace period + explicit `runtime.GC()` before leak check.

---

## df46325 — CLIENT KILL Goroutine Leak, Write Deadline, Backoff Reset

**Date:** June 2026 (v8.19.0)

**Summary:** Three fixes addressing the write-deadline reconnect storm and
related issues:

### 1. CLIENT KILL TYPE NORMAL — Close TCP After cancel()

Without closing the TCP connection, `ReadRESP` could block indefinitely even
after `cancel()`. Fixed by closing TCP conn after context cancel in the CLIENT
KILL path.

### 2. Write Deadline — Removed from Replication Flush

The 10-second `SetWriteDeadline` before `bufio.Writer.Flush()` caused a
reconnect/FULLRESYNC storm during slave RDB loading. The stall is bounded
by definition — the slave will finish loading.

### 3. Backoff Reset — Reset on Successful Reconnect

Without resetting backoff on success, rapid CLIENT KILL cycles could cause
exponential backoff to blow up, preventing timely reconnection.

---

## f250ad3 — Backlog WAL Never Truncated (Unbounded Growth)

**Date:** Aug 2026

**Summary:** `BacklogWAL.Truncate` existed but was never called from
production code, so `backlog.wal` grew without limit — 37GB per node after
the 100GB scale test (~106GB across the cluster) — and every restart re-read
the whole file into memory via `os.ReadFile` (17min + 30GB RSS peak on a
38GB file, real behavior observed during the fix's rollout).

**Root cause:** The truncate-trigger heuristic documented in
`BacklogWAL.Truncate` ("truncate when the WAL file exceeds 2x the backlog
size") was never wired up. The WAL was append-only by design but nothing
ever called Truncate.

**Fix:**
- `BacklogWAL` gained a `fileMu` RWMutex so the Truncate/Close file
  replacement serializes against concurrent Flush writes (no lost-data
  window); `Append` now reads `len(buf)` under `bufMu` (pre-existing race
  exposed by `-race`); Truncate's all-consumed case was fixed (a fully
  consumed WAL was never emptied).
- `ReplicationManager` truncates once right after `SetBacklogWAL` replay
  (cleans stale multi-GB files at startup) and periodically from
  `PropagateCommand` when the file exceeds 2× backlog size, throttled by an
  atomic counter + CAS gate so the per-command hot path takes no lock.

**Regression guards:**
- `TestBacklogWAL_Truncate_AllConsumed`
- `TestBacklogWAL_Truncate_ConcurrentAppend`
- `TestReplicationManager_WALTruncateTriggered`
- Full replication suite + `TestRegressionDuplicateWindowMeasurement`,
  `TestRegressionSnapshotFullresyncOffset`, `TestRegressionPsyncReconnectNoLoss`

**Deployment:** rolled out to the 3-node 10.1.2.16 cluster; all three
`backlog.wal` files went from 33–37GB to 4KB (~106GB reclaimed).

---

## e322b7c — Rejected Conditional EXPIRE/PEXPIRE Propagated as PEXPIREAT

**Date:** Aug 2026

**Summary:** When a conditional `EXPIRE`/`PEXPIRE` (with `NX`/`XX`/`GT`/`LT`)
was REJECTED on the master (e.g. `EXPIRE k 200 NX` on a key that already has
a TTL → returns 0, master TTL unchanged), the master still propagated the
canonicalized `PEXPIREAT k <absoluteMS>` to the replica, forcing the rejected
absolute expiry onto the replica and causing a TTL drift (master kept 100s,
slave jumped to 200s).

**Root cause:** `processRequest` (handler_core.go) canonicalizes `EXPIRE`/
`PEXPIRE` to `PEXPIREAT` for deterministic replay *inside the propagation
block*, dropping the `NX`/`XX`/`GT`/`LT` condition. The gate
`!isErrorResponse(resp)` treated a returned integer `0` as "propagate this
write", so a rejected conditional write (master side did not change TTL) was
still replayed unconditionally on the replica as `PEXPIREAT`.

**Fix:** For `EXPIRE`/`PEXPIRE` only, propagation is now additionally gated on
`isPositiveIntegerResp(resp)` — i.e. the command must have returned `1`
(master actually wrote a TTL). A returned `0` means no TTL was written, so
nothing is propagated, keeping the replica consistent with the master.
Added `isPositiveIntegerResp` helper in replication_helper.go.

**Regression guards:**
- `TestRegressionCanonicalExpireConditionRejected` (new — reproduces the drift:
  NX rejected → slave TTL must stay consistent with master)
- Existing `TestRegressionCanonicalExpire` / `TestRegressionCanonicalExpireOnExistingKey`
  still pass (successful EXPIRE still propagates)

---

## Summary

| Commit | Date | Scope | Problem |
|--------|------|-------|---------|
| `24e19c2` | May 2026 | FULLRESYNC | Replay hole — pre-snapshot offset, no backlog |
| `6299525` | May 2026 | Snapshot offset | Eliminated lost-write window (pre-View capture) |
| `8b05096` | May 2026 | Multi-fix | TOCTOU, deadlock, offset drift, shutdown race |
| `c2dd4c7` | May 2026 | Test | Deflake goroutine leak test |
| `df46325` | Jun 2026 | Multi-fix | CLIENT KILL leak, write deadline, backoff reset |
| `f250ad3` | Aug 2026 | Backlog WAL | Never truncated — unbounded growth, multi-GB startup replay |
| `e322b7c` | Aug 2026 | Replication | Rejected conditional EXPIRE/PEXPIRE propagated as PEXPIREAT (slave TTL drift) |
