# Replication Failure Modes

This document catalogs all known replication-specific failure modes, their root
causes, fixes, and how to detect them.

---

## TOCTOU: Offset Capture vs Slave Registration

**Status:** ✅ Fixed (May 2026)

**Bug:** `currentOffset` was captured BEFORE `AddSlave` in both FULLRESYNC and
CONTINUE paths. Writes that propagated between capture and registration were
permanently lost — not in backlog (range used stale offset) and not delivered
to the slave (not yet in propagation snapshot).

**Fix:**
1. FULLRESYNC: `AddSlave` performed BEFORE capturing `currentOffset` under `writeMu`.
   Backlog `[snapshotOffset, currentOffset)` now includes ALL writes during RDB
   generation + the final window.
2. CONTINUE: `SendBacklogData` (original order), then gap-fill
   `[capturedOffset, postAddOffset)` after `AddSlave`. Gap is typically 0-2 commands.
3. `writeMu` separation: Added `SlaveConnection.writeMu` to serialize write I/O.

**Files:** `internal/replication/psync.go`, `internal/server/replication_handler.go` + `handler_core.go`

**Regression guards:**
- `TestRegressionSnapshotFullresyncOffset`
- `TestRegressionPsyncReconnectNoLoss`

---

## Snapshot Offset Ordering (Three Historical Bugs)

**Status:** ✅ Fixed (May 2026)

### Bug 1 (Original)

FULLRESYNC sent `+FULLRESYNC` with pre-snapshot offset and NO backlog. Commands
during RDB generation were silently dropped. Replay hole on slave.

**Commit:** `24e19c2` (identified failure, later fixed)

### Bug 2 (First Fix)

`snapshotOffset` captured AFTER `GenerateRDB`, backlog sent under `slaveConn.mu`.
Eliminated structural corruption but had ~100ms–2s lost-write window between
RDB generation completion and offset capture.

**Commit:** No dedicated commit — intermediate state

### Bug 3 (Fix)

`snapshotOffset` captured BEFORE `GenerateRDB`. Eliminates lost-write window
entirely. Residual microsecond duplicate window remained (bounded, tested).

**Commit:** `6299525`

**Root insight:** The invariant `store.Set() → PropagateCommand()` means any
write with `offset < snapshotOffset` committed before `GetMasterReplOffset()`
→ visible in MVCC snapshot. Writes with `offset >= snapshotOffset` are in the
backlog.

### Bug 4 — Linearizable Boundary (Issue #3)

`store.snapshotMu` RWMutex atomically binds `GetMasterReplOffset()` →
`GenerateRDBWithSnapshotLock(View)`. FULLRESYNC holds the write lock across
the capture+View, normal writes hold a read lock via `retryUpdate`. No write
can land in both RDB and backlog — **zero duplicate window**.

**Commit:** `d5e210d` (followed by doc sync in this patch)

See `docs/failures/snapshot-inconsistency.md §4` and `docs/arch-boundaries.md §1`.

---

## Premature Write Deadline (Reconnect Storm)

**Status:** ✅ Fixed (v8.19.0)

**Root cause:** A 10-second `SetWriteDeadline` on the slave's TCP connection
before `bufio.Writer.Flush()` was intended to prevent indefinite blocking during
slave RDB loading. Instead it created a reconnect/FULLRESYNC storm:

1. Slave connects, FULLRESYNC starts, RDB sent
2. Slave begins loading RDB (CPU-bound, no TCP reads)
3. Backlog data written to TCP buffer → buffer fills → `Flush()` waits
4. Deadline fires after 10s → `Flush()` returns error
5. `bufio.Writer` is now in unknown state (partial flush possible)
6. Connection is semantically broken — must be discarded
7. Slave gets removed from propagation, reconnects
8. New FULLRESYNC triggered, repeat from step 1

**Key insight:** Replication write stalls during RDB loading are **bounded by
definition**. The slave will finish loading and drain the TCP buffer. A timed-out
`bufio.Writer` must be discarded entirely — you cannot retry a partial flush.

**Files:** `internal/replication/slave.go`, `internal/replication/psync.go`

**Correct strategy:**
- Normal replication writes: wait patiently (bounded stall)
- True interruption (disconnect): let `handleSlaveReplicationConnection` detect
  via `ReadRESP` error, clean up naturally
- Resource leaks (goroutines): fix at source (`conn.Close`, `h.Shutdown`)
- Reconnection storms: fix by resetting backoff on success

**The three real fixes (v8.19.0):**
1. `CLIENT KILL TYPE NORMAL`: close TCP conn after `cancel()` — unblocks
   `ReadRESP`, goroutine exits
2. `soak_test.go`: add `h.Shutdown()` to cleanup — goroutine check no longer
   false-positive
3. `reconnect.go`: reset retries on any successful reconnect — backoff doesn't
   blow up under rapid CLIENT KILL

**See:** `docs/failures/replication-write-deadline.md`

---

## Backlog Exhaustion / Replication Thrash

**Status:** ✅ Bounded (known envelope)

**Mechanism:** Under heavy write load during a slave disconnection, the fixed-size
backlog ring buffer (default 1 MB) overflows. When the slave reconnects, its
offset is no longer in the backlog, forcing FULLRESYNC instead of PSYNC CONTINUE.
Repeated FULLRESYNCs under high write throughput prevent replication lag from
ever closing.

**Related failure:** Same root cause as the write-deadline storm — any mechanism
that disconnects the slave under sustained write load will exhaust the backlog.

**Mitigations:**
- Short strict soak verifies reconnect lifecycle correctness
- Long non-strict soak tracks FULLRESYNC frequency as a degradation signal
- `PressureMonitor.CheckDegradation` enforces `reconnectCount < MaxReconnectCount`

**See:** `docs/failures/backlog-exhaustion.md`, `docs/failures/replication-thrash.md`

---

## Slave Offset Drift

**Status:** ✅ Fixed (May 2026)

**Root cause:** `readCommandLoop` was counting PING, REPLCONF GETACK, and SELECT
in `lastOffset`. The master's `PropagateCommand` does NOT count these (it only
counts data commands). The slave's offset drifted ahead of the master's, making
every PSYNC attempt result in a FULLRESYNC.

**Fix:** `readCommandLoop` now handles PING (→PONG), REPLCONF GETACK (→ACK
offset), and SELECT (ignored) without incrementing `lastOffset`.

**After FULLRESYNC:** `lastOffset` is reset from the offset in the `+FULLRESYNC
replid <snapshotOffset>` response.

**Commit:** `8b05096` (part of a larger correctness fix)

**See:** [architecture.md](architecture.md#slave-offset-tracking)

---

## Shutdown Race (Replication Connections)

**Status:** ✅ Fixed

**Race:** During SIGINT/SIGTERM, `handleSlaveReplicationConnection` could be
blocked on `ReadRESP` while `db.Close()` was called, causing a panic. Similarly,
`reconnectLoop` could try to reconnect after `replMgr.Stop()` had already cleaned
up.

**Fix:** Strict ordering enforced by `Handler.Shutdown()`:

```
replMgr.Stop() → close slave TCP conns → ReadRESP unblocks
→ cancel() → all goroutines see ctx.Done()
→ handler.Shutdown() → wg.Wait → backupMgr.Wait() → 0 goroutines left
→ db.Close() → safe
```

**See:** [architecture.md](architecture.md#shutdown-lifecycle),
`docs/failures/shutdown-race.md`

---

## Duplicate-Window Residual (Closed — Issue #3)

**Status:** ✅ Fixed — zero duplicate window (linearizable, `store.snapshotMu`).

Previously the dual-timeline architecture (Badger MVCC timestamp ≠ replication
offset) created a microsecond window where a write could appear in both RDB and
backlog. This is now closed by the RWMutex binding described above.

**Historical thresholds (pre-fix, for reference):**

| Command | Effect | Threshold |
|---------|--------|-----------|
| INCR | Off-by-1 overcount | ≤ 2 (regression test) |
| LPUSH | ~50% duplicate ratio in concurrent floods | ≤ 70% (regression test) |
| SET / HSET / SADD / ZADD | Duplicate is idempotent — no effect | N/A |

**Current assertion:** `TestRegressionDuplicateWindowMeasurement` asserts
**zero** window (`gap == 0`, `dup ratio == 0`).

**See:** [verification.md](verification.md#tier-3-duplicate-window-regression),
`docs/failures/snapshot-inconsistency.md §4`.
