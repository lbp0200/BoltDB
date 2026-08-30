# Replication Architecture

## Overview

BoltDB implements Redis-compatible master-replica replication using the PSYNC protocol.
A BoltDB instance can act as a replica of a Redis master (or another BoltDB master) via
`SLAVEOF` / `REPLICAOF`.

## Dual-Timeline Architecture

Unlike single-threaded Redis, BoltDB has two independent timelines:

| Timeline | Mechanism | Granularity |
|----------|-----------|-------------|
| MVCC snapshot | BadgerDB transaction version | Per-write commit timestamp |
| Replication offset | backlog write watermark (`GetMasterReplOffset` = `backlog.GetCurrentOffset`) | Per-propagated command bytes |

This duality is the source of both flexibility and complexity.
The write ordering invariant that connects them is:

```
store.Set() (badger commit) → PropagateCommand() (backlog.Append = offset)
```

Any write that committed to BadgerDB before `GetMasterReplOffset()` was called is
guaranteed visible in the MVCC snapshot. Writes that committed after are covered
by the backlog.

## Handshake Sequence

When a BoltDB replica connects to a master:

```
PING                          → PONG
REPLCONF listening-port <p>   → OK
REPLCONF capa eof             → OK
REPLCONF capa psync2          → OK
PSYNC <replid> <offset>       → FULLRESYNC | CONTINUE
```

### PSYNC CONTINUE

If the master has the slave's replication ID and the slave's offset is still in
the backlog, a CONTINUE response is sent. The master then streams backlog data
from the slave's offset to current.

**Gap-fill mechanism:** `AddSlave` is performed after `SendBacklogData`. Any
commands that arrived between the offset capture and the slave registration
are gap-filled: `[capturedOffset, postAddOffset)` is replayed after the slave
is registered in the propagation snapshot.

### PSYNC FULLRESYNC

If the slave's offset is no longer in the backlog (or the replication ID changed),
a FULLRESYNC is triggered:

```
+FULLRESYNC <replid> <snapshotOffset>
<RDB binary data>
<backlog from snapshotOffset to currentOffset>
```

**snapshotOffset** is captured BEFORE `db.View()`, not after. This eliminates
the lost-write window that was present in earlier versions (see
[failure-modes.md](failure-modes.md)).

## Backlog

The backlog is a fixed-size ring buffer of replicated commands. Default size:
1 MB. When the buffer wraps, the oldest entries are evicted. If a slave's offset
falls outside the current backlog range, a FULLRESYNC is forced.

Key interactions:
- Backlog overflow under heavy write load during slave disconnection → FULLRESYNC
- FULLRESYNC storms under sustained write + reconnect cycling (see [failure-modes.md](failure-modes.md))

## Slave Offset Tracking

The slave maintains `lastOffset` tracking the last byte offset acknowledged from
the master. This must match `GetMasterReplOffset()` (the backlog watermark)
byte-for-byte for PSYNC to work.

**Commands NOT counted in offset (by either master or slave):**
- PING
- REPLCONF GETACK
- SELECT

These are excluded because `PropagateCommand` (master) only counts data-changing
commands. If the slave counted them while the master did not, the slave's offset
would drift ahead, forcing spurious FULLRESYNCs.

After a FULLRESYNC, the slave's `lastOffset` is reset from the offset in the
`+FULLRESYNC` response.

## RDB Snapshot (GenerateRDB)

RDB generation for FULLRESYNC follows these invariants:

- All reads happen in a single `db.View` transaction → consistent point-in-time snapshot
- `PrefetchValues: false` on the TYPE_ iterator → no prefetch memory pressure
- Value keys and list/hash/set/zset data are read with explicit `txn.Get()` /
  sub-iterators from the same transaction
- TOCTOU eliminated: TYPE_ record and corresponding value data are never read
  in separate transactions
- List last node may lack `:next` key; `readListInTxn` handles `ErrKeyNotFound`
  gracefully (break iteration)

### Stream RDB (consumer groups)

`WriteStreamKeyValue` uses RDB type **15** and encodes stream **entries plus**
consumer groups, consumers, and PEL. Legacy type **5** (entries only) is still
loadable for older snapshots.

After FULLRESYNC with type-15 RDB, `XGROUP` / PEL state is restored via
`XGroupRestore`.

## Shutdown Lifecycle

Shutdown contract involving replication (enforced by `Handler.Shutdown()` + `main.go`):

```
close listener → ServeTCP returns
→ replMgr.Stop()       (close slave TCP connections → unblock reads)
→ cancel()             (cancel root context → all goroutines see Done)
→ handler.Shutdown()   (close all client TCP conns + WaitGroup.Wait)
→ backupMgr.Wait()     (wait for in-flight BGSAVE goroutine → no DB access after)
→ db.Close()           (deferred — guaranteed: 0 goroutines accessing DB)
```

- `handleSlaveReplicationConnection` is tracked in `Handler.wg`; its TCP connection
  is closed by `replMgr.Stop()`
- `reconnectLoop` is tracked in `SlaveReconnector.wg`; `replMgr.Stop()` closes
  `stopCh` + master connection
- NO goroutine should call any DB method after `backupMgr.Wait()` returns

### Replica-side reconnector (contract was previously unimplemented)

The second bullet above did **not** hold until 2026-08-30: `replMgr.Stop()` never
touched `slaveReconnector`. Only `StopSlaveReplication` (i.e. `SLAVEOF NO ONE`)
closed `stopCh`, so a server shutting down **while it was a replica** left
`reconnectLoop` running forever — `DefaultReconnectConfig.MaxRetries == 0` means
unbounded retries with a 1s base backoff — and each attempt reaches
`LoadRDB`/`executeReplicatedCommand`, i.e. the store after `db.Close()`.
Observed in regressions as a backoff ladder continuing ~30s after the test's
`Close()` had already run.

The reconnector's own tests did not catch it because they call `sr.Stop()`
explicitly, which proves the reconnector *can* be stopped, not that shutdown
stops it. `TestReplicationManagerStopStopsSlaveReconnector` now asserts the
contract through `ReplicationManager.Stop()` (fails pre-fix: attempts 1 → 3
after `Stop()`; passes post-fix).

Ordering constraint when stopping: `SlaveReconnector.Stop()` does `wg.Wait()`,
and `reconnectLoop` acquires `rm.mu`, so `Stop()` must release `rm.mu` before
waiting. Worst-case shutdown latency added is one `dialMaster` timeout (5s) plus
an in-flight RDB apply.
