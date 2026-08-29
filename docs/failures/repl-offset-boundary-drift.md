# Replication Offset Boundary Drift

## Symptoms

- Master log: `PSYNC CONTINUE offset 非命令边界，降级为全量同步
  current_offset=1844104 offset=1843962` — the replica asked to resume at an
  offset that is not the start of a command, forcing a needless FULLRESYNC.
  Under reconnect chaos this repeats (the `K:HASH:47` mis-frame → infinite
  resync class guarded by `StartsAtCommandBoundary`).
- A joining replica receives a byte stream that begins **inside** a command, so
  `ReadRESP` mis-parses the first command and every subsequent framing is
  shifted. Applies then either fail (→ the skip path advances `lastOffset`
  without applying, silently diverging) or corrupt key names.
- Reproduced as: a list element present on the master and absent on the slave
  with no duplicate to account for it.

## Root cause

The master tracks the replication offset as **two independent timelines**:

| | advanced by | property |
|---|---|---|
| `ReplicationBacklog.offset` | `Append()` under `rb.mu` | contiguous, always a command boundary |
| `ReplicationManager.masterReplOffset` | `IncrementReplOffset(len(cmdBytes))` | **sum of lengths, completion order** |

`PropagateCommand` (`replication.go:341-344`) does them as two unlocked steps:

```go
cmdOffset := backlog.Append(cmdBytes)            // ring moves to X+len, boundary-exact
rm.IncrementReplOffset(int64(len(cmdBytes)))     // counter += len, later, unordered
```

With concurrent propagations the counter can therefore lead or lag the ring:
W1 appends `[0,59)`, W2 appends `[59,98)`, W2 increments first →
`GetMasterReplOffset() == 39`, which is **inside** W1's command. Nothing
couples the two, so no amount of locking around the *reader* fixes it.

## Why it matters

Three consumers assume the value is boundary-exact:

1. `+FULLRESYNC <replid> <offset>` — the replica stores it verbatim as its own
   `lastOffset` (`reconnect.go` `sendPSYNC`).
2. `SendBacklogData` → `backlog.GetRange(snapshotOffset, currentOffset)` slices
   the ring **at** that offset, so the replica's stream starts there.
3. `PSYNC CONTINUE` validation — `StartsAtCommandBoundary` catches a bad offset
   but only on the *reply* path; the master itself advertised it in step 1.

`replication_handler.go` captures `snapshotOffset = GetMasterReplOffset()` and
`currentOffset = GetMasterReplOffset()`, so both the advertised offset and the
gap slice inherit the hazard.

## Evidence (deterministic, no racing)

`internal/replication/repl_offset_boundary_test.go` — both tests currently
`t.Skip`ped as known-open defects:

```
master repl offset diverged from the backlog watermark: offset=39 watermark=98
GetMasterReplOffset()=39 is not a command boundary (A=[0,59), B=[59,98))
FULLRESYNC gap-fill stream starts mid-command: snapshotOffset=61 ... first byte="\r"
```

Field evidence: `1844104 - 1843962 = 142` — the same value the regression loop
had recorded as `stable lag=142`.

## Fix (not implemented — decision pending)

Make the backlog watermark the single source of truth: derive
`GetMasterReplOffset()` from `backlog.GetCurrentOffset()` and drop
`IncrementReplOffset` from `PropagateCommand`. The ring offset is advanced
under `rb.mu` together with the bytes it indexes, so it is boundary-exact by
construction.

Constraints the fix must respect:

- **Never move backwards.** On startup `masterReplOffset` is restored from
  `LoadMasterReplOffset()` while the ring is restored from `LoadBacklog()`;
  after a crash the ring is empty but the persisted offset is not. Seeding the
  ring to `max(persistedOffset, backlogOffset)` (or rejecting a lower value)
  keeps `INFO master_repl_offset` monotonic and preserves PSYNC CONTINUE.
- `cmdOffset` from `Append` stays as the per-command offset for live push and
  the WAL.
- `CatchUpAndEnableSlave`'s loop and the propMu/Ready handshake are unchanged:
  they only need a boundary-exact end offset.

## Prevention

- Unskip both tests in `repl_offset_boundary_test.go` when the fix lands.
- Convergence gates must require `lag == 0`. `TestRegressionSnapshotFullresyncOffset`
  now does; **`waitReplicationConvergence` in `cmd/integration/soak_replication_test.go`
  still accepts "stable lag" as converged** and shares this blind spot
  (a replica frozen in reconnect backoff, or parked behind a mis-framed tail,
  looks converged).
- Tightening that gate exposed the mask: with `lag == 0` required, 2/2 runs
  converged exactly (`mo == so`) with perfect list multisets, i.e. the earlier
  "lost element" reports were the gate declaring convergence too early, not a
  delivery loss.

## Open follow-up

The slave `SlaveReconnector` keeps backing off (retry=3..6, up to 32s) for
~30s *after* its test closed the server — see `master_addr` entries dated after
`--- PASS` in a regressions run. Verify that `Close()` stops the reconnector in
the same shutdown order the real server uses (`replMgr.Stop()` → `cancel()` →
`handler.Shutdown()` → `db.Close()`); a reconnector that outlives `db.Close()`
would access a closed store.
