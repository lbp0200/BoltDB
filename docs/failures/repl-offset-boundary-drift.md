# Replication Offset Boundary Drift

**Status: fixed (2026-08-30).** The replication offset is now the backlog's
contiguous write watermark — one source of truth instead of two. See
"Fix (implemented)" below.

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

## Evidence (measured)

`internal/replication/repl_offset_boundary_test.go` →
`TestFullresyncAdvertisedOffsetIsServable` drives the handler's real FULLRESYNC
sequence (snapshot lock → advertise → generate RDB → slice the ring) while four
writers propagate, and requires the advertised offset to slice out whole
commands:

| build | unservable windows | rate |
|---|---|---|
| pre-fix (`088ce37`) | 10/545, 0/486, 8/548 | **~1.1%** of joins |
| post-fix | 0/1628, 0/1620, 0/1759 | **0** over ~4.9k windows |

The observed bad first bytes were `'\n'` and `'3'` — the tail of a RESP array
header, i.e. exactly a mid-command start. At ~1% per join this matches the
field log (one `非命令边界` downgrade within ~6 storm joins).

Field arithmetic: `1844104 - 1843962 = 142`, the same value the regression loop
had recorded as `stable lag=142`.

### Correction of an earlier claim

The first version of this analysis "proved" the defect with a hand-arranged
interleaving (two `Append`s, then the length-sums in the opposite order) and
reported `offset=39` inside `[0,59)`. That interleaving is *possible* but the
runtime almost never produces it: a tight sampler over 20000 reads saw the
counter below the ring 2700 times and **mid-command 0 times**, because a lag by
a whole suffix command is still a boundary. The rate only appears when the
sampling window is the real one — the FULLRESYNC critical section blocks
*commits* but not `PropagateCommand`, so the counter drifts for the whole RDB
generation. Numbers above are from that shape. The mid-command framing was
overstated; the divergence and its consequence were not.

## Fix (implemented)

The offset **is** the backlog watermark; the parallel counter is gone.

- `backlog.SetOffset(o)` — forward-only watermark move, for restore/tests.
- `GetMasterReplOffset()` → `backlog.GetCurrentOffset()`;
  `SetMasterReplOffset()` → `backlog.SetOffset()`.
- `masterReplOffset` field and `IncrementReplOffset` deleted;
  `PropagateCommand` advances the offset only via `backlog.Append`.
- `HandlePSync` no longer reads the field directly — the CONTINUE check and the
  byte range now come from the same number (they previously compared a request
  against the counter while ranging the ring).
- `Stop()` persists `SaveMasterReplOffset(bOff)` from the same ring read as
  `SaveBacklog`, so the two persisted values cannot disagree. It reads under
  `rb.mu` directly because `Stop()` already holds `rm.mu` (Lock-then-RLock on
  the same goroutine would self-deadlock).

Semantics deliberately changed: a persisted offset whose backlog was **not**
restored is no longer resurrected as the watermark (crash or truncated
`LoadBacklog` ⇒ watermark 0 ⇒ reconnecting slaves FULLRESYNC). Seeding an empty
ring to the old value would let `HandlePSync` satisfy a CONTINUE out of
zero-filled bytes. `TestReplicationManager_PersistedReplId` was updated to that
expectation and `TestReplicationManager_PersistedOffsetWithBacklog` added for
the clean-restart path, which still preserves the offset.

`CatchUpAndEnableSlave` and the propMu/Ready handshake are untouched: they only
needed a boundary-exact end offset, which they now always get.

## Prevention

- `TestFullresyncAdvertisedOffsetIsServable` is the permanent guard; do not
  reframe it as `GetMasterReplOffset() == backlog.GetCurrentOffset()` — with
  concurrent writers those two reads are not an atomic pair, so that assertion
  tests the sampler, not the implementation.
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
