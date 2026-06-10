# Replication Correctness

## Correctness Envelope

Current verification status:

| Guarantee | Status | Evidence |
|-----------|--------|----------|
| Offset correctness | ✅ | `readCommandLoop` excludes PING/REPLCONF GETACK/SELECT; `lastOffset` reset on FULLRESYNC |
| PSYNC CONTINUE | ✅ | Gap-fill after AddSlave; `writeMu` serialization; regression tests pass |
| Lifecycle chaos strict equality | ✅ | `SOAK_REPL_STRICT_EQUALITY=1` passes under short strict soak (5-15m) |
| SPOP canonicalization | ✅ | SPOP propagates as `SREM key member...` — no random selection on replica |
| Duplicate-window regression | ✅ | `TestRegressionDuplicateWindowMeasurement` quantifies INCR/LPUSH window — bounded by thresholds |
| Long strict soak (6h) | ⚠️ Known limitation | Will show duplicate-window drift unless commit-seq ↔ repl-offset mapping is implemented. Use for L0/retry/goroutine/health/basin/evolution analysis, NOT strict equality. |

## FULLRESYNC Semantics

**No lost writes.** All writes are guaranteed to be in either the RDB snapshot or
the backlog. Proof:

1. `snapshotOffset` is captured BEFORE `db.View()` starts
2. Any write with `offset < snapshotOffset` committed to Badger before
   `GetMasterReplOffset()` → visible in MVCC snapshot → IN RDB
3. Writes with `offset >= snapshotOffset` are covered by backlog
   `[snapshotOffset, currentOffset)`

**Residual duplicate window:** Writes that committed between the
`GetMasterReplOffset()` call and `db.View()` start are in both RDB and backlog
(microsecond window, typically 0 writes). This is bounded and tested.

## Offset Equality Is Not Visibility Equality

The replication offset (`lastOffset`) measures **transport progress** — how many
bytes the slave has consumed from the backlog. It does NOT measure **application
visibility** — whether the corresponding data has been committed to BadgerDB and
is queryable.

This is a general property of log-based replication systems (Kafka consumer
offset, MySQL relay log, Redis replication offset) and is not specific to
BoltDB.

**Consequence:** `WaitForReplicaSync` (used in regression tests) checks
`slaveOffset ≥ masterOffset`. If the slave processes commands asynchronously,
the offset may match even though some commands are still in-flight. Data
convergence requires an additional poll loop that verifies the application-level
state (e.g., `XLen` returns the expected count).

**Guarantee breakdown:**

| Layer | What it measures | When it's stable |
|-------|-----------------|------------------|
| Transport offset | Bytes consumed from backlog | After `readCommandLoop` processes command |
| Application visibility | Data written to BadgerDB | After `executeReplicatedCommand` returns |
| Backlog durability | Command persisted on master | After `backlog.Append()` |

For most commands (SET, SADD, HSET, etc.) the gap between transport and
visibility is sub-millisecond — the `executeReplicatedCommand` call is
synchronous within the `readCommandLoop` goroutine. The gap only widens under
two conditions:

1. **Re-replication during FULLRESYNC:** RDB load + backlog replay are
   sequential but the RDB load phase is bulk — the backlog replay hasn't
   started yet when the RDB sync completes.
2. **Stream writes (XADD):** Stream metadata is updated atomically but the
   visibility check (`XLen`) reads a separate key range; under concurrent
   backlog replay the query may observe a partial state. This is an artifact
   of the test, not a production concern — production readers see a single
   consistent snapshot via Badger MVCC.

**Test pattern for stream convergence** (see `deterministic_replay_test.go`):

```go
// DON'T: assume offset equality means data equality
slave.WaitForReplicaSync(ctx, master, slave, timeout)

// DO: poll the application-level state
deadline := time.Now().Add(15 * time.Second)
for time.Now().Before(deadline) {
    mLen, _ := master.XLen(...).Result()
    sLen, _ := slave.XLen(...).Result()
    if mLen == sLen { break }
    time.Sleep(200 * time.Millisecond)
}
```

## Deterministic Replay

Some commands are non-deterministic when replayed on a replica. These are
canonicalized before propagation:

### Fixed Commands

| Command | Risk | Fix | Location |
|---------|------|-----|----------|
| XADD with `*` | CRITICAL | `args[idPos] = []byte(resultID)` replaces `*` with resolved ID | `handler.go:5912` |
| EXPIRE / PEXPIRE | HIGH | Translated to `PEXPIREAT` with absolute timestamp | `handler.go:527` |
| SPOP | HIGH | Canonicalized to `SREM key member...` with specific members | `handler.go:3391-3431` |

### Known OK (Deterministic or Read-Only)

| Command | Reasoning |
|---------|-----------|
| ZPOPMIN / ZPOPMAX | Deterministic sorted-set order; same logic on replica |
| SINTERSTORE / SUNIONSTORE / SDIFFSTORE | Deterministic iteration; no map iteration in store layer |
| RANDOMKEY | Read-only (no propagation) |
| SRANDMEMBER / ZRANDMEMBER | Read-only (no propagation) |
| SCAN / SSCAN / HSCAN / ZSCAN | Read-only (no propagation) |

## SPOP Canonicalization (Reference Pattern)

The SPOP fix is the reference pattern for any future nondeterministic write command:

```
handler computes result → propagates SREM with deterministic member list
                          instead of propagating SPOP
```

This avoids bypassing `executeReplicatedCommand` — the canonicalized command
enters the general propagation path at `handler.go:528`.

## Commands Handled in executeReplicatedCommand

All commands currently implemented in `internal/replication/psync.go`:

### String
SET, SETEX, PSETEX, SETNX, GETSET, GETDEL, GETEX, MSET, MSETNX,
INCRBYFLOAT, SETRANGE, DEL, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT,
PERSIST, RENAME, RENAMENX, INCR, INCRBY, DECR, DECRBY, APPEND,
**SETBIT, BITOP, BITFIELD**

### List
RPUSH, LPUSH, LPOP, RPOP, LPUSHX, RPUSHX, LINSERT, RPOPLPUSH, LMOVE,
LSET, LREM, LTRIM, BLPOP, BRPOP, BLMOVE, BRPOPLPUSH
(Blocking variants use non-blocking equivalents on replica)

### Set
SADD, SREM, SMOVE, SPOP, SINTERSTORE, SUNIONSTORE, SDIFFSTORE

### Hash
HSET, HMSET, HINCRBY, HINCRBYFLOAT, HDEL, HSETNX

### Sorted Set
ZADD, ZINCRBY, ZREM, ZPOPMAX, ZPOPMIN, ZMPOP, ZREMRANGEBYRANK,
ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZUNIONSTORE, ZINTERSTORE,
ZDIFFSTORE, ZRANGESTORE

### Geo
GEOADD, **GEOSEARCHSTORE**

### Stream
XADD, XDEL, XTRIM, **XACK, XCLAIM, XGROUP** (CREATE/DESTROY/SETID/DELCONSUMER)

### HyperLogLog
**PFADD, PFMERGE**

### Replication-Specific Non-Blocking Equivalents

| Original (Blocking) | Replica Equivalent | Rationale |
|---------------------|-------------------|-----------|
| BLPOP | LPOP on each key in order | Master already resolved the blocking pop |
| BRPOP | RPOP on each key in order | Same as above |
| BLMOVE | LMOVE | Non-blocking version is deterministic |
| BRPOPLPUSH | RPOPLPUSH | Non-blocking version is deterministic |
| BZPOPMIN | ZPOPMIN on each key in order | Master resolved the blocking pop |
| BZPOPMAX | ZPOPMAX on each key in order | Same as above |

### Replication-Specific Stateful Operations

| Command | Replica Behavior |
|---------|-----------------|
| COPY | Type-aware: GET+SET for string, RPUSH for list, HSET for hash, SADD for set, ZADD for zset |
| ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE | Full arg parsing (numkeys/keys/WEIGHTS/AGGREGATE) → store functions |
| ZRANGESTORE | Full arg parsing (BYSCORE/BYLEX/REV/LIMIT) → ZRange/ZRangeByScore/ZRangeByLex + ZAdd |
| GEOSEARCHSTORE | Full arg parsing (FROMMEMBER/FROMLONLAT/BYRADIUS/COUNT/STOREDIST) → GeoSearchStore |
| BITOP | Store function call with full arg reconstruction from received args |
| XGROUP | Subcommand dispatch: CREATE/DESTROY/SETID/DELCONSUMER → store functions |

## Replication Data Loss — Known Gaps

All P1-P2 write commands are now replicated. Remaining gaps are module-level
commands with no store layer implementation:

| Command | Priority | Status |
|---------|----------|--------|
| JSON.\* (all writes) | P3 | Module command |
| TS.\* (all writes) | P3 | Module command |

**Impact:** Limited to module-specific data types not part of core Redis compatibility.
All core Redis data structures have full replication coverage.
