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
SET, SETEX, PSETEX, SETNX, GETSET, MSET, MSETNX, INCRBYFLOAT, SETRANGE,
DEL, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, PERSIST, RENAME, RENAMENX,
INCR, INCRBY, DECR, DECRBY, APPEND

### List
RPUSH, LPUSH, LPOP, RPOP, LPUSHX, RPUSHX, LINSERT, RPOPLPUSH, LMOVE,
LSET, LREM, LTRIM, **BLPOP, BRPOP, BLMOVE, BRPOPLPUSH**
(Blocking variants use non-blocking equivalents on replica)

### Set
SADD, SREM, SMOVE, SPOP, SINTERSTORE, SUNIONSTORE, SDIFFSTORE

### Hash
HSET, HMSET, HINCRBY, HINCRBYFLOAT, HDEL, **HSETNX**

### Sorted Set
ZADD, ZINCRBY, ZREM, ZPOPMAX, ZPOPMIN, ZREMRANGEBYRANK,
ZREMRANGEBYSCORE, ZREMRANGEBYLEX

### Geo
GEOADD

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

## Replication Data Loss — Known Gaps

These commands are propagated via `isWriteCommand` → `handler.go:528-531`
but silently skipped in `executeReplicatedCommand`:

| Command | Priority | Status |
|---------|----------|--------|
| SETBIT, BITOP, BITFIELD | P2 | Store layer not implemented |
| COPY | P2 | Store layer not implemented |
| ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE | P2 | Complex argument parsing |
| ZRANGESTORE, GEOSEARCHSTORE | P2 | Complex argument parsing |
| JSON.\* (all writes) | P3 | Module command |
| TS.\* (all writes) | P3 | Module command |

**Impact:** Master and replica datasets diverge when these commands are used.
This is a silent data loss — commands are dropped without error.
