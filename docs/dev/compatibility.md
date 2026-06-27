# BoltDB Redis Compatibility

This document tracks known behavioral deviations between BoltDB and Redis.
All items below have been verified via automated compatibility test suites
(redis-py 153 tests, node-redis 110 tests, redis-cli 77 tests).

## Fixed Deviations (now Redis-compatible)

| Command | Issue | Fix | Since |
|---------|-------|-----|-------|
| `RENAME k k` | Destructive: deleted source then failed to copy | Added early return for `key == newKey` (no-op, returns +OK) | `TestBoundary_RENAME_SameKey`, `TestBoundary_RENAME_SameKey_PreservesValue` |
| `SETEX k 0 v` | Accepted TTL=0 and stored value | Added `seconds <= 0` validation → `ERR invalid expire time` | `TestBoundary_SetEX_ZeroTTL`, `TestBoundary_SETEX_TTLEdgeCases` |
| `PSETEX k 0 v` | Accepted TTL=0 and stored value | Added `milliseconds <= 0` validation → `ERR invalid expire time` | `TestBoundary_PSETEX_TTLEdgeCases` |

## Known Deviations (by design)

| Command | BoltDB Behavior | Redis Behavior | Reason |
|---------|----------------|----------------|--------|
| `EVAL` / `EVALSHA` | Not implemented | Full Lua scripting | Lua sandbox escape risk + maintenance cost |
| `ACL` / `FUNCTION` | Not implemented | Full ACL system | P2, added on demand |
| `MIGRATE` | Not implemented | Key migration between instances | Not applicable (embedded DB) |
| `WAIT` | Returns 0 (no-op) | Blocks until replicas acknowledge | No replication ACK protocol |
| `LATENCY` | Returns empty | Hardware-level latency tracking | Not applicable |
| `OBJECT` | Partial | Full internal encoding info | Low priority |

## Test Coverage

Compatibility is verified by three independent test suites:

| Suite | Tests | Language | Coverage |
|-------|-------|----------|----------|
| `redis_py_compat.py` | 153 | Python (redis-py) | String, Hash, List, Set, SortedSet, PubSub, Transactions, Streams |
| `redis_node_compat.mjs` | 110 | Node.js (node-redis) | Same scope, client-specific edge cases |
| `redis_cli_compat.sh` | 77 | redis-cli | RESP wire format, pipeline, inline commands |

Run all suites:
```bash
python3 scripts/redis_py_compat.py
node scripts/redis_node_compat.mjs
bash scripts/redis_cli_compat.sh
```

## Adding New Deviations

When a new deviation is discovered:

1. **If it's a bug**: Fix it (like RENAME/SETEX above) and add a regression test
   in `handler_boundary_test.go` or the relevant `_test.go` file.

2. **If it's by design**: Add a row to "Known Deviations" above with a clear
   reason. Add a test that documents the behavior (use `t.Logf` to note the
   deviation, don't fail the test).

3. **If it's ambiguous**: Check Redis source code for the canonical behavior.
   When in doubt, match Redis — users expect Redis semantics.
