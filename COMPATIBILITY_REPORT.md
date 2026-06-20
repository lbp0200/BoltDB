# BoltDB Redis Compatibility Report

> Analysis based on go-redis integration tests, redis-cli verification, RESP Shape test suite, and node-redis compatibility.
> Generated: 2026-06-21

## Test Results Summary

| Suite | Total | Pass | Fail | Rate |
|---|---|---|---|---|
| redis-cli compatibility tests | 77 | 77 | 0 | 100% |
| go-redis integration tests | 49 | 49 | 0 | 100% |
| RESP Shape contract tests | 24 | 24 | 0 | 100% |
| node-redis compatibility | ~110 | ~110 | 0 | 100% |

---

## Compatibility Matrix

| Feature Area | redis-cli | Protocol | Notes |
|---|---|---|---|
| **1. Transaction** | | | |
| MULTI | ✓ | +OK | |
| SET inside MULTI | ✓ | +QUEUED | Commands queued in transaction |
| DISCARD | ✓ | +OK | Discards queued commands |
| EXEC (with MULTI) | ✓ | *N array | Returns queued result array |
| EXEC (without MULTI) | ✓ | -ERR EXEC without MULTI | |
| DISCARD (without MULTI) | ✓ | -ERR DISCARD without MULTI | |
| WATCH | ✓ | +OK | |
| UNWATCH | ✓ | +OK | |
| **2. PubSub** | | | |
| SUBSCRIBE | ✓ | *3 array | subscribe/channel/count |
| PUBLISH | ✓ | :count | Integer subscriber count |
| UNSUBSCRIBE | ✓ | *3 array | unsubscribe/channel/count |
| PUBSUB CHANNELS | ✓ | *N array | Active channel list |
| PUBSUB NUMPAT | ✓ | :0 | Pattern subscription count |
| Message delivery | N/A | Push message | Verified via go-redis |
| **3. Timeout** | | | |
| EXPIRE | ✓ | :1 on success | |
| TTL (with expire) | ✓ | :seconds | Positive TTL value |
| TTL (no expire) | ✓ | :-1 | |
| TTL (non-existent) | ✓ | :-2 | |
| PTTL | ✓ | :milliseconds | |
| PERSIST | ✓ | :1 on success | |
| Key eviction | ✓ | Key removed after TTL | |
| BLPOP timeout | ✓ | *-1 | nil array on timeout |
| **4. WRONGTYPE** | | | |
| GET on list | ✓ | -WRONGTYPE | |
| GET on hash | ✓ | -WRONGTYPE | |
| LLEN on string | ✓ | -WRONGTYPE | |
| HGET on string | ✓ | -WRONGTYPE | |
| SMEMBERS on string | ✓ | -WRONGTYPE | |
| ZCARD on string | ✓ | -WRONGTYPE | |
| HLEN on string | ✓ | -WRONGTYPE | |
| HGETALL on string | ✓ | -WRONGTYPE | |
| HEXISTS on string | ✓ | -WRONGTYPE | |
| HKEYS on string | ✓ | -WRONGTYPE | |
| HVALS on string | ✓ | -WRONGTYPE | |
| HSTRLEN on string | ✓ | -WRONGTYPE | |
| STRLEN on list | ✓ | -WRONGTYPE | |
| LRANGE on string | ✓ | -WRONGTYPE | |
| RPOP on string | ✓ | -WRONGTYPE | |
| LINDEX on string | ✓ | -WRONGTYPE | |
| BZPOPMAX/BZPOPMIN on string | ✓ | -WRONGTYPE | |
| **5. Nil Response** | | | |
| GET non-existent | ✓ | $-1 | nil bulk string |
| LPOP empty list | ✓ | $-1 | nil bulk string |
| HGET non-existent field | ✓ | $-1 | nil bulk string |
| ZSCORE non-existent member | ✓ | $-1 | nil bulk string |
| LINDEX out of range | ✓ | $-1 | nil bulk string |
| SPOP empty set | ✓ | *-1 | nil array |
| RANDOMKEY on empty | ✓ | $-1 | nil bulk string |
| TYPE non-existent | ✓ | +none | |
| **6. Connection** | | | |
| Disconnect cleanup | ✓ | Clean subscriber state | |
| **7. SINTERCARD** | | | |
| Two sets | ✓ | :count | |
| Three sets | ✓ | :count | |
| Non-existent set | ✓ | :0 | |
| Single set | ✓ | :count | |
| Error: numkeys mismatch | ✓ | -ERR | |
| **8. SMISMEMBER** | | | |
| Mixed hits/misses | ✓ | *N array | Binary 0/1 results |
| Single hit | ✓ | *1 array | |
| Single miss | ✓ | *1 array | |
| Missing key | ✓ | *1 array | |
| **9. BZPOP (Blocking ZSet)** | | | |
| BZPOPMAX timeout | ✓ | *-1 | nil array |
| BZPOPMIN timeout | ✓ | *-1 | nil array |
| BZPOPMAX with data | ✓ | *3 array | key/member/score |
| BZPOPMIN with data | ✓ | *3 array | key/member/score |
| **10. HyperLogLog** | | | |
| PFADD (new) | ✓ | :1 | |
| PFADD (existing) | ✓ | :0 | |
| PFCOUNT | ✓ | :count | |
| PFMERGE | ✓ | +OK | |
| **11. Stream** | | | |
| XADD (explicit ID) | ✓ | $id | |
| XADD (auto ID) | ✓ | $id | auto-generated |
| XLEN | ✓ | :count | |
| XRANGE | ✓ | *N array | Entry list |

---

## Known Differences (Documented, Not Bugs)

| Area | BoltDB | Redis | Reason |
|---|---|---|---|
| RESP3 HELLO 3 | Supported: returns `%` Map (7 fields) | RESP3 protocol | P0 feature |
| CLUSTER MEET | Real TCP handshake over RESP port | Cluster bus port | Architecture decision |
| PubSub RESP3 | Push `>` type when respVersion=3 | Same format | RESP3 mode |
| Sentinel gossip | GossipProtocol on separate TCP port | Inter-sentinel port | Architecture decision |
| WATCH | Returns `+OK` | `+OK` | Fixed to match Redis |
| MULTI transaction | QUEUED per command | QUEUED | Fixed to work across connections |

---

## Notes

- redis-cli tests run locally against a fresh BoltDB instance per run.
- WRONGTYPE error messages match Redis exactly: `WRONGTYPE Operation against a key holding the wrong kind of value`
- Integer responses from redis-cli 8.x may drop the `(integer)` prefix; all functional checks pass.
- RESP3 HELLO 3 support verified through shape contract tests.
- Stream, HyperLogLog, SINTERCARD, SMISMEMBER, and BZPOP all fully compatible.
