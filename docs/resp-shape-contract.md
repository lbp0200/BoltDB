# RESP Shape Contract Suite

File: `internal/server/handler_resp_shape_test.go` — 24 tests guarding response structure.

Scope: validates the exact `proto.RESP` type (`NestedArray` vs `Array` vs `Integer` vs `BulkString` vs `SimpleString`) and nesting depth for every command that returns structured data. Covers:

| Family | Commands | What's guarded |
|--------|----------|----------------|
| SCAN | `SCAN`, `SSCAN`, `HSCAN`, `ZSCAN` | outer `[cursor, [data...]]` shape |
| GEO | `GEOPOS`, `GEORADIUS`, `GEOSEARCH` | `[member, [lon, lat]]` with modifiers, flat array without |
| STREAM | `XREAD`, `XRANGE`, `XREADGROUP`, `XAUTOCLAIM` (both modes), `XINFO CONSUMERS/GROUPS/STREAM`, `XPENDING` | per-entry `[id, [fields...]]` nesting |
| CLUSTER | `CLUSTER SLOTS` | `[start, end, [host, port, id]]` sub-arrays |
| PRIMITIVE | `SET`, `HSET`, `SADD`, `EXISTS`, `DEL`, `TYPE`, `GET` | return type correctness (Integer vs SimpleString vs nil BulkString) |
| ERROR | WrongType errors | `*proto.Error` not wrapped in `"ERR %v"` |

## Rules

- **New commands:** if response structure is not simple integer/simple string, must add shape test.
- **Changes to** `[]interface{}` / nested array / stream / geo / cluster / scan: must pass this suite.
- redis-py/node-redis compat = external validation; RESP shape suite = internal structural contract.

## Run

```bash
bash scripts/remote-test.sh -race -timeout 30s -run TestRESPShape ./internal/server/...
```
