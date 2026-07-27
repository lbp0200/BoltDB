# RDB Length Encoding Guard

File: `internal/replication/rdb_length_test.go` — 15 tests covering `readLength`/`writeLength` consistency at every boundary of the 6/14/32-bit encoding scheme.

## Boundary values

| Value | Encoding |
|-------|----------|
| 63 | max 6-bit |
| 64 | min 14-bit |
| 16383 | max 14-bit |
| 16384 | min 32-bit |
| 100000 | large 32-bit |

## Test pattern

Each test: store data → `GenerateRDB` → `LoadRDBWithStore` → verify data integrity. Covers string values, set members, list elements, hash fields, hash field values, and zset members at each boundary.

## Run

```bash
bash scripts/remote-test.sh -race -timeout 120s -run TestRDBLengthEncoding ./internal/replication/...
```
