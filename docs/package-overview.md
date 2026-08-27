# Package Overview

## Server Entry Points

| Entry | Description |
|-------|-------------|
| `cmd/boltDB/` | Main server binary. CLI: `-addr`, `-dir`, `-log-level`, `-cluster`, `-replicaof`, `-skip-startup-cleanup`, `-metrics-addr`, `-client-output-buffer-limit`, `-max-clients`, `-max-input-bytes`, `-idle-timeout` |
| `cmd/sentinel/main.go` | Standalone sentinel mode |
| `cmd/benchmark/` | Standalone benchmark tool |
| `cmd/evolution/` | Evolution trend analysis for soak results |
| `cmd/benchclean/` | Benchmark cleanup helper |
| `cmd/debug/` | Debug client tool |

## Internal Packages

| Package | Responsibility |
|---------|----------------|
| `internal/server/` | Redis protocol handler — command dispatch, client state, pub/sub, monitor |
| `internal/store/` | BadgerDB storage layer — all data types (string, list, hash, set, zset, geo, stream, JSON, timeseries, HLL) |
| `internal/replication/` | PSYNC replication — RDB snapshot, backlog, offset tracking |
| `internal/cluster/` | Redis Cluster — 16384 slots, CRC-16/XModem, gossip, config persistence |
| `internal/sentinel/` | Built-in sentinel — gossip, failover, master monitoring |
| `internal/proto/` | RESP protocol parser |
| `internal/logger/` | Zerolog structured logging |
| `internal/metrics/` | Prometheus HTTP endpoint, periodic snapshots |
| `internal/monitor/` | Pressure/degradation monitoring for soak tests |
| `internal/backup/` | Live backup — RDB + Badger BGSAVE |
| `internal/helper/` | Test helpers and tools |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BOLTDB_DIR` | Data directory path | temp dir |
| `BOLTDB_ADDR` | Listen address | `:6337` |
| `BOLTDB_LOG_LEVEL` | Log level | `warning` |

## Storage Key Prefixes

BadgerDB key prefixes, defined in `internal/store/define.go` and `sorted_set.go`:

`STRING`, `LIST`, `HASH`, `SET`, `JSON`, `TIMESERIES`, `STREAM`, `zset` (SortedSet), `geo:`, `hll:`

> `geo:` / `hll:` / `STREAM` 的内部类型键为 `GEOHASH` / `hyperloglog` / `STREAM`（见 `store.KeyType*`），与前缀一一对应；近期已补齐 `getKeyValueKey` 与 `TYPE`/`COPY`/`DUMP`/`RESTORE` 全链路覆盖，避免 TTL 与迁移盲区。
> 注意：`GEO` 为双前缀类型（`geo:` + `zset:` 索引），`RENAME`/`DEL`/`COPY`/`NextStartup` 均需同步清理双前缀，避免 `zset:` 孤儿残留；`TIMESERIES`（`TS:`）与 `STREAM`（`stream:`）同理需按前缀闭环处理。

## Deployment

- Docker: `deploy/docker/Dockerfile` (multi-stage, builds `./cmd/boltDB`)
- Systemd: `docs/deployment/systemd.md`
- Docs: `docs/deployment/` — Ubuntu, CentOS, Docker, Homebrew guides
