# Benchmark Baselines

This directory stores benchmark baseline data used by `scripts/guard_bench.sh` for performance regression detection.

## Files

| File | Source Package | Benchmarks | Baseline Frequency |
|------|---------------|------------|--------------------|
| `bench_baseline_proto.txt` | `internal/proto` | RESP protocol parsing | Update on protocol changes |
| `bench_baseline_store.txt` | `internal/store` | String (Set/Get/MGet), ZSet (ZAdd/ZRange/ZRank at 100/1K/10K) | Update on storage engine changes |
| `bench_baseline_server.txt` | `internal/server` | Command execution (PING/SET/GET/INCR/DEL/MGET/Pipeline), ParseScore, ResponseTypes | Update on handler dispatch changes |

## Updating Baselines

Baselines are cached in `testdata/` and referenced by `guard_bench.sh`. Update after intentional performance changes:

```bash
# Update all baselines
./scripts/guard_bench.sh --update          # proto
./scripts/guard_bench.sh --store --update   # store
./scripts/guard_bench.sh --server --update  # server
```

## Regression Guard

The `guard_bench.sh` script (integrated into CI as `bench-regression` job, Tier A) compares current benchmark results against the cached baseline using `benchstat`. It fails if any benchmark regresses more than 10% (configurable via `GUARD_THRESHOLD`).

Baselines are copied here from `testdata/` for documentation and version tracking. The authoritative copies live in `testdata/` (referenced by `guard_bench.sh`).