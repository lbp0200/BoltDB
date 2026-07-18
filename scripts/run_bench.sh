#!/bin/bash
set -euo pipefail

OUT_DIR="${1:-./testdata}"
mkdir -p "$OUT_DIR"

echo "=== Running RESP proto benchmarks ==="
go test -bench="." -benchmem -count=5 -timeout=300s ./internal/proto/ 2>/dev/null | grep -E "^(Benchmark|ok|FAIL|---)" > "$OUT_DIR/bench_proto.txt"
echo "Saved to $OUT_DIR/bench_proto.txt"

echo "=== Running store benchmarks ==="
go test -bench="BenchmarkStringSet|BenchmarkStringGet|BenchmarkStringMGet|BenchmarkZAdd_|BenchmarkZRange_|BenchmarkZRank_" -benchmem -count=5 -timeout=300s ./internal/store/ 2>/dev/null | grep -E "^(Benchmark|ok|FAIL|---)" > "$OUT_DIR/bench_store.txt"
echo "Saved to $OUT_DIR/bench_store.txt"

echo "=== Running server benchmarks ==="
go test -bench="BenchmarkExecuteCommand_|BenchmarkResponseTypes|BenchmarkPubSub" -benchmem -count=5 -timeout=600s ./internal/server/ 2>/dev/null | grep -E "^(Benchmark|ok|FAIL|---)" > "$OUT_DIR/bench_server.txt"
echo "Saved to $OUT_DIR/bench_server.txt"

echo "Done. Results in $OUT_DIR/"
