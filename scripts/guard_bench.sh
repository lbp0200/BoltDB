#!/bin/bash
# Benchmark regression guard.
# Usage:
#   ./scripts/guard_bench.sh              # compare proto benchmarks against baseline
#   ./scripts/guard_bench.sh --store      # compare store benchmarks
#   ./scripts/guard_bench.sh --server     # compare server benchmarks
#   ./scripts/guard_bench.sh --update     # update proto baseline
#   ./scripts/guard_bench.sh --store --update  # update store baseline
#   ./scripts/guard_bench.sh --server --update  # update server baseline
#
# Exits non-zero if any benchmark regresses >10% (configurable via GUARD_THRESHOLD).
# Dependencies: go, benchstat (go install golang.org/x/perf/cmd/benchstat@latest)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TESTDATA="$REPO_ROOT/testdata"
THRESHOLD="${GUARD_THRESHOLD:-10}"

UPDATE=false
TARGET="${1:-proto}"
[[ "${2:-}" == "--update" ]] && UPDATE=true
[[ "${1:-}" == "--update" ]] && UPDATE=true && TARGET=proto
[[ "${1:-}" == "--store" ]] && TARGET=store
[[ "${1:-}" == "--server" ]] && TARGET=server

case "$TARGET" in
  proto)
    PKG="$REPO_ROOT/internal/proto"
    PATTERN="."
    BASELINE="$TESTDATA/bench_baseline_proto.txt"
    CLEAN=false ;;
  store)
    PKG="$REPO_ROOT/internal/store"
    PATTERN="BenchmarkStringSet|BenchmarkStringGet|BenchmarkStringMGet|BenchmarkZAdd_|BenchmarkZRange_|BenchmarkZRank_"
    BASELINE="$TESTDATA/bench_baseline_store.txt"
    CLEAN=true ;;
  server)
    PKG="$REPO_ROOT/internal/server"
    PATTERN="BenchmarkExecuteCommand_|BenchmarkResponseTypes|BenchmarkPubSub|BenchmarkParseScore"
    BASELINE="$TESTDATA/bench_baseline_server.txt"
    CLEAN=false
    CLEAN_CMD='grep -E "^(Benchmark.*ns/op|ok |PASS|FAIL|---)"'
    TIMEOUT=600s ;;
esac

if ! command -v benchstat &>/dev/null; then
  echo "ERROR: benchstat not found. Install with:"
  echo "  go install golang.org/x/perf/cmd/benchstat@latest"
  exit 1
fi

RAW=$(mktemp)
CLEANED=$(mktemp)
trap "rm -f $RAW $CLEANED" EXIT

TIMEOUT="${TIMEOUT:-300s}"

echo "Running $TARGET benchmarks..."
go test -bench="$PATTERN" -benchmem -count=5 -timeout="$TIMEOUT" "$PKG" 2>/dev/null > "$RAW"

if $CLEAN; then
  # Strip badger log noise and reassemble benchmark name + result
  awk '
    /^goos:|^goarch:|^pkg:|^cpu:|^ok |^FAIL/ { print; next }
    /^Benchmark/ {
      split($0, parts, "\t")
      name = parts[1]
      gsub(/[[:space:]]+$/, "", name)
      if (index(name, "-") > 0) { current = name; next }
      next
    }
    {
      if (current != "") {
        gsub(/^[[:space:]]+/, "")
        if ($1 ~ /^[0-9]+$/) {
          rest = ""
          for (i = 2; i <= NF; i++) rest = rest " " $i
          gsub(/^[[:space:]]+/, "", rest)
          print current "\t" $1 "\t" rest
          current = ""
        }
      }
    }
  ' "$RAW" > "$CLEANED"
elif [ -n "${CLEAN_CMD:-}" ]; then
  eval "$CLEAN_CMD" "$RAW" > "$CLEANED"
else
  cp "$RAW" "$CLEANED"
fi

if $UPDATE; then
  cp "$CLEANED" "$BASELINE"
  echo "Updated $BASELINE"
  exit 0
fi

if [ ! -f "$BASELINE" ]; then
  echo "No baseline at $BASELINE. Run with --update first."
  exit 1
fi

echo ""
echo "=== benchstat: $TARGET ==="
benchstat "$BASELINE" "$CLEANED" | tee /tmp/benchstat_out.txt

# Check for regressions
REGRESSIONS=$(awk '
  /^[a-zA-Z]/ {
    for (i=1; i<=NF; i++) {
      if ($i ~ /^[+][0-9.]+%$/) {
        pct = $i
        gsub(/[+%]/, "", pct)
        if (pct + 0 > '"$THRESHOLD"') print
      }
    }
  }
' /tmp/benchstat_out.txt)

if [ -n "$REGRESSIONS" ]; then
  echo ""
  echo "✗ Performance regressions (> ${THRESHOLD}%):"
  echo "$REGRESSIONS"
  exit 1
fi

echo ""
echo "✓ All $TARGET benchmarks within ${THRESHOLD}% threshold."
