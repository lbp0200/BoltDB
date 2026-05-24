#!/usr/bin/env bash
# run_nightly_soak.sh — Unified entry point for nightly soak tests.
#
# Usage:
#   # Local run (1h standalone + 30min replication + evolution analysis)
#   ./scripts/run_nightly_soak.sh
#
#   # Shorter smoke test
#   SOAK_DURATION=10m SOAK_REPL_DURATION=5m ./scripts/run_nightly_soak.sh
#
#   # With failover oscillation job (continue-on-error)
#   INCLUDE_FAILOVER=1 ./scripts/run_nightly_soak.sh
#
#   # CI mode (full suite, machine-readable output)
#   CI_NIGHTLY_SOAK=1 ./scripts/run_nightly_soak.sh
#
# Environment variables:
#   SOAK_DURATION         Standalone soak duration (default: 1h)
#   SOAK_REPL_DURATION    Replication soak duration (default: 30m)
#   SOAK_CLIENTS          Concurrent clients for standalone (default: 50)
#   SOAK_REPL_WRITERS     Write goroutines for replication (default: 4)
#   SOAK_DATA_DIR         Data directory (default: /tmp/bolt-nightly/data)
#   SOAK_REPORT_DIR       Report/output directory (default: /tmp/bolt-nightly/report)
#   SOAK_JSONL_DIR        JSONL timeline directory (default: /tmp/bolt-nightly/jsonl)
#   INCLUDE_FAILOVER      If set, run failover oscillation test (default: unset)
#   CI_NIGHTLY_SOAK       If set, enable CI mode (full output, no prompts)
#   SKIP_EVOLUTION        If set, skip evolution analysis step

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP=$(date '+%Y%m%d-%H%M%S')

# === Configurable defaults ===
SOAK_DURATION="${SOAK_DURATION:-1h}"
SOAK_REPL_DURATION="${SOAK_REPL_DURATION:-30m}"
SOAK_CLIENTS="${SOAK_CLIENTS:-50}"
SOAK_REPL_WRITERS="${SOAK_REPL_WRITERS:-4}"
SOAK_DATA_DIR="${SOAK_DATA_DIR:-/tmp/bolt-nightly/data}"
SOAK_REPORT_DIR="${SOAK_REPORT_DIR:-/tmp/bolt-nightly/report}"
SOAK_JSONL_DIR="${SOAK_JSONL_DIR:-/tmp/bolt-nightly/jsonl}"
OUTPUT_LOG="${SOAK_REPORT_DIR}/nightly-${TIMESTAMP}.log"
CI_NIGHTLY_SOAK="${CI_NIGHTLY_SOAK:-}"
SKIP_EVOLUTION="${SKIP_EVOLUTION:-}"

mkdir -p "$SOAK_DATA_DIR" "$SOAK_REPORT_DIR" "$SOAK_REPORT_DIR/history" "$SOAK_JSONL_DIR"

# === Helpers ===
log()  { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$OUTPUT_LOG"; }
bail() { log "FATAL: $*"; exit 1; }
run_go_test() {
    local test_match="$1"
    local timeout="$2"
    local extra_flags="${3:-}"
    shift 3

    log "=== RUNNING: $test_match (timeout=$timeout) ==="

    # Inherit CI_NIGHTLY_SOAK and other env vars
    CI_NIGHTLY_SOAK="$CI_NIGHTLY_SOAK" \
    SOAK_DATA_DIR="$SOAK_DATA_DIR" \
    SOAK_REPORT_DIR="$SOAK_REPORT_DIR" \
    SOAK_JSONL_DIR="$SOAK_JSONL_DIR" \
    go test -race -timeout "$timeout" $extra_flags \
        "$REPO_DIR/cmd/integration/" -run "$test_match" -v \
        2>&1 | tee -a "$OUTPUT_LOG"

    local exit_code="${PIPESTATUS[0]}"
    if [ "$exit_code" -ne 0 ]; then
        log "FAILED: $test_match (exit=$exit_code)"
    else
        log "PASSED: $test_match"
    fi
    return "$exit_code"
}

print_banner() {
    log "========================================"
    log "  BoltDB Nightly Soak"
    log "  Started:  $(date)"
    log "  Duration: ${SOAK_DURATION} (standalone) + ${SOAK_REPL_DURATION} (replication)"
    log "  Report:   ${SOAK_REPORT_DIR}"
    log "========================================"
    log ""
    log "  Build: git rev-parse HEAD"
    git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null | tee -a "$OUTPUT_LOG"
    log ""
}

# === Main ===

print_banner

cd "$REPO_DIR"

# Step 1: Build binary (verify compilation)
log "=== BUILDING ==="
go build -o /dev/null "$REPO_DIR/cmd/boltDB/" 2>&1 | tee -a "$OUTPUT_LOG" || bail "build failed"

overall_exit=0

# Step 2: Standalone soak
run_go_test "TestSoak" "$SOAK_DURATION" "" || overall_exit=1

# Step 3: Replication soak
run_go_test "TestSoakReplication" "$SOAK_REPL_DURATION" "" || overall_exit=1

# Step 4: Failover oscillation (optional, continue-on-error)
if [ -n "${INCLUDE_FAILOVER:-}" ]; then
    log ""
    log "=== FAILOVER OSCILLATION JOB (continue-on-error) ==="
    FO_REPORT="${SOAK_REPORT_DIR}/failover-oscillation-${TIMESTAMP}.log"
    CI_NIGHTLY_SOAK="$CI_NIGHTLY_SOAK" \
    go test -race -timeout 180s \
        "$REPO_DIR/cmd/integration/" -run "TestRegressionFailoverOscillation" -v \
        2>&1 | tee "$FO_REPORT" || true
    log "Failover oscillation report: $FO_REPORT"
    # Extract oscillation result
    if grep -q "FAIL: oscillation detected" "$FO_REPORT" 2>/dev/null; then
        log "WARNING: failover oscillation DETECTED — see $FO_REPORT"
    elif grep -q "PASS:" "$FO_REPORT" 2>/dev/null; then
        log "FAILOVER: no oscillation detected, convergence stable"
    fi
fi

# Step 5: Evolution analysis (cross-run trend)
if [ -z "$SKIP_EVOLUTION" ]; then
    log ""
    log "=== EVOLUTION ANALYSIS ==="
    for prefix in standalone replication; do
        log "--- prefix: $prefix ---"
        go run "$REPO_DIR/cmd/evolution/" \
            -dir="$SOAK_REPORT_DIR" \
            -prefix="$prefix" \
            2>&1 | tee -a "$OUTPUT_LOG"
    done
fi

# Step 5b: Anomaly detection (commit-correlated signals)
if [ -z "$SKIP_EVOLUTION" ]; then
    log ""
    log "=== ANOMALY DETECTION ==="
    for prefix in standalone replication; do
        log "--- prefix: $prefix ---"
        ANOMALY_JSON="${SOAK_REPORT_DIR}/${prefix}-anomaly.json"
        go run "$REPO_DIR/cmd/evolution/" \
            -dir="$SOAK_REPORT_DIR" \
            -prefix="$prefix" \
            -anomaly-json \
            -repo="$REPO_DIR" \
            2>/dev/null > "$ANOMALY_JSON" || log "WARNING: anomaly detection failed for $prefix"

        if [ -f "$ANOMALY_JSON" ] && [ -s "$ANOMALY_JSON" ]; then
            # Generate human-readable markdown from the JSON
            go run "$REPO_DIR/cmd/evolution/" \
                -dir="$SOAK_REPORT_DIR" \
                -prefix="$prefix" \
                -anomaly \
                -repo="$REPO_DIR" \
                2>/dev/null > "${SOAK_REPORT_DIR}/${prefix}-anomaly.md" || true

            # Log summary
            ANOMALY_COUNT=$(python3 -c "import json; r=json.load(open('$ANOMALY_JSON')); print(len(r.get('anomalies',[])))" 2>/dev/null || echo "?")
            ANOMALY_STATUS=$(python3 -c "import json; r=json.load(open('$ANOMALY_JSON')); print('UNSTABLE' if not r.get('stable',True) else 'STABLE')" 2>/dev/null || echo "?")
            log "  $prefix: ${ANOMALY_COUNT} anomalies, status=${ANOMALY_STATUS}"

            # Print anomaly details to log
            if [ "$ANOMALY_COUNT" -gt 0 ] 2>/dev/null; then
                python3 -c "
import json
r = json.load(open('$ANOMALY_JSON'))
for a in r.get('anomalies',[]):
    c = a.get('commit','')
    print(f'  [{a[\"severity\"].upper()}] {a[\"type\"]}: {a[\"message\"]}' + (f' (commit={c})' if c else ''))
" 2>/dev/null | tee -a "$OUTPUT_LOG" || true
            fi
        else
            log "  $prefix: no anomaly data (insufficient history)"
        fi
    done
fi

# Step 6: Generate trajectory plot (if JSONL files exist)
JSONL_FILES=("$SOAK_JSONL_DIR"/*.jsonl)
if [ ${#JSONL_FILES[@]} -gt 0 ] && [ -f "$SCRIPT_DIR/plot_trajectory.py" ]; then
    log ""
    log "=== GENERATING TRAJECTORY PLOT ==="
    LATEST_JSONL=$(ls -t "$SOAK_JSONL_DIR"/*.jsonl 2>/dev/null | head -1)
    if [ -n "$LATEST_JSONL" ]; then
        PLOT_OUT="${SOAK_REPORT_DIR}/trajectory-${TIMESTAMP}.png"
        python3 "$SCRIPT_DIR/plot_trajectory.py" "$LATEST_JSONL" "$PLOT_OUT" \
            2>&1 | tee -a "$OUTPUT_LOG" || log "WARNING: plot generation failed (missing python deps?)"
        log "Trajectory plot: $PLOT_OUT"
    fi
fi

# Step 7: Summary report
log ""
log "=== SUMMARY ==="
SUMMARY_FILE="${SOAK_REPORT_DIR}/nightly-${TIMESTAMP}-summary.md"
{
    echo "# Nightly Soak Summary"
    echo ""
    echo "- **Date**: $(date)"
    echo "- **Commit**: $(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo 'unknown')"
    echo "- **Standalone Duration**: ${SOAK_DURATION}"
    echo "- **Replication Duration**: ${SOAK_REPL_DURATION}"
    echo "- **Overall Status**: $([ "$overall_exit" -eq 0 ] && echo 'PASS' || echo 'PARTIAL FAIL')"
    echo ""
    echo "## Reports"
    echo ""
    for f in "$SOAK_REPORT_DIR"/*-report.md; do
        [ -f "$f" ] && echo "- [$(basename "$f")]($f)" || true
    done
    echo ""
    echo "## Evolution Analysis"
    echo ""
    for f in "$SOAK_REPORT_DIR"/*-evolution.md; do
        [ -f "$f" ] && echo "- [$(basename "$f")]($f)" || true
    done
    echo ""
    echo "## Anomaly Detection"
    echo ""
    for f in "$SOAK_REPORT_DIR"/*-anomaly.json; do
        if [ -f "$f" ]; then
            NAME=$(basename "$f" -anomaly.json)
            ANOMALIES=$(python3 -c "import json; r=json.load(open('$f')); print(len(r.get('anomalies',[])))" 2>/dev/null || echo "?")
            STATUS=$(python3 -c "import json; r=json.load(open('$f')); print('UNSTABLE' if not r.get('stable',True) else 'STABLE')" 2>/dev/null || echo "?")
            echo "- **$NAME**: ${ANOMALIES} anomalies, **${STATUS}**"
        fi
    done
    for f in "$SOAK_REPORT_DIR"/*-anomaly.md; do
        [ -f "$f" ] && echo "  - [$(basename "$f")]($f)" || true
    done
    echo ""
    echo "## Failover Oscillation"
    if [ -f "${SOAK_REPORT_DIR}/failover-oscillation-${TIMESTAMP}.log" ]; then
        echo "- [Report](failover-oscillation-${TIMESTAMP}.log)"
        if grep -q "FAIL: oscillation detected" "${SOAK_REPORT_DIR}/failover-oscillation-${TIMESTAMP}.log" 2>/dev/null; then
            echo "- **OSCILLATION DETECTED**"
        fi
    else
        echo "- Not run (set INCLUDE_FAILOVER=1)"
    fi
    echo ""
    echo "## Raw Data"
    echo ""
    echo "- **JSONL**: ${SOAK_JSONL_DIR}/"
    echo "- **Full Log**: nightly-${TIMESTAMP}.log"
    echo "- **History**: ${SOAK_REPORT_DIR}/history/"
} > "$SUMMARY_FILE"

log "Summary: $SUMMARY_FILE"
log ""
log "========================================"
log "  Nightly soak complete: $(date)"
if [ "$overall_exit" -eq 0 ]; then
    log "  Overall: PASS"
else
    log "  Overall: PARTIAL FAIL (check reports above)"
fi
log "========================================"

exit "$overall_exit"
