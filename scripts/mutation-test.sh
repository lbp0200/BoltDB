#!/usr/bin/env bash
# Mutation test baseline script.
# Runs go-mutesting on targeted packages and records results.
#
# Usage:
#   bash scripts/mutation-test.sh                    # run on store (fastest)
#   bash scripts/mutation-test.sh ./internal/server/  # run on server
#   bash scripts/mutation-test.sh all                 # run all targeted packages
#
# The mutation score = killed / (killed + lived) * 100%.
# Results are appended to docs/plans/mutation-baseline.md.

set -euo pipefail

TARGET="${1:-store}"
BASELINE="docs/plans/mutation-baseline.md"
TIMEOUT="600s"

case "${TARGET}" in
  store)
    PKG="./internal/store/..."
    ;;
  server)
    PKG="./internal/server/..."
    ;;
  all)
    PKG="./internal/store/... ./internal/server/..."
    TIMEOUT="1800s"
    ;;
  *)
    PKG="${TARGET}"
    ;;
esac

echo "=== Mutation test: ${PKG} ==="
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Run go-mutesting with timeout
# --no-exec skips the built-in exec (uses go test under the hood)
# We capture both the machine-readable output and the summary
OUTPUT=$(go-mutesting --verbose "${PKG}" 2>&1 || true)
echo "${OUTPUT}"

# Extract summary line
SUMMARY=$(echo "${OUTPUT}" | grep "✓\|✗\|The mutation score" | tail -5)
echo "---"
echo "${SUMMARY}"

# Extract numeric score
SCORE=$(echo "${OUTPUT}" | grep "The mutation score" | grep -oP '[0-9]+\.[0-9]+' || echo "N/A")
KILLED=$(echo "${OUTPUT}" | grep -c "✓" || true)
LIVED=$(echo "${OUTPUT}" | grep -c "✗" || true)

{
  echo ""
  echo "## Run: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "| Target | Score | Killed | Lived | Total |"
  echo "|--------|-------|--------|-------|-------|"
  echo "| ${PKG} | ${SCORE}% | ${KILLED} | ${LIVED} | $((KILLED + LIVED)) |"
  echo ""
  echo '```'
  echo "${OUTPUT}"
  echo '```'
} >> "${BASELINE}"

echo ""
echo "=== Done. Score: ${SCORE}% (killed=${KILLED}, lived=${LIVED}) ==="
echo "Results appended to ${BASELINE}"
