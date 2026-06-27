#!/bin/bash
# Mutation testing for BoltDB using gremlins
# https://github.com/go-gremlins/gremlins
#
# Usage:
#   bash scripts/mutation-test.sh                  # dry-run (analysis only)
#   bash scripts/mutation-test.sh --run            # full mutation test
#   bash scripts/mutation-test.sh --run ./internal/store  # single package
#
# Quality gates are configured in .gremlins.yaml

set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v gremlins &>/dev/null; then
  echo "ERROR: gremlins not installed. Install with:"
  echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@latest"
  exit 1
fi

MODE="${1:---dry-run}"
shift 2>/dev/null || true

case "$MODE" in
  --dry-run|-d)
    echo "=== Mutation Testing (dry-run) ==="
    gremlins unleash --dry-run "$@" ./internal/store ./internal/server
    ;;
  --run|-r)
    echo "=== Mutation Testing (full) ==="
    gremlins unleash --output .gremlins-report.json "$@" ./internal/store ./internal/server
    echo ""
    echo "Report saved to .gremlins-report.json"
    ;;
  *)
    echo "Usage: $0 [--dry-run|--run] [gremlins flags...]"
    exit 1
    ;;
esac
