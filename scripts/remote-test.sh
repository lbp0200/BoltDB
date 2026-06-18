#!/bin/bash
set -euo pipefail

REMOTE_DIR="/home/elex-gm0135/projects/bolt"

usage() {
    echo "Usage: $0 [test-args]"
    echo "Sync code to remote Linux machine and run tests there."
    echo ""
    echo "Examples:"
    echo "  $0 -race -short ./internal/..."
    echo "  $0 -race -timeout 600s ./cmd/integration/..."
    echo "  $0 -race -timeout 30s -run TestRESPShape ./internal/server/..."
    echo ""
    echo "Default: go test -race -short ./internal/..."
}

ARGS=("$@")
if [ ${#ARGS[@]} -eq 0 ]; then
    ARGS=(-race -short ./internal/...)
fi

echo "=== Syncing code to remote..."
rsync -az --delete \
    --exclude '.git' \
    --exclude 'build' \
    --exclude 'tmp' \
    --exclude '*.test' \
    -e ssh \
    /Users/lbp/Projects/BoltDB/ \
    bolt-remote:"$REMOTE_DIR/"

# Build a safely-quoted argument string for the remote shell
QUOTED_ARGS=""
for arg in "${ARGS[@]}"; do
    QUOTED_ARGS+=" $(printf '%q' "$arg")"
done

echo "=== Running tests on remote: go test${QUOTED_ARGS}"
ssh bolt-remote \
    "export PATH=\$HOME/go/bin:/usr/local/go/bin:\$PATH && \
     cd $REMOTE_DIR && \
     go test${QUOTED_ARGS}"

echo "=== Done"
