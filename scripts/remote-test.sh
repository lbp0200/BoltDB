#!/bin/bash
set -euo pipefail

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

# Host-specific configurations
# (host, user, key, remote_dir, go_path)
HOST_CONFIGS=(
    "10.1.2.16:elex-gm0135:$HOME/.ssh/google_compute_engine:/home/elex-gm0135/projects/bolt:/usr/local/go/bin"
    "192.168.1.251:lbp:$HOME/.ssh/id_rsa:/media/hdd4t/projects/boltdb:/usr/local/go/bin"
    "10.1.15.22:elex:$HOME/.ssh/id_rsa.test:/Users/elex/Projects/boltDB:/Users/elex/go/go/bin"
)

# Auto-detect reachable remote host
REMOTE_HOST=""
REMOTE_USER=""
REMOTE_KEY=""
REMOTE_DIR=""
GO_BIN=""
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=2"

for entry in "${HOST_CONFIGS[@]}"; do
    IFS=':' read -r host user key dir go <<< "$entry"
    if ssh $SSH_OPTS -i "$key" -l "$user" "$host" "exit" &>/dev/null; then
        REMOTE_HOST="$host"
        REMOTE_USER="$user"
        REMOTE_KEY="$key"
        REMOTE_DIR="$dir"
        GO_BIN="$go"
        break
    fi
done

if [ -z "$REMOTE_HOST" ]; then
    echo "Error: No remote server reachable"
    exit 1
fi

echo "=== Remote host: $REMOTE_HOST ($REMOTE_USER)"

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
    --exclude 'mutation-test*.log' \
    --exclude '.mutation-test.pid' \
    --exclude '.gremlins-report-*.json' \
    -e "ssh $SSH_OPTS -i \"$REMOTE_KEY\" -l $REMOTE_USER" \
    /Users/lbp/Projects/BoltDB/ \
    "$REMOTE_HOST:$REMOTE_DIR/"

# Build a safely-quoted argument string for the remote shell
QUOTED_ARGS=""
for arg in "${ARGS[@]}"; do
    QUOTED_ARGS+=" $(printf '%q' "$arg")"
done

# === Memory guard: detect total RAM, limit test parallelism, set GOMEMLIMIT ===
# Prevents test suite from exhausting RAM and locking the remote host.
# GOMEMLIMIT is a *soft* target — Go can exceed it under allocation bursts.
# The hard cap comes from -p (package parallelism) and -count=1.
MEMGUARD_ARGS=""
GOMEMLIMIT_ENV=""
PARALLEL_ARG=""
MEMINFO=$(ssh $SSH_OPTS -i "$REMOTE_KEY" -l "$REMOTE_USER" "$REMOTE_HOST" \
    "cat /proc/meminfo 2>/dev/null | grep ^MemTotal | awk '{print \$2}'" 2>/dev/null || echo "")
if [ -n "$MEMINFO" ] && [ "$MEMINFO" -gt 0 ]; then
    TOTAL_MB=$((MEMINFO / 1024))
    # Reserve 2GB (2048 MiB) for OS; if the machine has < 4GB, reserve 50%.
    if [ "$TOTAL_MB" -le 4096 ]; then
        RESERVE_MB=$((TOTAL_MB / 2))
    else
        RESERVE_MB=2048
    fi
    MAX_MB=$((TOTAL_MB - RESERVE_MB))
    MAX_BYTES=$((MAX_MB * 1024 * 1024))
    GOMEMLIMIT_ENV="GOMEMLIMIT=${MAX_BYTES} "

    # Limit test binary parallelism to avoid N × BadgerDB × race multiplier OOM.
    # With -race, each BadgerDB instance (7 memtables × 64MB + 100MB index cache)
    # uses ~500MB+; running 16 test binaries simultaneously guarantees OOM.
    # Use -p=2 for full suites, but allow user-override via GOTESTPARALLEL.
    if [[ " ${ARGS[*]} " =~ " ./internal/..." ]] || [[ " ${ARGS[*]} " =~ " ./..." ]]; then
        if [ -z "${GOTESTPARALLEL:-}" ]; then
            PARALLEL_ARG="-p=2"
        else
            PARALLEL_ARG="-p=$GOTESTPARALLEL"
        fi
    fi

    echo "=== Memory guard: ${TOTAL_MB}MiB total → GOMEMLIMIT=${MAX_MB}MiB ${PARALLEL_ARG}"
fi

echo "=== Running tests on remote ($REMOTE_HOST): go test${QUOTED_ARGS} ${PARALLEL_ARG}"
ssh $SSH_OPTS -i "$REMOTE_KEY" -l "$REMOTE_USER" "$REMOTE_HOST" \
    "export PATH=\$HOME/go/bin:$GO_BIN:\$PATH && \
     export ${GOMEMLIMIT_ENV}GOGC=100 && \
     cd $REMOTE_DIR && \
     go test${QUOTED_ARGS} ${PARALLEL_ARG}"

echo "=== Done"
