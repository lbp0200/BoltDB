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
    -e "ssh $SSH_OPTS -i \"$REMOTE_KEY\" -l $REMOTE_USER" \
    /Users/lbp/Projects/BoltDB/ \
    "$REMOTE_HOST:$REMOTE_DIR/"

# Build a safely-quoted argument string for the remote shell
QUOTED_ARGS=""
for arg in "${ARGS[@]}"; do
    QUOTED_ARGS+=" $(printf '%q' "$arg")"
done

echo "=== Running tests on remote ($REMOTE_HOST): go test${QUOTED_ARGS}"
ssh $SSH_OPTS -i "$REMOTE_KEY" -l "$REMOTE_USER" "$REMOTE_HOST" \
    "export PATH=\$HOME/go/bin:$GO_BIN:\$PATH && \
     cd $REMOTE_DIR && \
     go test${QUOTED_ARGS}"

echo "=== Done"
