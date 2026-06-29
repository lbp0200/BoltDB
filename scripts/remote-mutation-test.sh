#!/bin/bash
# Remote mutation testing for BoltDB
# Runs gremlins on a remote Linux server in background (nohup).
#
# Usage:
#   bash scripts/remote-mutation-test.sh                  # start background run
#   bash scripts/remote-mutation-test.sh --status         # check if running
#   bash scripts/remote-mutation-test.sh --logs           # tail the log
#   bash scripts/remote-mutation-test.sh --stop           # kill the run
#   bash scripts/remote-mutation-test.sh --results        # show results summary
#
# Runs with: timeout-coefficient=1, no -race, workers=4
# Expected duration: 12-24h on 8-core server

set -euo pipefail

cd "$(dirname "$0")/.."

# Remote config
REMOTE_HOST="10.1.2.16"
REMOTE_USER="elex-gm0135"
REMOTE_KEY="$HOME/.ssh/google_compute_engine"
REMOTE_DIR="/home/elex-gm0135/projects/bolt"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5"
LOG_FILE="mutation-test-$(date +%Y%m%d-%H%M%S).log"
REMOTE_LOG="$REMOTE_DIR/mutation-test.log"
REMOTE_PID_FILE="$REMOTE_DIR/.mutation-test.pid"

ssh_cmd() {
    ssh $SSH_OPTS -i "$REMOTE_KEY" -l "$REMOTE_USER" "$REMOTE_HOST" "$@"
}

MODE="${1:-}"
case "$MODE" in
    --status|-s)
        echo "=== Mutation Test Status ==="
        if ssh_cmd "test -f $REMOTE_PID_FILE && kill -0 \$(head -1 $REMOTE_PID_FILE) 2>/dev/null" 2>/dev/null; then
            PID=$(ssh_cmd "head -1 $REMOTE_PID_FILE 2>/dev/null")
            echo "RUNNING (PID: $PID)"
            echo ""
            echo "=== Last 10 lines of log ==="
            ssh_cmd "tail -10 $REMOTE_LOG 2>/dev/null || echo 'No log yet'"
        else
            echo "NOT RUNNING"
            if ssh_cmd "test -f $REMOTE_LOG" 2>/dev/null; then
                echo ""
                echo "=== Last 20 lines of log ==="
                ssh_cmd "tail -20 $REMOTE_LOG"
            fi
        fi
        ;;
    --logs|-l)
        echo "=== Following mutation test log (Ctrl+C to stop) ==="
        ssh_cmd "tail -f $REMOTE_LOG"
        ;;
    --stop|-k)
        echo "=== Stopping mutation test ==="
        ssh_cmd "if test -f $REMOTE_PID_FILE; then kill \$(cat $REMOTE_PID_FILE) 2>/dev/null && echo 'Stopped'; else echo 'Not running'; fi"
        ;;
    --results|-r)
        echo "=== Mutation Test Results ==="
        echo "--- internal/server ---"
        ssh_cmd "if test -f $REMOTE_DIR/.gremlins-report-server.json; then cat $REMOTE_DIR/.gremlins-report-server.json | python3 -m json.tool 2>/dev/null | head -50 || cat $REMOTE_DIR/.gremlins-report-server.json | head -50; else echo 'No report yet'; fi"
        echo ""
        echo "--- internal/store ---"
        ssh_cmd "if test -f $REMOTE_DIR/.gremlins-report-store.json; then cat $REMOTE_DIR/.gremlins-report-store.json | python3 -m json.tool 2>/dev/null | head -50 || cat $REMOTE_DIR/.gremlins-report-store.json | head -50; else echo 'No report yet'; fi"
        ;;
    --help|-h)
        echo "Usage: $0 [--status|--logs|--stop|--results|--help]"
        echo ""
        echo "No args: start background mutation test on remote server"
        echo "  --status  : check if the run is still going"
        echo "  --logs    : tail the log in real-time"
        echo "  --stop    : kill the background run"
        echo "  --results : show the results report"
        ;;
    "")
        echo "=== Syncing code to remote ($REMOTE_HOST) ==="
        rsync -az --delete \
            --exclude '.git' \
            --exclude 'build' \
            --exclude 'tmp' \
            --exclude '*.test' \
            --exclude 'mutation-test*.log' \
            --exclude '.gremlins-report.json' \
            -e "ssh $SSH_OPTS -i \"$REMOTE_KEY\" -l $REMOTE_USER" \
            ./ \
            "$REMOTE_HOST:$REMOTE_DIR/"

        echo "=== Starting mutation test in background ==="
        ssh_cmd "cd $REMOTE_DIR && \
            export PATH=/usr/local/go/bin:\$HOME/go/bin:\$PATH && \
            nohup bash -c '
                echo \"Mutation test started at \$(date)\" > $REMOTE_LOG
                echo \$\$ > $REMOTE_PID_FILE
                echo \"Host: \$(hostname)\" >> $REMOTE_LOG
                echo \"Cores: \$(nproc)\" >> $REMOTE_LOG
                echo \"---\" >> $REMOTE_LOG

                echo \"=== Phase 1: internal/server ===\" >> $REMOTE_LOG
                gremlins unleash \
                    --output .gremlins-report-server.json \
                    --timeout-coefficient 1 \
                    --test-cpu 1 \
                    --workers 4 \
                    --threshold-efficacy 70.0 \
                    --threshold-mcover 50.0 \
                    ./internal/server \
                    >> $REMOTE_LOG 2>&1
                echo \"Server exit code: \$?\" >> $REMOTE_LOG

                echo \"=== Phase 2: internal/store ===\" >> $REMOTE_LOG
                gremlins unleash \
                    --output .gremlins-report-store.json \
                    --timeout-coefficient 1 \
                    --test-cpu 1 \
                    --workers 4 \
                    --threshold-efficacy 70.0 \
                    --threshold-mcover 50.0 \
                    ./internal/store \
                    >> $REMOTE_LOG 2>&1
                echo \"Store exit code: \$?\" >> $REMOTE_LOG

                echo \"---\" >> $REMOTE_LOG
                echo \"Mutation test finished at \$(date)\" >> $REMOTE_LOG
                rm -f $REMOTE_PID_FILE
            ' > /dev/null 2>&1 &"

        sleep 1
        echo ""
        echo "=== Run started! ==="
        echo "  Log:     $REMOTE_HOST:$REMOTE_LOG"
        echo "  Report:  $REMOTE_HOST:.gremlins-report.json"
        echo ""
        echo "  Check status:  $0 --status"
        echo "  Follow logs:   $0 --logs"
        echo "  Stop:          $0 --stop"
        echo "  Results:       $0 --results"
        ;;
esac
