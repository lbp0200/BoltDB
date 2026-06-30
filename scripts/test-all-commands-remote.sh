#!/bin/bash
# test-all-commands-remote.sh — 在远程服务器上后台运行全命令准确性测试
#
# 用法:
#   bash scripts/test-all-commands-remote.sh              # 全部测试
#   bash scripts/test-all-commands-remote.sh standalone    # 只跑单机测试
#   bash scripts/test-all-commands-remote.sh status        # 查看运行状态
#
# 会在 2.16 上启动 tmux session "boltdb-cmd-test"，测试结果写入日志。
set -euo pipefail

# ── Host config (同 remote-test.sh) ──────────────────────────
HOST_CONFIGS=(
    "10.1.2.16:elex-gm0135:$HOME/.ssh/google_compute_engine:/home/elex-gm0135/projects/bolt:/usr/local/go/bin"
    "192.168.1.251:lbp:$HOME/.ssh/id_rsa:/media/hdd4t/projects/boltdb:/usr/local/go/bin"
    "10.1.15.22:elex:$HOME/.ssh/id_rsa.test:/Users/elex/Projects/boltDB:/Users/elex/go/go/bin"
)

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5"
REMOTE_HOST=""
REMOTE_USER=""
REMOTE_KEY=""
REMOTE_DIR=""
GO_BIN=""

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
    echo "❌ No remote server reachable"
    exit 1
fi

SSH_CMD="ssh $SSH_OPTS -i $REMOTE_KEY -l $REMOTE_USER $REMOTE_HOST"
RSYNC_SSH="ssh $SSH_OPTS -i $REMOTE_KEY"
LOG_DIR="/tmp/boltdb-cmd-test"
TMUX_SESSION="boltdb-cmd-test"

# ── Status check ─────────────────────────────────────────────
if [ "${1:-}" = "status" ]; then
    echo "=== Checking tmux session '$TMUX_SESSION' on $REMOTE_HOST ==="
    $SSH_CMD "tmux has-session -t $TMUX_SESSION 2>/dev/null && echo 'Session running' || echo 'Session not found'"
    echo ""
    echo "=== Recent log output ==="
    $SSH_CMD "tail -50 $LOG_DIR/results.log 2>/dev/null || echo 'No log found'"
    exit 0
fi

# ── Rsync code ───────────────────────────────────────────────
echo "📦 Syncing code to $REMOTE_HOST..."
rsync -az --delete \
    --exclude '.git' \
    --exclude 'build' \
    --exclude 'tmp' \
    --exclude '*.test' \
    --exclude 'mutation-test*.log' \
    --exclude '.mutation-test.pid' \
    --exclude '.gremlins-report-*.json' \
    -e "$RSYNC_SSH" \
    /Users/lbp/Projects/BoltDB/ \
    "$REMOTE_HOST:$REMOTE_DIR/"

# ── Build test script ────────────────────────────────────────
MODE="${1:-all}"

TEST_SCRIPT=$(cat <<'REMOTE_EOF'
#!/bin/bash
set -uo pipefail

REMOTE_DIR="__REMOTE_DIR__"
GO_BIN="__GO_BIN__"
LOG_DIR="__LOG_DIR__"
MODE="__MODE__"

export PATH=$HOME/go/bin:$GO_BIN:$PATH
cd "$REMOTE_DIR"

mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/results.log"

echo "========================================" | tee "$LOG"
echo "BoltDB 全命令准确性测试" | tee -a "$LOG"
echo "开始时间: $(date)" | tee -a "$LOG"
echo "模式: $MODE" | tee -a "$LOG"
echo "========================================" | tee -a "$LOG"

FAILED=0

run_test() {
    local name="$1"
    local cmd="$2"
    echo "" | tee -a "$LOG"
    echo "▶ $name" | tee -a "$LOG"
    echo "  命令: $cmd" | tee -a "$LOG"
    echo "  开始: $(date)" | tee -a "$LOG"
    if eval "$cmd" >> "$LOG" 2>&1; then
        echo "  ✅ 通过" | tee -a "$LOG"
    else
        echo "  ❌ 失败 (exit $?)" | tee -a "$LOG"
        FAILED=$((FAILED + 1))
    fi
    echo "  结束: $(date)" | tee -a "$LOG"
}

# ── Phase 1: 单机全命令准确性测试 ─────────────────────────────
if [ "$MODE" = "all" ] || [ "$MODE" = "standalone" ]; then
    echo "" | tee -a "$LOG"
    echo "════════════════════════════════════════" | tee -a "$LOG"
    echo "Phase 1: 单机全命令准确性测试" | tee -a "$LOG"
    echo "════════════════════════════════════════" | tee -a "$LOG"

    # 单机全命令测试
    run_test "全命令准确性测试 (TestCommandCompleteness)" \
        "go test -race -v -timeout 300s -count=1 -run 'TestCommandCompleteness' ./cmd/integration/..."

    # 已有集成测试 (快速模式)
    run_test "已有集成测试 (Tier A)" \
        "go test -race -v -timeout 300s -count=1 -short ./cmd/integration/..."
fi

# ── Phase 2: 外部客户端兼容性测试 ─────────────────────────────
if [ "$MODE" = "all" ]; then
    echo "" | tee -a "$LOG"
    echo "════════════════════════════════════════" | tee -a "$LOG"
    echo "Phase 2: 外部客户端兼容性测试" | tee -a "$LOG"
    echo "════════════════════════════════════════" | tee -a "$LOG"

    # redis-py 兼容
    run_test "redis-py 兼容性测试" \
        "python3 scripts/redis_py_compat.py"

    # redis-cli 兼容
    run_test "redis-cli 兼容性测试" \
        "bash scripts/redis_cli_compat.sh"

    # node-redis 兼容 (如果 node 可用)
    if command -v node &>/dev/null; then
        run_test "node-redis 兼容性测试" \
            "node scripts/redis_node_compat.mjs"
    fi
fi

# ── Phase 3: RESP Shape 测试 ──────────────────────────────────
if [ "$MODE" = "all" ]; then
    echo "" | tee -a "$LOG"
    echo "════════════════════════════════════════" | tee -a "$LOG"
    echo "Phase 3: RESP 协议结构测试" | tee -a "$LOG"
    echo "════════════════════════════════════════" | tee -a "$LOG"

    run_test "RESP Shape 测试 (TestRESPShape)" \
        "go test -race -v -timeout 30s -count=1 -run TestRESPShape ./internal/server/..."
fi

# ── Summary ──────────────────────────────────────────────────
echo "" | tee -a "$LOG"
echo "========================================" | tee -a "$LOG"
echo "测试完成: $(date)" | tee -a "$LOG"
if [ $FAILED -gt 0 ]; then
    echo "❌ $FAILED 个测试套件失败" | tee -a "$LOG"
else
    echo "✅ 全部通过" | tee -a "$LOG"
fi
echo "日志: $LOG" | tee -a "$LOG"
echo "========================================" | tee -a "$LOG"
REMOTE_EOF
)

# Replace placeholders
TEST_SCRIPT="${TEST_SCRIPT/__REMOTE_DIR__/$REMOTE_DIR}"
TEST_SCRIPT="${TEST_SCRIPT/__GO_BIN__/$GO_BIN}"
TEST_SCRIPT="${TEST_SCRIPT/__LOG_DIR__/$LOG_DIR}"
TEST_SCRIPT="${TEST_SCRIPT/__MODE__/$MODE}"

# ── Kill existing session if any ─────────────────────────────
$SSH_CMD "tmux kill-session -t $TMUX_SESSION 2>/dev/null || true"

# ── Upload and run in tmux ───────────────────────────────────
echo "🚀 在 $REMOTE_HOST 上启动 tmux session '$TMUX_SESSION'..."

$SSH_CMD "mkdir -p $LOG_DIR && cat > $LOG_DIR/run.sh << 'SCRIPT_EOF'
$TEST_SCRIPT
SCRIPT_EOF
chmod +x $LOG_DIR/run.sh"

$SSH_CMD "tmux new-session -d -s $TMUX_SESSION 'bash $LOG_DIR/run.sh; echo \"=== 测试结束，按 Enter 退出 ===\"; read'"

echo ""
echo "✅ 已在 $REMOTE_HOST 上启动后台测试"
echo ""
echo "📋 查看方式:"
echo "   tmux attach (SSH 到 $REMOTE_HOST 后运行)"
echo "   bash scripts/test-all-commands-remote.sh status"
echo ""
echo "📁 日志位置: $REMOTE_HOST:$LOG_DIR/results.log"
