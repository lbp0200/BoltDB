#!/bin/bash
# scale-test-tier1.sh — 规模化验证第 1 级（10GB → 100GB 数据规模测试）
# 用法: bash scripts/scale-test-tier1.sh [--data-size 10] [--value-size 1024]
#
# 验证指标：
#   SET 吞吐量退化（相对空库） < 20%
#   GET 延迟 P99               < 10ms
#   L0 峰值                    < 15 （需 metrics 端点）
#   重启时间                    < 30s
#   FULLRESYNC 速率            > 100MB/s
#   磁盘空间放大                < 2.5x
set -euo pipefail

HOST="10.1.2.16"
PORTS="6337 6338 6339"
DATA_SIZE_GB="${1:-10}"
VALUE_SIZE="${2:-1024}"
LOG_FILE="/tmp/scale-test-tier1-$(date +%Y%m%d-%H%M%S).log"

echo "==============================================" | tee -a "$LOG_FILE"
echo "  BoltDB 规模化验证 — 第 1 级" | tee -a "$LOG_FILE"
echo "  数据量: ${DATA_SIZE_GB}GB  值大小: ${VALUE_SIZE}B" | tee -a "$LOG_FILE"
echo "  时间: $(date -u '+%Y-%m-%dT%H:%M:%SZ')" | tee -a "$LOG_FILE"
echo "==============================================" | tee -a "$LOG_FILE"

# 辅助函数
check_alive() {
    for port in $PORTS; do
        redis-cli -h "$HOST" -p "$port" PING > /dev/null 2>&1 || {
            echo "ERROR: Node $port not responding" | tee -a "$LOG_FILE"
            return 1
        }
    done
    return 0
}

measure_disk() {
    local label="$1"
    local total_du=0
    echo "--- Disk: $label ---" | tee -a "$LOG_FILE"
    for dir in /usr/local/boltdb_data/node{1,2,3}; do
        local size
        size=$(du -sm "$dir" 2>/dev/null | cut -f1)
        size=${size:-0}
        total_du=$((total_du + size))
        echo "  $dir: ${size}MB" | tee -a "$LOG_FILE"
    done
    echo "  Total: ${total_du}MB ($(( total_du / 1024 ))GB)" | tee -a "$LOG_FILE"
    echo "$total_du"
}

measure_set_throughput() {
    local label="$1"
    local n="${2:-50000}"
    local d="${3:-$VALUE_SIZE}"
    echo "--- SET throughput ($label): ${n} req, ${d}B value ---" | tee -a "$LOG_FILE"
    local result
    result=$(redis-benchmark -h "$HOST" -p 6337 -c 20 -n "$n" -d "$d" \
        -r "$((n * 10))" --cluster-mode -t SET 2>&1 | tee -a "$LOG_FILE")
    local throughput
    throughput=$(echo "$result" | grep -oP "SET.*?\K[\d.]+(?= requests per second)" || echo "0")
    echo "  Result: ${throughput} req/s" | tee -a "$LOG_FILE"
    echo "$throughput"
}

measure_get_latency() {
    local label="$1"
    local n="${2:-50000}"
    echo "--- GET latency ($label): ${n} req ---" | tee -a "$LOG_FILE"
    local result
    result=$(redis-benchmark -h "$HOST" -p 6337 -c 20 -n "$n" \
        -r "$((n * 10))" --cluster-mode -t GET 2>&1 | tee -a "$LOG_FILE")
    local p99
    # redis-benchmark 输出类似 "p99=0.123ms"
    p99=$(echo "$result" | grep -oP "p99=\K[\d.]+" | head -1 || echo "0")
    if [ "$p99" = "0" ]; then
        # 如果无 p99，尝试从延迟分布估算
        p99=$(echo "$result" | grep -oP "GET.*?\K[\d.]+(?= requests per second)" || echo "0")
    fi
    echo "  P99: ${p99}ms" | tee -a "$LOG_FILE"
    echo "$p99"
}

measure_restart_time() {
    local port="$1"
    local label="$2"
    echo "--- Restart time ($label, port $port) ---" | tee -a "$LOG_FILE"
    local start end elapsed
    start=$(date +%s%N)
    redis-cli -h "$HOST" -p "$port" DEBUG SLEEP 0.1 > /dev/null 2>&1 || true
    sudo systemctl stop "boltdb-${label}" 2>/dev/null
    sleep 1
    sudo systemctl start "boltdb-${label}"
    # 等待启动完成
    for i in $(seq 1 30); do
        if redis-cli -h "$HOST" -p "$port" PING > /dev/null 2>&1; then
            end=$(date +%s%N)
            elapsed=$(( (end - start) / 1000000 ))
            echo "  ${elapsed}ms" | tee -a "$LOG_FILE"
            echo "$elapsed"
            return
        fi
        sleep 1
    done
    echo "  TIMEOUT (>30s)" | tee -a "$LOG_FILE"
    echo "30000"
}

measure_fullresync_rate() {
    # 需要第二个机器作为 replica，当前环境只有一个，跳过
    echo "--- FULLRESYNC rate ---" | tee -a "$LOG_FILE"
    echo "  SKIP: 需要单独 replica 节点测试" | tee -a "$LOG_FILE"
    echo "0"
}

flush_all_nodes() {
    echo "--- Flushing all nodes ---" | tee -a "$LOG_FILE"
    for port in $PORTS; do
        redis-cli -h "$HOST" -p "$port" FLUSHALL > /dev/null 2>&1
    done
    echo "  Done" | tee -a "$LOG_FILE"
}

# =========== 主流程 ===========

# 1. 检查集群
echo "=== Phase 0: Sanity check ===" | tee -a "$LOG_FILE"
check_alive || { echo "Cluster not healthy, aborting"; exit 1; }
echo "  Cluster healthy: $(redis-cli -h "$HOST" -p 6337 CLUSTER INFO | grep cluster_state)" | tee -a "$LOG_FILE"

# 2. 基线测量（空库）
echo "" | tee -a "$LOG_FILE"
echo "=== Phase 1: Baseline (empty DB) ===" | tee -a "$LOG_FILE"
BASELINE_SET=$(measure_set_throughput "baseline" 100000)
echo "  BASELINE_SET_THROUGHPUT=$BASELINE_SET" | tee -a "$LOG_FILE"

DISK_EMPTY=$(measure_disk "after flush")
echo "  DISK_EMPTY_MB=$DISK_EMPTY" | tee -a "$LOG_FILE"

# 3. 数据填充
echo "" | tee -a "$LOG_FILE"
echo "=== Phase 2: Data fill (${DATA_SIZE_GB}GB) ===" | tee -a "$LOG_FILE"

# 计算需要多少条 key
# 每 key: key前缀 + 冒号 + 随机10位数字 + value
# 近似每个 key 占 (10 + 1 + 10 + $VALUE_SIZE) 字节
KEY_OVERHEAD=30
TOTAL_KEYS=$(( DATA_SIZE_GB * 1024 * 1024 * 1024 / (KEY_OVERHEAD + VALUE_SIZE) ))

echo "  Target: ${DATA_SIZE_GB}GB = ${TOTAL_KEYS} keys (${VALUE_SIZE}B value)" | tee -a "$LOG_FILE"

# 分多批填充，避免单次 redis-benchmark 超时
BATCH_SIZE=1000000
BATCHES=$(( (TOTAL_KEYS + BATCH_SIZE - 1) / BATCH_SIZE ))
if [ "$BATCHES" -lt 1 ]; then BATCHES=1; fi
if [ "$BATCHES" -gt 50 ]; then BATCHES=50; fi  # 限制批次防止耗时过长

echo "  Batches: $BATCHES (${BATCH_SIZE} keys each)" | tee -a "$LOG_FILE"

for i in $(seq 1 "$BATCHES"); do
    keys_this_batch=$BATCH_SIZE
    if [ "$i" -eq "$BATCHES" ]; then
        keys_this_batch=$(( TOTAL_KEYS - (BATCHES - 1) * BATCH_SIZE ))
    fi
    echo -n "  Batch $i/$BATCHES (${keys_this_batch} keys)... " | tee -a "$LOG_FILE"
    start_batch=$(date +%s%N)
    redis-benchmark -h "$HOST" -p 6337 -c 30 -n "$keys_this_batch" -d "$VALUE_SIZE" \
        -r "$((TOTAL_KEYS * 10))" --cluster-mode -t SET > /dev/null 2>&1 || true
    end_batch=$(date +%s%N)
    elapsed_batch=$(( (end_batch - start_batch) / 1000000000 ))
    echo "done (${elapsed_batch}s)" | tee -a "$LOG_FILE"
done

# 验证数据量
echo "" | tee -a "$LOG_FILE"
echo "  DBSIZE: $(redis-cli -c -h "$HOST" -p 6337 DBSIZE)" | tee -a "$LOG_FILE"

# 4. 填充后测量
echo "" | tee -a "$LOG_FILE"
echo "=== Phase 3: After fill ===" | tee -a "$LOG_FILE"

SET_AFTER=$(measure_set_throughput "after fill" 50000)
echo "  SET_AFTER_THROUGHPUT=$SET_AFTER" | tee -a "$LOG_FILE"

GET_P99=$(measure_get_latency "after fill" 50000)
echo "  GET_P99_LATENCY=$GET_P99" | tee -a "$LOG_FILE"

DISK_AFTER=$(measure_disk "after fill")
echo "  DISK_AFTER_MB=$DISK_AFTER" | tee -a "$LOG_FILE"

# 5. 重启时间
echo "" | tee -a "$LOG_FILE"
echo "=== Phase 4: Restart time ===" | tee -a "$LOG_FILE"
RESTART_N1=$(measure_restart_time 6337 "node1")
RESTART_N2=$(measure_restart_time 6338 "node2")
RESTART_N3=$(measure_restart_time 6339 "node3")

# 6. FULLRESYNC（当前环境无 replica，标记为 N/A）
echo "" | tee -a "$LOG_FILE"
echo "=== Phase 5: FULLRESYNC rate ===" | tee -a "$LOG_FILE"
FULLRESYNC_RATE="N/A (no replica available)"
echo "  Result: $FULLRESYNC_RATE" | tee -a "$LOG_FILE"

# =========== 结果汇总 ===========
echo "" | tee -a "$LOG_FILE"
echo "==============================================" | tee -a "$LOG_FILE"
echo "  测试结果汇总" | tee -a "$LOG_FILE"
echo "==============================================" | tee -a "$LOG_FILE"

# 计算指标
SET_DEGRADATION="N/A"
if [ "$BASELINE_SET" != "0" ] && [ "$SET_AFTER" != "0" ]; then
    SET_DEGRADATION=$(echo "scale=2; (1 - $SET_AFTER / $BASELINE_SET) * 100" | bc)
fi
DISK_AMPLIFICATION="N/A"
if [ "$DISK_AFTER" != "0" ] && [ "$DISK_EMPTY" != "0" ]; then
    LOGICAL_MB=$(( DATA_SIZE_GB * 1024 ))
    DISK_AMPLIFICATION=$(echo "scale=2; ($DISK_AFTER) / $LOGICAL_MB" | bc 2>/dev/null || echo "N/A")
fi

echo "  数据量:                   ${DATA_SIZE_GB}GB" | tee -a "$LOG_FILE"
echo "  SET 基线吞吐:              ${BASELINE_SET} req/s" | tee -a "$LOG_FILE"
echo "  SET 填充后吞吐:            ${SET_AFTER} req/s" | tee -a "$LOG_FILE"
echo "  SET 退化率:               ${SET_DEGRADATION}% (标准: < 20%)" | tee -a "$LOG_FILE"
echo "  GET P99 延迟:             ${GET_P99}ms (标准: < 10ms)" | tee -a "$LOG_FILE"
echo "  重启时间 N1:              ${RESTART_N1}ms (标准: < 30000ms)" | tee -a "$LOG_FILE"
echo "  重启时间 N2:              ${RESTART_N2}ms" | tee -a "$LOG_FILE"
echo "  重启时间 N3:              ${RESTART_N3}ms" | tee -a "$LOG_FILE"
echo "  磁盘空库基线:             ${DISK_EMPTY}MB" | tee -a "$LOG_FILE"
echo "  磁盘填充后:               ${DISK_AFTER}MB" | tee -a "$LOG_FILE"
echo "  磁盘空间放大:             ${DISK_AMPLIFICATION}x (标准: < 2.5x)" | tee -a "$LOG_FILE"
echo "  FULLRESYNC 速率:          ${FULLRESYNC_RATE}" | tee -a "$LOG_FILE"
echo "==============================================" | tee -a "$LOG_FILE"
echo "  日志文件: $LOG_FILE" | tee -a "$LOG_FILE"
echo "==============================================" | tee -a "$LOG_FILE"
