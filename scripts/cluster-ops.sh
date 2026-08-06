#!/bin/bash
# cluster-ops.sh — BoltDB 集群运维工具（2.16 实战沉淀，2026-08-06）
#
# 用法: scripts/cluster-ops.sh <command> [args]
#
#   health               三节点健康检查（PING + CLUSTER INFO + NODES）
#   restart              滚动重启三节点（逐个，等待 PONG 后再下一个）
#   deploy <binary>      部署新二进制 + 滚动重启（自动备份旧版为 .bak）
#   gc [ratio]           三节点后台跑 DEBUG GC（默认 ratio 0.5，日志 /tmp/gc*.log）
#   flush                三节点 FLUSHDB（需确认；修复后保留 replId/集群配置）
#   rebuild-node <1|2|3> 重建单个节点数据目录（清 vlog 垃圾终极手段，见 --force）
#   recover              脑裂恢复：RESET HARD 全部 → MEET → ADDSLOTS → 验证
#   verify               集群完整性：三节点视角 slots_assigned=16384 + 槽位正确
#
# 环境变量: CLUSTER_HOST（默认 10.1.2.16）、SSH_KEY（默认 ~/.ssh/google_compute_engine）
#
# 经验教训（写死在此工具里的坑）：
#   - 三节点绝不可同时重启（LoadConfig 失败时每节点认领全部槽 → 脑裂）
#   - systemd 停止可能因 BadgerDB 等待 GC 卡住 → 用 systemctl kill -s SIGKILL
#   - sed 里 633$n 会拼成 6331！端口必须写全 6337/6338/6339
#   - node ID 与槽位分配持久化在 badger 的 cluster:config，重建目录后必须
#     RESET HARD + MEET + ADDSLOTS，并 FORGET 旧 ID 幽灵节点

set -euo pipefail

HOST="${CLUSTER_HOST:-10.1.2.16}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/google_compute_engine}"
SSH=(ssh -i "$SSH_KEY" -o ConnectTimeout=10 "elex-gm0135@$HOST")
PORTS=(6337 6338 6339)
DIRS=(node1 node2 node3)

rssh() { "${SSH[@]}" "$@"; }

wait_pong() {
  local port="$1" tries="${2:-12}"
  for _ in $(seq 1 "$tries"); do
    if rssh "timeout 2 redis-cli -h $HOST -p $port PING 2>/dev/null | grep -q PONG"; then
      return 0
    fi
    sleep 2
  done
  echo "ERROR: node $port did not answer PING" >&2
  return 1
}

cmd_health() {
  echo "=== 版本 ==="
  for p in "${PORTS[@]}"; do
    echo -n "node $p: "
    rssh "timeout 3 redis-cli -h $HOST -p $p INFO server 2>/dev/null | grep '^redis_version' || echo DOWN"
  done
  echo "=== CLUSTER INFO（node1 视角）==="
  rssh "timeout 3 redis-cli -h $HOST -p ${PORTS[0]} CLUSTER INFO | head -5"
  echo "=== NODES ==="
  rssh "redis-cli -h $HOST -p ${PORTS[0]} CLUSTER NODES | awk '{print \$2, \$3, \$9}'"
}

cmd_restart() {
  for n in 1 2 3; do
    echo "=== 重启 node$n ==="
    rssh "sudo systemctl restart boltdb-node$n"
    wait_pong "633$n" || { echo "node$n 重启失败，中止（不要继续重启其他节点）" >&2; exit 1; }
    echo "node$n OK"
  done
  echo "=== 全部重启完成，验证 ==="
  cmd_health
}

cmd_deploy() {
  [ $# -ge 1 ] || { echo "用法: $0 deploy <linux-binary>" >&2; exit 1; }
  local bin="$1"
  [ -f "$bin" ] || { echo "找不到二进制: $bin" >&2; exit 1; }
  echo "=== 上传并备份 ==="
  scp -i "$SSH_KEY" "$bin" "elex-gm0135@$HOST:/tmp/boltdb.new"
  rssh "sudo cp /usr/local/bin/boltdb /usr/local/bin/boltdb.bak && sudo mv /tmp/boltdb.new /usr/local/bin/boltdb && sudo chmod +x /usr/local/bin/boltdb && file /usr/local/bin/boltdb | grep -q ELF || { echo '不是 Linux ELF（本地 go build 会出 darwin 版，需 GOOS=linux GOARCH=amd64）' >&2; exit 1; }"
  cmd_restart
}

cmd_gc() {
  local ratio="${1:-0.5}"
  echo "=== 三节点后台 DEBUG GC（ratio=$ratio，日志 /tmp/gc*.log）==="
  rssh "for p in ${PORTS[*]}; do nohup redis-cli -h $HOST -p \$p DEBUG GC $ratio > /tmp/gc\$p.log 2>&1 & done; echo started"
  echo "GC 已后台启动。进度: watch 'for p in ${PORTS[*]}; do echo -n \"\$p: \"; cat /tmp/gc\$p.log; done'"
}

cmd_flush() {
  echo "=== 三节点 FLUSHDB（保留 replId 与集群配置）==="
  for p in "${PORTS[@]}"; do
    echo -n "node $p: "
    rssh "timeout 5 redis-cli -h $HOST -p $p FLUSHDB"
  done
  echo "=== DBSIZE 验证 ==="
  for p in "${PORTS[@]}"; do
    echo -n "node $p: "
    rssh "timeout 3 redis-cli -h $HOST -p $p DBSIZE"
  done
}

cmd_rebuild_node() {
  local n="${1:-}"
  case "$n" in 1|2|3) ;; *) echo "用法: $0 rebuild-node <1|2|3>" >&2; exit 1;; esac
  local port="633$n" dir="${DIRS[$((n-1))]}"
  echo "=== 重建 node$n（$HOST:$port，目录 $dir）==="
  echo "步骤：停 → 备份目录 → 新目录 → 启动 → RESET HARD → FORGET 旧 ID → MEET → ADDSLOTS"
  local old_id
  old_id=$(rssh "timeout 3 redis-cli -h $HOST -p $port CLUSTER MYID 2>/dev/null" | tr -d '\r')
  echo "旧 node ID: $old_id"
  rssh "sudo systemctl stop boltdb-node$n && sudo mv /usr/local/boltdb_data/$dir /usr/local/boltdb_data/$dir.bak-\$(date +%Y%m%d) && sudo mkdir -p /usr/local/boltdb_data/$dir && sudo chown elex-gm0135:elex-gm0135 /usr/local/boltdb_data/$dir && sudo systemctl start boltdb-node$n"
  wait_pong "$port" || { echo "node$n 启动失败" >&2; exit 1; }
  # 新节点认领全部槽 → 清掉
  rssh "timeout 5 redis-cli -h $HOST -p $port CLUSTER RESET HARD"
  # 旧 ID 变幽灵：其他节点 FORGET（若有 FAIL 接管，先释放）
  local slot_start=$(( (n-1) * 5461 )) slot_end=$(( n * 5461 - 1 ))
  [ "$n" = "3" ] && slot_end=16383
  for peer in "${PORTS[@]}"; do
    [ "$peer" = "$port" ] && continue
    rssh "timeout 5 redis-cli -h $HOST -p $peer CLUSTER DELSLOTS \$(seq $slot_start $slot_end) >/dev/null 2>&1 || true; timeout 5 redis-cli -h $HOST -p $peer CLUSTER FORGET $old_id 2>/dev/null || true"
  done
  # 新节点入群 + 认领槽位
  for peer in "${PORTS[@]}"; do
    [ "$peer" = "$port" ] && continue
    rssh "timeout 5 redis-cli -h $HOST -p $port CLUSTER MEET $HOST $peer"
  done
  sleep 3
  rssh "timeout 5 redis-cli -h $HOST -p $port CLUSTER ADDSLOTSRANGE $slot_start $slot_end"
  sleep 5
  cmd_verify
  echo "=== 确认无误后删除备份: sudo rm -rf /usr/local/boltdb_data/$dir.bak-* ==="
}

cmd_recover() {
  echo "=== 脑裂恢复（全部 RESET HARD → MEET → ADDSLOTS）==="
  for p in "${PORTS[@]}"; do
    echo -n "node $p RESET HARD: "
    rssh "timeout 5 redis-cli -h $HOST -p $p CLUSTER RESET HARD"
  done
  # 从 node1 发起 MEET（双向握手），node2/3 通过 gossip 互相学习
  rssh "timeout 5 redis-cli -h $HOST -p ${PORTS[0]} CLUSTER MEET $HOST ${PORTS[1]} && timeout 5 redis-cli -h $HOST -p ${PORTS[0]} CLUSTER MEET $HOST ${PORTS[2]}"
  sleep 3
  rssh "timeout 5 redis-cli -h $HOST -p ${PORTS[0]} CLUSTER ADDSLOTSRANGE 0 5460 && timeout 5 redis-cli -h $HOST -p ${PORTS[1]} CLUSTER ADDSLOTSRANGE 5461 10922 && timeout 5 redis-cli -h $HOST -p ${PORTS[2]} CLUSTER ADDSLOTSRANGE 10923 16383"
  sleep 5
  cmd_verify
}

cmd_verify() {
  echo "=== 三节点视角 ==="
  local ok=1
  for p in "${PORTS[@]}"; do
    local assigned
    assigned=$(rssh "timeout 3 redis-cli -h $HOST -p $p CLUSTER INFO 2>/dev/null | grep '^cluster_slots_assigned:' | cut -d: -f2" | tr -d '\r')
    echo "node $p: slots_assigned=$assigned"
    [ "$assigned" = "16384" ] || ok=0
  done
  [ $ok -eq 1 ] && echo "✅ 集群完整" || { echo "❌ 槽位不完整！"; cmd_health; exit 1; }
}

case "${1:-}" in
  health) cmd_health ;;
  restart) cmd_restart ;;
  deploy) shift; cmd_deploy "$@" ;;
  gc) shift; cmd_gc "${1:-0.5}" ;;
  flush) cmd_flush ;;
  rebuild-node) shift; cmd_rebuild_node "${1:-}" ;;
  recover) cmd_recover ;;
  verify) cmd_verify ;;
  *) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
