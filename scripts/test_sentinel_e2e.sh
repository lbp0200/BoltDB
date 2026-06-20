#!/bin/bash
set -e

BIN=/tmp/bolt_cluster_test/boltDB
SENT=/tmp/bolt_cluster_test/sentinel
DIR=/tmp/bolt_cluster_test

# Clean up any leftover state
rm -rf $DIR/nodeA $DIR/nodeA_data $DIR/sentinelA_data $DIR/sentinelB_data
mkdir -p $DIR/nodeA_data $DIR/sentinelA_data $DIR/sentinelB_data

# Sentinel A config
cat > $DIR/sentinelA.conf << 'EOF'
sentinel monitor mymaster 127.0.0.1 6337 2
sentinel known-sentinel mymaster 127.0.0.1:26382
EOF

# Sentinel B config
cat > $DIR/sentinelB.conf << 'EOF'
sentinel monitor mymaster 127.0.0.1 6337 2
sentinel known-sentinel mymaster 127.0.0.1:26380
EOF

echo "=== Step 1: Start boltDB master (:6337) ==="
$BIN -dir=$DIR/nodeA_data -addr=:6337 -log-level=WARNING &
PID_MASTER=$!
sleep 2

echo "=== Step 2: Start Sentinel A (:26379, gossip:26380) ==="
$SENT -addr=:26379 -gossip-port=26380 -config=$DIR/sentinelA.conf -log-level=WARNING &
PID_SA=$!
sleep 2

echo "=== Step 3: Start Sentinel B (:26381, gossip:26382) ==="
$SENT -addr=:26381 -gossip-port=26382 -config=$DIR/sentinelB.conf -log-level=WARNING &
PID_SB=$!
sleep 2

echo ""
echo "=== Verify: PING ==="
redis-cli -p 26379 PING && echo "  A: OK" || echo "  A: FAIL"
redis-cli -p 26381 PING && echo "  B: OK" || echo "  B: FAIL"

echo ""
echo "=== Verify: GET-MASTER-ADDR-BY-NAME ==="
echo "  A: $(redis-cli -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster)"
echo "  B: $(redis-cli -p 26381 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster)"

echo ""
echo "=== Verify: initial state ==="
echo "  A state: $(redis-cli -p 26379 SENTINEL MASTERS | tr '\n' ' ')"
echo ""
echo "---"

# Sample the state
echo ""
echo "=== Step 4: Kill boltDB master (simulate crash) ==="
kill $PID_MASTER 2>/dev/null || true
wait $PID_MASTER 2>/dev/null || true
echo "  boltDB PID $PID_MASTER killed"

# Wait for downAfter (30s) + some margin
echo ""
echo "=== Step 5: Wait 35s for SDOWN/ODOWN detection ==="
for i in $(seq 1 35); do
    sleep 1
    if [ $((i % 5)) -eq 0 ]; then
        echo "  ... ${i}s"
    fi
done

echo ""
echo "=== Step 6: Check state after detection ==="
echo "  A masters:"
redis-cli -p 26379 SENTINEL MASTERS
echo "---"
echo "  B masters:"
redis-cli -p 26381 SENTINEL MASTERS
echo "---"

# Check state field
SA_STATE=$(redis-cli -p 26379 SENTINEL MASTERS | grep -A1 "^flags" | tail -1)
echo ""
echo "  A state: $SA_STATE"
SB_STATE=$(redis-cli -p 26381 SENTINEL MASTERS | grep -A1 "^flags" | tail -1)
echo "  B state: $SB_STATE"

# Also get-master-addr-by-name now (should still return addr even if down)
echo ""
echo "  A GET-MASTER-ADDR: $(redis-cli -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster)"
echo "  B GET-MASTER-ADDR: $(redis-cli -p 26381 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster)"

echo ""
echo "=== Step 7: Cleanup ==="
kill $PID_SA $PID_SB 2>/dev/null || true
wait $PID_SA $PID_SB 2>/dev/null || true

# Assess
echo ""
if echo "$SA_STATE" | grep -q "sdown\|odown"; then
    echo "RESULT: Sentinel A detected SDOWN/ODOWN ✓"
else
    echo "RESULT: Sentinel A did NOT detect SDOWN ✗ (state=$SA_STATE)"
fi
if echo "$SB_STATE" | grep -q "sdown\|odown"; then
    echo "RESULT: Sentinel B detected SDOWN/ODOWN ✓"
else
    echo "RESULT: Sentinel B did NOT detect SDOWN ✗ (state=$SB_STATE)"
fi
echo "=== Done ==="
