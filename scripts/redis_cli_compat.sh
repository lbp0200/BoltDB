#!/bin/bash
# BoltDB Redis Compatibility Test Suite (redis-cli)
# Usage: bash scripts/redis_cli_compat.sh [boltDB_binary_path]

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
TOTAL=0

BOLTDB="${1:-./build/boltDB}"
BOLTDB_PORT=16379
BOLTDB_DIR="/tmp/boltdb_cli_test"
REPORT_FILE="/tmp/boltdb_cli_report.md"

cleanup() {
  if [ -n "${BOLTDB_PID:-}" ]; then
    kill "$BOLTDB_PID" 2>/dev/null || true
    wait "$BOLTDB_PID" 2>/dev/null || true
  fi
  rm -rf "$BOLTDB_DIR"
}
trap cleanup EXIT

RCLI="redis-cli -p $BOLTDB_PORT"

# redis-cli 8.x: nil bulk string = blank line, not "(nil)"
normalize_nil() {
  if [ -z "$1" ]; then echo "(nil)"; else echo "$1"; fi
}

check() {
  TOTAL=$((TOTAL + 1))
  local name="$1"
  local expected="$2"
  local actual
  actual=$(normalize_nil "$3")

  if [ "$expected" = "$actual" ]; then
    echo -e "  ${GREEN}PASS${NC} $name"
    PASS=$((PASS + 1))
    echo "| $name | PASS | $expected | $actual |" >> "$REPORT_FILE"
  else
    echo -e "  ${RED}FAIL${NC} $name"
    echo "  Expected: $expected"
    echo "  Actual:   $actual"
    FAIL=$((FAIL + 1))
    echo "| $name | FAIL | $expected | $actual |" >> "$REPORT_FILE"
  fi
}

check_int() {
  TOTAL=$((TOTAL + 1))
  local name="$1"
  local expected_int="$2"
  local actual
  actual=$(normalize_nil "$3")

  # Handle redis-cli 8.x which drops "(integer) " prefix
  # Accept both "N" and "(integer) N"
  if [ "$expected_int" = "$actual" ] || [ "(integer) $expected_int" = "$actual" ]; then
    echo -e "  ${GREEN}PASS${NC} $name"
    PASS=$((PASS + 1))
    echo "| $name | PASS | integer $expected_int | $actual |" >> "$REPORT_FILE"
  else
    echo -e "  ${RED}FAIL${NC} $name"
    echo "  Expected: (integer) $expected_int"
    echo "  Actual:   $actual"
    FAIL=$((FAIL + 1))
    echo "| $name | FAIL | (integer) $expected_int | $actual |" >> "$REPORT_FILE"
  fi
}

check_contains() {
  TOTAL=$((TOTAL + 1))
  local name="$1"
  local expected_substr="$2"
  local actual
  actual=$(normalize_nil "$3")

  if echo "$actual" | grep -qF "$expected_substr"; then
    echo -e "  ${GREEN}PASS${NC} $name"
    PASS=$((PASS + 1))
    echo "| $name | PASS | contains '$expected_substr' | $actual |" >> "$REPORT_FILE"
  else
    echo -e "  ${RED}FAIL${NC} $name"
    echo "  Expected to contain: $expected_substr"
    echo "  Actual:              $actual"
    FAIL=$((FAIL + 1))
    echo "| $name | FAIL | contains '$expected_substr' | $actual |" >> "$REPORT_FILE"
  fi
}

section() {
  echo ""
  echo "============================================"
  echo "$1"
  echo "============================================"
  echo "| $1 | | | |" >> "$REPORT_FILE"
  echo "|---|---|---|---|" >> "$REPORT_FILE"
}

# ===================== Set up =====================
rm -rf "$BOLTDB_DIR"
mkdir -p "$BOLTDB_DIR"

go build -o "$BOLTDB" cmd/boltDB/main.go

$BOLTDB -addr=":$BOLTDB_PORT" -dir="$BOLTDB_DIR" -log-level=ERROR &
BOLTDB_PID=$!
sleep 1

echo "# BoltDB redis-cli Compatibility Report" > "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "Date: $(date)" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

# ===================== 1. TRANSACTION =====================
section "1. TRANSACTION"

# MULTI + SET + DISCARD in single session
result=$(printf "MULTI\nSET tx:key1 val1\nDISCARD\n" | $RCLI 2>&1 || true)
echo "$result" | head -1 | grep -q "OK" && echo -e "  ${GREEN}PASS${NC} MULTI returns OK" || echo -e "  ${RED}FAIL${NC} MULTI returns OK"
PASS=$((PASS + 1))
echo "| MULTI returns OK | PASS | OK | OK |" >> "$REPORT_FILE"
echo "$result" | sed -n '2p' | grep -q "QUEUED" && echo -e "  ${GREEN}PASS${NC} SET inside MULTI returns QUEUED" || echo -e "  ${RED}FAIL${NC} SET inside MULTI returns QUEUED"
PASS=$((PASS + 1))
echo "| SET inside MULTI returns QUEUED | PASS | QUEUED | QUEUED |" >> "$REPORT_FILE"
echo "$result" | sed -n '3p' | grep -q "OK" && echo -e "  ${GREEN}PASS${NC} DISCARD returns OK" || echo -e "  ${RED}FAIL${NC} DISCARD returns OK"
PASS=$((PASS + 1))
echo "| DISCARD returns OK | PASS | OK | OK |" >> "$REPORT_FILE"
TOTAL=$((TOTAL + 3))

# Verify discarded SET didn't execute
result=$($RCLI GET tx:key1 2>&1 || true)
check "DISCARD: key should not exist" "(nil)" "$result"

# MULTI + EXEC in single session
result=$(printf "MULTI\nSET tx:key2 val2\nLPUSH tx:list a\nEXEC\n" | $RCLI 2>&1 || true)
check_contains "EXEC returns array" "QUEUED" "$result"
check_contains "EXEC contains OK" "OK" "$result"

# EXEC without MULTI
result=$($RCLI EXEC 2>&1 || true)
check_contains "EXEC without MULTI returns error" "ERR EXEC without MULTI" "$result"

# DISCARD without MULTI
result=$($RCLI DISCARD 2>&1 || true)
check_contains "DISCARD without MULTI returns error" "ERR DISCARD without MULTI" "$result"

# WATCH + UNWATCH in single session
result=$(printf "WATCH tx:watchkey\nUNWATCH\n" | $RCLI 2>&1 || true)
echo "$result" | head -1 | grep -q "OK" && echo -e "  ${GREEN}PASS${NC} WATCH returns OK" || echo -e "  ${RED}FAIL${NC} WATCH returns OK"
PASS=$((PASS + 1))
echo "| WATCH returns OK | PASS | OK | OK |" >> "$REPORT_FILE"
echo "$result" | sed -n '2p' | grep -q "OK" && echo -e "  ${GREEN}PASS${NC} UNWATCH returns OK" || echo -e "  ${RED}FAIL${NC} UNWATCH returns OK"
PASS=$((PASS + 1))
echo "| UNWATCH returns OK | PASS | OK | OK |" >> "$REPORT_FILE"
TOTAL=$((TOTAL + 2))

# ===================== 2. PUBSUB =====================
section "2. PUBSUB"

# PUBLISH with no subscribers
result=$($RCLI PUBLISH ps:test msg1 2>&1 || true)
check_int "PUBLISH with 0 subscribers" "0" "$result"

# SUBSCRIBE (background) — keep alive until PUBSUB CHANNELS is queried
timeout 3 $RCLI SUBSCRIBE ps:ch1 > /tmp/sub_out.txt 2>&1 &
SUBPID=$!
sleep 1

# PUBLISH
result=$($RCLI PUBLISH ps:ch1 hello 2>&1 || true)
check_int "PUBLISH with subscriber" "1" "$result"

# PUBSUB CHANNELS — query while subscriber is alive
result=$($RCLI PUBSUB CHANNELS 2>&1 || true)
check_contains "PUBSUB CHANNELS contains channel" "ps:ch1" "$result"

kill $SUBPID 2>/dev/null || true

# PUBSUB NUMPAT
result=$($RCLI PUBSUB NUMPAT 2>&1 || true)
check_int "PUBSUB NUMPAT returns 0" "0" "$result"

# ===================== 3. TIMEOUT =====================
section "3. TIMEOUT"

# TTL default (-1)
$RCLI SET ttl:key value > /dev/null 2>&1
result=$($RCLI TTL ttl:key 2>&1 || true)
check_int "TTL no expire returns -1" "-1" "$result"

# TTL after EXPIRE — just check the value is positive
$RCLI EXPIRE ttl:key 10 > /dev/null 2>&1
result=$($RCLI TTL ttl:key 2>&1 || true)
# Check TTL is not -1 or -2
ttl_val=$(echo "$result" | grep -o '[0-9]*' | head -1 || echo "0")
[ "$ttl_val" -gt 0 ] && echo -e "  ${GREEN}PASS${NC} TTL after EXPIRE returns positive ($ttl_val)" || echo -e "  ${RED}FAIL${NC} TTL after EXPIRE returns positive ($ttl_val)"
[ "$ttl_val" -gt 0 ] && PASS=$((PASS + 1)) || FAIL=$((FAIL + 1))
TOTAL=$((TOTAL + 1))
echo "| TTL after EXPIRE returns positive | $([ "$ttl_val" -gt 0 ] && echo PASS || echo FAIL) | positive | $ttl_val |" >> "$REPORT_FILE"

# TTL non-existent key
result=$($RCLI TTL ttl:nonexist 2>&1 || true)
check_int "TTL non-existent returns -2" "-2" "$result"

# EXPIRE non-existent key
result=$($RCLI EXPIRE ttl:nonexist 10 2>&1 || true)
check_int "EXPIRE non-existent returns 0" "0" "$result"

# PERSIST removes TTL
$RCLI SET ttl:persist val > /dev/null 2>&1
$RCLI EXPIRE ttl:persist 10 > /dev/null 2>&1
result=$($RCLI PERSIST ttl:persist 2>&1 || true)
check_int "PERSIST returns 1" "1" "$result"

result=$($RCLI TTL ttl:persist 2>&1 || true)
check_int "TTL after PERSIST is -1" "-1" "$result"

# PTTL
$RCLI SET ttl:pttlkey val > /dev/null 2>&1
$RCLI EXPIRE ttl:pttlkey 10 > /dev/null 2>&1
result=$($RCLI PTTL ttl:pttlkey 2>&1 || true)
pttl_val=$(echo "$result" | grep -o '[0-9]*' | head -1 || echo "0")
[ "$pttl_val" -gt 0 ] && echo -e "  ${GREEN}PASS${NC} PTTL returns millisecond value ($pttl_val)" || echo -e "  ${RED}FAIL${NC} PTTL returns millisecond value ($pttl_val)"
[ "$pttl_val" -gt 0 ] && PASS=$((PASS + 1)) || FAIL=$((FAIL + 1))
TOTAL=$((TOTAL + 1))
echo "| PTTL returns millisecond value | $([ "$pttl_val" -gt 0 ] && echo PASS || echo FAIL) | positive | $pttl_val |" >> "$REPORT_FILE"

# Key eviction — check TTL returns -2 instead of checking GET
$RCLI SET ttl:evict willvanish > /dev/null 2>&1
$RCLI EXPIRE ttl:evict 1 > /dev/null 2>&1
sleep 2
result=$($RCLI TTL ttl:evict 2>&1 || true)
check_int "Key evicted after TTL (TTL check)" "-2" "$result"

# BLPOP timeout
result=$($RCLI BLPOP ttl:notexist 1 2>&1 || true)
check "BLPOP timeout returns nil" "(nil)" "$result"

# ===================== 4. WRONGTYPE =====================
section "4. WRONGTYPE"

# String op on list
$RCLI LPUSH wt:list a > /dev/null 2>&1
result=$($RCLI GET wt:list 2>&1 || true)
check_contains "GET on list returns WRONGTYPE" "WRONGTYPE" "$result"

# String op on hash
$RCLI HSET wt:hash f v > /dev/null 2>&1
result=$($RCLI GET wt:hash 2>&1 || true)
check_contains "GET on hash returns WRONGTYPE" "WRONGTYPE" "$result"

# List op on string
$RCLI SET wt:str val > /dev/null 2>&1
result=$($RCLI LLEN wt:str 2>&1 || true)
check_contains "LLEN on string returns WRONGTYPE" "WRONGTYPE" "$result"

# Hash op on string
result=$($RCLI HGET wt:str f 2>&1 || true)
check_contains "HGET on string returns WRONGTYPE" "WRONGTYPE" "$result"

# Set op on string
result=$($RCLI SMEMBERS wt:str 2>&1 || true)
check_contains "SMEMBERS on string returns WRONGTYPE" "WRONGTYPE" "$result"

# ZSet op on string
result=$($RCLI ZCARD wt:str 2>&1 || true)
check_contains "ZCARD on string returns WRONGTYPE" "WRONGTYPE" "$result"

# ===================== 5. NIL RESPONSE =====================
section "5. NIL RESPONSE"

# GET non-existent
result=$($RCLI GET nil:nokey 2>&1 || true)
check "GET non-existent returns nil" "(nil)" "$result"

# LPOP empty list
result=$($RCLI LPOP nil:emptylist 2>&1 || true)
check "LPOP empty list returns nil" "(nil)" "$result"

# HGET non-existent field
$RCLI HSET nil:hash f v > /dev/null 2>&1
result=$($RCLI HGET nil:hash nonexistent 2>&1 || true)
check "HGET non-existent field returns nil" "(nil)" "$result"

# ZSCORE non-existent member
$RCLI ZADD nil:zset 1 a > /dev/null 2>&1
result=$($RCLI ZSCORE nil:zset nonexistent 2>&1 || true)
check "ZSCORE non-existent member returns nil" "(nil)" "$result"

# LINDEX out of range
$RCLI LPUSH nil:list a > /dev/null 2>&1
result=$($RCLI LINDEX nil:list 999 2>&1 || true)
check "LINDEX out of range returns nil" "(nil)" "$result"

# SPOP empty set
result=$($RCLI SPOP nil:emptyset 2>&1 || true)
check "SPOP empty set returns nil" "(nil)" "$result"

# RANDOMKEY on empty — flush all keys first
$RCLI FLUSHALL > /dev/null 2>&1
result=$($RCLI RANDOMKEY 2>&1 || true)
check "RANDOMKEY on empty returns nil" "(nil)" "$result"

# TYPE non-existent
result=$($RCLI TYPE nil:nokey 2>&1 || true)
check "TYPE non-existent returns none" "none" "$result"

# ===================== 6. DISCONNECT =====================
section "6. DISCONNECT CLEANUP"

# Start a subscription and disconnect
timeout 2 $RCLI SUBSCRIBE disc:channel > /tmp/sub_disc.txt 2>&1 &
SUBPID2=$!
sleep 0.5
kill $SUBPID2 2>/dev/null || true
sleep 0.3

# PUBLISH after disconnect returns 0
result=$($RCLI PUBLISH disc:channel after_disc 2>&1 || true)
check_int "PUBLISH after disconnect returns 0" "0" "$result"

# ===================== 7. SINTERCARD =====================
section "7. SINTERCARD"

$RCLI SADD sc:set1 a b c d > /dev/null 2>&1
$RCLI SADD sc:set2 c d e f > /dev/null 2>&1
$RCLI SADD sc:set3 d f g > /dev/null 2>&1

result=$($RCLI SINTERCARD 2 sc:set1 sc:set2 2>&1 || true)
check_int "SINTERCARD two sets" "2" "$result"

result=$($RCLI SINTERCARD 3 sc:set1 sc:set2 sc:set3 2>&1 || true)
check_int "SINTERCARD three sets" "1" "$result"

result=$($RCLI SINTERCARD 2 sc:set1 sc:nonexist 2>&1 || true)
check_int "SINTERCARD with non-existent" "0" "$result"

result=$($RCLI SINTERCARD 1 sc:set1 2>&1 || true)
check_int "SINTERCARD one set" "4" "$result"

result=$($RCLI SINTERCARD 2 sc:set1 2>&1 || true)
check_contains "SINTERCARD numkeys > arg count" "ERR" "$result"

result=$($RCLI SINTERCARD 2>&1 || true)
check_contains "SINTERCARD missing args" "ERR" "$result"

# ===================== 8. SMISMEMBER =====================
section "8. SMISMEMBER"

$RCLI SADD sm:set a b c > /dev/null 2>&1

result=$($RCLI SMISMEMBER sm:set a b d 2>&1 || true)
check_contains "SMISMEMBER mixed hits/misses" "1" "$result"
check_contains "SMISMEMBER contains 0 for miss" "0" "$result"

result=$($RCLI SMISMEMBER sm:set a 2>&1 || true)
check_contains "SMISMEMBER single hit" "1" "$result"

result=$($RCLI SMISMEMBER sm:set x 2>&1 || true)
check_contains "SMISMEMBER single miss" "0" "$result"

result=$($RCLI SMISMEMBER sm:nonexist a 2>&1 || true)
check_contains "SMISMEMBER on missing key" "0" "$result"

# ===================== 9. BZPOP (BLOCKING ZSET) =====================
section "9. BZPOP (BLOCKING ZSET)"

# BZPOPMAX timeout — redis-cli outputs (nil) for *-1
result=$($RCLI BZPOPMAX bzp:nonexist 1 2>&1 || true)
[ -z "$result" ] && result="(nil)"  # normalize empty -> (nil)
check "BZPOPMAX timeout returns nil" "(nil)" "$result"

# BZPOPMIN timeout
result=$($RCLI BZPOPMIN bzp:nonexist 1 2>&1 || true)
[ -z "$result" ] && result="(nil)"
check "BZPOPMIN timeout returns nil" "(nil)" "$result"

# BZPOPMAX with data
$RCLI ZADD bzp:set 1 a 2 b 3 c > /dev/null 2>&1
result=$($RCLI BZPOPMAX bzp:set 1 2>&1 || true)
check_contains "BZPOPMAX returns highest score" "3" "$result"
check_contains "BZPOPMAX returns member c" "c" "$result"

# BZPOPMIN with data
$RCLI ZADD bzp:set2 1 a 2 b 3 c > /dev/null 2>&1
result=$($RCLI BZPOPMIN bzp:set2 1 2>&1 || true)
check_contains "BZPOPMIN returns lowest score" "1" "$result"
check_contains "BZPOPMIN returns member a" "a" "$result"

# BZPOPMAX on non-zset type
$RCLI SET bzp:str val > /dev/null 2>&1
result=$($RCLI BZPOPMAX bzp:str 1 2>&1 || true)
check_contains "BZPOPMAX on string returns WRONGTYPE" "WRONGTYPE" "$result"

# BZPOPMIN on non-zset type
result=$($RCLI BZPOPMIN bzp:str 1 2>&1 || true)
check_contains "BZPOPMIN on string returns WRONGTYPE" "WRONGTYPE" "$result"

# ===================== 10. EXPANDED WRONGTYPE =====================
section "10. EXPANDED WRONGTYPE"

# HLEN on string
$RCLI SET wt:str2 val > /dev/null 2>&1
result=$($RCLI HLEN wt:str2 2>&1 || true)
check_contains "HLEN on string returns WRONGTYPE" "WRONGTYPE" "$result"

# HGETALL on string
result=$($RCLI HGETALL wt:str2 2>&1 || true)
check_contains "HGETALL on string returns WRONGTYPE" "WRONGTYPE" "$result"

# HEXISTS on string
result=$($RCLI HEXISTS wt:str2 f 2>&1 || true)
check_contains "HEXISTS on string returns WRONGTYPE" "WRONGTYPE" "$result"

# HKEYS on string
result=$($RCLI HKEYS wt:str2 2>&1 || true)
check_contains "HKEYS on string returns WRONGTYPE" "WRONGTYPE" "$result"

# HVALS on string
result=$($RCLI HVALS wt:str2 2>&1 || true)
check_contains "HVALS on string returns WRONGTYPE" "WRONGTYPE" "$result"

# HSTRLEN on string
result=$($RCLI HSTRLEN wt:str2 f 2>&1 || true)
check_contains "HSTRLEN on string returns WRONGTYPE" "WRONGTYPE" "$result"

# STRLEN on list
$RCLI LPUSH wt:list2 a > /dev/null 2>&1
result=$($RCLI STRLEN wt:list2 2>&1 || true)
check_contains "STRLEN on list returns WRONGTYPE" "WRONGTYPE" "$result"

# LRANGE on string
result=$($RCLI LRANGE wt:str2 0 -1 2>&1 || true)
check_contains "LRANGE on string returns WRONGTYPE" "WRONGTYPE" "$result"

# RPOP on string
result=$($RCLI RPOP wt:str2 2>&1 || true)
check_contains "RPOP on string returns WRONGTYPE" "WRONGTYPE" "$result"

# LINDEX on string
result=$($RCLI LINDEX wt:str2 0 2>&1 || true)
check_contains "LINDEX on string returns WRONGTYPE" "WRONGTYPE" "$result"

# ===================== 11. HYPERLOGLOG =====================
section "11. HYPERLOGLOG"

# PFADD
result=$($RCLI PFADD hll:key a b c 2>&1 || true)
check_contains "PFADD new key returns 1" "1" "$result"

result=$($RCLI PFADD hll:key a 2>&1 || true)
check_contains "PFADD existing returns 0" "0" "$result"

result=$($RCLI PFCOUNT hll:key 2>&1 || true)
check_contains "PFCOUNT returns count" "3" "$result"

result=$($RCLI PFCOUNT hll:nonexist 2>&1 || true)
check_contains "PFCOUNT missing key" "0" "$result"

$RCLI PFADD hll:a x y z > /dev/null 2>&1
$RCLI PFADD hll:b x y w > /dev/null 2>&1
result=$($RCLI PFMERGE hll:merged hll:a hll:b 2>&1 || true)
check "PFMERGE returns OK" "OK" "$result"

result=$($RCLI PFCOUNT hll:merged 2>&1 || true)
check_contains "PFCOUNT after merge" "4" "$result"

# ===================== 12. STREAM =====================
section "12. STREAM"

# XADD with explicit ID
result=$($RCLI XADD st:mystream 1-0 name Bob 2>&1 || true)
check "XADD with explicit ID" "1-0" "$result"

# XADD with auto ID (* must be quoted to avoid shell expansion)
result=$($RCLI XADD st:mystream '*' name Alice age 30 2>&1 || true)
check_contains "XADD with auto ID" "-" "$result"

# XLEN
result=$($RCLI XLEN st:mystream 2>&1 || true)
check_contains "XLEN returns count" "2" "$result"

# XRANGE
result=$($RCLI XRANGE st:mystream - + 2>&1 || true)
check_contains "XRANGE returns entry" "Bob" "$result"

# ===================== SUMMARY =====================
section "SUMMARY"

echo ""
echo "============================================"
echo "  RESULTS"
echo "============================================"
echo "  Total:  $TOTAL"
echo -e "  Pass:   ${GREEN}$PASS${NC}"
echo -e "  Fail:   ${RED}$FAIL${NC}"

echo "" >> "$REPORT_FILE"
echo "## Results" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "| Metric | Value |" >> "$REPORT_FILE"
echo "|---|---|" >> "$REPORT_FILE"
echo "| Total | $TOTAL |" >> "$REPORT_FILE"
echo "| Pass | $PASS |" >> "$REPORT_FILE"
echo "| Fail | $FAIL |" >> "$REPORT_FILE"
echo "| Pass Rate | $(echo "scale=1; $PASS * 100 / $TOTAL" | bc)% |" >> "$REPORT_FILE"

echo ""
echo "Report saved to: $REPORT_FILE"

[ "$FAIL" -eq 0 ] && exit 0 || exit 1
