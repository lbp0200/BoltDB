#!/bin/bash

# BoltDB 压力测试脚本
# 使用方法: ./benchmark.sh [测试类型]
# 测试类型: basic, hash, list, set, zset, all (默认: basic)

TEST_TYPE=${1:-basic}
SERVER_ADDR="127.0.0.1"
SERVER_PORT="6337"
DATA_DIR="./benchmark_data"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== BoltDB 压力测试 ===${NC}"
echo "测试类型: $TEST_TYPE"
echo "服务器: $SERVER_ADDR:$SERVER_PORT"
echo ""

# 检查 redis-benchmark 是否安装
if ! command -v redis-benchmark &> /dev/null; then
    echo -e "${RED}错误: 未找到 redis-benchmark 工具${NC}"
    echo "请先安装 Redis:"
    echo "  macOS: brew install redis"
    echo "  Linux: sudo apt-get install redis-tools"
    exit 1
fi

# 检查服务器是否运行
if ! nc -z $SERVER_ADDR $SERVER_PORT 2>/dev/null; then
    echo -e "${YELLOW}警告: 服务器未运行在 $SERVER_ADDR:$SERVER_PORT${NC}"
    echo "请先启动 BoltDB 服务器:"
    echo "  go run cmd/boltDB/main.go -addr :$SERVER_PORT -dir $DATA_DIR"
    echo ""
    read -p "是否现在启动服务器? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "启动服务器..."
        go run cmd/boltDB/main.go -addr :$SERVER_PORT -dir $DATA_DIR &
        SERVER_PID=$!
        sleep 2
        echo -e "${GREEN}服务器已启动 (PID: $SERVER_PID)${NC}"
        echo ""
    else
        exit 1
    fi
else
    SERVER_PID=""
fi

# 测试函数
run_test() {
    local name=$1
    local commands=$2
    local requests=${3:-100000}
    local clients=${4:-50}
    
    echo -e "${YELLOW}=== $name ===${NC}"
    redis-benchmark -h $SERVER_ADDR -p $SERVER_PORT \
        -n $requests -c $clients -t $commands
    echo ""
}

# 根据测试类型运行不同的测试
case $TEST_TYPE in
    basic)
        echo -e "${GREEN}基础性能测试${NC}"
        run_test "SET/GET" "set,get" 100000 50
        run_test "INCR" "incr" 100000 50
        ;;
    
    hash)
        echo -e "${GREEN}Hash 类型测试${NC}"
        run_test "HSET/HGET" "hset,hget" 100000 50
        run_test "HGETALL" "hgetall" 50000 50
        ;;
    
    list)
        echo -e "${GREEN}List 类型测试${NC}"
        run_test "LPUSH/RPUSH" "lpush,rpush" 100000 50
        run_test "LPOP/RPOP" "lpop,rpop" 100000 50
        run_test "LRANGE" "lrange" 50000 50
        ;;
    
    set)
        echo -e "${GREEN}Set 类型测试${NC}"
        run_test "SADD/SPOP" "sadd,spop" 100000 50
        run_test "SMEMBERS" "smembers" 50000 50
        ;;
    
    zset)
        echo -e "${GREEN}Sorted Set 类型测试${NC}"
        run_test "ZADD" "zadd" 100000 50
        run_test "ZRANGE" "zrange" 50000 50
        run_test "ZRANK" "zrank" 50000 50
        ;;
    
    all)
        echo -e "${GREEN}完整测试套件${NC}"
        run_test "SET/GET" "set,get" 100000 50
        run_test "Hash" "hset,hget" 100000 50
        run_test "List" "lpush,rpush" 100000 50
        run_test "Set" "sadd,spop" 100000 50
        run_test "Sorted Set" "zadd,zrange" 50000 50
        ;;
    
    *)
        echo -e "${RED}未知的测试类型: $TEST_TYPE${NC}"
        echo "可用类型: basic, hash, list, set, zset, all"
        exit 1
        ;;
esac

# 清理
if [ ! -z "$SERVER_PID" ]; then
    echo -e "${YELLOW}停止服务器 (PID: $SERVER_PID)${NC}"
    kill $SERVER_PID 2>/dev/null
fi

echo -e "${GREEN}测试完成!${NC}"

