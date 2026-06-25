#!/bin/bash

# 测试脚本，模拟 redis-benchmark 的行为

SERVER_ADDR="127.0.0.1"
SERVER_PORT="6337"

echo "=== 测试 1: CONFIG GET * ==="
echo -e "*3\r\n\$6\r\nCONFIG\r\n\$3\r\nGET\r\n\$1\r\n*\r\n" | nc $SERVER_ADDR $SERVER_PORT

echo ""
echo "=== 测试 2: PING ==="
echo -e "*1\r\n\$4\r\nPING\r\n" | nc $SERVER_ADDR $SERVER_PORT

echo ""
echo "=== 测试 3: CONFIG GET * 然后 PING (模拟 redis-benchmark) ==="
(echo -e "*3\r\n\$6\r\nCONFIG\r\n\$3\r\nGET\r\n\$1\r\n*\r\n"; sleep 0.1; echo -e "*1\r\n\$4\r\nPING\r\n") | nc $SERVER_ADDR $SERVER_PORT

echo ""
echo "=== 测试完成 ==="

