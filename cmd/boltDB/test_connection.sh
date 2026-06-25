#!/bin/bash

# 测试 BoltDB 服务器连接
SERVER_ADDR="127.0.0.1"
SERVER_PORT="6337"

echo "测试 BoltDB 服务器连接..."

# 测试 PING
echo "1. 测试 PING 命令"
redis-cli -h $SERVER_ADDR -p $SERVER_PORT PING
if [ $? -ne 0 ]; then
    echo "❌ PING 失败"
    exit 1
fi

# 测试 CONFIG GET
echo "2. 测试 CONFIG GET 命令"
redis-cli -h $SERVER_ADDR -p $SERVER_PORT CONFIG GET "*"
if [ $? -ne 0 ]; then
    echo "❌ CONFIG GET 失败"
    exit 1
fi

# 测试 SET/GET
echo "3. 测试 SET/GET 命令"
redis-cli -h $SERVER_ADDR -p $SERVER_PORT SET test_key "test_value"
redis-cli -h $SERVER_ADDR -p $SERVER_PORT GET test_key
if [ $? -ne 0 ]; then
    echo "❌ SET/GET 失败"
    exit 1
fi

echo "✅ 所有测试通过！"

