# Redis服务器测试指南

本文档介绍如何测试BoltDB Redis服务器的实现。

## 测试类型

### 1. 单元测试（Unit Tests）

直接测试`executeCommand`函数，不涉及网络通信。

**运行方式：**
```bash
go test ./internal/server -run TestExecuteCommand -v
```

**特点：**
- 快速执行
- 不依赖网络
- 适合测试命令逻辑

### 2. 集成测试（Integration Tests）

通过TCP连接测试完整的请求-响应流程。

**运行方式：**
```bash
go test ./internal/server -run TestTCPIntegration -v
```

**特点：**
- 测试完整的网络协议栈
- 验证RESP协议解析
- 更接近真实使用场景

### 3. 并发测试（Concurrency Tests）

测试多客户端并发连接。

**运行方式：**
```bash
go test ./internal/server -run TestConcurrentConnections -v
```

### 4. 性能测试（Benchmark Tests）

测试命令执行性能。

**运行方式：**
```bash
go test ./internal/server -bench=. -benchmem
```

## 使用Redis客户端测试

### 使用redis-cli

```bash
# 启动服务器
go run cmd/boltDB/main.go -addr :6337

# 在另一个终端使用redis-cli
redis-cli -p 6337

# 测试命令
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET key1 value1
OK
127.0.0.1:6379> GET key1
"value1"
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> HSET user:1 name Alice age 30
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Alice"
3) "age"
4) "30"
```

### 使用Go Redis客户端

```go
package main

import (
    "context"
    "fmt"
    "github.com/go-redis/redis/v8"
)

func main() {
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    ctx := context.Background()

    // 测试基本命令
    err := rdb.Set(ctx, "key", "value", 0).Err()
    if err != nil {
        panic(err)
    }

    val, err := rdb.Get(ctx, "key").Result()
    if err != nil {
        panic(err)
    }
    fmt.Println("key", val)

    // 测试Hash
    rdb.HSet(ctx, "user:1", "name", "Alice", "age", "30")
    result := rdb.HGetAll(ctx, "user:1").Val()
    fmt.Println(result)
}
```

### 使用Python Redis客户端

```python
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 测试基本命令
r.set('key1', 'value1')
print(r.get('key1'))

# 测试Hash
r.hset('user:1', mapping={'name': 'Alice', 'age': '30'})
print(r.hgetall('user:1'))

# 测试List
r.lpush('mylist', 'world', 'hello')
print(r.lrange('mylist', 0, -1))

# 测试Set
r.sadd('myset', 'member1', 'member2')
print(r.smembers('myset'))

# 测试SortedSet
r.zadd('zset', {'member1': 1.0, 'member2': 2.0})
print(r.zrange('zset', 0, -1, withscores=True))
```

## 测试覆盖的命令

### String命令
- SET, GET, SETEX, PSETEX, SETNX, GETSET
- MGET, MSET, MSETNX
- INCR, INCRBY, DECR, DECRBY, INCRBYFLOAT
- APPEND, STRLEN, GETRANGE, SETRANGE

### 通用键管理命令
- DEL, EXISTS, TYPE
- EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT
- TTL, PTTL, PERSIST
- RENAME, RENAMENX
- KEYS, SCAN, RANDOMKEY

### List命令
- LPUSH, RPUSH, LPOP, RPOP
- LLEN, LINDEX, LRANGE
- LSET, LTRIM, LINSERT, LREM
- RPOPLPUSH
- LPUSHX, RPUSHX
- BLPOP, BRPOP, BRPOPLPUSH

### Hash命令
- HSET, HGET, HDEL, HLEN
- HGETALL, HEXISTS, HKEYS, HVALS
- HMSET, HMGET
- HSETNX
- HINCRBY, HINCRBYFLOAT
- HSTRLEN

### Set命令
- SADD, SREM, SCARD, SISMEMBER
- SMEMBERS, SPOP, SRANDMEMBER
- SMOVE
- SINTER, SUNION, SDIFF
- SINTERSTORE, SUNIONSTORE, SDIFFSTORE

### SortedSet命令
- ZADD, ZREM, ZCARD, ZSCORE
- ZRANGE, ZREVRANGE
- ZRANK, ZREVRANK
- ZCOUNT, ZINCRBY

## 运行所有测试

```bash
# 运行所有单元测试
go test ./internal/server -v

# 运行所有测试并显示覆盖率
go test ./internal/server -cover

# 生成详细的覆盖率报告
go test ./internal/server -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## 测试最佳实践

1. **隔离测试**：每个测试使用独立的数据库实例（通过`t.TempDir()`）
2. **清理资源**：确保测试后关闭数据库连接
3. **并发安全**：测试并发场景确保线程安全
4. **错误处理**：测试各种错误情况
5. **边界条件**：测试空值、最大值、负数等边界情况

## 故障排查

### 测试失败常见原因

1. **端口冲突**：如果测试服务器无法启动，可能是端口被占用
2. **超时问题**：增加等待时间或检查服务器是否正常启动
3. **协议解析错误**：检查RESP协议格式是否正确
4. **类型断言失败**：检查响应类型是否符合预期

### 调试技巧

```go
// 在测试中添加调试输出
t.Logf("Response: %+v", resp)
t.Logf("Response type: %T", resp)

// 打印原始响应
respStr := resp.String()
t.Logf("Raw response: %q", respStr)
```

