# BoltDB 集成测试说明

## 概述

`main_test.go` 提供了完整的集成测试，使用 Redis 客户端库连接 BoltDB 服务器，测试所有 Redis 命令的正确性。

## 安装依赖

在运行测试之前，需要安装 Redis 客户端库：

```bash
go get github.com/go-redis/redis/v8
go mod tidy
```

## 运行测试

### 运行所有测试

```bash
# 在项目根目录
go test ./cmd/boltDB -v

# 或在 cmd/boltDB 目录
cd cmd/boltDB
go test -v
```

### 运行特定测试

```bash
# 测试String命令
go test ./cmd/boltDB -run TestStringCommands -v

# 测试Hash命令
go test ./cmd/boltDB -run TestHashCommands -v

# 测试List命令
go test ./cmd/boltDB -run TestListCommands -v

# 测试Set命令
go test ./cmd/boltDB -run TestSetCommands -v

# 测试SortedSet命令
go test ./cmd/boltDB -run TestSortedSetCommands -v

# 测试通用命令
go test ./cmd/boltDB -run TestGeneralCommands -v

# 测试并发操作
go test ./cmd/boltDB -run TestConcurrentOperations -v

# 测试错误处理
go test ./cmd/boltDB -run TestErrorHandling -v
```

## 测试覆盖

### 1. String 命令测试 (`TestStringCommands`)
- ✅ SET/GET
- ✅ SETEX/PSETEX
- ✅ SETNX
- ✅ GETSET
- ✅ MGET/MSET
- ✅ INCR/INCRBY/DECR/DECRBY
- ✅ APPEND
- ✅ STRLEN

### 2. Hash 命令测试 (`TestHashCommands`)
- ✅ HSET/HGET
- ✅ HGETALL
- ✅ HEXISTS
- ✅ HLEN
- ✅ HKEYS/HVALS
- ✅ HMSET/HMGET
- ✅ HSETNX
- ✅ HINCRBY
- ✅ HDEL

### 3. List 命令测试 (`TestListCommands`)
- ✅ LPUSH/RPUSH
- ✅ LLEN
- ✅ LINDEX
- ✅ LRANGE
- ✅ LPOP/RPOP
- ✅ LSET
- ✅ LTRIM

### 4. Set 命令测试 (`TestSetCommands`)
- ✅ SADD
- ✅ SCARD
- ✅ SISMEMBER
- ✅ SMEMBERS
- ✅ SREM
- ✅ SPOP
- ✅ SINTER

### 5. SortedSet 命令测试 (`TestSortedSetCommands`)
- ✅ ZADD
- ✅ ZCARD
- ✅ ZSCORE
- ✅ ZRANGE/ZREVRANGE
- ✅ ZRANK/ZREVRANK
- ✅ ZCOUNT
- ✅ ZINCRBY
- ✅ ZREM

### 6. 通用命令测试 (`TestGeneralCommands`)
- ✅ PING
- ✅ EXISTS
- ✅ TYPE
- ✅ DEL
- ✅ EXPIRE/TTL
- ✅ PERSIST

### 7. 并发操作测试 (`TestConcurrentOperations`)
- ✅ 多goroutine并发写入
- ✅ 数据一致性验证

### 8. 错误处理测试 (`TestErrorHandling`)
- ✅ 不存在的键
- ✅ 错误的命令参数
- ✅ 类型错误

## 测试架构

每个测试函数都会：
1. 调用 `setupTestServer()` 启动测试服务器
2. 创建 Redis 客户端连接到服务器
3. 执行各种 Redis 命令
4. 验证返回值和数据正确性
5. 调用 `teardownTestServer()` 清理资源

## 测试服务器配置

- **地址**: 127.0.0.1:0 (随机端口)
- **数据库**: 临时目录（每个测试独立）
- **压缩**: 默认使用 LZ4 压缩

## 示例输出

```
=== RUN   TestStringCommands
--- PASS: TestStringCommands (0.05s)
=== RUN   TestHashCommands
--- PASS: TestHashCommands (0.03s)
=== RUN   TestListCommands
--- PASS: TestListCommands (0.02s)
=== RUN   TestSetCommands
--- PASS: TestSetCommands (0.02s)
=== RUN   TestSortedSetCommands
--- PASS: TestSortedSetCommands (0.03s)
=== RUN   TestGeneralCommands
--- PASS: TestGeneralCommands (0.02s)
=== RUN   TestConcurrentOperations
--- PASS: TestConcurrentOperations (0.10s)
=== RUN   TestErrorHandling
--- PASS: TestErrorHandling (0.01s)
PASS
ok      github.com/lbp0200/BoltDB/cmd/boltDB    0.280s
```

## 故障排查

### 1. 依赖问题

如果遇到 `could not import github.com/go-redis/redis/v8` 错误：

```bash
go get github.com/go-redis/redis/v8
go mod tidy
```

### 2. 端口冲突

测试使用随机端口，通常不会冲突。如果遇到端口问题，检查是否有其他进程占用。

### 3. 测试超时

如果测试运行时间过长，可以增加超时时间：

```bash
go test ./cmd/boltDB -timeout 30s
```

### 4. 数据验证失败

如果数据验证失败，检查：
- 服务器是否正确启动
- Redis 客户端连接是否正常
- 命令实现是否正确

## 持续集成

可以在 CI/CD 流程中运行这些测试：

```yaml
# .github/workflows/test.yml
- name: Run Integration Tests
  run: |
    go get github.com/go-redis/redis/v8
    go test ./cmd/boltDB -v
```

## 扩展测试

要添加新的测试用例：

1. 在 `main_test.go` 中添加新的测试函数
2. 使用 `setupTestServer(t)` 和 `teardownTestServer(t)`
3. 使用 `testClient` 执行 Redis 命令
4. 使用 `assert` 验证结果

示例：

```go
func TestNewCommand(t *testing.T) {
    setupTestServer(t)
    defer teardownTestServer(t)
    
    ctx := context.Background()
    // 你的测试代码
}
```

