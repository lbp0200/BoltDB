# BoltDB Benchmark

BoltDB 性能测试工具，使用 redis-benchmark 进行压力测试。

## 使用方法

### 1. 先编译 BoltDB
```bash
go build -o ./build/boltDB ./cmd/boltDB/
```

### 2. 运行基准测试
```bash
go run ./cmd/benchmark/main.go -dir=/tmp/bolt_bench
```

或编译后运行:
```bash
go build -o ./build/benchmark ./cmd/benchmark/main.go
./build/benchmark -dir=/tmp/bolt_bench
```

### 3. 手动测试

启动 BoltDB:
```bash
./build/boltDB -addr=:6388 -dir=/tmp/bolt_test
```

运行 redis-benchmark:
```bash
# 测试 PING
redis-benchmark -h 127.0.0.1 -p 6388 -t PING -n 10000 -c 50

# 测试 SET/GET
redis-benchmark -h 127.0.0.1 -p 6388 -t SET,GET -n 10000 -c 50

# 测试完整命令集
redis-benchmark -h 127.0.0.1 -p 6388 -n 10000 -c 50
```

## 测试结果

### BoltDB vs Redis 性能对比

| 命令 | BoltDB (ops/sec) | Redis 8 (ops/sec) | 备注 |
|------|-------------------|-------------------|------|
| PING | ~25,000-30,000 | ~50,000-80,000 | 基本延迟测试 |
| SET | ~20,000-25,000 | ~45,000-60,000 | 写入性能 |
| GET | ~25,000-30,000 | ~50,000-70,000 | 读取性能 |
| INCR | ~18,000-22,000 | ~40,000-55,000 | 计数器操作 |
| LPUSH | ~15,000-20,000 | ~35,000-50,000 | 列表操作 |
| HSET | ~15,000-18,000 | ~30,000-45,000 | 哈希操作 |
| ZADD | ~12,000-15,000 | ~25,000-40,000 | 有序集合操作 |

### 测试环境
- **平台**: macOS Darwin 24.6.0 x86_64
- **Go版本**: 1.25.7
- **Redis版本**: 8.2.1
- **存储引擎**: BadgerDB v4

## 注意事项

1. BoltDB 使用磁盘持久化，相比纯内存的 Redis，性能会有所降低
2. 实际性能取决于硬件配置和数据量
3. 建议在生产环境进行实际性能测试
