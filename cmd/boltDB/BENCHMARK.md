# Redis Benchmark 压力测试指南

本文档介绍如何使用 `redis-benchmark` 工具对 BoltDB 服务器进行压力测试。

## 前置条件

1. **安装 Redis**（用于获取 `redis-benchmark` 工具）
   ```bash
   # macOS
   brew install redis

   # Linux (Ubuntu/Debian)
   sudo apt-get install redis-tools

   # 或者从 Redis 源码编译
   git clone https://github.com/redis/redis.git
   cd redis
   make
   # redis-benchmark 位于 src/ 目录
   ```

2. **启动 BoltDB 服务器**
   ```bash
   go run cmd/boltDB/main.go -addr=:6337 -dir=./data
   go run cmd/boltDB/main.go -addr :6337 -log-level DEBUG
   ```

3. **测试连接**（可选）
   ```bash
   # 使用测试脚本
   cd cmd/boltDB
   ./test_connection.sh

   # 或手动测试
   redis-cli -h 127.0.0.1 -p 6337 PING
   redis-cli -h 127.0.0.1 -p 6337 CONFIG GET "*"
   ```

## 基本使用

### 1. 快速测试（默认配置）

```bash
redis-benchmark -h 127.0.0.1 -p 6337
```

这会运行默认的测试套件，包括：
- 100,000 个请求
- 50 个并发客户端

### 2. 自定义参数

```bash
# 指定请求数和并发数
redis-benchmark -h 127.0.0.1 -p 6337 -n 10000 -c 100

# 仅测试特定命令
redis-benchmark -h 127.0.0.1 -p 6337 -t set
redis-benchmark -h 127.0.0.1 -p 6337 -t get
redis-benchmark -h 127.0.0.1 -p 6337 -t set,get,incr
```

### 3. 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-h` | 服务器主机 | 127.0.0.1 |
| `-p` | 服务器端口 | 6337 |
| `-c` | 并发客户端数 | 50 |
| `-n` | 总请求数 | 100000 |
| `-d` | 数据大小（字节） | 3 |
| `-t` | 测试的命令集 | 所有 |
| `-P` | 管道请求数 | 1 |
| `-r` | 随机 key 范围 | 0 |
| `-l` | 循环测试 | 否 |
| `--csv` | CSV 格式输出 | 否 |

## 压力测试示例

### 1. 混合负载测试

```bash
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -t set,get
```

### 2. 高并发测试

```bash
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 1000 -t set,get
```

### 3. 大数据值测试

```bash
redis-benchmark -h 127.0.0.1 -p 6337 -n 10000 -c 50 -t set,get -d 1024
```

### 4. 管道测试

```bash
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -P 10 -t set,get
```

### 5. 各类数据类型测试

```bash
# Hash
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -t hset,hget,hgetall

# List
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -t lpush,rpush,lpop,rpop

# Set
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -t sadd,spop,smembers

# Sorted Set
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -t zadd,zrange,zrank
```

### 6. 长时间压力测试

```bash
# 循环测试（持续运行直到手动停止）
redis-benchmark -h 127.0.0.1 -p 6337 -l -t set,get
```

### 7. 随机 key 测试

```bash
# 使用随机 key 范围（避免缓存热点）
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -r 1000000 -t set,get
```

## 结果解读

### 示例输出

```
====== SET ======
  100000 requests completed in 1.92 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

99.94% <= 1 milliseconds
100.00% <= 2 milliseconds
100.00% <= 3 milliseconds
100.00% <= 4 milliseconds
100.00% <= 5 milliseconds
100.00% <= 6 milliseconds
100.00% <= 7 milliseconds
100.00% <= 8 milliseconds
100.00% <= 10 milliseconds
100.00% <= 11 milliseconds
52083.33 requests per second
```

### 关键指标

- **requests per second (RPS)**: 每秒处理的请求数，越高越好
- **延迟分布**: 百分比延迟（如 99.94% <= 1ms），越低越好
- **失败率**: 失败的请求比例，越低越好

## 性能对比

### BoltDB vs Redis（内存模式）

以下是在相同硬件条件下的性能对比数据（仅供参考，实际性能取决于硬件配置）：

| 命令 | BoltDB (RPS) | Redis (RPS) | 比例 |
|------|-------------|-------------|------|
| PING | ~48,000 | ~100,000 | ~48% |
| SET  | ~31,000 | ~90,000 | ~34% |
| GET  | ~34,000 | ~95,000 | ~36% |
| INCR | ~24,000 | ~85,000 | ~28% |

### 说明

- BoltDB 基于 BadgerDB（LSM-Tree），数据持久化到磁盘
- Redis 将所有数据存储在内存中
- BoltDB 的优势在于可以存储远超内存容量的数据（可达 100TB）
- 对于写操作（SET），BoltDB 的性能接近 Redis 的 35-40%
- 对于读操作（GET），接近 35-40%
- 建议使用 SSD 以获得更好性能

## 测试脚本

BoltDB 提供了便捷的测试脚本：

### test_benchmark.sh

一键运行基准测试：

```bash
# 运行默认测试
./cmd/boltDB/test_benchmark.sh

# 指定服务端端口
./cmd/boltDB/test_benchmark.sh 6380
```

### test_benchmark_detailed.sh

运行详细的基准测试，包含所有数据类型：

```bash
./cmd/boltDB/test_benchmark_detailed.sh
```

### test_benchmark_go.go

Go 语言测试工具（用于调试）：

```bash
go run cmd/boltDB/test_benchmark_go.go
```

## 常见问题

### 1. 连接被拒绝

```
Could not connect to Redis at 127.0.0.1:6337: Connection refused
```

**解决方法**：
```bash
# 确保 BoltDB 服务器正在运行
go run cmd/boltDB/main.go -addr :6337 -dir ./data &

# 检查端口
lsof -i :6337
```

### 2. 连接被关闭

```
Error: Server closed the connection
```

**解决方法**：
- 检查 BoltDB 服务器日志
- 启用 DEBUG 日志级别查看详细错误
- 检查网络连接和防火墙设置

### 3. 性能不理想

如果测试结果远低于预期：

1. **检查硬件**：
   ```bash
   # CPU 使用率
   top -l 1 | grep boltDB
   
   # 磁盘 IO
   iostat -d -c 2
   
   # 内存使用
   vm_stat
   ```

2. **优化配置**：
   - 使用 SSD 而不是 HDD
   - 增加系统文件描述符限制
   - 调整 BadgerDB 配置

3. **检查网络**：
   ```bash
   # 延迟测试
   ping -c 10 127.0.0.1
   
   # 端口检查
   netstat -an | grep 6337
   ```

## 测试结果记录

建议每次测试后记录结果以便对比：

```bash
# 保存结果到文件
redis-benchmark -h 127.0.0.1 -p 6337 -n 100000 -c 50 -t set,get > results_$(date +%Y%m%d_%H%M%S).txt

# 对比多次结果
cat results_*.txt | grep "requests per second"
```

## 更多资源

- Redis Benchmark 官方文档: https://redis.io/docs/management/optimization/benchmarks/
- BoltDB 性能调优: 参考根目录 README.md 的性能章节
