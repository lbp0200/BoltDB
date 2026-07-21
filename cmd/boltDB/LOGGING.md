# 日志系统使用指南

BoltDB 服务器使用 **zerolog** 作为日志库，这是一个高性能、零分配的 JSON 日志库。

## 为什么选择 zerolog？

- ✅ **高性能**: 零分配设计，性能优异
- ✅ **结构化日志**: 支持 JSON 格式，便于日志分析
- ✅ **彩色控制台输出**: 开发时友好的控制台格式
- ✅ **丰富的字段支持**: 可以添加任意字段到日志中
- ✅ **社区活跃**: 广泛使用，维护良好

## 日志级别

日志系统支持以下级别（从低到高）：

- **TRACE**: 最详细的跟踪信息
- **DEBUG**: 详细的调试信息，包括每个命令的执行、连接的建立和关闭、RESP 协议的读写等
- **INFO**: 一般信息，如服务器启动、配置信息等
- **WARN**: 警告信息，如连接错误、命令执行异常等
- **ERROR**: 错误信息，如内部错误、致命问题等
- **FATAL**: 致命错误，程序会退出
- **PANIC**: 恐慌错误，程序会 panic

## 默认行为

- **默认日志级别**: `WARN` (WARNING)
- DEBUG 级别的日志默认**关闭**，以减少日志输出
- 日志输出到**控制台**，带颜色和格式化

## 使用方法

### 方法 1: 命令行参数

```bash
# 使用 DEBUG 级别（最详细）
go run ./cmd/boltDB/ -addr :6337 -dir ./data -log-level DEBUG

# 使用 INFO 级别
go run ./cmd/boltDB/ -addr :6337 -dir ./data -log-level INFO

# 使用 WARN 级别（默认）
go run ./cmd/boltDB/ -addr :6337 -dir ./data -log-level WARN

# 使用 ERROR 级别（最少日志）
go run ./cmd/boltDB/ -addr :6337 -dir ./data -log-level ERROR
```

### 方法 2: 环境变量

```bash
# 设置环境变量
export BOLTDB_LOG_LEVEL=DEBUG

# 启动服务器（会自动读取环境变量）
go run ./cmd/boltDB/ -addr :6337 -dir ./data
```

### 优先级

命令行参数 `-log-level` 的优先级高于环境变量 `BOLTDB_LOG_LEVEL`。

## 日志格式

### 控制台输出（开发环境）

启用 DEBUG 级别时，日志输出如下：

```
10:30:45.123 DBG 新连接建立 remote_addr=127.0.0.1:54321
10:30:45.124 DBG 执行命令 remote_addr=127.0.0.1:54321 command=PING arg_count=0
10:30:45.125 DBG 发送响应 remote_addr=127.0.0.1:54321 command=PING response_type=SimpleString
10:30:45.126 WRN 写入响应失败 remote_addr=127.0.0.1:54321 error="connection closed"
```

### JSON 输出（生产环境）

如果需要 JSON 格式（例如输出到文件或日志收集系统），可以修改 `internal/logger/logger.go`：

```go
// 将 ConsoleWriter 改为 os.Stdout（JSON 格式）
Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
```

JSON 格式示例：
```json
{"level":"debug","remote_addr":"127.0.0.1:54321","command":"PING","time":"2024-01-15T10:30:45.124Z","message":"执行命令"}
```

## 结构化日志的优势

zerolog 支持结构化日志，可以方便地添加字段：

```go
// 在代码中使用
logger.Logger.Debug().
    Str("remote_addr", remoteAddr).
    Str("command", cmd).
    Int("arg_count", len(args)-1).
    Msg("执行命令")
```

这样的好处：
- 可以轻松过滤和搜索特定字段
- 可以集成到日志分析系统（如 ELK、Loki）
- 性能更好（零分配）

## 排查 redis-benchmark 问题

当 `redis-benchmark` 出现 "Server closed the connection" 错误时，可以按以下步骤排查：

### 1. 启用 DEBUG 级别日志

```bash
# 启动服务器，启用 DEBUG 日志
go run ./cmd/boltDB/ -addr :6337 -dir ./data -log-level DEBUG
```

### 2. 在另一个终端运行 redis-benchmark

```bash
redis-benchmark -h 127.0.0.1 -p 6337 -t ping -n 1 -c 1
```

### 3. 查看服务器日志

观察服务器端的日志输出，重点关注：

- **连接建立**: `新连接建立 remote_addr=...`
- **命令接收**: `执行命令 remote_addr=... command=... arg_count=...`
- **响应发送**: `发送响应 remote_addr=... command=... response_type=...`
- **错误信息**: `读取请求失败`、`写入响应失败`、`刷新缓冲区失败` 等

### 4. 常见问题排查

#### 问题 1: 连接立即关闭

如果看到：
```
DBG 新连接建立 remote_addr=127.0.0.1:54321
DBG 连接关闭 remote_addr=127.0.0.1:54321
```

可能原因：
- 客户端发送了无效的命令格式
- 服务器无法解析 RESP 协议

#### 问题 2: 命令执行失败

如果看到：
```
DBG 执行命令 remote_addr=... command=CONFIG arg_count=1
ERR 命令执行返回 nil remote_addr=... command=CONFIG
```

可能原因：
- 命令未实现或处理逻辑有误

#### 问题 3: 响应发送失败

如果看到：
```
DBG 发送响应 remote_addr=... command=PING response_type=SimpleString
WRN 写入响应失败 remote_addr=... error="write: broken pipe"
```

可能原因：
- 客户端在服务器发送响应前关闭了连接
- 网络问题

## 日志输出到文件

如果需要将日志保存到文件：

```bash
# 保存日志到文件
go run ./cmd/boltDB/ -addr :6337 -dir ./data -log-level DEBUG > server.log 2>&1

# 同时查看和控制台输出
go run ./cmd/boltDB/ -addr :6337 -dir ./data -log-level DEBUG | tee server.log
```

## 性能考虑

- **DEBUG 级别**: 会产生大量日志，可能影响性能，仅用于调试
- **INFO 级别**: 适度的日志输出，适合生产环境监控
- **WARN/ERROR 级别**: 最少的日志输出，适合生产环境

建议在生产环境中使用 `WARN` 或 `ERROR` 级别。

## 更多信息

- zerolog 官方文档: https://github.com/rs/zerolog
- 结构化日志最佳实践: https://www.structuredlogs.com/
