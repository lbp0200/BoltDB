# BoltDB 服务器运行指南

## 快速开始

### 方式1：直接运行（开发模式）

```bash
# 使用默认配置（监听 :6337，数据库存储在临时目录）
go run ./cmd/boltDB/

# 指定监听地址
go run ./cmd/boltDB/ -addr :6380

# 指定数据库目录
go run ./cmd/boltDB/ -dir /path/to/data

# 同时指定地址和目录
go run ./cmd/boltDB/ -addr :6380 -dir /path/to/data
```

### 方式2：构建后运行（生产模式）

```bash
# 构建可执行文件
go build -o ./build/boltDB ./cmd/boltDB/

# 运行
./build/boltDB

# 指定参数运行
./build/boltDB -dir /path/to/data -addr :6380
```

### 方式3：安装到系统路径

```bash
# 安装到 $GOPATH/bin 或 $GOBIN
go install github.com/lbp0200/BoltDB/cmd/boltDB

# 然后直接运行（如果 $GOPATH/bin 在 PATH 中）
boltDB -addr :6380 -dir /path/to/data
```

## 命令行参数

- `-addr`: 监听地址（默认: `:6337`）
  - 示例: `-addr :6337` 或 `-addr 0.0.0.0:6380`

- `-dir`: BadgerDB 数据存储目录（默认: 系统临时目录）
  - 示例: `-dir /var/lib/boltDB` 或 `-dir ./data`

- `-log-level`: 日志级别（默认: WARNING）
  - 示例: `-log-level info` 或 `-log-level debug`

- `-cluster`: 启用集群模式（默认: false）

- `-replicaof`: 主从复制，格式 `host:port`
  - 示例: `-replicaof 127.0.0.1:6337`

- `-skip-startup-cleanup`: 跳过启动时数据完整性检查

- `-client-output-buffer-limit`: 客户端输出缓冲区硬限制（字节，0 = 无限制）

- `-metrics-addr`: Prometheus 指标监听地址（默认: 空 = 禁用）
  - 示例: `-metrics-addr :6338`

## 使用示例

### 1. 启动服务器（默认配置）

```bash
go run ./cmd/boltDB/
```

输出：
```
BoltDB 服务器启动，监听地址: :6337
```

### 2. 使用自定义端口和目录

```bash
go run ./cmd/boltDB/ -addr :6380 -dir ./boltDB-data
```

### 3. 使用 redis-cli 连接测试

在另一个终端：

```bash
# 连接到默认端口
redis-cli -p 6337

# 或连接到自定义端口
redis-cli -p 6380

# 测试命令
127.0.0.1:6337> PING
PONG
127.0.0.1:6337> SET key1 value1
OK
127.0.0.1:6337> GET key1
"value1"
127.0.0.1:6337> HSET user:1 name Alice age 30
(integer) 2
127.0.0.1:6337> HGETALL user:1
1) "name"
2) "Alice"
3) "age"
4) "30"
```

### 4. 后台运行（Linux/macOS）

```bash
# 使用 nohup
nohup ./build/boltDB -dir /var/lib/boltDB > boltDB.log 2>&1 &

# 或使用 systemd（创建服务文件）
```

### 5. 使用 systemd 管理（Linux）

创建 `/etc/systemd/system/boltDB.service`:

```ini
[Unit]
Description=BoltDB Redis-compatible Database Server
After=network.target

[Service]
Type=simple
User=bolt
ExecStart=/usr/local/bin/boltDB -addr :6337 -dir /var/lib/boltDB -log-level info
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

然后：

```bash
sudo systemctl daemon-reload
sudo systemctl enable boltDB
sudo systemctl start boltDB
sudo systemctl status boltDB
```

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `BOLTDB_DIR` | 数据目录路径 | 临时目录 |
| `BOLTDB_ADDR` | 监听地址 | `:6337` |
| `BOLTDB_LOG_LEVEL` | 日志级别 | `warning` |
| `BOLTDB_LOG_FILE` | 日志文件路径（为空则输出到控制台） | 空 |
| `BOLTDB_PASSWORD` | AUTH 密码（设置后客户端需 AUTH 认证） | 空（无需认证） |

## 故障排查

### 1. 端口被占用

```bash
# 检查端口占用
lsof -i :6337
# 或
netstat -an | grep 6337

# 使用其他端口
go run ./cmd/boltDB/ -addr :6380
```

### 2. 权限问题

```bash
# 确保有写入数据库目录的权限
mkdir -p /var/lib/boltDB
chmod 755 /var/lib/boltDB
```

### 3. 依赖问题

```bash
# 确保所有依赖已安装
go mod download
go mod tidy
```

## 性能优化

### 1. 使用持久化目录

避免使用临时目录，使用持久化存储：

```bash
./build/boltDB -dir /var/lib/boltDB
```

### 2. 生产环境建议

- 使用 SSD 以获得更好的随机读性能
- 使用固定端口（如 6337）
- 使用持久化数据目录
- 配置日志记录
- 使用进程管理器（systemd, supervisor等）
- 配置防火墙规则

## 停止服务器

- 前台运行：按 `Ctrl+C`
- 后台运行：使用 `kill` 命令或 systemd 管理

```bash
# 查找进程
ps aux | grep boltDB

# 停止进程
kill <PID>

# 或使用 systemd
sudo systemctl stop boltDB
```

## 更多文档

| 文档 | 说明 |
|------|------|
| [监控指南](../../docs/monitoring.md) | Prometheus 指标配置、指标全集、Grafana 面板建议 |
| [RESP Shape Contract](../../docs/resp-shape-contract.md) | 响应结构契约测试（24 个形状校验） |
| [RDB Length Encoding](../../docs/rdb-length-encoding.md) | 6/14/32-bit 长度编码边界测试 |
| [写背压](../../docs/backpressure.md) | L0 写节流机制、阈值说明 |
| [包结构概览](../../docs/package-overview.md) | 入口点、内部包职责、存储前缀、部署 |
| [复制架构](../../docs/replication/architecture.md) | PSYNC、backlog、FULLRESYNC、RDB 快照、关闭生命周期 |
| [测试分 tiers](../../docs/plans/test-tiers.md) | Tier A/B/C 测试策略与命令 |
| [设计约束](../../docs/design-constraints.md) | 架构决策、关闭顺序、退化门禁 |
| [部署文档](../../docs/deployment/) | Docker、systemd、Ubuntu、CentOS、Homebrew |
| [故障模式](../../docs/failures/) | L0 坍缩、backlog 耗尽、写超时风暴等已知故障分析 |
