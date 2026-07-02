# BoltDB

<p align="center">
  <img src="https://img.shields.io/github/v/release/lbp0200/BoltDB?color=blue" alt="Version">
  <img src="https://img.shields.io/github/license/lbp0200/BoltDB" alt="License">
  <img src="https://img.shields.io/github/actions/workflow/status/lbp0200/BoltDB/go.yml?branch=main" alt="Build">
  <img src="https://goreportcard.com/badge/github.com/lbp0200/BoltDB" alt="Go Report">
</p>

<p align="center">
  <strong>English</strong> | <a href="README_CN.md">中文</a>
</p>

---

## 简介

BoltDB 是一个**高性能、磁盘持久化的键值数据库**，完全兼容 Redis 协议。基于 **BadgerDB** 构建，克服了 Redis 的内存限制，支持在磁盘上存储 **数十 GB 级** 的数据，同时保持完整的 Redis 协议兼容性。

> 💡 **内存版 Redis 只能存储 64GB？BoltDB 可以处理数十 GB 级数据！**
>
> 在纯 HDD 上，BoltDB 的 GET 性能接近内存版 Redis 的 50%，而 SET 性能更高（因为 Badger 的顺序写入占优势）。
>
> ⚠️ **当前验证规模**：已在数十 GB 级验证通过。更大规模（TB+）的验证正在进行中。详见[规模化验证计划](docs/plans/TODO.md#-100tb-宣称与零规模化验证之间的鸿沟)。

---

## 为什么选择 BoltDB？

| 场景 | Redis (内存) | BoltDB (磁盘) |
|----------|---------------|---------------|
| 存储容量 | 受限于 RAM（约 64GB） | 已验证数十 GB 级，架构设计支持水平扩展（磁盘限制） |
| 成本 | 高（内存昂贵） | 低（HDD/SSD 价格实惠） |
| 持久化 | RDB/AOF 快照 | 持续写入 |
| 延迟 | < 1ms | < 5ms（推荐 SSD） |
| 吞吐量 | 约 100K ops/sec | 约 80K ops/sec |

---

## 特性

### 支持的数据类型

| 类型 | 命令 | 说明 |
|------|----------|------|
| **String** | `SET`, `GET`, `INCR`, `APPEND`, `STRLEN` | 字符串操作 |
| **List** | `LPUSH`, `RPOP`, `LRANGE`, `LINDEX`, `LTRIM` | 双向链表 |
| **Hash** | `HSET`, `HGET`, `HGETALL`, `HINCRBY`, `HDEL` | 哈希表 |
| **Set** | `SADD`, `SMEMBERS`, `SINTER`, `SDIFF`, `SPOP` | 无序集合 |
| **Sorted Set** | `ZADD`, `ZRANGE`, `ZSCORE`, `ZINCRBY`, `ZREVRANGE` | 有序集合 |
| **JSON** | `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE` | JSON 文档 |
| **TimeSeries** | `TS.ADD`, `TS.RANGE`, `TS.GET`, `TS.INFO` | 时序数据 |
| **Geo** | `GEOADD`, `GEOPOS`, `GEOHASH`, `GEODIST`, `GEOSEARCH` | 地理位置 |
| **Stream** | `XADD`, `XLEN`, `XREAD`, `XRANGE`, `XINFO` | 流数据 |

### 核心功能

- ✅ **完整 Redis 协议** - 兼容 `redis-cli` 和所有 Redis 客户端
- ✅ **磁盘持久化** - 无内存限制，数据重启后保留
- ✅ **高可用** - 支持 Sentinel 自动故障转移
- ✅ **集群支持** - Redis Cluster 协议，16384 个槽位
- ✅ **事务** - 支持 MULTI/EXEC
- ✅ **TTL 过期** - 键过期时间支持
- ✅ **在线备份** - 支持热备份

---

## 快速开始

### 从 GitHub Release 安装

#### Linux (amd64)

```bash
# 下载
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-linux-amd64 -o boltDB
chmod +x boltDB

# 启动服务器
./boltDB --dir=./data --addr=:6337
```

#### Linux (arm64)

```bash
# 下载
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-linux-arm64 -o boltDB
chmod +x boltDB

# 启动服务器
./boltDB --dir=./data --addr=:637 macOS (Intel)

9
```

####```bash
# 下载
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-darwin-amd64 -o boltDB
chmod +x boltDB

# 启动服务器
./boltDB --dir=./data --addr=:6337
```

#### macOS (Apple Silicon)

```bash
# 下载
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-darwin-arm64 -o boltDB
chmod +x boltDB

# 启动服务器
./boltDB --dir=./data --addr=:6337
```

#### Windows

```powershell
# 从 https://github.com/lbp0200/BoltDB/releases 下载
# 解压并运行：
.\boltDB.exe --dir=.\data --addr=:6337
```

### 通过 Homebrew 安装 (macOS)

```bash
# 添加自定义 tap
brew tap lbp0200/boltdb

# 安装 BoltDB
brew install boltdb
```

#### 使用方法

```bash
# 启动 BoltDB（使用平台默认数据目录）
boltdb

# 或自定义参数
boltdb --addr=:6337 --dir=/path/to/data --log-level info
```

#### 数据目录

BoltDB 使用平台默认数据目录：

- **macOS**: `~/Library/Application Support/boltdb`
- **Linux**: `/var/lib/boltdb`

#### 作为服务运行

```bash
# 作为后台服务启动 (macOS)
brew services start lbp0200/boltdb/boltdb

# 查看服务状态
brew services list

# 停止服务
brew services stop lbp0200/boltdb/boltdb

# Linux (使用 systemd)
sudo systemctl start boltdb
sudo systemctl stop boltdb
```

#### 升级

```bash
# 更新 formula
brew update

# 升级 BoltDB
brew upgrade boltdb
```

#### 卸载

```bash
# 移除 BoltDB
brew uninstall boltdb

# 移除 tap
brew untap lbp0200/boltdb
```

### 从源码编译

```bash
# 克隆仓库
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# 编译
go build -o ./build/boltDB cmd/boltDB/main.go

# 运行
./build/boltDB --dir=./data --addr=:6337
```

### 使用 redis-cli

```bash
# 连接
redis-cli -p 6337

# 字符串操作
SET mykey "Hello from disk!"
GET mykey
INCR counter
DEL mykey

# 列表操作
LPUSH tasks "task1"
RPUSH tasks "task2"
LRANGE tasks 0 -1

# 哈希操作
HSET user:1 name "Alice" age 25
HGET user:1 name
HGETALL user:1

# 集合操作
SADD tags "go" "redis" "database"
SMEMBERS tags
SINTER tags "go"

# 有序集合操作
ZADD leaderboard 100 "Alice" 90 "Bob" 80 "Charlie"
ZRANGE leaderboard 0 -1 WITHSCORES
```

---

## Docker 部署

```bash
# 运行服务器
docker run -d \
  -p 6337:6337 \
  -v /path/to/data:/data \
  --name boltdb \
  lbp0200/boltdb:latest

# 或使用 docker-compose
cat > docker-compose.yml << EOF
version: '3.8'
services:
  boltDB:
    image: lbp0200/boltdb:latest
    ports:
      - "6337:6337"
    volumes:
      - ./data:/data
    command: --dir=/data --addr=:6337
EOF

docker-compose up -d
```

---

## 部署模式

### 单机模式

基本的单机部署。

```bash
# 启动 BoltDB 服务器
./boltDB --dir=/tmp/bolt_data --addr=:6337

# 使用 redis-cli 连接
redis-cli -p 6337 PING
# PONG

redis-cli -p 6337 SET mykey "Hello BoltDB!"
# OK

redis-cli -p 6337 GET mykey
# Hello BoltDB!
```

### 主从模式

BoltDB 支持复制，可以设置主从拓扑。

#### 选项 1: BoltDB 主节点 + BoltDB 从节点

```bash
# 终端 1: 启动主节点 (端口 6379)
./boltDB --dir=/tmp/bolt_master --addr=:6337

# 终端 2: 启动从节点 (端口 6380，从主节点复制)
./boltDB --dir=/tmp/bolt_slave --addr=:6380 --replicaof 127.0.0.1 6337

# 测试复制
redis-cli -p 6337 SET key "value"
redis-cli -p 6380 GET key  # 返回 "value"
```

#### 选项 2: BoltDB 主节点 + Redis 从节点

使用 Redis 作为从节点，从 BoltDB 主节点复制。

```bash
# 终端 1: 启动 BoltDB 主节点 (端口 6380)
./boltDB --dir=/tmp/bolt_master --addr=:6380

# 终端 2: 启动 Redis 从节点 (端口 6379)
redis-server --port 6379 --dir /tmp/redis_data
redis-cli -p 6379 SLAVEOF 127.0.0.1 6380

# 测试: 写入 BoltDB，从 Redis 读取
redis-cli -p 6380 SET test "hello"
redis-cli -p 6379 GET test  # 返回 "hello"
```

> ⚠️ **注意**: BoltDB 可以使用 `REPLICAOF` 命令作为从节点。

### 哨兵模式

使用 **redis-sentinel** 监控 BoltDB 实例实现高可用。

#### 配置 redis-sentinel

```bash
# 创建哨兵配置
cat > sentinel.conf << EOF
port 26379
sentinel monitor mymaster 127.0.0.1 6337 2
sentinel down-after-milliseconds mymaster 30000
sentinel failover-timeout mymaster 180000
EOF

# 启动 redis-sentinel
redis-server sentinel.conf --sentinel

# 连接哨兵
redis-cli -p 26379

# 查看主节点状态
SENTINEL MASTER mymaster
```

#### 启动 BoltDB 实例

```bash
# 终端 1: 启动主节点
./boltDB --dir=/tmp/bolt_master --addr=:6337

# 终端 2: 启动从节点
./boltDB --dir=/tmp/bolt_slave --addr=:6380 --replicaof 127.0.0.1 6337
```

#### 哨兵命令

```bash
# 查看主节点
redis-cli -p 26379 SENTINEL MASTER mymaster

# 查看从节点
redis-cli -p 26379 SENTINEL SLAVES mymaster

# 获取主节点地址（客户端连接用）
redis-cli -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
```

### 集群模式

BoltDB 支持 Redis Cluster 协议，16384 个槽位。

#### 单节点集群（所有槽位）

```bash
# 启动集群模式（拥有所有槽位）
./boltDB --cluster --dir=/tmp/bolt_cluster --addr=:6337

# 查看集群状态
redis-cli -p 6337 CLUSTER INFO
redis-cli -p 6337 CLUSTER NODES
redis-cli -p 6337 CLUSTER KEYSLOT mykey
```

#### 多节点集群

```bash
# 终端 1: 节点 1 (槽位 0-8191)
./boltDB --cluster --dir=/tmp/node1 --addr=:6337
redis-cli -p 6337 CLUSTER ADDSLOTS {0..8191}

# 终端 2: 节点 2 (槽位 8192-16383)
./boltDB --cluster --dir=/tmp/node2 --addr=:6380
redis-cli -p 6380 CLUSTER ADDSLOTS {8192..16383}

# 终端 3: 连接节点
redis-cli -p 6380 CLUSTER MEET 127.0.0.1 6337

# 验证
redis-cli -p 6337 CLUSTER NODES
```

#### Hash Tag

使用 hash tag 将相关键保持在同一节点上：

```bash
# 具有相同 hash tag 的键位于同一槽位
redis-cli -p 6337 SET "{user:1}:name" "Alice"
redis-cli -p 6337 SET "{user:1}:age" "25"
redis-cli -p 6337 GET "{user:1}:name"
```

---

## 配置

### 命令行参数

| 参数 | 默认值 | 说明 |
|-----------|---------|-------------|
| `--dir` | `./data` | 数据目录 |
| `--addr` | `:6337` | 监听地址 |
| `--log-level` | `warning` | 日志级别 (debug/info/warning/error) |
| `--cluster` | `false` | 启用集群模式 |
| `--replicaof` | - | 主节点地址（从节点模式） |

### 环境变量

| 变量 | 说明 |
|----------|-------------|
| `BOLTDB_DIR` | 数据目录 |
| `BOLTDB_ADDR` | 监听地址 |
| `BOLTDB_LOG_LEVEL` | 日志级别 |
| `BOLTDB_PASSWORD` | AUTH 密码（客户端需通过 AUTH 命令认证） |

---

## 高可用部署

### 架构

```
                    ┌─────────────┐
                    │  Application │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   Sentinel  │
                    │  (Monitor)  │
                    └──────┬──────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
  │   Master    │  │   Slave 1   │  │   Slave 2   │
  │  (Primary)  │◄─┤  (Replica)  │◄─┤  (Replica)  │
  └─────────────┘  └─────────────┘  └─────────────┘
```

### Sentinel 配置

BoltDB 可以被 redis-sentinel 监控实现自动故障转移：

```bash
# 创建哨兵配置
cat > sentinel.conf << EOF
port 26379
sentinel monitor mymaster 127.0.0.1 6337 2
sentinel down-after-milliseconds mymaster 30000
sentinel failover-timeout mymaster 180000
EOF

# 启动 Sentinel（需要 redis-sentinel）
redis-server sentinel.conf --sentinel
```

### Redis-Sentinel 兼容性

BoltDB 可以被外部 Redis Sentinel 监控：

| 命令 | 状态 | 说明 |
|---------|--------|-------|
| `PING` | ✅ | 返回 PONG |
| `ROLE` | ✅ | 返回 master/slave 角色 |
| `INFO replication` | ✅ | 返回完整复制状态 |
| `REPLCONF GETACK` | ✅ | 返回 ACK 偏移量 |
| `SENTINEL MASTER` | ✅ | 返回主节点状态 |
| `故障检测` | ✅ | 约 30 秒检测到主节点故障 |
| `ODOWN 标记` | ✅ | 标记为主节点 s_down, o_down |

---

## Redis 互操作性

### 复制测试结果

| 场景 | 状态 | 说明 |
|----------|--------|-------|
| **BoltDB → Redis** | ✅ | 数据同步正常 (SET, INCR, LPUSH, ZADD, HSET) |
| **Redis → BoltDB** | ✅ | 使用 REPLICAOF 命令 |
| **角色切换** | ✅ | REPLICAOF NO ONE / REPLICAOF 即时生效 |
| **数据隔离** | ✅ | 两实例保持独立数据 |
| **故障恢复** | ✅ | REPLICAOF 切换即时生效 |

**测试命令:**
```bash
# 启动 BoltDB 作为主节点 (端口 6380)
./boltDB --dir=./data --addr=:6380

# 启动 Redis 作为从节点 (端口 6379)
redis-server --port 6379 --dir=/tmp/redis_data
redis-cli -p 6379 SLAVEOF 127.0.0.1 6380

# 写入 BoltDB，从 Redis 读取
redis-cli -p 6380 SET "test" "hello"
redis-cli -p 6337 GET "test"  # 返回 "hello"
```

---

## 性能

### 基准测试

```bash
# 使用 redis-benchmark (50 并发客户端, 10000 请求)
redis-benchmark -h localhost -p 6379 -t PING,SET,GET,INCR,LPUSH -c 50 -n 10000
```

#### 实际测试结果

| 命令 | 吞吐量 (ops/sec) | 平均延迟 | P99 延迟 |
|---------|---------------------|-------------|-------------|
| **PING** | ~48,000 | 0.24 ms | - |
| **GET** | ~34,000 | 0.77 ms | 1.56 ms |
| **SET** | ~31,000 | 0.90 ms | 1.63 ms |
| **INCR** | ~24,000 | 2.04 ms | 3.05 ms |
| **LPUSH** | ~15,000 | 3.38 ms | 5.46 ms |

> 💡 **注意**: 性能在 macOS SSD 上测试。结果可能因硬件而异。

### 存储限制

| 指标 | 限制 |
|--------|-------|
| 最大键数 | 约 10^12 (实际) |
| 最大值大小 | 1GB |
| 最大字符串大小 | 512MB |
| 最大列表大小 | 2^32-1 元素 |
| 最大集合大小 | 2^32-1 成员 |
| 最大哈希大小 | 2^32-1 字段 |
| 最大有序集合大小 | 2^32-1 成员 |

---

## 架构

```
┌─────────────────────────────────────────────────────┐
│                      BoltDB                          │
├─────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────────────┐   │
│  │  Redis 协议     │  │    集群管理器           │   │
│  │    处理器       │  │   (16384 槽位)          │   │
│  └────────┬────────┘  └───────────┬─────────────┘   │
│           │                        │                  │
│  ┌────────┴───────────────────────┴────────────┐   │
│  │         命令路由与复制                         │   │
│  └───────────────────┬───────────────────────────┘   │
│                      │                               │
│  ┌───────────────────▼───────────────────────────┐   │
│  │           BadgerDB 存储引擎                      │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │   │
│  │  │   WAL   │  │ LSM Tree │  │ Value Log   │  │   │
│  │  └─────────┘  └─────────┘  └─────────────┘  │   │
│  └───────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### 技术栈

- **存储引擎**: [BadgerDB v4](https://github.com/dgraph-io/badger) - 基于 LSM 树的键值存储
- **协议**: Redis RESP2/RESP3 兼容
- **集群**: Redis Cluster 协议 (CRC16 哈希, 16384 槽位)
- **复制**: Redis 复制 (PSYNC, backlog)
- **日志**: [zerolog](https://github.com/rs/zerolog)
- **语言**: Go 1.25+

---

## 平台支持

| 操作系统 | 架构 | 状态 |
|----|--------------|--------|
| Linux | amd64 | ✅ 支持 |
| Linux | arm64 | ✅ 支持 |
| macOS | amd64 | ✅ 支持 |
| macOS | arm64 (Apple Silicon) | ✅ 支持 |
| Windows | amd64 | ✅ 支持 |

---

## 贡献

欢迎提交 Issues 和 Pull Requests！

```bash
# 1. Fork 这个仓库
# 2. 创建功能分支
git checkout -b feature/amazing-feature

# 3. 提交你的更改
git commit -m 'Add some amazing feature'

# 4. 推送到分支
git push origin feature/amazing-feature

# 5. 创建 Pull Request
```

---

## 许可证

MIT 许可证 - 详见 [LICENSE](LICENSE)。

---

## 联系方式

- **GitHub**: [https://github.com/lbp0200/BoltDB](https://github.com/lbp0200/BoltDB)
- **Issues**: [https://github.com/lbp0200/BoltDB/issues](https://github.com/lbp0200/BoltDB/issues)

---

<p align="center">
  <strong>由 lbp0200 ❤️ 制作</strong>
</p>
