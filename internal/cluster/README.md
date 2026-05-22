# Redis Cluster 模式支持

BoltDB 实现了 Redis Cluster 模式的核心功能，支持槽位分片、节点管理和集群命令。

## 功能特性

### 1. 槽位管理
- **槽位计算**: 使用 CRC-16/XModem 算法计算键的槽位
- **Hash Tag 支持**: 支持 `{tag}` 语法，确保相同 tag 的键映射到同一槽位
- **槽位分配**: 支持将槽位分配给不同节点

### 2. 节点管理
- **节点发现**: 通过 `CLUSTER MEET` 命令添加新节点
- **节点状态**: 跟踪节点的 master/slave 状态、ping/pong 时间
- **节点移除**: 支持 `CLUSTER FORGET` 移除节点

### 3. 集群命令

#### CLUSTER NODES
返回集群中所有节点的信息，格式与 Redis 兼容。

```bash
CLUSTER NODES
```

#### CLUSTER SLOTS
返回槽位分配信息，显示每个槽位范围对应的节点。

```bash
CLUSTER SLOTS
```

#### CLUSTER INFO
返回集群状态信息，包括槽位分配、节点数量等。

```bash
CLUSTER INFO
```

#### CLUSTER KEYSLOT
计算指定键的槽位。

```bash
CLUSTER KEYSLOT mykey
```

#### CLUSTER MEET
将新节点添加到集群。

```bash
CLUSTER MEET 127.0.0.1 6380
```

#### CLUSTER ADDSLOTS
将槽位分配给当前节点。

```bash
CLUSTER ADDSLOTS 0 1 2 3
```

#### CLUSTER MYID
返回当前节点的 ID。

```bash
CLUSTER MYID
```

#### CLUSTER EPOCH
返回当前配置纪元。

```bash
CLUSTER EPOCH
```

## 使用示例

### 启动集群模式

在 `main.go` 中初始化集群：

```go
import (
    "github.com/lbp0200/BoltDB/internal/cluster"
    "github.com/lbp0200/BoltDB/internal/server"
    "github.com/lbp0200/BoltDB/internal/store"
)

func main() {
    // 创建存储
    db, err := store.NewBoltDBStore("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 创建集群
    cluster, err := cluster.NewCluster(db, "", "127.0.0.1:6379")
    if err != nil {
        log.Fatal(err)
    }

    // 创建处理器
    handler := &server.Handler{
        Db:      db,
        Cluster: cluster,
    }

    // 启动服务器
    ln, err := net.Listen("tcp", ":6379")
    if err != nil {
        log.Fatal(err)
    }
    handler.ServeTCP(ln)
}
```

### 使用 redis-cli 测试

```bash
# 连接服务器
redis-cli -p 6379

# 查看集群信息
CLUSTER INFO

# 查看节点列表
CLUSTER NODES

# 查看槽位分配
CLUSTER SLOTS

# 计算键的槽位
CLUSTER KEYSLOT mykey

# 添加新节点
CLUSTER MEET 127.0.0.1 6380

# 分配槽位
CLUSTER ADDSLOTS 0 1 2 3
```

## 架构设计

### 核心组件

1. **Node**: 表示集群中的一个节点
   - 节点 ID（40 字符十六进制）
   - 地址和端口
   - 槽位范围
   - 节点状态（master/slave）

2. **Cluster**: 集群管理器
   - 节点管理
   - 槽位分配
   - 配置纪元管理

3. **Slot**: 槽位计算
   - CRC-16/XModem 算法
   - Hash tag 支持

### 槽位分配

- 默认情况下，所有槽位（0-16383）分配给当前节点
- 可以通过 `CLUSTER ADDSLOTS` 或 `CLUSTER SETSLOT` 重新分配
- 槽位分配信息存储在内存中，重启后需要重新配置

### 重定向处理

当客户端访问不属于当前节点的槽位时，服务器会返回 `MOVED` 重定向错误：

```
MOVED <slot> <node-address>
```

客户端应该根据重定向信息连接到正确的节点。

## 限制和注意事项

1. **持久化**: 槽位分配已通过 BadgerDB 持久化（`cluster:config` key），重启后自动恢复
2. **迁移**: 槽位迁移功能尚未实现
3. **故障转移**: 自动故障转移功能尚未实现（需选举协议）
4. **复制**: Slave 节点复制功能尚未完全实现
5. **Gossip**: 基础框架已实现（周期性 PING/PFAIL 检测、过期清理），但未通过 TCP 发送真实消息

## 已实现

- [x] 槽位分配持久化（SaveConfig/LoadConfig，slot/node 变更自动保存）
- [x] 节点间 Gossip 基础框架（`internal/cluster/gossip.go`）

## 未来计划

- [ ] 槽位迁移（MIGRATE）
- [ ] 自动故障转移
- [ ] 完整的 Slave 复制
- [ ] CLUSTER MEET 真实 TCP 握手

