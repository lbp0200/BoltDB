# Redis Cluster 模式支持

BoltDB 实现了完整的 Redis Cluster 核心功能：槽位分片、真实 TCP Gossip 协议、PFAIL/FAIL 故障检测、槽位迁移、MOVED/ASK 重定向。

## 功能状态

| 模块 | 状态 | 说明 |
|------|------|------|
| **Cluster Bus（独立 TCP 连接）** | ✅ | 独立端口 `dataPort+10000`，持久 TCP 连接管理，自动重连 |
| **Gossip 真实 PING/PONG** | ✅ | 通过 Bus TCP 发送真实 PING/PONG，JSON payload 携带元数据 |
| **Gossip Payload** | ✅ | epoch / slot_owners / PFAIL / gossip section 全量交换 |
| **PFAIL → FAIL 晋升** | ✅ | 多节点确认阈值 → FAIL 标记 + 槽位接管 |
| **槽位视图同步** | ✅ | epoch 仲裁，`applyGossipPayload` 冲突解决 |
| **SETSLOT NODE 传播** | ✅ | 跨节点 SETSLOT 变更自动同步 |
| **MOVED 重定向** | ✅ | 槽位不属于本节点时返回 MOVED |
| **MIGRATING / IMPORTING** | ✅ | 槽位迁移状态支持 |
| **ASK 重定向** | ✅ | IMPORTING 槽位临时放行 + ASK |
| **ASKING 命令** | ✅ | 客户端声明 ASKING 后允许访问迁移中的键 |
| **MIGRATE 命令** | ✅ | `MIGRATE host port key db timeout [COPY] [REPLACE]` |
| **CLUSTER MEET** | ✅ | 内部 MEET + 用户 MEET 都触发 `Bus.Connect()` |
| **Cluster 持久化** | ✅ | 状态每 30s 写入 BadgerDB，重启自动恢复 |
| **集成测试** | ✅ | 33 个测试覆盖全链路 |
| **CLUSTER ADDSLOTS / DELSLOTS** | ✅ | 槽位分配管理 |
| **CLUSTER FORGET** | ✅ | 节点移除 |
| **Hash Tag 支持** | ✅ | `{tag}` 语法，同 tag 键映射相同槽位 |

## 使用示例

### 启动集群模式

```bash
go run cmd/boltDB/main.go -cluster -addr=:6337 -dir=/tmp/bolt_node1
```

在第二个终端启动另一个节点并加入集群：

```bash
go run cmd/boltDB/main.go -cluster -addr=:6338 -dir=/tmp/bolt_node2
redis-cli -p 6338 CLUSTER MEET 127.0.0.1 6337
```

### 分配槽位

```bash
# 在节点1上分配槽位 0-8191
redis-cli -p 6337 CLUSTER ADDSLOTS 0 1 2 ... 8191

# 在节点2上分配槽位 8192-16383
redis-cli -p 6338 CLUSTER ADDSLOTS 8192 8193 ... 16383

# 查看集群状态
redis-cli -p 6337 CLUSTER NODES
redis-cli -p 6337 CLUSTER SLOTS
redis-cli -p 6337 CLUSTER INFO
```

### 常用命令

```bash
# 集群信息
CLUSTER INFO
CLUSTER NODES
CLUSTER SLOTS

# 槽位操作
CLUSTER KEYSLOT mykey               # 计算键的槽位
CLUSTER ADDSLOTS 0 1 2              # 分配槽位
CLUSTER DELSLOTS 0 1 2              # 删除槽位

# 节点管理
CLUSTER MEET 127.0.0.1 6380         # 添加节点
CLUSTER FORGET <node-id>            # 移除节点
CLUSTER MYID                        # 当前节点 ID

# 槽位迁移
CLUSTER SETSLOT 10000 MIGRATING <target-node-id>
CLUSTER SETSLOT 10000 IMPORTING <source-node-id>
CLUSTER SETSLOT 10000 NODE <target-node-id>

# 数据迁移
MIGRATE 127.0.0.1 6380 key 0 5000 [COPY] [REPLACE]
```

## 架构设计

### 核心组件

1. **Node** (`node.go`): 集群节点
   - 40 字符十六进制节点 ID
   - 地址和端口
   - 所属槽位范围
   - 角色（master/slave）、标志（myself/fail/pfail）
   - 线程安全（`sync.RWMutex`）

2. **Cluster** (`cluster.go`): 集群管理器
   - 节点表管理
   - 槽位分配（16384 槽 → Node 映射）
   - 配置纪元（epoch）仲裁
   - PFAIL 报告跟踪
   - 持久化（SaveConfig / LoadConfig）

3. **ClusterBus** (`bus.go`): 集群总线
   - 独立 TCP 监听（`dataPort + 10000`）
   - 持久连接管理（自动重连：1s → 2s → 4s → 8s 指数退避）
   - 线程安全读写 + graceful shutdown
   - PING / PONG 消息收发

4. **Gossiper** (`gossip.go`): Gossip 协议
   - 周期性 SendPING（默认 1s）
   - PFAIL 超时检测（默认 5s 无响应 → PFAIL）
   - 过期节点清理（30s 无响应 → 移除）
   - Context 生命周期继承

5. **Slot** (`slot.go`): 槽位计算
   - CRC-16/XModem 算法
   - Hash tag 支持 `{tag}`

### 槽位分配

- 共 16384 个槽位（0-16383）
- 未分配的槽位默认由本节点持有
- 分配持久化到 BadgerDB（key: `cluster:config`），重启自动恢复
- 配置变更通过 gossip 传播到所有节点

### 重定向处理

当客户端访问不属于当前节点的键时：

```
# 永久重定向（槽位属于其他节点）
MOVED 1234 127.0.0.1:6338

# 临时重定向（槽位正在迁出）
ASK 1234 127.0.0.1:6338
```

客户端需根据 `MOVED`/`ASK` 连接到正确节点。`ASK` 需要先发送 `ASKING` 命令再执行操作。

### 故障检测

1. 每个节点周期 ping 所有已知节点
2. 5 秒无 pong 响应 → 标记 PFAIL（主观下线）
3. PFAIL 通过 gossip 传播
4. 收到足够多节点确认 PFAIL → 晋升 FAIL（客观下线）
5. FAIL 标记持久化，重启后不丢失
6. FAIL 节点恢复后自动清除标记

### 槽位迁移流程

```
源节点                         目标节点
  │                               │
  ├── SETSLOT <slot> MIGRATING ──►│
  │◄── SETSLOT <slot> IMPORTING ──┤
  │                               │
  ├── MIGRATE key... ────────────►│  (逐个键迁移)
  │                               │
  ├── SETSLOT <slot> NODE ──────►│  (完成迁移)
  │                               │
  │        (Gossip 传播)          │
  │◄──────────────────────────────┤
```

## 技术说明

- **Bus 端口**: 数据端口 + 10000（如 `:16337` 对应数据端口 `:6337`）
- **Gossip 周期**: 默认 1 秒
- **PFAIL 超时**: 5 秒无响应
- **节点过期**: 30 秒无响应
- **持久化间隔**: 状态变更后立即写入（非周期性）
- **连接重试**: 指数退避 1s → 2s → 4s → 8s（上限 8s）
