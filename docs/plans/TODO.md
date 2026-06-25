# BoltDB 待办列表

## 当前状态

| 指标 | 值 |
|------|----|
| RESP3 Null 覆盖 | 34/34 命令（100%） |
| redis-py compat | 153/153 (100%) |
| node-redis compat | 110/110 (100%) |
| redis-cli compat | 77/77 (100%) |
| timer 泄漏 | 8/8 已修复 |
| isWriteCommand | 94/94 完整 |
| goroutine leak test | 通过 |

---

## P3：架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL / FUNCTION / MIGRATE | P2，按需补充 |

---

## P4：CI / 发布

| 项 | 状态 |
|----|------|
| v8.29.0 发布 | ✅ 已推（tag `v8.29.0`） |
| brew formula 安装路径 | ✅ 改用 bash `${HOME}` 避免安装隔离 |
| github actions CI 通过 | 🟡 待 Actions 运行验证 |
| 测试分层（Tier A/B/C） | ✅ 已实施 |

## P5：测试容量治理

| 项 | 状态 |
|----|------|
| 定位 FAIL 持久化 `dirty=true` 缺失 | ✅ `internal/cluster/bus.go` — FAIL 晋升时未设 dirty，导致重启后 FAIL 状态丢失 |
| bus.go 单元测试 | ✅ `internal/cluster/bus_test.go` — 14 测试覆盖 address/RESP/start-stop/payload/pfail |
| 测试三层架构设计 | ✅ `docs/plans/test-tiers.md` |
| Tier A 脚本 | ✅ `scripts/test-tier-a.sh` — lint + unit + fast integration |
| Tier B 脚本 | ✅ `scripts/test-tier-b.sh` — 多节点 cluster / goroutine leak / replication / sentinel / regression |
| Tier C 脚本 | ✅ `scripts/test-tier-c.sh` — soak / heavy regression / fuzz stress |
| CI 工作流拆分 | ✅ `.github/workflows/go.yml` — test-fast (PR gate) + test-heavy (post-merge) |
| 夜间工作流更新 | ✅ `.github/workflows/nightly-soak.yml` — 增加 cluster soak |

---

## Cluster Bus + 真实 Gossip 已完成

| 项 | 状态 | 说明 |
|----|------|------|
| ClusterBus（dataPort+10000 独立监听、持久 TCP 连接管理） | ✅ | `internal/cluster/bus.go` |
| MEET 建立总线连接 | ✅ | 内部 MEET + 用户 MEET 都调 `Bus.Connect()` |
| Gossip 真实 PING/PONG（代替本地时间戳） | ✅ | `SendPING()` 通过 bus TCP 发送；fallback 本地模式保留 |
| 多节点集成测试（`TestClusterMultiNode`） | ✅ | 启动 2 节点→MEET→验证 CLUSTER NODES + bus peers + PongRecv |
| Shutdown 集成（`main.go`） | ✅ | Gossip.Stop() → Bus.Stop() → cancel() → ... |
| `formatNodeLine` 竞态修复 | ✅ | 读 Node 字段时加 `node.mu.RLock()` |

**已知限制（后续可做）：**
- 尚未携带 gossip payload（节点/槽位视图交换）
- PFAIL/FAIL 仍基于本地 timer（可通过 gossip 传播升级）
- 无集群范围槽位同步

## Cluster 全功能完成

| 模块 | 状态 | 说明 |
|------|------|------|
| Cluster Bus (TCP 持久连接) | ✅ | `internal/cluster/bus.go` — 独立端口 data+10000 |
| Gossip 真实 PING/PONG | ✅ | TCP 传输，JSON payload |
| Gossip Payload | ✅ | epoch / slot_owners / PFAIL / gossip section |
| PFAIL 传播 + FAIL 晋升 | ✅ | 多节点确认 → 阈值 → FAIL + 槽位接管 |
| 槽位视图同步 | ✅ | epoch 裁决，`applyGossipPayload` 冲突解决 |
| SETSLOT NODE 传播 | ✅ | 跨节点同步 |
| MOVED 重定向 | ✅ | 跨节点 key 引导 |
| MIGRATING/IMPORTING + ASK | ✅ | slot 迁移路由 |
| ASKING 命令 | ✅ | IMPORTING slot 临时放行 |
| MIGRATE 命令 | ✅ | `MIGRATE host port key db timeout [COPY] [REPLACE]` |
| Cluster 持久化 | ✅ | gossip 学到的状态每 30s 写 Badger |
| 集成测试 (33) | ✅ | 覆盖全链路 |
| 修复: `formatNodeLine` data race | ✅ | |
| 修复: `ReadRESP` null bulk string | ✅ | `$-1` 支持 |
| 修复: `ReadRESP` Error 类型 `-` | ✅ | |
| 修复: RESTORE 参数检查 | ✅ | `len(args) < 4` → `len(args) < 3` |
| 修复: `BuildGossipPayload` 虚假 epoch | ✅ | 移除 `nodeEpoch == 0` fallback |
| 修复: goroutine leak 测试 XRead | ✅ | 兼容 null response 变化 |

---

## 审计结果（仅存档，不需修复）

| 审计 | 结果 |
|------|------|
| isWriteCommand | 91 条完整，0 缺失 |
| timer 模式 | 8 处 production `time.After()` 全部修复 |
| TODO/FIXME/BUG 注释 | 代码库中 0 处 |
| SlaveInstance data race | 已修复（sync.RWMutex + getter） |
| 3× double close(stopCh) | 已修复（sync.Once） |
| CLUSTER ADDSLOTS/DELSLOTS | 已修复 |
| RDB write error 吞没 | 已修复（9 处加日志） |
| ReplAckOffset data race | 已修复（atomic.Int64） |
| SaveConfig RLock deadlock | 已修复（分拆 saveConfigLocked） |

---

## 附录：Verification Commands

```bash
# All unit tests
bash scripts/remote-test.sh -race -short ./internal/...

# RESP shape suite
bash scripts/remote-test.sh -race -timeout 30s -run TestRESPShape ./internal/server/...

# Goroutine leak
bash scripts/remote-test.sh -race -timeout 120s -run TestGoroutineLeak ./cmd/integration/...

# Compatibility
python3 scripts/redis_py_compat.py
node scripts/redis_node_compat.mjs
bash scripts/redis_cli_compat.sh

# Linter
golangci-lint run --timeout 5m
```
