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

---

## P2：测试质量加固（Test Quality Hardening）

### 背景

代码覆盖率很高（~1500 个测试函数），但**大量测试只断言「没报错」，不验证返回值是否正确**。
历史上已有多批 bug（并发竞争、复制一致性、错误吞没、涌现行为）在单元测试全部通过的情况下流到生产/soak。

三个递进阶段：

| 阶段 | 投入 | 目标 |
|------|------|------|
| P2a — 低 hanging fruit | 几天 | 补弱断言、去 stub、加值验证 |
| P2b — 中等投入 | 一到两周 | 隔离加固、错误注入、变异测试引入 |
| P2c — 高 ROI | 按需 | 属性测试、集成测试并行化 |

### P2a — 低 hanging fruit

### P2a — 已完成

| 项 | 状态 |
|----|------|
| Store coverage 测试补值验证 | ✅ 6 个弱断言测试已加固 |
| TestConcurrentConnections 并发值验证 | ✅ SET/GET 值正确性验证 |
| Cluster stub 测试加实质性断言 | ✅ 3 个 stub 测试已加固 |
| 复制端到端数据完整性 | ✅ TestFullResyncDataIntegrity (7 子测试) + TestHandlePSync_FullResyncDataIntegrity |
| 集成测试状态隔离文档 | ✅ setupTest 补充注释文档 |

### P2b — 已完成

| 项 | 说明 | 状态 |
|----|------|------|
| **RDB TTL 持久化修复** | 发现 `ExpiresAt` 秒/纳秒不匹配 + 编码器双重 `time.Now()` 漂移 | ✅ |
| **Error Injection 层** | `ErrorInjector` + 10 个 store 方法注入点 + 21 个测试 (store+handler) | ✅ |
| **并发原子性测试** | ZRevRank 并发/HSet count 准确度/MULTI/EXEC 错误传播 | ✅ |
| **Gossip 行为测试** | PingNoPeers 空 peer 验证 + PFAIL payload flags/epoch 增强 | ✅ |
| **变异测试基线** | go-mutesting 脚本 + manual 回退 + 首次弱测试发现 (ttl==0) | ✅ |
| **TTL 双格式批量修复** | 系统性审计 9 个 `.ExpiresAt()` 读取点，修复 6 个真实 bug（PTTL/EXPIRETIME/DUMP/BitField/Rename） | ✅ |
| **SET 命令修饰符** | 实现 EX/PX/EXAT/PXAT/NX/XX/GET/KEEPTTL 完整支持 | ✅ |
| **比较操作符变异** | manual-mutation.sh 增加 `>↔>=`/`<↔<=` 变异，覆盖 ttl>0 类缺口 | ✅ |

---

### P2b — 中等投入

| 项 | 说明 |
|----|------|
| **变异测试引入** | 配置 `go-mutesting`，先跑基线，找到最弱的测试文件逐步加固 |
| **Error injection 测试** | 给 store 加可注入错误的 wrapper，测试高层错误处理路径（14 处曾被吞掉） |
| **复制端到端完整性** | 正式实现 `TestFullResyncDataIntegrity` 并加入 Tier B 门禁 |
| **gossip 行为测试** | 给 `TestGossip_PingNoPeers` 等加 payload 验证，确保传播生效 |
| **并发原子性测试** | 参考历史 HSet 并发 count 破坏、SPopN stale 结果等 bug，写多 goroutine 竞争测试 |

### P2c — 高 ROI（按需）

| 项 | 说明 | 状态 |
|----|------|------|
| **集成测试独立化** | `StartIsolatedServer` 工厂 + 3 个示例测试（SetGet/TTL/MultiDataType） | ✅ 基础设施就绪 |
| **属性测试（stateful testing）** | 用 `rapid` 实现 6 个属性测试（SET/GET/DEL/LPush/LPop/SAdd/INCR） | ✅ 基础覆盖 |
| **Soak 门槛前移** | `TestDegradationGate` 加入 Tier B 门禁 | ✅ |

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

## 已知限制（有意识跳过或需独立规划）

| 限制 | 说明 | 优先级 |
|------|------|--------|
| **Regression 测试超时** | `TestRegression*` 套件需要 600s 超时；当前 Tier B/C 脚本用 120s 不够。已确认非 flaky，需提升超时配置。 | 🟡 低 |
| **handler.go 体积** | 8807 行。已按命令族拆分出 4 个文件（string/zset/hash/admin）共 ~550 行，但 handler.go 净增反超。剩余 bulk（SET 族/PF 族/Stream/Geo/HyperLogLog 等）仍集中在一个文件。 | 🟡 低 |
| **SET KEEPTTL/GET 持久化传播** | `SET key value KEEPTTL` 和 `SET key value GET` 已完整实现。KEEPTTL 先读取原 TTL 再写入值并重新应用（handler 层 + store 层已完成）。`GET` 返回旧值已实现；旧值不传播到复制 backlog 属于架构级限制（非当前版本目标）。 | ✅ 已完成 |
| **Soak/nightly 测试** | 仅 CI 环境（GitHub Actions cron）运行；本地需手动触发 `SOAK_DURATION=1h bash scripts/test-tier-c.sh`。文档见 `docs/nightly-soak-promp.md`。 | 🟢 无需处理 |

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
