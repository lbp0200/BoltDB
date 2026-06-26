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
| handler.go 拆分 | 8824→0 行，拆为 24 个文件，无单文件超 1136 行 |

---

## 后续可做（Cluster 进阶）

| 项 | 说明 | 优先级 |
|----|------|--------|
| Gossip payload 携带 | 节点/槽位视图交换，当前 PING/PONG 不携带集群状态 | P2 |
| PFAIL/FAIL gossip 传播 | 当前基于本地 timer，可通过 gossip 传播升级为全局判定 | P2 |
| 集群范围槽位同步 | 跨节点槽位分配/迁移状态的完整同步 | P3 |

---

## 架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL / FUNCTION / MIGRATE | P2，按需补充 |

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
