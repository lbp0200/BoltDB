# BoltDB 待办列表

## 当前状态

| 指标 | 值 |
|------|----|
| RESP3 Null 覆盖 | 34/34 命令（100%） |
| redis-py compat | 153/153 (100%) |
| node-redis compat | 110/110 (100%) |
| redis-cli compat | 77/77 (100%) |
| timer 泄漏 | 8/8 已修复 |
| isWriteCommand | 91/91 完整 |
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
