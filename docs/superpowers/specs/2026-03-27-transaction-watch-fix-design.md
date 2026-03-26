# Transaction/WATCH 命令行为修复设计

## 问题描述

BoltDB 的事务（MULTI/EXEC/DISCARD/WATCH/UNWATCH）实现与 Redis 行为不一致：

### 问题 1：MULTI 覆盖 WATCH 状态
- **当前行为**：`WATCH key` → `MULTI` → `SET key value` → `EXEC`
  - WATCH 记录了 key，但 MULTI 创建新事务时清空了 WatchKeys
  - EXEC 时 IsWatching=false，事务正常执行
- **Redis 行为**：WATCH 开启监视模式，MULTI 不会清除监视状态

### 问题 2：MULTI nested 检测错误
- **当前行为**：只在 `Commands > 0` 时报错
- **Redis 行为**：只要在事务中（已执行过 MULTI），再次执行 MULTI 就报错

### 问题 3：WATCH inside MULTI 检测错误
- **当前行为**：只在 `Commands > 0` 时报错
- **Redis 行为**：只要在事务中，执行 WATCH 就报错

### 问题 4：WATCH 乐观锁不工作
- **当前行为**：使用 `Exists(key)` 检查，仅检测键是否存在变化
- **Redis 行为**：跟踪所有写命令（dirty flag），任何修改都导致 EXEC 失败

---

## 设计方案

### 核心思路

参考 Redis 的 dirty flag 机制：任何修改被监视键的命令都应导致事务失败。

### 修复 1：MULTI/WATCH 交互

**问题**：`MULTI` 创建新 `TransactionState`，丢失 `WATCH` 设置的 `WatchKeys` 和 `IsWatching`。

**修复**：
- MULTI 不再创建新事务，而是设置 `InTransaction = true`
- WATCH 如果没有正在监视的键，也复用现有事务状态

```go
case "MULTI":
    if h.transaction != nil && h.transaction.InTransaction {
        return proto.NewError("ERR MULTI calls can not be nested")
    }
    if h.transaction == nil {
        h.transaction = &TransactionState{}
    }
    h.transaction.InTransaction = true
    h.transaction.Commands = make([]TransactionCommand, 0) // 清除排队的命令
    // 注意：不重置 WatchKeys 和 IsWatching，允许 WATCH 在 MULTI 之前
    return proto.NewSimpleString("OK")
```

### 修复 2：MULTI nested 检测

**修复**：只要 `InTransaction == true` 就报错，不检查 Commands 长度。

### 修复 3：WATCH inside MULTI 检测

**修复**：只要 `InTransaction == true` 且已有排队命令，就报错。

```go
case "WATCH":
    if len(args) < 1 {
        return proto.NewError("ERR wrong number of arguments for 'WATCH' command")
    }
    if h.transaction != nil && h.transaction.InTransaction && len(h.transaction.Commands) > 0 {
        return proto.NewError("ERR WATCH inside MULTI is not allowed")
    }
    // ... 记录 WatchKeys
```

### 修复 4：Dirty Flag 跟踪

**数据结构变更**：

```go
type TransactionState struct {
    Commands      []TransactionCommand  // 排队的命令
    WatchKeys     map[string]struct{}   // 监控的键
    IsWatching    bool                  // 是否处于监视状态
    InTransaction bool                  // 是否在事务中（MULTI 已执行）
    DirtyKeys     map[string]struct{}   // 被修改的键（新增）
}
```

**跟踪点**：在任何可能修改键的命令执行前，标记所有涉及的键为 dirty。

需要标记 dirty 的命令（写命令）：
- SET, SETNX, SETEX, PSETEX
- APPEND, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- DEL, RENAME, RENAMENX
- EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, PERSIST, TTL/PTTL（修改）
- SETRANGE
- GETSET（先读后写）
- LPUSH, RPUSH, LSET, LREM, LTRIM, SORT（修改列表）
- SADD, SREM, SPOP, SINTERSTORE, SUNIONSTORE, SDIFFSTORE（修改集合）
- ZADD, ZREM, ZINCRBY, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE（修改有序集合）
- HSET, HSETNX, HINCRBY, HINCRBYFLOAT, HDEL, HREAME（修改哈希）
- JSONSET, JSONDEL（修改 JSON）
- GEOADD, GEODEL（修改地理索引）
- TSADD, TSREM, TSINCRBY, TSDECRBY, TSREPLACE（修改时间序列）
- PFADD, PFMERGE（修改 HyperLogLog）
- COPY, REPLACE（复制/替换）

**检测逻辑**：

```go
case "EXEC":
    if h.transaction == nil || !h.transaction.InTransaction {
        return proto.NewError("ERR EXEC without MULTI")
    }
    // 检查 dirty keys
    if h.transaction.IsWatching && len(h.transaction.WatchKeys) > 0 {
        for watchKey := range h.transaction.WatchKeys {
            if _, dirty := h.transaction.DirtyKeys[watchKey]; dirty {
                // 键被修改，事务失败
                h.resetTransaction()
                return nil // 返回 nil 表示 WATCH 失败
            }
        }
    }
    // 执行所有排队的命令...
```

**标记点**：在命令执行前标记 dirty。

```go
// 在 executeCommand 或各命令处理中添加：
if h.transaction != nil && h.transaction.IsWatching {
    // 标记所有涉及的键为 dirty
    for _, key := range keys {
        h.transaction.DirtyKeys[key] = struct{}{}
    }
}
```

### 修复 5：WATCH 执行后开启监视

当前 `IsWatching` 仅表示"是否应该监视"，实际应该在 WATCH 命令后设置为 true。

---

## 状态转换图

```
初始状态: transaction = nil
                │
                ▼
┌─────────────────────────────────────────┐
│           WATCH key                     │
│  transaction = {WatchKeys: {key},        │
│                 IsWatching: true,        │
│                 DirtyKeys: {}}           │
└─────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────┐
│           MULTI                         │
│  InTransaction = true                    │
│  (不清除 WatchKeys/IsWatching/DirtyKeys) │
└─────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────┐
│         SET key value                    │
│  Commands = [{SET, [key, value]}]       │
│  DirtyKeys[key] = {}                    │
└─────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────┐
│           EXEC                          │
│  检查 DirtyKeys[watchKey]                 │
│  如有 → return nil                       │
│  否则 → 执行 Commands                    │
└─────────────────────────────────────────┘
```

---

## 测试计划

### 测试用例

1. **WATCH + MULTI + EXEC 成功**
   - WATCH key → MULTI → SET key value → EXEC → 成功

2. **WATCH + MULTI + 修改监视键 + EXEC 失败**
   - WATCH key → MULTI → SET other value → SET key newvalue → EXEC → nil

3. **MULTI nested 报错**
   - MULTI → MULTI → 报错

4. **WATCH inside MULTI 报错**
   - MULTI → WATCH key → 报错

5. **EXEC without MULTI 报错**
   - EXEC → 报错

6. **DISCARD without MULTI 报错**
   - DISCARD → 报错

7. **UNWATCH 清除监视**
   - WATCH key → UNWATCH → EXEC → 正常执行（不清除 DirtyKeys 因为已无监视）

8. **值未变但键被修改的场景**
   - WATCH key → SET key "same" → EXEC → nil（应失败，因为键被标记为 dirty）

---

## 影响范围

- `internal/server/handler.go`：
  - `TransactionState` 结构体
  - `MULTI` 命令处理
  - `WATCH` 命令处理
  - `EXEC` 命令处理
  - 需要在所有写命令执行前标记 dirty

- `cmd/integration/integration_test.go`：
  - 扩展 `TestTransactionExtended` 覆盖上述场景

---

## 风险评估

- **高风险**：修改 `TransactionState` 结构体影响所有事务相关代码
- **中风险**：需要在大量命令执行路径中添加 dirty 标记逻辑
- **缓解**：使用辅助函数统一标记，避免重复代码
