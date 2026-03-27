# Test Depth Improvement Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan.

**Goal:** 系统性提升测试深度，覆盖边界条件、错误处理、并发安全三类场景，减少 bug 漏过。

**Architecture:** 在现有测试文件基础上增加三类深度测试函数，复用现有 fixture 和测试框架。

---

## Background

现有测试主要覆盖 happy path，存在以下问题：
- 边界条件未充分测试（空值、极限值、临界长度）
- 错误处理路径未充分验证（类型错误、参数错误、状态错误）
- 并发安全场景缺失（多 client 竞争、事务冲突）

---

## Three Test Categories

### 1. 边界条件测试 (Boundary)

测试数据结构和命令在极限情况下的行为：
- 空值：`GET` 不存在的 key、`LPOP` 空 list、`SMEMBERS` 空 set
- 极限值：极大 value、超长 key、极大集合（超过内存限制）
- 临界长度：最大 key 长度、value 长度、集合元素数量
- 溢出边界：整数递增到最大值、列表索引越界

**命名约定：** `Test<Type>Boundary_<Scenario>`

```go
func TestStringBoundary_EmptyKey()
func TestStringBoundary_MaxValueSize()
func TestListBoundary_EmptyListPop()
func TestListBoundary_OverflowIndex()
func TestIncrBoundary_MaxInt64()
```

### 2. 错误处理测试 (Error)

测试命令在错误参数或错误状态下的行为：
- 类型错误：`INCR` hash key、`LPUSH` string key
- 参数错误：缺少必需参数、参数数量过多、参数类型错误
- 状态错误：已删除的 key、已关闭的连接、过期资源

**命名约定：** `Test<Type>Error_<Scenario>`

```go
func TestStringError_TypeMismatch()
func TestHashError_InvalidField()
func TestListError_InvalidIndex()
func TestCommandError_WrongNumberOfArguments()
```

### 3. 并发安全测试 (Concurrent)

测试多个 client 同时操作时的数据一致性和正确性：
- 读写冲突：client A 读同一 key 时 client B 写入
- 写写冲突：多个 client 同时写同一 key
- 边界竞态：key 过期/删除的瞬间访问
- 事务并发：WATCH 下的并发修改

**命名约定：** `Test<Type>Concurrent_<Scenario>`

```go
func TestStringConcurrent_ReadWriteConflict()
func TestListConcurrent_PushPopRace()
func TestIncrConcurrent_ConcurrentIncrement()
func TestTransactionConcurrent_WatchConflict()
```

---

## Test File Structure

在现有测试文件基础上扩展，每类测试独立函数：

```
internal/server/handler_commands_test.go
├── TestStringCommands           (现有)
├── TestStringBoundary_EmptyKey  (新增)
├── TestStringBoundary_MaxSize   (新增)
├── TestStringError_TypeMismatch  (新增)
└── TestStringConcurrent_Race    (新增)

internal/server/handler_coverage_test.go
└── [继续添加其他命令的深度测试]

cmd/integration/integration_test.go
└── [并发测试使用真实 server + go-redis 多 client]
```

---

## Implementation Order

按模块优先级逐步实现：

| 阶段 | 模块 | 原因 |
|------|------|------|
| 1 | String | 最常用，边界清晰 |
| 2 | List | 操作复杂，边界多 |
| 3 | Hash | 字段操作边界 |
| 4 | Set | 集合操作边界 |
| 5 | SortedSet | 分值边界 |
| 6 | Cluster | 槽位边界、redirect |
| 7 | Replication | 主从同步边界 |
| 8 | Sentinel | 故障转移边界 |

---

## Concurrency Test Patterns

### 多 client 并发模式

```go
func TestStringConcurrent_ReadWriteConflict(t *testing.T) {
    setupTestServer(t)
    defer teardownTestServer(t)

    var wg sync.WaitGroup
    const numGoroutines = 10

    // 写操作
    wg.Add(1)
    go func() {
        defer wg.Done()
        for i := 0; i < 100; i++ {
            testClient.Set(ctx, "concurrent_key", i, 0)
        }
    }()

    // 读操作
    for i := 0; i < numGoroutines; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                testClient.Get(ctx, "concurrent_key")
            }
        }()
    }

    wg.Wait()
    // 验证最终值一致
}
```

### 竞态条件模式

```go
func TestKeyExpiredConcurrent_AccessDuringExpiry(t *testing.T) {
    setupTestServer(t)
    defer teardownTestServer(t)

    // 设置 1ms 过期的 key
    testClient.Set(ctx, "expiring_key", "value", 1*time.Millisecond)

    var wg sync.WaitGroup
    done := make(chan bool)

    // 持续读取直到过期
    wg.Add(1)
    go func() {
        defer wg.Done()
        for i := 0; i < 1000; i++ {
            testClient.Get(ctx, "expiring_key")
            select {
            case <-done:
                return
            default:
            }
        }
    }()

    // 等待过期
    time.Sleep(10 * time.Millisecond)
    close(done)
    wg.Wait()
}
```

---

## Boundary Test Patterns

### 极限值测试

```go
func TestStringBoundary_MaxValueSize(t *testing.T) {
    // Redis 最大 value 大小 512MB
    maxSize := 512 * 1024 * 1024
    largeValue := make([]byte, maxSize)

    err := testClient.Set(ctx, "max_value", largeValue, 0).Err()
    assert.NoError(t, err)

    val, err := testClient.Get(ctx, "max_value").Bytes()
    assert.NoError(t, err)
    assert.Equal(t, maxSize, len(val))
}
```

### 空集合边界

```go
func TestListBoundary_EmptyListPop(t *testing.T) {
    // 已有 key 但不是 list 类型
    testClient.Set(ctx, "not_a_list", "value", 0)

    // LPOP 空 list 应返回 nil
    result, err := testClient.LPop(ctx, "nonexistent_list").Result()
    assert.Equal(t, redis.Nil, err)
    assert.Empty(t, result)
}
```

---

## Error Test Patterns

### 类型错误

```go
func TestStringError_TypeMismatch(t *testing.T) {
    // 设置 hash 类型
    testClient.HSet(ctx, "myhash", "field", "value")

    // 对 hash key 使用 string 命令应返回错误
    err := testClient.Append(ctx, "myhash", "extra").Err()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "WRONGTYPE")
}
```

---

## Reuse of Existing Fixtures

- `internal/server/handler_test.go` 的 `setupTestHandler()` — 单元测试用
- `cmd/integration/integration_test.go` 的 `setupTestServer()` / `teardownTestServer()` — 集成测试用
- 并发测试使用全局 `testClient` 或创建额外 client

---

## Commit Strategy

每完成一个模块 commit 一次，格式：
```
test: add depth tests for <module> (boundary + error + concurrent)
```

---

## Success Criteria

1. 边界条件测试覆盖所有数据类型的临界场景
2. 错误处理测试覆盖所有命令的类型/参数错误路径
3. 并发测试覆盖高频竞态场景
4. 无新增 flaky test（使用适当同步机制）
5. 测试执行时间不超过现有时间的 2x
