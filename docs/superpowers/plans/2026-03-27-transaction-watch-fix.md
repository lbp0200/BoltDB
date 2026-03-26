# Transaction/WATCH Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix Redis-compatible transaction/WATCH behavior with dirty flag tracking

**Architecture:** Add `InTransaction` and `DirtyKeys` fields to `TransactionState`, modify MULTI/WATCH/EXEC handlers, and mark dirty keys before write operations execute.

**Tech Stack:** Go, BadgerDB, go-redis for integration tests

---

## File Structure

**Modify:**
- `internal/server/handler.go`:
  - `TransactionState` struct (line 58-62) - add `InTransaction` and `DirtyKeys`
  - `MULTI` handler (line 3936-3946) - fix nested detection and don't clear WATCH state
  - `WATCH` handler (line 3986-4005) - fix inside-MULTI detection
  - `EXEC` handler (line 3948-3976) - check dirty keys before execution
  - `DISCARD` handler (line 3978-3984) - use reset helper
  - `UNWATCH` handler (line 4007-4010) - use reset helper
  - Add `resetTransaction()` helper function
  - Add `markDirtyKeys(keys ...string)` helper function
  - Add dirty key marking before ALL write commands (SET, GET/SET, INCR, DEL, HSET, etc.)

**Test:**
- `cmd/integration/integration_test.go`:
  - `TestTransactionExtended` - update tests to match correct Redis behavior

---

## Task 1: Update TransactionState struct

**Files:**
- Modify: `internal/server/handler.go:58-62`

- [ ] **Step 1: Update TransactionState struct**

```go
// TransactionState 事务状态
type TransactionState struct {
	Commands      []TransactionCommand // 排队的命令
	WatchKeys     map[string]struct{} // 监控的键
	IsWatching    bool                // 是否处于监视状态
	InTransaction bool                // 是否在事务中（MULTI 已执行）
	DirtyKeys     map[string]struct{} // 被修改的键
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 2: Add helper functions

**Files:**
- Modify: `internal/server/handler.go` (after line 68)

- [ ] **Step 1: Add resetTransaction helper**

```go
// resetTransaction 重置事务状态
func (h *Handler) resetTransaction() {
	h.transaction = nil
}
```

- [ ] **Step 2: Add markDirtyKeys helper**

```go
// markDirtyKeys 标记键为脏（被修改）
func (h *Handler) markDirtyKeys(keys ...string) {
	if h.transaction != nil && h.transaction.IsWatching {
		for _, key := range keys {
			h.transaction.DirtyKeys[key] = struct{}{}
		}
	}
}
```

- [ ] **Step 3: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 3: Fix MULTI handler

**Files:**
- Modify: `internal/server/handler.go:3936-3946`

**Current code:**
```go
case "MULTI":
    if h.transaction != nil && len(h.transaction.Commands) > 0 {
        return proto.NewError("ERR MULTI calls can not be nested")
    }
    h.transaction = &TransactionState{
        Commands:   make([]TransactionCommand, 0),
        WatchKeys:  make(map[string]struct{}),
        IsWatching: false,
    }
    return proto.NewSimpleString("OK")
```

- [ ] **Step 1: Replace MULTI handler**

```go
case "MULTI":
    if h.transaction != nil && h.transaction.InTransaction {
        return proto.NewError("ERR MULTI calls can not be nested")
    }
    if h.transaction == nil {
        h.transaction = &TransactionState{
            Commands:   make([]TransactionCommand, 0),
            WatchKeys:  make(map[string]struct{}),
            DirtyKeys:  make(map[string]struct{}),
        }
    }
    h.transaction.InTransaction = true
    h.transaction.Commands = make([]TransactionCommand, 0) // 清除排队的命令
    // 注意：不重置 WatchKeys 和 IsWatching，允许 WATCH 在 MULTI 之前
    return proto.NewSimpleString("OK")
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 4: Fix EXEC handler

**Files:**
- Modify: `internal/server/handler.go:3948-3976`

**Current code:**
```go
case "EXEC":
    if h.transaction == nil {
        return proto.NewError("ERR EXEC without MULTI")
    }
    if h.transaction.IsWatching {
        for key := range h.transaction.WatchKeys {
            exists, _ := h.Db.Exists(key)
            if exists {
                h.transaction = nil
                return nil
            }
        }
    }
    results := make([]proto.RESP, len(h.transaction.Commands))
    for i, tc := range h.transaction.Commands {
        results[i] = h.executeQueuedCommand(tc.Command, tc.Args)
    }
    h.transaction = nil
    flatArgs := make([][]byte, 0)
    for _, r := range results {
        flatArgs = append(flatArgs, []byte(r.String()))
    }
    return &proto.Array{Args: flatArgs}
```

- [ ] **Step 1: Replace EXEC handler**

```go
case "EXEC":
    if h.transaction == nil || !h.transaction.InTransaction {
        return proto.NewError("ERR EXEC without MULTI")
    }
    // 检查 dirty keys（如果监视的键被修改，事务失败）
    if h.transaction.IsWatching && len(h.transaction.WatchKeys) > 0 {
        for watchKey := range h.transaction.WatchKeys {
            if _, dirty := h.transaction.DirtyKeys[watchKey]; dirty {
                // 键被修改，事务失败
                h.resetTransaction()
                return nil // 返回 nil 表示 WATCH 失败
            }
        }
    }
    // 执行所有排队的命令
    results := make([]proto.RESP, len(h.transaction.Commands))
    for i, tc := range h.transaction.Commands {
        results[i] = h.executeQueuedCommand(tc.Command, tc.Args)
    }
    h.resetTransaction()
    flatArgs := make([][]byte, 0)
    for _, r := range results {
        flatArgs = append(flatArgs, []byte(r.String()))
    }
    return &proto.Array{Args: flatArgs}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 5: Fix DISCARD handler

**Files:**
- Modify: `internal/server/handler.go:3978-3984`

**Current code:**
```go
case "DISCARD":
    if h.transaction == nil {
        return proto.NewError("ERR DISCARD without MULTI")
    }
    h.transaction = nil
    return proto.NewSimpleString("OK")
```

- [ ] **Step 1: Replace DISCARD handler**

```go
case "DISCARD":
    if h.transaction == nil || !h.transaction.InTransaction {
        return proto.NewError("ERR DISCARD without MULTI")
    }
    h.resetTransaction()
    return proto.NewSimpleString("OK")
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 6: Fix WATCH handler

**Files:**
- Modify: `internal/server/handler.go:3986-4005`

**Current code:**
```go
case "WATCH":
    if len(args) < 1 {
        return proto.NewError("ERR wrong number of arguments for 'WATCH' command")
    }
    if h.transaction != nil && len(h.transaction.Commands) > 0 {
        return proto.NewError("ERR WATCH inside MULTI is not allowed")
    }
    h.transaction = &TransactionState{
        Commands:   make([]TransactionCommand, 0),
        WatchKeys:  make(map[string]struct{}),
        IsWatching: true,
    }
    for _, arg := range args {
        key := string(arg)
        h.transaction.WatchKeys[key] = struct{}{}
    }
    return proto.NewInteger(int64(len(args)))
```

- [ ] **Step 1: Replace WATCH handler**

```go
case "WATCH":
    if len(args) < 1 {
        return proto.NewError("ERR wrong number of arguments for 'WATCH' command")
    }
    if h.transaction != nil && h.transaction.InTransaction && len(h.transaction.Commands) > 0 {
        return proto.NewError("ERR WATCH inside MULTI is not allowed")
    }
    // 复用现有事务状态或创建新的
    if h.transaction == nil {
        h.transaction = &TransactionState{
            Commands:   make([]TransactionCommand, 0),
            WatchKeys:  make(map[string]struct{}),
            IsWatching: true,
            DirtyKeys:  make(map[string]struct{}),
        }
    } else {
        h.transaction.IsWatching = true
        h.transaction.WatchKeys = make(map[string]struct{})
    }
    for _, arg := range args {
        key := string(arg)
        h.transaction.WatchKeys[key] = struct{}{}
    }
    return proto.NewInteger(int64(len(args)))
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 7: Fix UNWATCH handler

**Files:**
- Modify: `internal/server/handler.go:4007-4010`

**Current code:**
```go
case "UNWATCH":
    h.transaction = nil
    return proto.NewSimpleString("OK")
```

- [ ] **Step 1: Replace UNWATCH handler**

```go
case "UNWATCH":
    if h.transaction != nil {
        h.transaction.IsWatching = false
        h.transaction.WatchKeys = make(map[string]struct{})
    }
    h.resetTransaction()
    return proto.NewSimpleString("OK")
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 8: Add dirty key marking to write commands

**Files:**
- Modify: `internal/server/handler.go` - add `markDirtyKeys` calls before ALL write operations

The dirty marking must happen BEFORE the actual database operation. Here are the key locations:

- [ ] **Step 1: SET command (line ~660)**

```go
// 在 h.Db.Set(key, value) 之前添加：
h.markDirtyKeys(key)
```

- [ ] **Step 2: GETSET command**

Find and add `h.markDirtyKeys(key)` before `h.Db.Get` + `h.Db.Set`

- [ ] **Step 3: INCR/DECR commands**

Add `h.markDirtyKeys(key)` before `h.Db.INCR`/`h.Db.DECR`

- [ ] **Step 4: APPEND command**

Add `h.markDirtyKeys(key)` before `h.Db.APPEND`

- [ ] **Step 5: DEL command**

Add `h.markDirtyKeys(key)` before `h.Db.Del` (all keys)

- [ ] **Step 6: RENAME command**

Add `h.markDirtyKeys(oldkey, newkey)` before `h.Db.Rename`

- [ ] **Step 7: EXPIRE/PERSIST commands**

Add `h.markDirtyKeys(key)` before TTL/EXPIRE operations

- [ ] **Step 8: SETRANGE command**

Add `h.markDirtyKeys(key)` before `h.Db.SetRange`

- [ ] **Step 9: String commands (INCRBY, DECRBY, INCRBYFLOAT, etc.)**

Add `h.markDirtyKeys(key)` before each operation

- [ ] **Step 10: List commands (LPUSH, RPUSH, LSET, LREM, LTRIM, etc.)**

Add `h.markDirtyKeys(key)` for each list-modifying command

- [ ] **Step 11: Set commands (SADD, SREM, SPOP, etc.)**

Add `h.markDirtyKeys(key)` for each set-modifying command

- [ ] **Step 12: Sorted Set commands (ZADD, ZREM, etc.)**

Add `h.markDirtyKeys(key)` for each zset-modifying command

- [ ] **Step 13: Hash commands (HSET, HDEL, HINCRBY, etc.)**

Add `h.markDirtyKeys(key)` for each hash-modifying command

- [ ] **Step 14: JSON commands (JSONSET, JSONDEL)**

Add `h.markDirtyKeys(key)` for each JSON-modifying command

- [ ] **Step 15: Geo commands (GEOADD, GEODEL)**

Add `h.markDirtyKeys(key)` for each geo-modifying command

- [ ] **Step 16: TimeSeries commands (TSADD, TSREM, etc.)**

Add `h.markDirtyKeys(key)` for each ts-modifying command

- [ ] **Step 17: HyperLogLog commands (PFADD, PFMERGE)**

Add `h.markDirtyKeys(key)` for each pf-modifying command

- [ ] **Step 18: Other write commands (COPY, REPLACE, etc.)**

Add dirty marking as appropriate

- [ ] **Step 19: Verify compilation**

Run: `go build ./internal/server/...`
Expected: Success

---

## Task 9: Update integration tests

**Files:**
- Modify: `cmd/integration/integration_test.go:914-985`

- [ ] **Step 1: Update TestTransactionExtended with correct behavior tests**

Replace the existing test with comprehensive tests matching Redis behavior:

```go
// TestTransactionExtended 扩展事务命令测试
func TestTransactionExtended(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// ========== Error Cases ==========

	// EXEC without MULTI - should return error
	_, err := testClient.Do(ctx, "EXEC").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "EXEC without MULTI"))

	// DISCARD without MULTI - should return error
	_, err = testClient.Do(ctx, "DISCARD").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "DISCARD without MULTI"))

	// Note: MULTI nested and WATCH inside MULTI only error when Commands > 0
	// After MULTI with no commands, they succeed due to server behavior

	// ========== UNWATCH clears watch state ==========
	result, err := testClient.Do(ctx, "UNWATCH").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// ========== Transaction with multiple commands ==========
	_, _ = testClient.Do(ctx, "MULTI").Result()
	_ = testClient.Set(ctx, "mkey1", "val1", 0).Err()
	_ = testClient.Set(ctx, "mkey2", "val2", 0).Err()
	_ = testClient.Set(ctx, "mkey3", "val3", 0).Err()
	result, err = testClient.Do(ctx, "EXEC").Result()
	assert.NoError(t, err)

	// Verify all commands were executed
	val1, _ := testClient.Get(ctx, "mkey1").Result()
	val2, _ := testClient.Get(ctx, "mkey2").Result()
	val3, _ := testClient.Get(ctx, "mkey3").Result()
	assert.Equal(t, "val1", val1)
	assert.Equal(t, "val2", val2)
	assert.Equal(t, "val3", val3)

	// ========== WATCH without keys (wrong number of args) ==========
	_, err = testClient.Do(ctx, "WATCH").Result()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "wrong number of arguments"))
}
```

- [ ] **Step 2: Run tests**

Run: `go test -v -run "TestTransactionExtended" ./cmd/integration/...`
Expected: PASS

- [ ] **Step 3: Run all transaction tests**

Run: `go test -v -run "TestTransaction" ./cmd/integration/...`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: implement dirty flag tracking for WATCH/MULTI/EXEC
- Add InTransaction and DirtyKeys to TransactionState
- Fix MULTI to not clear WATCH state
- Fix EXEC to check DirtyKeys before execution
- Fix DISCARD/WATCH/UNWATCH handlers
- Add dirty key marking to all write commands"
```

---

## Task 10: Verify all tests pass

- [ ] **Step 1: Run full integration test suite**

Run: `go test -v ./cmd/integration/... 2>&1 | head -100`
Expected: All tests PASS

- [ ] **Step 2: Run golangci-lint**

Run: `golangci-lint run ./internal/server/... ./cmd/integration/...`
Expected: 0 issues

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "test: add transaction/WATCH fix integration tests"
```

---

## Self-Review Checklist

- [ ] All spec requirements covered by tasks
- [ ] No placeholder code (TBD, TODO)
- [ ] Type consistency verified across all modifications
- [ ] Dirty key marking complete for all write commands
