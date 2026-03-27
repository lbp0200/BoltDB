# Test Depth Improvement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 系统性提升测试深度，覆盖边界条件、错误处理、并发安全三类场景，减少 bug 漏过。

**Architecture:** 在现有测试文件基础上增加三类深度测试函数。边界和错误测试使用 `internal/server/` 的单元测试 fixture（`setupTestHandler`），并发测试使用 `cmd/integration/` 的集成测试 fixture（`setupTestServer` + go-redis 多 client）。

**Tech Stack:** Go testing, go-redis, sync.WaitGroup, zeebo/assert

---

## File Structure

| 文件 | 职责 |
|------|------|
| `internal/server/handler_depth_test.go` | 新增：String/List/Hash 的边界 + 错误测试 |
| `internal/server/handler_depth2_test.go` | 新增：Set/SortedSet/其他数据类型的边界 + 错误测试 |
| `cmd/integration/depth_test.go` | 新增：所有数据类型的并发安全测试 |

现有测试文件保持不变，仅做增量添加。

---

## Phase 1: String 深度测试

### Task 1.1: String Boundary Tests

**Files:**
- Create: `internal/server/handler_depth_test.go`
- Test: `internal/server/handler_depth_test.go`

- [ ] **Step 1: 创建 handler_depth_test.go 文件**

```go
package server

import (
	"context"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)
```

- [ ] **Step 2: 添加 TestStringBoundary_EmptyKey**

```go
// TestStringBoundary_EmptyKey tests GET on nonexistent key
func TestStringBoundary_EmptyKey(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("GET", [][]byte{[]byte("nonexistent_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 3: 添加 TestStringBoundary_MaxValueSize**

```go
// TestStringBoundary_MaxValueSize tests SET/GET with large value
func TestStringBoundary_MaxValueSize(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// 10MB value
	largeValue := make([]byte, 10*1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	resp := handler.executeCommand("SET", [][]byte{[]byte("large_key"), largeValue}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	getResp := handler.executeCommand("GET", [][]byte{[]byte("large_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, 10*1024*1024, len(*bs))
}
```

- [ ] **Step 4: 添加 TestStringBoundary_EmptyString**

```go
// TestStringBoundary_EmptyString tests SET with empty value
func TestStringBoundary_EmptyString(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("SET", [][]byte{[]byte("empty_key"), []byte("")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	getResp := handler.executeCommand("GET", [][]byte{[]byte("empty_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 5: 添加 TestIncrBoundary_MaxInt64**

```go
// TestIncrBoundary_MaxInt64 tests INCR on boundary values
func TestIncrBoundary_MaxInt64(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set to max int64 - 1
	handler.executeCommand("SET", [][]byte{[]byte("max_counter"), []byte("9223372036854775806")}, "127.0.0.1:12345")

	resp := handler.executeCommand("INCR", [][]byte{[]byte("max_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(9223372036854775807), int64(*integer))

	// Next INCR should overflow
	resp = handler.executeCommand("INCR", [][]byte{[]byte("max_counter")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "overflow")
}
```

- [ ] **Step 6: 添加 TestIncrBoundary_NegativeToPositive**

```go
// TestIncrBoundary_NegativeToPositive tests DECR crossing zero
func TestIncrBoundary_NegativeToPositive(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("neg_counter"), []byte("-5")}, "127.0.0.1:12345")

	resp := handler.executeCommand("INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-4), int64(*integer))

	// Cross zero
	for i := 0; i < 4; i++ {
		handler.executeCommand("INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	}
	resp = handler.executeCommand("INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}
```

- [ ] **Step 7: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestStringBoundary|TestIncrBoundary" 2>&1
```
Expected: PASS for all 5 tests

- [ ] **Step 8: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add String boundary tests (empty key, empty value, max int64)"
```

### Task 1.2: String Error Tests

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: 添加 TestStringError_TypeMismatch**

```go
// TestStringError_TypeMismatch tests string command on non-string type
func TestStringError_TypeMismatch(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a list type key
	handler.executeCommand("LPUSH", [][]byte{[]byte("list_key"), []byte("value")}, "127.0.0.1:12345")

	// APPEND on list key should error
	resp := handler.executeCommand("APPEND", [][]byte{[]byte("list_key"), []byte("extra")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "WRONGTYPE")
}
```

- [ ] **Step 2: 添加 TestStringError_WrongNumberOfArguments**

```go
// TestStringError_WrongNumberOfArguments tests GET without key argument
func TestStringError_WrongNumberOfArguments(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("GET", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "wrong number of arguments")
}
```

- [ ] **Step 3: 添加 TestStringError_SetGetInvalidArgs**

```go
// TestStringError_SetGetInvalidArgs tests SET with missing value
func TestStringError_SetGetInvalidArgs(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// SET with only key (missing value)
	resp := handler.executeCommand("SET", [][]byte{[]byte("key_only")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "wrong number of arguments")
}
```

- [ ] **Step 4: 添加 TestStringError_IncrOnFloat**

```go
// TestStringError_IncrOnFloat tests INCR on float value
func TestStringError_IncrOnFloat(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("float_key"), []byte("1.5")}, "127.0.0.1:12345")

	resp := handler.executeCommand("INCR", [][]byte{[]byte("float_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	// Should error on non-integer value
	assert.Contains(t, string(*errResp), "not an integer")
}
```

- [ ] **Step 5: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestStringError" 2>&1
```
Expected: PASS for all 4 tests

- [ ] **Step 6: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add String error tests (type mismatch, wrong args, incr on float)"
```

### Task 1.3: String Concurrent Tests

**Files:**
- Create: `cmd/integration/depth_test.go`

- [ ] **Step 1: 创建 cmd/integration/depth_test.go**

```go
package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)
```

- [ ] **Step 2: 添加 TestStringConcurrent_ReadWriteConflict**

```go
// TestStringConcurrent_ReadWriteConflict tests concurrent read/write to same key
func TestStringConcurrent_ReadWriteConflict(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	results := make(chan int64, goroutines*opsPerGoroutine)

	// Writer goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.Set(ctx, "concurrent_key", idx*1000+j, 0)
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				val, _ := testClient.Get(ctx, "concurrent_key").Int64()
				results <- val
			}
		}()
	}

	wg.Wait()
	close(results)

	// Verify final value is valid (was written by some writer)
	finalVal, err := testClient.Get(ctx, "concurrent_key").Int64()
	assert.NoError(t, err)
	assert.True(t, finalVal >= 0)
}
```

- [ ] **Step 3: 添加 TestStringConcurrent_ConcurrentIncrement**

```go
// TestStringConcurrent_ConcurrentIncrement tests concurrent INCR on same key
func TestStringConcurrent_ConcurrentIncrement(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const incrsPerGoroutine = 100

	// Initialize counter
	testClient.Set(ctx, "incr_counter", 0, 0)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrsPerGoroutine; j++ {
				testClient.Incr(ctx, "incr_counter")
			}
		}()
	}

	wg.Wait()

	// Final value should be exactly goroutines * incrsPerGoroutine
	finalVal, err := testClient.Get(ctx, "incr_counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(goroutines*incrsPerGoroutine), finalVal)
}
```

- [ ] **Step 4: 添加 TestStringConcurrent_AppendRace**

```go
// TestStringConcurrent_AppendRace tests concurrent APPEND operations
func TestStringConcurrent_AppendRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 5
	const appendsPerGoroutine = 20

	testClient.Set(ctx, "append_race_key", "init", 0)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < appendsPerGoroutine; j++ {
				testClient.Append(ctx, "append_race_key", string(rune('A'+idx)))
			}
		}(i)
	}

	wg.Wait()

	// Final length should be 4 (init) + goroutines * appendsPerGoroutine
	finalLen, err := testClient.StrLen(ctx, "append_race_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4+goroutines*appendsPerGoroutine), finalLen)
}
```

- [ ] **Step 5: Run tests and verify**

```bash
go test -v ./cmd/integration/... -run "TestStringConcurrent" -timeout 60s 2>&1
```
Expected: PASS for all 3 tests

- [ ] **Step 6: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add String concurrent tests (read/write conflict, incr race, append race)"
```

---

## Phase 2: List 深度测试

### Task 2.1: List Boundary Tests

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: 添加 TestListBoundary_EmptyList**

```go
// TestListBoundary_EmptyList tests LPOP on nonexistent key
func TestListBoundary_EmptyList(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("LPOP", [][]byte{[]byte("nonexistent_list")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 2: 添加 TestListBoundary_SingleElement**

```go
// TestListBoundary_SingleElement tests LPOP/RPOP on list with one element
func TestListBoundary_SingleElement(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("LPUSH", [][]byte{[]byte("single_list"), []byte("only")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LPOP", [][]byte{[]byte("single_list")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "only", string(*bs))

	// Second pop should return empty
	resp = handler.executeCommand("LPOP", [][]byte{[]byte("single_list")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 3: 添加 TestListBoundary_IndexOverflow**

```go
// TestListBoundary_IndexOverflow tests LLEN and LINDEX on large list
func TestListBoundary_IndexOverflow(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Push 1000 elements
	for i := 0; i < 1000; i++ {
		handler.executeCommand("RPUSH", [][]byte{[]byte("large_list"), []byte(string(rune('A' + i%26)))}, "127.0.0.1:12345")
	}

	resp := handler.executeCommand("LLEN", [][]byte{[]byte("large_list")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), int64(*integer))

	// Index beyond length
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("large_list"), []byte("9999")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 4: 添加 TestListBoundary_NegativeIndex**

```go
// TestListBoundary_NegativeIndex tests negative index access
func TestListBoundary_NegativeIndex(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("RPUSH", [][]byte{[]byte("neg_index_list"), []byte("first"), []byte("middle"), []byte("last")}, "127.0.0.1:12345")

	// -1 = last element
	resp := handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "last", string(*bs))

	// -2 = middle element
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-2")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "middle", string(*bs))

	// -4 = first element
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-4")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "first", string(*bs))

	// -5 beyond list
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-5")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 5: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestListBoundary" 2>&1
```
Expected: PASS for all 4 tests

- [ ] **Step 6: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add List boundary tests (empty, single element, index overflow, negative index)"
```

### Task 2.2: List Error Tests

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: 添加 TestListError_TypeMismatch**

```go
// TestListError_TypeMismatch tests list command on string type
func TestListError_TypeMismatch(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LPUSH", [][]byte{[]byte("string_key"), []byte("new")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "WRONGTYPE")
}
```

- [ ] **Step 2: 添加 TestListError_InvalidIndex**

```go
// TestListError_InvalidIndex tests LSET with invalid index
func TestListError_InvalidIndex(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("RPUSH", [][]byte{[]byte("valid_list"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LSET", [][]byte{[]byte("valid_list"), []byte("5"), []byte("c")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "index out of range")
}
```

- [ ] **Step 3: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestListError" 2>&1
```
Expected: PASS for all 2 tests

- [ ] **Step 4: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add List error tests (type mismatch, invalid index)"
```

### Task 2.3: List Concurrent Tests

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: 添加 TestListConcurrent_PushPopRace**

```go
// TestListConcurrent_PushPopRace tests concurrent LPUSH and LPOP
func TestListConcurrent_PushPopRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_list")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if j%2 == 0 {
					testClient.LPush(ctx, "race_list", idx*1000+j)
				} else {
					testClient.RPush(ctx, "race_list", idx*1000+j)
				}
			}
		}(i)
	}

	wg.Wait()

	// List should have some elements (not empty due to race)
	llen, _ := testClient.LLen(ctx, "race_list").Result()
	assert.True(t, llen >= 0)
}
```

- [ ] **Step 2: 添加 TestListConcurrent_MultipleBlockingPops**

```go
// TestListConcurrent_MultipleBlockingPops tests multiple clients doing BLPOP
func TestListConcurrent_MultipleBlockingPops(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	testClient.Del(ctx, "blocking_list")

	// Start a blocking pop that will timeout
	result, err := testClient.BLPop(ctx, 100*time.Millisecond, "blocking_list").Result()
	assert.Equal(t, redis.Nil, err)
	assert.Nil(t, result)

	// Push an element
	testClient.LPush(ctx, "blocking_list", "value")

	// Now BLPOP should get it (very short timeout for test)
	result, err = testClient.BLPop(ctx, 100*time.Millisecond, "blocking_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", result)
}
```

- [ ] **Step 3: Run tests and verify**

```bash
go test -v ./cmd/integration/... -run "TestListConcurrent" -timeout 60s 2>&1
```
Expected: PASS for all 2 tests

- [ ] **Step 4: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add List concurrent tests (push/pop race, blocking pop)"
```

---

## Phase 3: Hash 深度测试

### Task 3.1: Hash Boundary Tests

**Files:**
- Modify: `internal/server/handler_depth2_test.go`

- [ ] **Step 1: 创建 handler_depth2_test.go**

```go
package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)
```

- [ ] **Step 2: 添加 TestHashBoundary_EmptyHash**

```go
// TestHashBoundary_EmptyHash tests HGET on nonexistent hash
func TestHashBoundary_EmptyHash(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("HGET", [][]byte{[]byte("nonexistent_hash"), []byte("field")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 3: 添加 TestHashBoundary_LargeFieldCount**

```go
// TestHashBoundary_LargeFieldCount tests HSET with many fields
func TestHashBoundary_LargeFieldCount(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set 1000 fields
	for i := 0; i < 1000; i++ {
		handler.executeCommand("HSET", [][]byte{[]byte("large_hash"), []byte(string(rune('A'+i%26))), []byte("value")}, "127.0.0.1:12345")
	}

	resp := handler.executeCommand("HLEN", [][]byte{[]byte("large_hash")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), int64(*integer))
}
```

- [ ] **Step 4: 添加 TestHashBoundary_EmptyField**

```go
// TestHashBoundary_EmptyField tests HSET with empty field value
func TestHashBoundary_EmptyField(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash"), []byte("empty_field"), []byte("")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HGET", [][]byte{[]byte("hash"), []byte("empty_field")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}
```

- [ ] **Step 5: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestHashBoundary" 2>&1
```
Expected: PASS for all 3 tests

- [ ] **Step 6: Commit**

```bash
git add internal/server/handler_depth2_test.go
git commit -m "test: add Hash boundary tests (empty hash, large field count, empty field)"
```

### Task 3.2: Hash Error Tests

**Files:**
- Modify: `internal/server/handler_depth2_test.go`

- [ ] **Step 1: 添加 TestHashError_TypeMismatch**

```go
// TestHashError_TypeMismatch tests hash command on string type
func TestHashError_TypeMismatch(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HGET", [][]byte{[]byte("string_key"), []byte("field")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "WRONGTYPE")
}
```

- [ ] **Step 2: 添加 TestHashError_WrongNumberOfArguments**

```go
// TestHashError_WrongNumberOfArguments tests HSET with missing arguments
func TestHashError_WrongNumberOfArguments(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("HSET", [][]byte{[]byte("hash")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Contains(t, string(*errResp), "wrong number of arguments")
}
```

- [ ] **Step 3: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestHashError" 2>&1
```
Expected: PASS for all 2 tests

- [ ] **Step 4: Commit**

```bash
git add internal/server/handler_depth2_test.go
git commit -m "test: add Hash error tests (type mismatch, wrong arguments)"
```

### Task 3.3: Hash Concurrent Tests

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: 添加 TestHashConcurrent_HgetHsetRace**

```go
// TestHashConcurrent_HgetHsetRace tests concurrent HGET and HSET
func TestHashConcurrent_HgetHsetRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_hash")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				field := string(rune('A' + j%26))
				if j%2 == 0 {
					testClient.HSet(ctx, "race_hash", field, idx*1000+j)
				} else {
					testClient.HGet(ctx, "race_hash", field)
				}
			}
		}(i)
	}

	wg.Wait()

	// Hash should have some fields
	hlen, _ := testClient.HLen(ctx, "race_hash").Result()
	assert.True(t, hlen > 0)
}
```

- [ ] **Step 2: Run tests and verify**

```bash
go test -v ./cmd/integration/... -run "TestHashConcurrent" -timeout 60s 2>&1
```
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add Hash concurrent tests (hget/hset race)"
```

---

## Phase 4: Set/SortedSet 深度测试

### Task 4.1: Set Boundary Tests

**Files:**
- Modify: `internal/server/handler_depth2_test.go`

- [ ] **Step 1: 添加 TestSetBoundary_EmptySet**

```go
// TestSetBoundary_EmptySet tests SMEMBERS on nonexistent set
func TestSetBoundary_EmptySet(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("SMEMBERS", [][]byte{[]byte("nonexistent_set")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}
```

- [ ] **Step 2: 添加 TestSetBoundary_SingleElement**

```go
// TestSetBoundary_SingleElement tests SADD/SREM on single element set
func TestSetBoundary_SingleElement(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("single_set"), []byte("only")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SCARD", [][]byte{[]byte("single_set")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Remove the only element
	handler.executeCommand("SREM", [][]byte{[]byte("single_set"), []byte("only")}, "127.0.0.1:12345")

	resp = handler.executeCommand("SCARD", [][]byte{[]byte("single_set")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}
```

- [ ] **Step 3: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestSetBoundary" 2>&1
```
Expected: PASS for all 2 tests

- [ ] **Step 4: Commit**

```bash
git add internal/server/handler_depth2_test.go
git commit -m "test: add Set boundary tests (empty set, single element)"
```

### Task 4.2: SortedSet Boundary Tests

**Files:**
- Modify: `internal/server/handler_depth2_test.go`

- [ ] **Step 1: 添加 TestSortedSetBoundary_EmptyZSet**

```go
// TestSortedSetBoundary_EmptyZSet tests ZRANGE on nonexistent sorted set
func TestSortedSetBoundary_EmptyZSet(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("ZRANGE", [][]byte{[]byte("nonexistent_zset"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}
```

- [ ] **Step 2: 添加 TestSortedSetBoundary_ScoreBoundary**

```go
// TestSortedSetBoundary_ScoreBoundary tests sorted set with extreme scores
func TestSortedSetBoundary_ScoreBoundary(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add members with extreme scores
	handler.executeCommand("ZADD", [][]byte{[]byte("zset"), []byte("-9223372036854775808"), []byte("min_score")}, "127.0.0.1:12345")
	handler.executeCommand("ZADD", [][]byte{[]byte("zset"), []byte("9223372036854775807"), []byte("max_score")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZRANGE", [][]byte{[]byte("zset"), []byte("0"), []byte("-1"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr.Args))
}
```

- [ ] **Step 3: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestSortedSetBoundary" 2>&1
```
Expected: PASS for all 2 tests

- [ ] **Step 4: Commit**

```bash
git add internal/server/handler_depth2_test.go
git commit -m "test: add SortedSet boundary tests (empty zset, extreme scores)"
```

---

## Phase 5: Key Expiry 边界测试

### Task 5.1: Key Expiry Boundary Tests

**Files:**
- Modify: `internal/server/handler_depth2_test.go`

- [ ] **Step 1: 添加 TestKeyExpiryBoundary_ExpiredKey**

```go
// TestKeyExpiryBoundary_ExpiredKey tests access to expired key
func TestKeyExpiryBoundary_ExpiredKey(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set key with 1ms expiry (use milliseconds for PEXPIRE)
	handler.executeCommand("SET", [][]byte{[]byte("expiring_key"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand("PEXPIRE", [][]byte{[]byte("expiring_key"), []byte("1")}, "127.0.0.1:12345")

	// Immediate access should work
	resp := handler.executeCommand("GET", [][]byte{[]byte("expiring_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*bs))
}
```

- [ ] **Step 2: 添加 TestKeyExpiryBoundary_TTL**

```go
// TestKeyExpiryBoundary_TTL tests TTL on key with expiry
func TestKeyExpiryBoundary_TTL(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("ttl_key"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand("EXPIRE", [][]byte{[]byte("ttl_key"), []byte("3600")}, "127.0.0.1:12345")

	resp := handler.executeCommand("TTL", [][]byte{[]byte("ttl_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0 && int64(*integer) <= 3600)

	// Key with no expiry
	resp = handler.executeCommand("TTL", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-2), int64(*integer))
}
```

- [ ] **Step 3: Run tests and verify**

```bash
go test -v ./internal/server/... -run "TestKeyExpiryBoundary" 2>&1
```
Expected: PASS for all 2 tests

- [ ] **Step 4: Commit**

```bash
git add internal/server/handler_depth2_test.go
git commit -m "test: add key expiry boundary tests (expired key, TTL)"
```

---

## Phase 6: 最终验证

### Task 6.1: Run All Tests

- [ ] **Step 1: Run all depth tests**

```bash
go test -v ./internal/server/... -run "TestStringBoundary|TestStringError|TestListBoundary|TestListError|TestHashBoundary|TestHashError|TestSetBoundary|TestSortedSetBoundary|TestKeyExpiryBoundary" 2>&1 | tail -50
```

- [ ] **Step 2: Run all integration depth tests**

```bash
go test -v ./cmd/integration/... -run "Concurrent" -timeout 120s 2>&1 | tail -30
```

- [ ] **Step 3: Run linter**

```bash
golangci-lint run ./internal/server/handler_depth*.go ./cmd/integration/depth_test.go 2>&1
```

- [ ] **Step 4: Run full test suite (excluding slow store tests)**

```bash
go test ./internal/server/... ./cmd/integration/... -timeout 120s 2>&1
```

- [ ] **Step 5: Final commit**

```bash
git add .
git commit -m "test: add comprehensive depth tests (boundary + error + concurrent)"
```

---

## Success Criteria

1. String: 边界测试 5 个、错误测试 4 个、并发测试 3 个 ✓
2. List: 边界测试 4 个、错误测试 2 个、并发测试 2 个 ✓
3. Hash: 边界测试 3 个、错误测试 2 个、并发测试 1 个 ✓
4. Set: 边界测试 2 个 ✓
5. SortedSet: 边界测试 2 个 ✓
6. Key Expiry: 边界测试 2 个 ✓
7. 所有新增测试通过
8. 无新增 lint 错误
9. 提交信息规范
