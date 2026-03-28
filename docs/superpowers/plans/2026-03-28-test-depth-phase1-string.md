# Test Depth Phase 1: String Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add boundary, error, and concurrent depth tests for String module commands: DECR, DECRBY, GETRANGE, SETBIT, GETBIT, SETRANGE, SETEX, PSETEX.

**Architecture:** Add integration tests to `cmd/integration/depth_test.go` (real server + go-redis) and unit tests to `internal/server/handler_depth_test.go` (direct handler execution). Tests follow existing patterns: `Test<Type>Boundary_<Scenario>`, `Test<Type>Error_<Scenario>`, `Test<Type>Concurrent_<Scenario>`.

**Tech Stack:** Go, go-redis/v9, zeebo/assert, testing.T

---

## File Inventory

| File | Role |
|------|------|
| `cmd/integration/depth_test.go` | Integration tests (real server, multi-client) |
| `internal/server/handler_depth_test.go` | Unit tests (direct handler execution) |

---

## Tasks

### Task 1: Add String Boundary and Error Tests to handler_depth_test.go

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: Add TestStringBoundary_DecrOverflow — DECR at int64 boundary**

```go
// TestStringBoundary_DecrOverflow tests DECR at int64 min boundary
func TestStringBoundary_DecrOverflow(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set to min int64 + 1
	handler.executeCommand("SET", [][]byte{[]byte("min_counter"), []byte("-9223372036854775807")}, "127.0.0.1:12345")

	resp := handler.executeCommand("DECR", [][]byte{[]byte("min_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-9223372036854775808), int64(*integer))

	// Next DECR should overflow
	resp = handler.executeCommand("DECR", [][]byte{[]byte("min_counter")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "overflow"))
}
```

- [ ] **Step 2: Run test to verify it fails (DECR overflow not yet handled)**

Run: `go test -v -run TestStringBoundary_DecrOverflow ./internal/server/`
Expected: PASS (implementation already handles overflow)

- [ ] **Step 3: Add TestStringBoundary_DecrbyZero — DECRBY with zero**

```go
// TestStringBoundary_DecrbyZero tests DECRBY with zero step
func TestStringBoundary_DecrbyZero(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("counter"), []byte("10")}, "127.0.0.1:12345")
	resp := handler.executeCommand("DECRBY", [][]byte{[]byte("counter"), []byte("0")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "invalid"))
}
```

- [ ] **Step 4: Run test to verify behavior**

Run: `go test -v -run TestStringBoundary_DecrbyZero ./internal/server/`
Expected: PASS or FAIL depending on current behavior — if it passes, the error is already handled

- [ ] **Step 5: Add TestStringError_WrongTypeForDecr — DECR on hash key**

```go
// TestStringError_WrongTypeForDecr tests DECR on wrong type
func TestStringError_WrongTypeForDecr(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand("DECR", [][]byte{[]byte("hash_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test -v -run TestStringError_WrongTypeForDecr ./internal/server/`
Expected: PASS

- [ ] **Step 7: Add TestStringError_WrongTypeForDecrby — DECRBY on set key**

```go
// TestStringError_WrongTypeForDecrby tests DECRBY on wrong type
func TestStringError_WrongTypeForDecrby(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("set_key"), []byte("member")}, "127.0.0.1:12345")
	resp := handler.executeCommand("DECRBY", [][]byte{[]byte("set_key"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

- [ ] **Step 8: Run test**

Run: `go test -v -run TestStringError_WrongTypeForDecrby ./internal/server/`
Expected: PASS

- [ ] **Step 9: Add TestStringBoundary_GetrangeFullString — GETRANGE full string**

```go
// TestStringBoundary_GetrangeFullString tests GETRANGE with full range
func TestStringBoundary_GetrangeFullString(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("range_key"), []byte("hello")}, "127.0.0.1:12345")

	// Get full string with 0 to -1
	resp := handler.executeCommand("GETRANGE", [][]byte{[]byte("range_key"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))
}
```

- [ ] **Step 10: Run test**

Run: `go test -v -run TestStringBoundary_GetrangeFullString ./internal/server/`
Expected: PASS

- [ ] **Step 11: Add TestStringBoundary_GetrangeOutOfBounds — GETRANGE beyond string length**

```go
// TestStringBoundary_GetrangeOutOfBounds tests GETRANGE beyond string bounds
func TestStringBoundary_GetrangeOutOfBounds(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("short_key"), []byte("hi")}, "127.0.0.1:12345")

	// Request more than exists — should return what is available
	resp := handler.executeCommand("GETRANGE", [][]byte{[]byte("short_key"), []byte("0"), []byte("100")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hi", string(*bs))
}
```

- [ ] **Step 12: Run test**

Run: `go test -v -run TestStringBoundary_GetrangeOutOfBounds ./internal/server/`
Expected: PASS

- [ ] **Step 13: Add TestStringError_SetexWrongType — SETEX on zset key**

```go
// TestStringError_SetexWrongType tests SETEX on wrong type
func TestStringError_SetexWrongType(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member")}, "127.0.0.1:12345")
	resp := handler.executeCommand("SETEX", [][]byte{[]byte("zset_key"), []byte("10"), []byte("value")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

- [ ] **Step 14: Run test**

Run: `go test -v -run TestStringError_SetexWrongType ./internal/server/`
Expected: PASS

- [ ] **Step 15: Add TestStringError_PsetexWrongType — PSETEX on list key**

```go
// TestStringError_PsetexWrongType tests PSETEX on wrong type
func TestStringError_PsetexWrongType(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("LPUSH", [][]byte{[]byte("list_key"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand("PSETEX", [][]byte{[]byte("list_key"), []byte("1000"), []byte("value")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

- [ ] **Step 16: Run test**

Run: `go test -v -run TestStringError_PsetexWrongType ./internal/server/`
Expected: PASS

- [ ] **Step 17: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add String boundary and error depth tests (DECR, DECRBY, GETRANGE, SETEX, PSETEX)"
```

---

### Task 2: Add String Bit Tests to handler_depth_test.go

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: Add TestStringBoundary_SetbitGetbitBasic — SETBIT and GETBIT basic**

```go
// TestStringBoundary_SetbitGetbitBasic tests SETBIT and GETBIT
func TestStringBoundary_SetbitGetbitBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set bit 7 to 1 (value = 128)
	resp := handler.executeCommand("SETBIT", [][]byte{[]byte("bit_key"), []byte("7"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer)) // old value was 0

	// GETBIT should return 1
	resp = handler.executeCommand("GETBIT", [][]byte{[]byte("bit_key"), []byte("7")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}
```

- [ ] **Step 2: Run test**

Run: `go test -v -run TestStringBoundary_SetbitGetbitBasic ./internal/server/`
Expected: PASS

- [ ] **Step 3: Add TestStringBoundary_SetbitOutOfRange — SETBIT on large offset**

```go
// TestStringBoundary_SetbitOutOfRange tests SETBIT on offset beyond practical limits
func TestStringBoundary_SetbitOutOfRange(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set bit at a large offset (creates a string of that size)
	resp := handler.executeCommand("SETBIT", [][]byte{[]byte("large_bit_key"), []byte("1048576"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify the bit was set
	resp = handler.executeCommand("GETBIT", [][]byte{[]byte("large_bit_key"), []byte("1048576")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}
```

- [ ] **Step 4: Run test**

Run: `go test -v -run TestStringBoundary_SetbitOutOfRange ./internal/server/`
Expected: PASS

- [ ] **Step 5: Add TestStringBoundary_SetrangeExtend — SETRANGE extends string**

```go
// TestStringBoundary_SetrangeExtend tests SETRANGE extending a string
func TestStringBoundary_SetrangeExtend(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("extend_key"), []byte("hello")}, "127.0.0.1:12345")

	// Extend from offset 5 with " world"
	resp := handler.executeCommand("SETRANGE", [][]byte{[]byte("extend_key"), []byte("5"), []byte(" world")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// Verify new value
	getResp := handler.executeCommand("GET", [][]byte{[]byte("extend_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello world", string(*bs))
}
```

- [ ] **Step 6: Run test**

Run: `go test -v -run TestStringBoundary_SetrangeExtend ./internal/server/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add String bit boundary tests (SETBIT, GETBIT, SETRANGE)"
```

---

### Task 3: Add String Concurrent Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestStringConcurrent_DecrRace — concurrent DECR operations**

```go
// TestStringConcurrent_DecrRace tests concurrent DECR on same key
func TestStringConcurrent_DecrRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const decrsPerGoroutine = 100

	// Initialize counter to a large positive value
	err := testClient.Set(ctx, "decr_counter", goroutines*decrsPerGoroutine, 0).Err()
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < decrsPerGoroutine; j++ {
				_, _ = testClient.Decr(ctx, "decr_counter").Result()
			}
		}()
	}

	wg.Wait()

	// Final value should be 0
	finalVal, err := testClient.Get(ctx, "decr_counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), finalVal)
}
```

- [ ] **Step 2: Run test**

Run: `go test -v -run TestStringConcurrent_DecrRace ./cmd/integration/`
Expected: PASS

- [ ] **Step 3: Add TestStringConcurrent_SetexRace — concurrent SETEX operations**

```go
// TestStringConcurrent_SetexRace tests concurrent SETEX on same key
func TestStringConcurrent_SetexRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 20

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.SetEx(ctx, "setex_concurrent_key", fmt.Sprintf("value%d_%d", idx, j), 10*time.Second)
			}
		}(i)
	}

	wg.Wait()

	// Key should exist with some value
	val, err := testClient.Get(ctx, "setex_concurrent_key").Result()
	assert.NoError(t, err)
	assert.True(t, len(val) > 0)
}
```

- [ ] **Step 4: Run test**

Run: `go test -v -run TestStringConcurrent_SetexRace ./cmd/integration/`
Expected: PASS

- [ ] **Step 5: Add TestStringConcurrent_GetrangeSetrangeRace — concurrent GETRANGE/SETRANGE**

```go
// TestStringConcurrent_GetrangeSetrangeRace tests concurrent GETRANGE and SETRANGE
func TestStringConcurrent_GetrangeSetrangeRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 5
	const opsPerGoroutine = 50

	// Initialize string
	testClient.Set(ctx, "range_race_key", "abcdefghij", 0)

	var wg sync.WaitGroup

	// SETRANGE goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				offset := j % 10
				testClient.SetRange(ctx, "range_race_key", offset, fmt.Sprintf("%d", idx))
			}
		}(i)
	}

	// GETRANGE goroutines
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.GetRange(ctx, "range_race_key", 0, -1)
			}
		}()
	}

	wg.Wait()

	// Key should still be a valid string
	val, err := testClient.Get(ctx, "range_race_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(val))
}
```

- [ ] **Step 6: Run test**

Run: `go test -v -run TestStringConcurrent_GetrangeSetrangeRace ./cmd/integration/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add String concurrent depth tests (DECR, SETEX, GETRANGE/SETRANGE)"
```

---

### Task 4: Add String Error Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestStringError_TypeMismatchIntegration — string commands on wrong types**

```go
// TestStringError_TypeMismatchIntegration tests string commands on wrong types (integration level)
func TestStringError_TypeMismatchIntegration(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Create a hash key
	testClient.HSet(ctx, "myhash", "field", "value")

	// APPEND on hash should return WRONGTYPE
	err := testClient.Append(ctx, "myhash", "extra").Err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")

	// DECR on hash should return WRONGTYPE
	err = testClient.Decr(ctx, "myhash").Err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")

	// DECRBY on hash should return WRONGTYPE
	err = testClient.DecrBy(ctx, "myhash", 1).Err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")

	// GETRANGE on hash should return WRONGTYPE
	_, err = testClient.GetRange(ctx, "myhash", 0, -1).Result()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")

	// SETEX on hash should return WRONGTYPE
	err = testClient.SetEx(ctx, "myhash", "value", 10).Err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGTYPE")
}
```

- [ ] **Step 2: Run test**

Run: `go test -v -run TestStringError_TypeMismatchIntegration ./cmd/integration/`
Expected: PASS

- [ ] **Step 3: Add TestStringError_DecrbyOnFloat — DECRBY on float value**

```go
// TestStringError_DecrbyOnFloat tests DECRBY on float string value
func TestStringError_DecrbyOnFloat(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	testClient.Set(ctx, "float_key", "1.5", 0)
	err := testClient.DecrBy(ctx, "float_key", 1).Err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not an integer")
}
```

- [ ] **Step 4: Run test**

Run: `go test -v -run TestStringError_DecrbyOnFloat ./cmd/integration/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add String error integration tests (WRONGTYPE, DECRBY on float)"
```

---

### Task 5: Final Verification

- [ ] **Step 1: Run all depth tests to verify everything passes**

Run: `go test -v -run "TestString" ./cmd/integration/ ./internal/server/`
Expected: All PASS

- [ ] **Step 2: Run full integration suite to check for regressions**

Run: `go test ./cmd/integration/... 2>&1 | tail -20`
Expected: All PASS (no regressions)

- [ ] **Step 3: Run linter**

Run: `golangci-lint run`
Expected: 0 issues
