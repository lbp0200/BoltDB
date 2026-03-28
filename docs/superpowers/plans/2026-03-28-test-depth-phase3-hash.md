# Test Depth Phase 3: Hash Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add boundary, error, and concurrent depth tests for Hash module commands: HSET, HGET, HSETNX, HINCRBY, HINCRBYFLOAT, HDEL, HLEN, HKEYS, HVALS, HGETALL, HEXISTS, HMSET, HMGET, HSTREN, HRANDFIELD.

**Architecture:** Add unit tests to `internal/server/handler_depth_test.go` (direct handler execution) and integration tests to `cmd/integration/depth_test.go` (real server + go-redis). Tests follow existing patterns: `TestHashBoundary_<Scenario>`, `TestHashError_<Scenario>`, `TestHashConcurrent_<Scenario>`.

**Tech Stack:** Go, go-redis/v9, zeebo/assert, testing.T

---

## File Inventory

| File | Role |
|------|------|
| `internal/server/handler_depth_test.go` | Unit tests (direct handler execution) |
| `cmd/integration/depth_test.go` | Integration tests (real server, multi-client) |
| `internal/store/hash.go` | Hash storage implementation |

---

## Tasks

### Task 1: Add Hash Boundary and Error Tests to handler_depth_test.go

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: Add TestHashBoundary_HsetNxBasic — HSETNX sets new field**

```go
// TestHashBoundary_HsetNxBasic tests HSETNX sets field only if not exists
func TestHashBoundary_HsetNxBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("val1")}, "127.0.0.1:12345")

	// HSETNX on existing field should return 0
	resp := handler.executeCommand("HSETNX", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("new_val")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// HSETNX on new field should return 1
	resp = handler.executeCommand("HSETNX", [][]byte{[]byte("hash_key"), []byte("field2"), []byte("val2")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}
```

Run: `go test -v -run TestHashBoundary_HsetNxBasic ./internal/server/` — Expected: PASS

- [ ] **Step 2: Add TestHashBoundary_HincrbyBasic — HINCRBY increments field**

```go
// TestHashBoundary_HincrbyBasic tests HINCRBY increments field value
func TestHashBoundary_HincrbyBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("10")}, "127.0.0.1:12345")

	// HINCRBY should increment
	resp := handler.executeCommand("HINCRBY", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("5")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(15), int64(*integer))
}
```

Run: `go test -v -run TestHashBoundary_HincrbyBasic ./internal/server/` — Expected: PASS

- [ ] **Step 3: Add TestHashBoundary_HincrbyNegative — HINCRBY with negative increment**

```go
// TestHashBoundary_HincrbyNegative tests HINCRBY with negative increment
func TestHashBoundary_HincrbyNegative(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("10")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HINCRBY", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("-3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(7), int64(*integer))
}
```

Run: `go test -v -run TestHashBoundary_HincrbyNegative ./internal/server/` — Expected: PASS

- [ ] **Step 4: Add TestHashBoundary_HincrbyFloatBasic — HINCRBYFLOAT increments field**

```go
// TestHashBoundary_HincrbyFloatBasic tests HINCRBYFLOAT increments field value
func TestHashBoundary_HincrbyFloatBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("10.5")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HINCRBYFLOAT", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("2.5")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "13", string(*bs)) // 10.5 + 2.5 = 13
}
```

Run: `go test -v -run TestHashBoundary_HincrbyFloatBasic ./internal/server/` — Expected: PASS

- [ ] **Step 5: Add TestHashBoundary_HrandfieldBasic — HRANDFIELD returns random field**

```go
// TestHashBoundary_HrandfieldBasic tests HRANDFIELD returns random field
func TestHashBoundary_HrandfieldBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("val1")}, "127.0.0.1:12345")
	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field2"), []byte("val2")}, "127.0.0.1:12345")
	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field3"), []byte("val3")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HRANDFIELD", [][]byte{[]byte("hash_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(string(*bs)) > 0)
}
```

Run: `go test -v -run TestHashBoundary_HrandfieldBasic ./internal/server/` — Expected: PASS

- [ ] **Step 6: Add TestHashBoundary_HrandfieldWithValues — HRANDFIELD with count and WITHVALUES**

```go
// TestHashBoundary_HrandfieldWithValues tests HRANDFIELD with count and WITHVALUES
func TestHashBoundary_HrandfieldWithValues(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("val1")}, "127.0.0.1:12345")
	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field2"), []byte("val2")}, "127.0.0.1:12345")

	// HRANDFIELD key 2 WITHVALUES returns array of [field1, val1, field2, val2]
	resp := handler.executeCommand("HRANDFIELD", [][]byte{[]byte("hash_key"), []byte("2"), []byte("WITHVALUES")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr))
}
```

Run: `go test -v -run TestHashBoundary_HrandfieldWithValues ./internal/server/` — Expected: PASS

- [ ] **Step 7: Add TestHashError_WrongTypeForHset — HSET on string key**

```go
// TestHashError_WrongTypeForHset tests HSET on wrong type returns WRONGTYPE
func TestHashError_WrongTypeForHset(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HSET", [][]byte{[]byte("string_key"), []byte("field"), []byte("val")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestHashError_WrongTypeForHset ./internal/server/` — Expected: PASS

- [ ] **Step 8: Add TestHashError_WrongTypeForHget — HGET on string key**

```go
// TestHashError_WrongTypeForHget tests HGET on wrong type returns WRONGTYPE
func TestHashError_WrongTypeForHget(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HGET", [][]byte{[]byte("string_key"), []byte("field")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestHashError_WrongTypeForHget ./internal/server/` — Expected: PASS

- [ ] **Step 9: Add TestHashError_HincrbyOnNonNumeric — HINCRBY on non-numeric value**

```go
// TestHashError_HincrbyOnNonNumeric tests HINCRBY on non-numeric value returns error
func TestHashError_HincrbyOnNonNumeric(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field"), []byte("not_a_number")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HINCRBY", [][]byte{[]byte("hash_key"), []byte("field"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}
```

Run: `go test -v -run TestHashError_HincrbyOnNonNumeric ./internal/server/` — Expected: PASS

- [ ] **Step 10: Add TestHashError_HincrbyFloatOnNonNumeric — HINCRBYFLOAT on non-numeric value**

```go
// TestHashError_HincrbyFloatOnNonNumeric tests HINCRBYFLOAT on non-numeric value returns error
func TestHashError_HincrbyFloatOnNonNumeric(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash_key"), []byte("field"), []byte("not_a_number")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HINCRBYFLOAT", [][]byte{[]byte("hash_key"), []byte("field"), []byte("1.5")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not a valid float"))
}
```

Run: `go test -v -run TestHashError_HincrbyFloatOnNonNumeric ./internal/server/` — Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add Hash boundary and error depth tests (HSETNX, HINCRBY, HINCRBYFLOAT, HRANDFIELD)"
```

---

### Task 2: Add Hash Concurrent Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestHashConcurrent_HgetHsetRace — concurrent HGET and HSET**

```go
// TestHashConcurrent_HgetHsetRace tests concurrent HGET and HSET on same key
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
				field := fmt.Sprintf("field%d", j%10)
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

Run: `go test -v -run TestHashConcurrent_HgetHsetRace ./cmd/integration/` — Expected: PASS

- [ ] **Step 2: Add TestHashConcurrent_HincrbyRace — concurrent HINCRBY on same field**

```go
// TestHashConcurrent_HincrbyRace tests concurrent HINCRBY on same field
func TestHashConcurrent_HincrbyRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const incrsPerGoroutine = 100

	testClient.Del(ctx, "incr_hash")
	testClient.HSet(ctx, "incr_hash", "counter", "0")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrsPerGoroutine; j++ {
				testClient.HIncrBy(ctx, "incr_hash", "counter", 1)
			}
		}()
	}

	wg.Wait()

	// Final value should be exactly goroutines * incrsPerGoroutine
	finalVal, err := testClient.HGet(ctx, "incr_hash", "counter").Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(goroutines*incrsPerGoroutine), finalVal)
}
```

Run: `go test -v -run TestHashConcurrent_HincrbyRace ./cmd/integration/` — Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add Hash concurrent depth tests (HGET/HSET race, HINCRBY race)"
```

---

### Task 3: Add Hash Error Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestHashError_WrongTypeIntegration — hash commands on wrong types**

```go
// TestHashError_WrongTypeIntegration tests hash commands on wrong types (integration level)
func TestHashError_WrongTypeIntegration(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Create a string key
	testClient.Set(ctx, "string_key", "value", 0)

	// HSET on string should return WRONGTYPE
	err := testClient.HSet(ctx, "string_key", "field", "val").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// HGET on string should return WRONGTYPE
	err = testClient.HGet(ctx, "string_key", "field").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// HINCRBY on string should return WRONGTYPE
	err = testClient.HIncrBy(ctx, "string_key", "field", 1).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// HDEL on string should return WRONGTYPE
	err = testClient.HDel(ctx, "string_key", "field").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}
```

Run: `go test -v -run TestHashError_WrongTypeIntegration ./cmd/integration/` — Expected: PASS

- [ ] **Step 2: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add Hash error integration tests (WRONGTYPE)"
```

---

### Task 4: Final Verification

- [ ] **Step 1: Run all Hash depth tests**

Run: `go test -v -run "TestHash" ./cmd/integration/ ./internal/server/`
Expected: All PASS

- [ ] **Step 2: Run full integration suite**

Run: `go test ./cmd/integration/... 2>&1 | tail -5`
Expected: All PASS

- [ ] **Step 3: Run linter**

Run: `golangci-lint run`
Expected: 0 issues
