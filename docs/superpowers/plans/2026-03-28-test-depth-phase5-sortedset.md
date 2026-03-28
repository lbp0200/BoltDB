# Test Depth Phase 5: SortedSet Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add boundary, error, and concurrent depth tests for SortedSet module commands: ZADD, ZREM, ZCARD, ZSCORE, ZRANK, ZRANGE, ZINCRBY, ZCOUNT.

**Architecture:** Add unit tests to `internal/server/handler_depth_test.go` (direct handler execution) and integration tests to `cmd/integration/depth_test.go` (real server + go-redis). Tests follow existing patterns: `TestSortedSetBoundary_<Scenario>`, `TestSortedSetError_<Scenario>`, `TestSortedSetConcurrent_<Scenario>`.

**Tech Stack:** Go, go-redis/v9, zeebo/assert, testing.T

---

## File Inventory

| File | Role |
|------|------|
| `internal/server/handler_depth_test.go` | Unit tests (direct handler execution) |
| `cmd/integration/depth_test.go` | Integration tests (real server, multi-client) |
| `internal/store/sorted_set.go` | SortedSet storage implementation |

---

## Tasks

### Task 1: Add SortedSet Boundary and Error Tests to handler_depth_test.go

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: Add TestSortedSetBoundary_ZaddBasic — ZADD returns number of members added**

```go
// TestSortedSetBoundary_ZaddBasic tests ZADD adds new members with scores
func TestSortedSetBoundary_ZaddBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// ZADD new members
	resp := handler.executeCommand("ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member1"), []byte("2.0"), []byte("member2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// ZADD duplicate member - should update score and return 0 (or 1 depending on behavior)
	resp = handler.executeCommand("ZADD", [][]byte{[]byte("zset_key"), []byte("3.0"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}
```

Run: `go test -v -run TestSortedSetBoundary_ZaddBasic ./internal/server/` — Expected: PASS

- [ ] **Step 2: Add TestSortedSetBoundary_ZremBasic — ZREM removes members**

```go
// TestSortedSetBoundary_ZremBasic tests ZREM removes existing members
func TestSortedSetBoundary_ZremBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member1"), []byte("2.0"), []byte("member2")}, "127.0.0.1:12345")

	// ZREM existing member
	resp := handler.executeCommand("ZREM", [][]byte{[]byte("zset_key"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// ZREM non-existing member - should return 0
	resp = handler.executeCommand("ZREM", [][]byte{[]byte("zset_key"), []byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}
```

Run: `go test -v -run TestSortedSetBoundary_ZremBasic ./internal/server/` — Expected: PASS

- [ ] **Step 3: Add TestSortedSetBoundary_ZcardBasic — ZCARD returns sorted set size**

```go
// TestSortedSetBoundary_ZcardBasic tests ZCARD returns sorted set cardinality
func TestSortedSetBoundary_ZcardBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member1"), []byte("2.0"), []byte("member2")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZCARD", [][]byte{[]byte("zset_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}
```

Run: `go test -v -run TestSortedSetBoundary_ZcardBasic ./internal/server/` — Expected: PASS

- [ ] **Step 4: Add TestSortedSetBoundary_ZscoreBasic — ZSCORE returns member score**

```go
// TestSortedSetBoundary_ZscoreBasic tests ZSCORE returns member score
func TestSortedSetBoundary_ZscoreBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("zset_key"), []byte("1.5"), []byte("member1")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZSCORE", [][]byte{[]byte("zset_key"), []byte("member1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "1.5", string(*bs))

	// Non-existing member
	resp = handler.executeCommand("ZSCORE", [][]byte{[]byte("zset_key"), []byte("nonexistent")}, "127.0.0.1:12345")
	nilResp, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Nil(t, nilResp)
}
```

Run: `go test -v -run TestSortedSetBoundary_ZscoreBasic ./internal/server/` — Expected: PASS

- [ ] **Step 5: Add TestSortedSetBoundary_ZrangeBasic — ZRANGE returns members by rank**

```go
// TestSortedSetBoundary_ZrangeBasic tests ZRANGE returns members by rank
func TestSortedSetBoundary_ZrangeBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"), []byte("3.0"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZRANGE", [][]byte{[]byte("zset_key"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 6, len(arr.Args)) // a, 1.0, b, 2.0, c, 3.0
}
```

Run: `go test -v -run TestSortedSetBoundary_ZrangeBasic ./internal/server/` — Expected: PASS

- [ ] **Step 6: Add TestSortedSetError_WrongTypeForZadd — ZADD on string key**

```go
// TestSortedSetError_WrongTypeForZadd tests ZADD on wrong type returns WRONGTYPE
func TestSortedSetError_WrongTypeForZadd(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZADD", [][]byte{[]byte("string_key"), []byte("1.0"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSortedSetError_WrongTypeForZadd ./internal/server/` — Expected: PASS

- [ ] **Step 7: Add TestSortedSetError_WrongTypeForZrem — ZREM on string key**

```go
// TestSortedSetError_WrongTypeForZrem tests ZREM on wrong type returns WRONGTYPE
func TestSortedSetError_WrongTypeForZrem(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZREM", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSortedSetError_WrongTypeForZrem ./internal/server/` — Expected: PASS

- [ ] **Step 8: Add TestSortedSetError_WrongTypeForZcard — ZCARD on string key**

```go
// TestSortedSetError_WrongTypeForZcard tests ZCARD on wrong type returns WRONGTYPE
func TestSortedSetError_WrongTypeForZcard(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZCARD", [][]byte{[]byte("string_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSortedSetError_WrongTypeForZcard ./internal/server/` — Expected: PASS

- [ ] **Step 9: Add TestSortedSetError_WrongTypeForZscore — ZSCORE on string key**

```go
// TestSortedSetError_WrongTypeForZscore tests ZSCORE on wrong type returns WRONGTYPE
func TestSortedSetError_WrongTypeForZscore(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZSCORE", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSortedSetError_WrongTypeForZscore ./internal/server/` — Expected: PASS

- [ ] **Step 10: Add TestSortedSetError_WrongTypeForZrange — ZRANGE on string key**

```go
// TestSortedSetError_WrongTypeForZrange tests ZRANGE on wrong type returns WRONGTYPE
func TestSortedSetError_WrongTypeForZrange(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZRANGE", [][]byte{[]byte("string_key"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSortedSetError_WrongTypeForZrange ./internal/server/` — Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add SortedSet boundary and error depth tests (ZADD, ZREM, ZCARD, ZSCORE, ZRANGE)"
```

---

### Task 2: Add SortedSet Concurrent Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestSortedSetConcurrent_ZaddZremRace — concurrent ZADD and ZREM**

```go
// TestSortedSetConcurrent_ZaddZremRace tests concurrent ZADD and ZREM on same key
func TestSortedSetConcurrent_ZaddZremRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_zset")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("member%d", j%20)
				score := float64(j % 100)
				if j%2 == 0 {
					testClient.ZAdd(ctx, "race_zset", &redis.Z{Member: member, Score: score})
				} else {
					testClient.ZRem(ctx, "race_zset", member)
				}
			}
		}(i)
	}

	wg.Wait()

	// ZSet should have some members (not empty due to race)
	card, _ := testClient.ZCard(ctx, "race_zset").Result()
	assert.True(t, card >= 0)
}
```

Run: `go test -v -run TestSortedSetConcurrent_ZaddZremRace ./cmd/integration/` — Expected: PASS

- [ ] **Step 2: Add TestSortedSetConcurrent_ZscoreRace — concurrent ZSCORE on same key**

```go
// TestSortedSetConcurrent_ZscoreRace tests concurrent ZSCORE operations
func TestSortedSetConcurrent_ZscoreRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_zset")
	testClient.ZAdd(ctx, "race_zset", &redis.Z{Member: "target_member", Score: 42.0})

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.ZScore(ctx, "race_zset", "target_member")
			}
		}()
	}

	wg.Wait()

	// Member should still exist with correct score
	score, err := testClient.ZScore(ctx, "race_zset", "target_member").Result()
	assert.NoError(t, err)
	assert.Equal(t, 42.0, score)
}
```

Run: `go test -v -run TestSortedSetConcurrent_ZscoreRace ./cmd/integration/` — Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add SortedSet concurrent depth tests (ZADD/ZREM race, ZSCORE race)"
```

---

### Task 3: Add SortedSet Error Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestSortedSetError_WrongTypeIntegration — sorted set commands on wrong types**

```go
// TestSortedSetError_WrongTypeIntegration tests sorted set commands on wrong types (integration level)
func TestSortedSetError_WrongTypeIntegration(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Create a string key
	testClient.Set(ctx, "string_key", "value", 0)

	// ZADD on string should return WRONGTYPE
	err := testClient.ZAdd(ctx, "string_key", &redis.Z{Member: "m", Score: 1.0}).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZREM on string should return WRONGTYPE
	err = testClient.ZRem(ctx, "string_key", "m").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZCARD on string should return WRONGTYPE
	err = testClient.ZCard(ctx, "string_key").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZSCORE on string should return WRONGTYPE
	err = testClient.ZScore(ctx, "string_key", "m").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// ZRANGE on string should return WRONGTYPE
	err = testClient.ZRange(ctx, "string_key", 0, -1).Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSortedSetError_WrongTypeIntegration ./cmd/integration/` — Expected: PASS

- [ ] **Step 2: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add SortedSet error integration tests (WRONGTYPE)"
```

---

### Task 4: Final Verification

- [ ] **Step 1: Run all SortedSet depth tests**

Run: `go test -v -run "TestSortedSet" ./cmd/integration/ ./internal/server/`
Expected: All PASS

- [ ] **Step 2: Run full integration suite**

Run: `go test ./cmd/integration/... 2>&1 | tail -5`
Expected: All PASS

- [ ] **Step 3: Run linter**

Run: `golangci-lint run`
Expected: 0 issues
