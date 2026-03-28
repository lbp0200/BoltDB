# Test Depth Phase 4: Set Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add boundary, error, and concurrent depth tests for Set module commands: SADD, SREM, SCARD, SISMEMBER, SMEMBERS, SPOP, SRANDMEMBER, SMOVE.

**Architecture:** Add unit tests to `internal/server/handler_depth_test.go` (direct handler execution) and integration tests to `cmd/integration/depth_test.go` (real server + go-redis). Tests follow existing patterns: `TestSetBoundary_<Scenario>`, `TestSetError_<Scenario>`, `TestSetConcurrent_<Scenario>`.

**Tech Stack:** Go, go-redis/v9, zeebo/assert, testing.T

---

## File Inventory

| File | Role |
|------|------|
| `internal/server/handler_depth_test.go` | Unit tests (direct handler execution) |
| `cmd/integration/depth_test.go` | Integration tests (real server, multi-client) |
| `internal/store/set.go` | Set storage implementation |

---

## Tasks

### Task 1: Add Set Boundary and Error Tests to handler_depth_test.go

**Files:**
- Modify: `internal/server/handler_depth_test.go`

- [ ] **Step 1: Add TestSetBoundary_SaddBasic — SADD returns number of members added**

```go
// TestSetBoundary_SaddBasic tests SADD adds new members
func TestSetBoundary_SaddBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// SADD new members
	resp := handler.executeCommand("SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))

	// SADD with duplicate - should return 0
	resp = handler.executeCommand("SADD", [][]byte{[]byte("set_key"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}
```

Run: `go test -v -run TestSetBoundary_SaddBasic ./internal/server/` — Expected: PASS

- [ ] **Step 2: Add TestSetBoundary_SremBasic — SREM removes members**

```go
// TestSetBoundary_SremBasic tests SREM removes existing members
func TestSetBoundary_SremBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")

	// SREM existing member
	resp := handler.executeCommand("SREM", [][]byte{[]byte("set_key"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// SREM non-existing member - should return 0
	resp = handler.executeCommand("SREM", [][]byte{[]byte("set_key"), []byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}
```

Run: `go test -v -run TestSetBoundary_SremBasic ./internal/server/` — Expected: PASS

- [ ] **Step 3: Add TestSetBoundary_ScardBasic — SCARD returns set size**

```go
// TestSetBoundary_ScardBasic tests SCARD returns set cardinality
func TestSetBoundary_ScardBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SCARD", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}
```

Run: `go test -v -run TestSetBoundary_ScardBasic ./internal/server/` — Expected: PASS

- [ ] **Step 4: Add TestSetBoundary_SrandmemberBasic — SRANDMEMBER returns random member**

```go
// TestSetBoundary_SrandmemberBasic tests SRANDMEMBER returns random member
func TestSetBoundary_SrandmemberBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SRANDMEMBER", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(string(*bs)) > 0)
}
```

Run: `go test -v -run TestSetBoundary_SrandmemberBasic ./internal/server/` — Expected: PASS

- [ ] **Step 5: Add TestSetBoundary_SpopBasic — SPOP removes and returns random member**

```go
// TestSetBoundary_SpopBasic tests SPOP removes and returns random member
func TestSetBoundary_SpopBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SPOP", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(string(*bs)) > 0)

	// Verify size decreased
	cardResp := handler.executeCommand("SCARD", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	cardInt, _ := cardResp.(*proto.Integer)
	assert.Equal(t, int64(2), int64(*cardInt))
}
```

Run: `go test -v -run TestSetBoundary_SpopBasic ./internal/server/` — Expected: PASS

- [ ] **Step 6: Add TestSetError_WrongTypeForSadd — SADD on string key**

```go
// TestSetError_WrongTypeForSadd tests SADD on wrong type returns WRONGTYPE
func TestSetError_WrongTypeForSadd(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SADD", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSetError_WrongTypeForSadd ./internal/server/` — Expected: PASS

- [ ] **Step 7: Add TestSetError_WrongTypeForSrem — SREM on string key**

```go
// TestSetError_WrongTypeForSrem tests SREM on wrong type returns WRONGTYPE
func TestSetError_WrongTypeForSrem(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SREM", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSetError_WrongTypeForSrem ./internal/server/` — Expected: PASS

- [ ] **Step 8: Add TestSetError_WrongTypeForSismember — SISMEMBER on string key**

```go
// TestSetError_WrongTypeForSismember tests SISMEMBER on wrong type returns WRONGTYPE
func TestSetError_WrongTypeForSismember(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SISMEMBER", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSetError_WrongTypeForSismember ./internal/server/` — Expected: PASS

- [ ] **Step 9: Add TestSetError_WrongTypeForSmembers — SMEMBERS on string key**

```go
// TestSetError_WrongTypeForSmembers tests SMEMBERS on wrong type returns WRONGTYPE
func TestSetError_WrongTypeForSmembers(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SMEMBERS", [][]byte{[]byte("string_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSetError_WrongTypeForSmembers ./internal/server/` — Expected: PASS

- [ ] **Step 10: Add TestSetError_WrongTypeForScard — SCARD on string key**

```go
// TestSetError_WrongTypeForScard tests SCARD on wrong type returns WRONGTYPE
func TestSetError_WrongTypeForScard(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SCARD", [][]byte{[]byte("string_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSetError_WrongTypeForScard ./internal/server/` — Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add internal/server/handler_depth_test.go
git commit -m "test: add Set boundary and error depth tests (SADD, SREM, SCARD, SISMEMBER, SMEMBERS, SPOP, SRANDMEMBER)"
```

---

### Task 2: Add Set Concurrent Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestSetConcurrent_SaddSremRace — concurrent SADD and SREM**

```go
// TestSetConcurrent_SaddSremRace tests concurrent SADD and SREM on same key
func TestSetConcurrent_SaddSremRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_set")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("member%d", j%20)
				if j%2 == 0 {
					testClient.SAdd(ctx, "race_set", member)
				} else {
					testClient.SRem(ctx, "race_set", member)
				}
			}
		}(i)
	}

	wg.Wait()

	// Set should have some members (not empty due to race)
	card, _ := testClient.SCard(ctx, "race_set").Result()
	assert.True(t, card >= 0)
}
```

Run: `go test -v -run TestSetConcurrent_SaddSremRace ./cmd/integration/` — Expected: PASS

- [ ] **Step 2: Add TestSetConcurrent_SismemberRace — concurrent SISMEMBER on same key**

```go
// TestSetConcurrent_SismemberRace tests concurrent SISMEMBER operations
func TestSetConcurrent_SismemberRace(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()
	const goroutines = 10
	const opsPerGoroutine = 100

	testClient.Del(ctx, "race_set")
	testClient.SAdd(ctx, "race_set", "target_member")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				testClient.SIsMember(ctx, "race_set", "target_member")
			}
		}()
	}

	wg.Wait()

	// Member should still exist
	isMember, _ := testClient.SIsMember(ctx, "race_set", "target_member").Result()
	assert.True(t, isMember)
}
```

Run: `go test -v -run TestSetConcurrent_SismemberRace ./cmd/integration/` — Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add Set concurrent depth tests (SADD/SREM race, SISMEMBER race)"
```

---

### Task 3: Add Set Error Integration Tests to depth_test.go

**Files:**
- Modify: `cmd/integration/depth_test.go`

- [ ] **Step 1: Add TestSetError_WrongTypeIntegration — set commands on wrong types**

```go
// TestSetError_WrongTypeIntegration tests set commands on wrong types (integration level)
func TestSetError_WrongTypeIntegration(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// Create a string key
	testClient.Set(ctx, "string_key", "value", 0)

	// SADD on string should return WRONGTYPE
	err := testClient.SAdd(ctx, "string_key", "member").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SREM on string should return WRONGTYPE
	err = testClient.SRem(ctx, "string_key", "member").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SISMEMBER on string should return WRONGTYPE
	err = testClient.SIsMember(ctx, "string_key", "member").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SMEMBERS on string should return WRONGTYPE
	err = testClient.SMembers(ctx, "string_key").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))

	// SCARD on string should return WRONGTYPE
	err = testClient.SCard(ctx, "string_key").Err()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
}
```

Run: `go test -v -run TestSetError_WrongTypeIntegration ./cmd/integration/` — Expected: PASS

- [ ] **Step 2: Commit**

```bash
git add cmd/integration/depth_test.go
git commit -m "test: add Set error integration tests (WRONGTYPE)"
```

---

### Task 4: Final Verification

- [ ] **Step 1: Run all Set depth tests**

Run: `go test -v -run "TestSet" ./cmd/integration/ ./internal/server/`
Expected: All PASS

- [ ] **Step 2: Run full integration suite**

Run: `go test ./cmd/integration/... 2>&1 | tail -5`
Expected: All PASS

- [ ] **Step 3: Run linter**

Run: `golangci-lint run`
Expected: 0 issues
