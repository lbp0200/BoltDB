# Test Coverage Improvement Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Increase overall test coverage from 53% to over 70%

**Architecture:** Focus on packages with lowest coverage: server (38.4%), sentinel (42.1%), replication (51.0%). Add unit tests for uncovered functions and command handlers.

**Tech Stack:** Go testing, go test -cover, go tool cover

---

## Current Coverage Analysis

| Package | Current | Target | Gap |
|---------|---------|--------|-----|
| server | 38.4% | 70% | +31.6% |
| sentinel | 42.1% | 70% | +27.9% |
| replication | 51.0% | 70% | +19.0% |
| backup | 51.8% | 70% | +18.2% |
| cluster | 61.2% | 70% | +8.8% |
| store | 62.5% | 70% | +7.5% |
| proto | 78.8% | 70% | ✓ |
| logger | 76.3% | 70% | ✓ |
| helper | 84.0% | 70% | ✓ |

---

## Task 1: Improve Server Package Coverage (38.4% → 65%)

### Files to Modify

**Test files to create:**
- `internal/server/handler_coverage2_test.go` - new test file for uncovered commands
- `internal/server/handler_server2_test.go` - additional server tests

### Step 1: Analyze uncovered commands

Run: `go test -coverprofile=/tmp/server_cov.out ./internal/server/... 2>&1 && go tool cover -func=/tmp/server_cov.out | grep -E "executeCommand.*[0-9]+\.[0-9]%"`

### Step 2: Add tests for uncovered Redis commands

Focus on commands with low coverage in executeCommand switch:
- BITFIELD, BITPOS, BITOP
- SORT, SCAN
- GEO commands
- XREAD, XREADGROUP (stream commands)
- PUBSUB commands
- CLIENT commands
- CONFIG GET/SET

**Step 3: Run tests**

Run: `go test -cover ./internal/server/...`
Expected: Coverage should increase significantly

### Step 4: Commit

```bash
git add internal/server/
git commit -m "test: add server package coverage tests"
```

---

## Task 2: Improve Sentinel Package Coverage (42.1% → 65%)

### Files to Modify

**Test file:** `internal/sentinel/gossip_test.go`

### Step 1: Analyze uncovered functions

Run: `go test -coverprofile=/tmp/sentinel_cov.out ./internal/sentinel/... 2>&1 && go tool cover -func=/tmp/sentinel_cov.out | grep -E "0.0%"`

### Step 2: Add tests for gossip protocol

Test these 0% coverage functions:
- GossipProtocol.Start()
- GossipProtocol.Stop()
- handleConnection
- handleMessage
- handleHello
- handlePing/handlePong

```go
// Example test structure
func TestGossipProtocol_Start(t *testing.T) {
    gp := NewGossipProtocol("localhost:26379", "localhost:26380")
    gp.Start()
    time.Sleep(100 * time.Millisecond)
    gp.Stop()
}
```

### Step 3: Add tests for master monitoring

Test MasterInstance:
- StartMonitoring()
- checkMaster()

### Step 4: Run tests

Run: `go test -cover ./internal/sentinel/...`

### Step 5: Commit

```bash
git add internal/sentinel/
git commit -m "test: add sentinel package coverage tests"
```

---

## Task 3: Improve Replication Package Coverage (51.0% → 65%)

### Files to Modify

**Test file:** `internal/replication/master_test.go`

### Step 1: Analyze uncovered functions

Run: `go test -coverprofile=/tmp/repl_cov.out ./internal/replication/... 2>&1 && go tool cover -func=/tmp/repl_cov.out | grep -E "0.0%"`

### Step 2: Add tests for MasterConnection

Test network functions (requires mock or real connection):
- NewMasterConnection (with dial timeout)
- SendCommand
- ReadResponse
- ReadBulkString
- readUntilEOF

```go
// Use mock net.Conn
func TestMasterConnection_SendCommand(t *testing.T) {
    mock := &mockMasterConn{...}
    mc := &MasterConnection{
        Conn: mock,
        Writer: bufio.NewWriter(mock),
    }
    err := mc.SendCommand([][]byte{[]byte("PING")})
    assert.NoError(t, err)
}
```

### Step 3: Add tests for PSYNC

Test replication functions:
- StartSlaveReplication
- StopSlaveReplication

### Step 4: Run tests

Run: `go test -cover ./internal/replication/...`

### Step 5: Commit

```bash
git add internal/replication/
git commit -m "test: add replication package coverage tests"
```

---

## Task 4: Improve Backup Package Coverage (51.8% → 70%)

### Files to Modify

**Test file:** `internal/backup/backup_coverage_test.go`

### Step 1: Add tests for incremental backup

```go
func TestBadgerBackupManager_IncrementalBackup(t *testing.T) {
    // Setup test store
    dir := t.TempDir()
    opts := badger.DefaultOptions(dir)
    db, err := badger.Open(opts)
    assert.NoError(t, err)
    defer db.Close()

    bbm := NewBadgerBackupManager(db)
    backupFile, err := bbm.IncrementalBackup(dir, 0)
    assert.NoError(t, err)
    assert.True(t, len(backupFile) > 0)
}

func TestBadgerBackupManager_GetBackupInfo(t *testing.T) {
    dir := t.TempDir()
    opts := badger.DefaultOptions(dir)
    db, err := badger.Open(opts)
    assert.NoError(t, err)
    defer db.Close()

    bbm := NewBadgerBackupManager(db)
    info := bbm.GetBackupInfo()
    assert.True(t, info != nil)
}
```

### Step 2: Run tests

Run: `go test -cover ./internal/backup/...`

### Step 3: Commit

```bash
git add internal/backup/
git commit -m "test: add backup package coverage tests"
```

---

## Task 5: Improve Store Package Coverage (62.5% → 70%)

### Files to Modify

**Test file:** `internal/store/coverage_test.go`

### Step 1: Analyze uncovered functions

Run: `go test -coverprofile=/tmp/store_cov.out ./internal/store/... 2>&1 && go tool cover -func=/tmp/store_cov.out | grep -E "0.0%"`

### Step 2: Add tests for legacy and cleanup functions

- readRDBExpireTime
- restoreLegacy
- getListData
- NextStartup
- checkDataExists
- cleanupOrphaned* functions

### Step 3: Run tests

Run: `go test -cover ./internal/store/...`

### Step 4: Commit

```bash
git add internal/store/
git commit -m "test: add store package coverage tests"
```

---

## Task 6: Final Verification

### Step 1: Run full coverage check

Run: `go test -coverprofile=coverage.out ./... 2>&1 && go tool cover -func=coverage.out | grep "total:"`

### Step 2: Verify target reached

Expected: Overall coverage > 70%

### Step 3: Run all tests

Run: `go test ./...`

### Step 4: Run linter

Run: `golangci-lint run ./...`

### Step 5: Final commit

```bash
git add .
git commit -m "test: improve coverage to 70%+"
```

---

## Execution Notes

- Each task should take 15-30 minutes
- Focus on high-impact functions first (executeCommand, processRequest)
- Use table-driven tests for command coverage
- Network functions may require mock connections
- Commit after each task for incremental progress
