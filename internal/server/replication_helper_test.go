package server

import (
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/zeebo/assert"
)

func TestIsWriteCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		cmd      string
		expected bool
	}{
		// String commands
		{"SET", true},
		{"SETEX", true},
		{"PSETEX", true},
		{"SETNX", true},
		{"GETSET", true},
		{"MSET", true},
		{"MSETNX", true},
		{"INCR", true},
		{"INCRBY", true},
		{"DECR", true},
		{"DECRBY", true},
		{"INCRBYFLOAT", true},
		{"APPEND", true},
		{"SETRANGE", true},
		{"GET", false},

		// Key commands
		{"DEL", true},
		{"EXPIRE", true},
		{"EXPIREAT", true},
		{"PEXPIRE", true},
		{"PEXPIREAT", true},
		{"PERSIST", true},
		{"RENAME", true},
		{"RENAMENX", true},
		{"EXISTS", false},
		{"TYPE", false},
		{"TTL", false},
		{"PTTL", false},

		// List commands
		{"LPUSH", true},
		{"RPUSH", true},
		{"LPOP", true},
		{"RPOP", true},
		{"LSET", true},
		{"LTRIM", true},
		{"LINSERT", true},
		{"LREM", true},
		{"RPOPLPUSH", true},
		{"LPUSHX", true},
		{"RPUSHX", true},
		{"LRANGE", false},
		{"LLEN", false},

		// Hash commands
		{"HSET", true},
		{"HDEL", true},
		{"HMSET", true},
		{"HSETNX", true},
		{"HINCRBY", true},
		{"HINCRBYFLOAT", true},
		{"HGET", false},
		{"HMGET", false},
		{"HGETALL", false},
		{"HLEN", false},

		// Set commands
		{"SADD", true},
		{"SREM", true},
		{"SPOP", true}, // SPOP is a write command (propagates as SREM internally)
		{"SMOVE", true},
		{"SINTERSTORE", true},
		{"SUNIONSTORE", true},
		{"SDIFFSTORE", true},
		{"SMEMBERS", false},
		{"SISMEMBER", false},
		{"SCARD", false},

		// Sorted set commands
		{"ZADD", true},
		{"ZREM", true},
		{"ZINCRBY", true},
		{"ZRANGE", false},
		{"ZRANK", false},
		{"ZSCORE", false},

		// GEO commands
		{"GEOADD", true},
		{"GEOSEARCHSTORE", true},
		{"GEOPOS", false},
		{"GEODIST", false},

		// Stream commands
		{"XADD", true},
		{"XDEL", true},
		{"XACK", true},
		{"XCLAIM", true},
		{"XREADGROUP", true},
		{"XAUTOCLAIM", true},
		{"XGROUP", true},
		{"XTRIM", true},
		{"XRANGE", false},
		{"XREAD", false},

		// Unknown commands
		{"UNKNOWN", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.cmd, func(t *testing.T) {
			result := isWriteCommand(tt.cmd)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldPropagateCommand(t *testing.T) {
	t.Parallel()
	// Propagated via generic processRequest path
	assert.True(t, shouldPropagateCommand("SET"))
	assert.True(t, shouldPropagateCommand("SREM"))
	assert.True(t, shouldPropagateCommand("SORT"))
	// Stream/TS extensions now have slave handlers
	assert.True(t, shouldPropagateCommand("XACKDEL"))
	assert.True(t, shouldPropagateCommand("BZMPOP"))
	assert.True(t, shouldPropagateCommand("TS.MADD"))
	assert.True(t, shouldPropagateCommand("XNACK"))
	// PEL mutators must enter the stream (live XCLAIM depends on XREADGROUP prop)
	assert.True(t, shouldPropagateCommand("XREADGROUP"))
	assert.True(t, shouldPropagateCommand("XCLAIM"))
	assert.True(t, shouldPropagateCommand("XAUTOCLAIM"))
	// SPOP: handler-only SREM path
	assert.False(t, shouldPropagateCommand("SPOP"))
	// Still excluded (external side effects / no store path)
	assert.False(t, shouldPropagateCommand("MIGRATE"))
	assert.False(t, shouldPropagateCommand("PUBLISH"))
}

func TestIsErrorResponse(t *testing.T) {
	t.Parallel()
	assert.True(t, isErrorResponse(nil))
	assert.True(t, isErrorResponse(proto.NewError("ERR boom")))
	// Value-type Error (if ever boxed without pointer)
	var errVal proto.Error = "ERR val"
	assert.True(t, isErrorResponse(errVal))
	assert.False(t, isErrorResponse(proto.OK))
	assert.False(t, isErrorResponse(proto.NewInteger(1)))
	assert.False(t, isErrorResponse(proto.NewBulkString([]byte("x"))))
	assert.False(t, isErrorResponse(proto.NewSimpleString("PONG")))
}

// TestProcessRequestPropagateGate documents the oracle used in processRequest:
// write + shouldPropagate + NOT error → may enter backlog.
// Mutating isErrorResponse to always false would reintroduce failed-write prop.
func TestProcessRequestPropagateGate(t *testing.T) {
	t.Parallel()
	shouldEnterBacklog := func(cmd string, resp proto.RESP) bool {
		return isWriteCommand(cmd) && shouldPropagateCommand(cmd) && !isErrorResponse(resp)
	}
	assert.True(t, shouldEnterBacklog("SET", proto.OK))
	assert.True(t, shouldEnterBacklog("XREADGROUP", &proto.NestedArray{}))
	assert.False(t, shouldEnterBacklog("GET", proto.NewBulkString([]byte("v"))))
	assert.False(t, shouldEnterBacklog("SPOP", proto.NewBulkString([]byte("m"))))
	assert.False(t, shouldEnterBacklog("SET", proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")))
	assert.False(t, shouldEnterBacklog("SET", nil))
	assert.False(t, shouldEnterBacklog("MIGRATE", proto.OK))
	assert.False(t, shouldEnterBacklog("PUBLISH", proto.NewInteger(1)))
}

// TestProcessRequest_WrongTypeDoesNotAdvanceOffset is an end-to-end gate:
// WRONGTYPE must not enter the replication backlog (offset unchanged).
// Kills mutation that drops `!isErrorResponse(resp)` from processRequest.
func TestProcessRequest_WrongTypeDoesNotAdvanceOffset(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Replication = replication.NewReplicationManager(handler.Db)

	// Seed a hash so SADD hits WRONGTYPE
	setReq := &proto.Array{Args: [][]byte{[]byte("HSET"), []byte("wt:k"), []byte("f"), []byte("v")}}
	resp := handler.processRequest(setReq, nil, "127.0.0.1:1", nil, nil, state)
	assert.False(t, isErrorResponse(resp))
	before := handler.Replication.GetMasterReplOffset()
	assert.True(t, before > 0)

	wrong := &proto.Array{Args: [][]byte{[]byte("SADD"), []byte("wt:k"), []byte("member")}}
	resp = handler.processRequest(wrong, nil, "127.0.0.1:1", nil, nil, state)
	assert.True(t, isErrorResponse(resp))
	afterWrong := handler.Replication.GetMasterReplOffset()
	assert.Equal(t, before, afterWrong)

	// Successful write still advances offset
	okReq := &proto.Array{Args: [][]byte{[]byte("SET"), []byte("wt:ok"), []byte("1")}}
	resp = handler.processRequest(okReq, nil, "127.0.0.1:1", nil, nil, state)
	assert.False(t, isErrorResponse(resp))
	afterOK := handler.Replication.GetMasterReplOffset()
	assert.True(t, afterOK > afterWrong)
}

// TestProcessRequest_WriteFenceBlocksFullresyncLock is the production-path
// Issue #3 guard: processRequest SET must take snapshotMu.RLock, so a FULLRESYNC
// write lock already held blocks the SET. If the fence is removed from
// processRequest (retryUpdate no longer RLocks), SET would finish under the WR.
func TestProcessRequest_WriteFenceBlocksFullresyncLock(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Replication = replication.NewReplicationManager(handler.Db)
	defer handler.Replication.Stop()

	handler.Db.SnapshotMuLock()
	done := make(chan proto.RESP, 1)
	go func() {
		req := &proto.Array{Args: [][]byte{[]byte("SET"), []byte("fence:k"), []byte("v")}}
		done <- handler.processRequest(req, nil, "127.0.0.1:1", nil, nil, state)
	}()
	select {
	case resp := <-done:
		t.Fatalf("processRequest SET returned while FULLRESYNC write lock held (%v) — Issue #3 fence missing", resp)
	case <-time.After(80 * time.Millisecond):
	}
	handler.Db.SnapshotMuUnlock()
	select {
	case resp := <-done:
		assert.False(t, isErrorResponse(resp))
	case <-time.After(2 * time.Second):
		t.Fatal("processRequest SET did not complete after write lock release")
	}
}

func TestProcessRequest_EXECFenceBlocksFullresyncLock(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Replication = replication.NewReplicationManager(handler.Db)
	defer handler.Replication.Stop()

	multi := handler.processRequest(&proto.Array{Args: [][]byte{[]byte("MULTI")}}, nil, "127.0.0.1:1", nil, nil, state)
	assert.False(t, isErrorResponse(multi))
	queued := handler.processRequest(&proto.Array{Args: [][]byte{[]byte("SET"), []byte("exec:k"), []byte("v")}}, nil, "127.0.0.1:1", nil, nil, state)
	assert.False(t, isErrorResponse(queued))

	handler.Db.SnapshotMuLock()
	done := make(chan proto.RESP, 1)
	go func() {
		done <- handler.processRequest(&proto.Array{Args: [][]byte{[]byte("EXEC")}}, nil, "127.0.0.1:1", nil, nil, state)
	}()
	select {
	case resp := <-done:
		t.Fatalf("processRequest EXEC returned while FULLRESYNC write lock held (%v) — EXEC fence missing", resp)
	case <-time.After(80 * time.Millisecond):
	}
	handler.Db.SnapshotMuUnlock()
	select {
	case resp := <-done:
		assert.False(t, isErrorResponse(resp))
	case <-time.After(2 * time.Second):
		t.Fatal("processRequest EXEC did not complete after write lock release")
	}
}

func TestProcessRequest_ReadDoesNotTakeWriteFence(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	set := handler.processRequest(&proto.Array{Args: [][]byte{[]byte("SET"), []byte("ro:k"), []byte("v")}}, nil, "127.0.0.1:1", nil, nil, state)
	assert.False(t, isErrorResponse(set))

	handler.Db.SnapshotMuLock()
	defer handler.Db.SnapshotMuUnlock()
	done := make(chan proto.RESP, 1)
	go func() {
		done <- handler.processRequest(&proto.Array{Args: [][]byte{[]byte("GET"), []byte("ro:k")}}, nil, "127.0.0.1:1", nil, nil, state)
	}()
	select {
	case resp := <-done:
		assert.False(t, isErrorResponse(resp))
	case <-time.After(2 * time.Second):
		t.Fatal("GET blocked on snapshotMu write lock — reads must not take the Issue #3 fence")
	}
}

func TestIsWriteCommand_CaseSensitivity(t *testing.T) {
	t.Parallel()
	// isWriteCommand should be case sensitive
	assert.False(t, isWriteCommand("set"))
	assert.False(t, isWriteCommand("Set"))
	assert.False(t, isWriteCommand("get"))
}
