package server

import (
	"strconv"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestExecuteCommand_DUMP_NonExistent_Coverage tests DUMP command on non-existent key
func TestExecuteCommand_DUMP_NonExistent_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// DUMP on non-existent key returns nil BulkString
	resp := handler.executeCommand(state, "DUMP", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, *bs == nil)
}

// TestExecuteCommand_OBJECT_REFCOUNT_Coverage tests OBJECT REFCOUNT command
func TestExecuteCommand_OBJECT_REFCOUNT_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key
	handler.executeCommand(state, "SET", [][]byte{[]byte("testkey"), []byte("testvalue")}, "127.0.0.1:12345")

	// OBJECT REFCOUNT returns the number of references (always 1 for a normal key)
	resp := handler.executeCommand(state, "OBJECT", [][]byte{[]byte("REFCOUNT"), []byte("testkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_OBJECT_FREQ_Coverage tests OBJECT FREQ: always 0 for an
// existing key (BoltDB has no LFU), nil for a non-existent key — consistent
// with the other OBJECT subcommands (REFCOUNT/ENCODING/IDLETIME).
func TestExecuteCommand_OBJECT_FREQ_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Non-existent key → nil (RESP2 empty bulk, RESP3 null)
	resp := handler.executeCommand(state, "OBJECT", [][]byte{[]byte("FREQ"), []byte("freq_nonexist")}, "127.0.0.1:12345")
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	// NewBulkString(nil) 返回非 nil 指针、值为 nil
	assert.Equal(t, proto.BulkString(nil), *bulk)

	// Existing key → 0 (no LFU support)
	handler.executeCommand(state, "SET", [][]byte{[]byte("freq_key"), []byte("v")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "OBJECT", [][]byte{[]byte("FREQ"), []byte("freq_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_OBJECT_IDLETIME_Coverage tests OBJECT IDLETIME: 0 for an
// existing key (BadgerDB tracks no access time), nil for a non-existent key —
// consistent with the other OBJECT subcommands (REFCOUNT/ENCODING/FREQ).
func TestExecuteCommand_OBJECT_IDLETIME_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Non-existent key → nil (RESP2 empty bulk, RESP3 null)
	resp := handler.executeCommand(state, "OBJECT", [][]byte{[]byte("IDLETIME"), []byte("idle_nonexist")}, "127.0.0.1:12345")
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	// NewBulkString(nil) 返回非 nil 指针、值为 nil
	assert.Equal(t, proto.BulkString(nil), *bulk)

	// Existing key → 0 (no access-time tracking)
	handler.executeCommand(state, "SET", [][]byte{[]byte("idle_key"), []byte("v")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "OBJECT", [][]byte{[]byte("IDLETIME"), []byte("idle_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_CLIENT_NOEVICT2_Coverage tests CLIENT NOEVICT command
func TestExecuteCommand_CLIENT_NOEVICT2_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Default: noevict off
	assert.False(t, state.noEvict.Load())

	// CLIENT NOEVICT ON — must set the noEvict flag
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("NOEVICT"), []byte("ON")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
	assert.True(t, state.noEvict.Load())

	// CLIENT NOEVICT OFF — must clear the flag
	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("NOEVICT"), []byte("OFF")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
	assert.False(t, state.noEvict.Load())
}

// TestExecuteCommand_CLIENT_NOEVICT_Error_Coverage tests CLIENT NOEVICT with invalid args
func TestExecuteCommand_CLIENT_NOEVICT_Error_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// CLIENT NOEVICT with invalid arg
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("NOEVICT")}, "127.0.0.1:12345")
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "wrong number of arguments"))
}

// TestExecuteCommand_CLIENT_TRACKING_Coverage2 tests CLIENT TRACKING command
func TestExecuteCommand_CLIENT_TRACKING_Coverage2(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// CLIENT TRACKING on
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("TRACKING"), []byte("ON")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// CLIENT TRACKING off
	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("TRACKING"), []byte("OFF")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_CLIENT_TRACKING_Error_Coverage tests CLIENT TRACKING with invalid args
func TestExecuteCommand_CLIENT_TRACKING_Error_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// CLIENT TRACKING with invalid arg
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("TRACKING"), []byte("INVALID")}, "127.0.0.1:12345")
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "ERR"))
}

// TestExecuteCommand_PFINFO_Coverage tests PFINFO command
func TestExecuteCommand_PFINFO_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// PFADD first
	handler.executeCommand(state, "PFADD", [][]byte{[]byte("myhyperloglog"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	// PFINFO
	resp := handler.executeCommand(state, "PFINFO", [][]byte{[]byte("myhyperloglog")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 2)
}

// TestExecuteCommand_SWAPDB_Coverage2 tests SWAPDB command
func TestExecuteCommand_SWAPDB_Coverage2(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key in database 0
	handler.executeCommand(state, "SET", [][]byte{[]byte("key0"), []byte("value0")}, "127.0.0.1:12345")

	// Select database 1 and set a key
	handler.executeCommand(state, "SELECT", [][]byte{[]byte("1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("key1"), []byte("value1")}, "127.0.0.1:12345")

	// SWAPDB 0 1
	resp := handler.executeCommand(state, "SWAPDB", [][]byte{[]byte("0"), []byte("1")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_RANDOMKEY_Coverage2 tests RANDOMKEY command
func TestExecuteCommand_RANDOMKEY_Coverage2(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set some keys
	handler.executeCommand(state, "SET", [][]byte{[]byte("key1"), []byte("value1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("key2"), []byte("value2")}, "127.0.0.1:12345")

	// RANDOMKEY
	resp := handler.executeCommand(state, "RANDOMKEY", nil, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, string(*bs) == "key1" || string(*bs) == "key2")
}

// TestExecuteCommand_CLIENT_SETINFO_Coverage verifies CLIENT SETINFO stores
// LIB-NAME/LIB-VER on the connection (previously a no-op that returned OK
// without persisting anything), visible in CLIENT INFO output.
func TestExecuteCommand_CLIENT_SETINFO_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Invalid option rejected
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("SETINFO"), []byte("BOGUS"), []byte("x")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))

	// LIB-NAME stored
	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("SETINFO"), []byte("LIB-NAME"), []byte("go-redis")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
	assert.Equal(t, "go-redis", state.clientInfo.LibName)

	// LIB-VER stored
	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("SETINFO"), []byte("LIB-VER"), []byte("9.17.2")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
	assert.Equal(t, "9.17.2", state.clientInfo.LibVer)

	// CLIENT INFO shows both values
	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("INFO")}, "127.0.0.1:12345")
	info, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*info), "lib-name=go-redis"))
	assert.True(t, strings.Contains(string(*info), "lib-ver=9.17.2"))
}

// TestExecuteCommand_BITFIELD_RO_Coverage tests BITFIELD_RO (read-only
// variant, GET-only): reads succeed, non-GET operations are rejected.
func TestExecuteCommand_BITFIELD_RO_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SET a bit value first via BITFIELD
	resp := handler.executeCommand(state, "BITFIELD", [][]byte{[]byte("bfro"), []byte("SET"), []byte("u8"), []byte("0"), []byte("65")}, "127.0.0.1:12345")
	assert.NotNil(t, resp)

	// BITFIELD_RO GET returns the stored value (single op → Integer)
	resp = handler.executeCommand(state, "BITFIELD_RO", [][]byte{[]byte("bfro"), []byte("GET"), []byte("u8"), []byte("0")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(65), int64(*integer))

	// Non-GET operation must be rejected
	resp = handler.executeCommand(state, "BITFIELD_RO", [][]byte{[]byte("bfro"), []byte("SET"), []byte("u8"), []byte("0"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "only supports the GET"))
}

// TestExecuteCommand_TS_REVRANGE_Coverage tests TS.REVRANGE (reverse range):
// returns timestamps in descending order, COUNT caps the result.
func TestExecuteCommand_TS_REVRANGE_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add three points at increasing timestamps
	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tsrev"), []byte("100"), []byte("1.0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tsrev"), []byte("200"), []byte("2.0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tsrev"), []byte("300"), []byte("3.0")}, "127.0.0.1:12345")

	// Full reverse range: [300, 200, 100]
	resp := handler.executeCommand(state, "TS.REVRANGE", [][]byte{[]byte("tsrev"), []byte("-"), []byte("+")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 6, len(arr.Args)) // 3 points × (timestamp, value)
	assert.Equal(t, "300", string(arr.Args[0]))
	assert.Equal(t, "200", string(arr.Args[2]))
	assert.Equal(t, "100", string(arr.Args[4]))

	// COUNT 1 caps the result to one point
	resp = handler.executeCommand(state, "TS.REVRANGE", [][]byte{[]byte("tsrev"), []byte("-"), []byte("+"), []byte("COUNT"), []byte("1")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "300", string(arr.Args[0]))
}

// =====================================================================
// Regression tests for behavior fixes that shipped without配套测试
// =====================================================================

// TestCLIENT_SETNAME_LengthLimit verifies commit 736a4f1: names up to 128
// bytes are accepted; 129 bytes must return an error (Redis compat).
func TestCLIENT_SETNAME_LengthLimit(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// 128 bytes: should succeed
	name128 := strings.Repeat("a", 128)
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("SETNAME"), []byte(name128)}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// 129 bytes: must error
	name129 := strings.Repeat("b", 129)
	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("SETNAME"), []byte(name129)}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR CLIENT NAME must not be longer than 128"))
}

// TestCONFIG_SET_UnknownParam verifies commit de9d5f8: known no-op params
// (save, appendonly, etc.) are silently accepted; unknown params must error.
func TestCONFIG_SET_UnknownParam(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Known no-op params must be accepted silently
	for _, param := range []string{"save", "appendonly", "maxmemory", "maxmemory-policy", "slowlog-log-slower-than", "slowlog-max-len"} {
		resp := handler.executeCommand(state, "CONFIG", [][]byte{[]byte("SET"), []byte(param), []byte("0")}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)
	}

	// Unknown param must error
	resp := handler.executeCommand(state, "CONFIG", [][]byte{[]byte("SET"), []byte("unknown-param-xyz"), []byte("0")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR unsupported config parameter"))
}

// TestCOPY_PreservesTTL verifies commit ddfc36e: COPY must propagate the
// source key's TTL to the destination key (Redis compat).
func TestCOPY_PreservesTTL(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SETEX key 120 value → key has TTL ≈ 120s
	handler.executeCommand(state, "SETEX", [][]byte{[]byte("copy_src"), []byte("120"), []byte("hello")}, "127.0.0.1:12345")

	// COPY copy_src → copy_dst
	resp := handler.executeCommand(state, "COPY", [][]byte{[]byte("copy_src"), []byte("copy_dst")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify value copied
	getResp := handler.executeCommand(state, "GET", [][]byte{[]byte("copy_dst")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))

	// Verify TTL propagated (should be ≈ 120, allow 2s tolerance for test runtime)
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("copy_dst")}, "127.0.0.1:12345")
	ttlInt, ok := ttlResp.(*proto.Integer)
	assert.True(t, ok)
	ttl := int64(*ttlInt)
	assert.True(t, ttl > 100 && ttl <= 120) // within tolerance
}

// TestCOPY_NoTTLKey verifies COPY of a key without TTL does NOT set a TTL
// on the destination (TTL should be -1, meaning no expiry).
func TestCOPY_NoTTLKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("notTL_src"), []byte("val")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "COPY", [][]byte{[]byte("notTL_src"), []byte("notTL_dst")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("notTL_dst")}, "127.0.0.1:12345")
	ttlInt, ok := ttlResp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*ttlInt)) // -1 = no expiry
}

// TestXINFO_CONSUMERS_IdleField verifies commit 13454f3: XINFO CONSUMERS
// reports the field name "idle" (computed ms since last seen) instead of the
// old "seen" field, and the value must be a non-negative integer.
func TestXINFO_CONSUMERS_IdleField(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry, create group and consumer
	handler.executeCommand(state, "XADD", [][]byte{[]byte("xinf_stream"), []byte("1"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xinf_stream"), []byte("xinf_grp"), []byte("0")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
	resp = handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATECONSUMER"), []byte("xinf_stream"), []byte("xinf_grp"), []byte("xinf_cons")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// XINFO CONSUMERS must list the consumer with name/idle field pairs
	infoResp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("CONSUMERS"), []byte("xinf_stream"), []byte("xinf_grp")}, "127.0.0.1:12345")
	arr, ok := infoResp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Elems))

	consumerArr, ok := arr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(consumerArr.Elems)) // name, <value>, idle, <value>
	assert.Equal(t, "name", string(*consumerArr.Elems[0].(*proto.BulkString)))
	assert.Equal(t, "xinf_cons", string(*consumerArr.Elems[1].(*proto.BulkString)))
	assert.Equal(t, "idle", string(*consumerArr.Elems[2].(*proto.BulkString)))
	idleStr := string(*consumerArr.Elems[3].(*proto.BulkString))
	idleVal, err := strconv.ParseInt(idleStr, 10, 64)
	assert.NoError(t, err)
	assert.True(t, idleVal >= 0)
}
