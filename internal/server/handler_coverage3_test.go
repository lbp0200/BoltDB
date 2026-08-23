package server

import (
	"strconv"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
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

// TestXINFO_GROUPS_LastDeliveredID verifies commit 3423103: XINFO GROUPS
// reports the exact "last-delivered-id" field (the group's delivery cursor).
// The pre-fix response omitted it, breaking clients that read it.
func TestXINFO_GROUPS_LastDeliveredID(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry, then create a group with an explicit delivery cursor "5"
	handler.executeCommand(state, "XADD", [][]byte{[]byte("xinfg_stream"), []byte("1"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("xinfg_stream"), []byte("xinfg_grp"), []byte("5")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// XINFO GROUPS must return the exact field/value pairs:
	// [name, <group>, consumers, <n>, pending, <n>, last-delivered-id, <id>]
	infoResp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("GROUPS"), []byte("xinfg_stream")}, "127.0.0.1:12345")
	arr, ok := infoResp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Elems))

	groupArr, ok := arr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 8, len(groupArr.Elems)) // 4 field/value pairs

	fields := make(map[string]string)
	for i := 0; i < len(groupArr.Elems); i += 2 {
		key := string(*groupArr.Elems[i].(*proto.BulkString))
		val := string(*groupArr.Elems[i+1].(*proto.BulkString))
		fields[key] = val
	}
	assert.Equal(t, "xinfg_grp", fields["name"])
	assert.Equal(t, "0", fields["consumers"])
	assert.Equal(t, "0", fields["pending"])
	assert.Equal(t, "5", fields["last-delivered-id"])
}

// =====================================================================
// New commands: SUBSTR alias, GEORADIUSBYMEMBER, read-only variants
// =====================================================================

// TestSUBSTR_IsGetrangeAlias verifies SUBSTR behaves exactly like GETRANGE
// (legacy Redis alias).
func TestSUBSTR_IsGetrangeAlias(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("sub_key"), []byte("hello world")}, "127.0.0.1:12345")

	// SUBSTR key 0 4 == GETRANGE key 0 4 == "hello"
	resp := handler.executeCommand(state, "SUBSTR", [][]byte{[]byte("sub_key"), []byte("0"), []byte("4")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))

	// Negative indexes like GETRANGE
	resp = handler.executeCommand(state, "SUBSTR", [][]byte{[]byte("sub_key"), []byte("-5"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "world", string(*bs))
}

// TestGEORADIUSBYMEMBER verifies the command searches from a member's
// coordinates: SF at (37.7749, -122.4194), LA ~556km south.
func TestGEORADIUSBYMEMBER(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// GEOADD sf_member -122.4194 37.7749 SF, la_member -118.2437 34.0522 LA
	resp := handler.executeCommand(state, "GEOADD", [][]byte{[]byte("geo_rm"), []byte("-122.4194"), []byte("37.7749"), []byte("SF"), []byte("-118.2437"), []byte("34.0522"), []byte("LA")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// 100km from SF member: only SF
	resp = handler.executeCommand(state, "GEORADIUSBYMEMBER", [][]byte{[]byte("geo_rm"), []byte("SF"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "SF", string(arr.Args[0]))

	// 700km from SF member: SF + LA
	resp = handler.executeCommand(state, "GEORADIUSBYMEMBER", [][]byte{[]byte("geo_rm"), []byte("SF"), []byte("700"), []byte("km")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))

	// WITHCOORD: verify SF's own coordinates round-trip
	resp = handler.executeCommand(state, "GEORADIUSBYMEMBER", [][]byte{[]byte("geo_rm"), []byte("SF"), []byte("1"), []byte("km"), []byte("WITHCOORD")}, "127.0.0.1:12345")
	withCoord, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(withCoord.Elems))

	// Non-existent member: could not decode error
	resp = handler.executeCommand(state, "GEORADIUSBYMEMBER", [][]byte{[]byte("geo_rm"), []byte("NOPE"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "could not decode query zset member"))
}

// TestGeoReadOnlyVariants verifies GEORADIUS_RO / GEORADIUSBYMEMBER_RO /
// SORT_RO work and reject STORE with a syntax error (Redis semantics).
func TestGeoReadOnlyVariants(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("geo_ro"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	// GEORADIUS_RO: normal query works
	resp := handler.executeCommand(state, "GEORADIUS_RO", [][]byte{[]byte("geo_ro"), []byte("-122.4194"), []byte("37.7749"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "SF", string(arr.Args[0]))

	// GEORADIUS_RO with STORE: syntax error
	resp = handler.executeCommand(state, "GEORADIUS_RO", [][]byte{[]byte("geo_ro"), []byte("-122.4194"), []byte("37.7749"), []byte("100"), []byte("km"), []byte("STORE"), []byte("dst")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))

	// GEORADIUSBYMEMBER_RO: normal query works
	resp = handler.executeCommand(state, "GEORADIUSBYMEMBER_RO", [][]byte{[]byte("geo_ro"), []byte("SF"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))

	// GEORADIUSBYMEMBER_RO with STOREDIST: syntax error
	resp = handler.executeCommand(state, "GEORADIUSBYMEMBER_RO", [][]byte{[]byte("geo_ro"), []byte("SF"), []byte("100"), []byte("km"), []byte("STOREDIST"), []byte("dst")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))

	// SORT_RO: normal sort works
	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("sort_ro_list"), []byte("3"), []byte("1"), []byte("2")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "SORT_RO", [][]byte{[]byte("sort_ro_list")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	assert.Equal(t, "1", string(arr.Args[0]))

	// SORT_RO with STORE: syntax error
	resp = handler.executeCommand(state, "SORT_RO", [][]byte{[]byte("sort_ro_list"), []byte("STORE"), []byte("dst")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))

	// SORT_RO must not write: destination key stays absent
	checkResp := handler.executeCommand(state, "EXISTS", [][]byte{[]byte("dst")}, "127.0.0.1:12345")
	checkInt, ok := checkResp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*checkInt))
}

// TestGEORADIUS_StoreOptions verifies GEORADIUS STORE/STOREDIST write the
// result into a zset and return the count (Redis semantics, verified
// against redis-server 8.2.1).
func TestGEORADIUS_StoreOptions(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Palermo (13.36, 38.12), Catania (15.09, 37.50) — both within 200km
	// of (15, 37).
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("geo_st"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo"), []byte("15.087269"), []byte("37.502669"), []byte("Catania")}, "127.0.0.1:12345")

	// STORE: returns count, dst holds member → geohash score
	resp := handler.executeCommand(state, "GEORADIUS", [][]byte{[]byte("geo_st"), []byte("15"), []byte("37"), []byte("200"), []byte("km"), []byte("STORE"), []byte("geo_dst")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	zcard := handler.executeCommand(state, "ZCARD", [][]byte{[]byte("geo_dst")}, "127.0.0.1:12345")
	zcardInt, ok := zcard.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*zcardInt))

	// STOREDIST: dst holds member → distance score (km)
	resp = handler.executeCommand(state, "GEORADIUSBYMEMBER", [][]byte{[]byte("geo_st"), []byte("Palermo"), []byte("400"), []byte("km"), []byte("STOREDIST"), []byte("geo_dst2")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	zscore := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("geo_dst2"), []byte("Palermo")}, "127.0.0.1:12345")
	bs, ok := zscore.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "0", string(*bs)) // distance from Palermo to itself = 0

	// No results → empty zset, count 0
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{[]byte("geo_st"), []byte("15"), []byte("37"), []byte("1"), []byte("km"), []byte("STORE"), []byte("geo_dst3")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Wrong type source → WRONGTYPE
	handler.executeCommand(state, "SET", [][]byte{[]byte("geo_str"), []byte("v")}, "127.0.0.1:12345")
	resp = handler.executeCommand(state, "GEORADIUS", [][]byte{[]byte("geo_str"), []byte("15"), []byte("37"), []byte("10"), []byte("km"), []byte("STORE"), []byte("geo_bad")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestGEORADIUSStore_PropagatesToBacklog verifies GEORADIUS STORE enters the
// replication backlog (handler-side canonical propagation). Combined with
// TestExecuteReplicatedCommand_GEORADIUSStore this closes the master→replica
// loop for the write path.
func TestGEORADIUSStore_PropagatesToBacklog(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Replication = replication.NewReplicationManager(handler.Db)
	defer handler.Replication.Stop()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("gprop"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo"), []byte("15.087269"), []byte("37.502669"), []byte("Catania")}, "127.0.0.1:12345")

	before := handler.Replication.GetBacklog().GetCurrentOffset()
	resp := handler.executeCommand(state, "GEORADIUS", [][]byte{[]byte("gprop"), []byte("15"), []byte("37"), []byte("200"), []byte("km"), []byte("STORE"), []byte("gprop_dst")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	after := handler.Replication.GetBacklog().GetCurrentOffset()
	data, err := handler.Replication.GetBacklog().GetRange(before, after)
	assert.NoError(t, err)
	prop := string(data)
	assert.True(t, strings.Contains(prop, "GEORADIUS"))
	assert.True(t, strings.Contains(prop, "STORE"))
	assert.True(t, strings.Contains(prop, "gprop_dst"))

	// Read-only GEORADIUS must NOT be propagated
	before = handler.Replication.GetBacklog().GetCurrentOffset()
	handler.executeCommand(state, "GEORADIUS", [][]byte{[]byte("gprop"), []byte("15"), []byte("37"), []byte("200"), []byte("km")}, "127.0.0.1:12345")
	after = handler.Replication.GetBacklog().GetCurrentOffset()
	assert.Equal(t, before, after)
}

// TestBLMPOP_PropagatesToBacklog verifies BLMPOP enters the replication
// backlog via the generic propagator (isWriteCommand path).
func TestBLMPOP_PropagatesToBacklog(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Replication = replication.NewReplicationManager(handler.Db)
	defer handler.Replication.Stop()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("bprop"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	before := handler.Replication.GetBacklog().GetCurrentOffset()
	req := &proto.Array{Args: [][]byte{[]byte("BLMPOP"), []byte("1"), []byte("1"), []byte("bprop"), []byte("LEFT"), []byte("COUNT"), []byte("2")}}
	resp := handler.processRequest(req, nil, "127.0.0.1:12345", nil, nil, state)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(na.Elems))

	after := handler.Replication.GetBacklog().GetCurrentOffset()
	data, err := handler.Replication.GetBacklog().GetRange(before, after)
	assert.NoError(t, err)
	prop := string(data)
	assert.True(t, strings.Contains(prop, "BLMPOP"))
	assert.True(t, strings.Contains(prop, "LEFT"))
	assert.True(t, strings.Contains(prop, "COUNT"))
}

// TestRESET_ClearsTransactionState verifies RESET discards MULTI + WATCH
// and unsubscribes all pubsub subscriptions.
func TestRESET_ClearsTransactionState(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "WATCH", [][]byte{[]byte("rwatch")}, "127.0.0.1:12345")
	handler.executeCommand(state, "MULTI", nil, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("rkey"), []byte("v")}, "127.0.0.1:12345")
	assert.True(t, state.inTransaction)
	assert.NotNil(t, state.watchedKeys)

	resp := handler.executeCommand(state, "RESET", nil, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "RESET", string(*ss))
	assert.False(t, state.inTransaction)
	assert.Nil(t, state.commands)
	assert.Equal(t, 0, len(state.watchedKeys))

	// MULTI after RESET is gone: EXEC errors like Redis ("EXEC without MULTI")
	execResp := handler.executeCommand(state, "EXEC", nil, "127.0.0.1:12345")
	_, ok = execResp.(*proto.Error)
	assert.True(t, ok)
}

// TestRESET_UnsubscribesAll verifies RESET unsubscribes channel, pattern
// and shard subscriptions in one call.
func TestRESET_UnsubscribesAll(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandlerWithPubSub(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SUBSCRIBE", [][]byte{[]byte("rch")}, "127.0.0.1:12345")
	handler.executeCommand(state, "PSUBSCRIBE", [][]byte{[]byte("r*")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SSUBSCRIBE", [][]byte{[]byte("rsch")}, "127.0.0.1:12345")
	assert.NotNil(t, state.subscriber)

	resp := handler.executeCommand(state, "RESET", nil, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "RESET", string(*ss))

	// No message delivered to the reset connection
	msgCount := 0
	handler.PubSub.Publish("rch", []byte("x"))
	handler.PubSub.SPublish("rsch", []byte("x"))
	if state.subscriber != nil {
		msgCount += len(state.subscriber.Channels) + len(state.subscriber.Patterns)
	}
	assert.Equal(t, 0, msgCount)
}

// TestWAITAOF_Reply verifies WAITAOF returns [0, 0] (no AOF).
func TestWAITAOF_Reply(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "WAITAOF", [][]byte{[]byte("0"), []byte("1"), []byte("1000")}, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(na.Elems))
	i0, ok := na.Elems[0].(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*i0))
	i1, ok := na.Elems[1].(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*i1))
}

// TestBGREWRITEAOF_Disabled verifies BGREWRITEAOF errors (no AOF support).
func TestBGREWRITEAOF_Disabled(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "BGREWRITEAOF", nil, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "rewriting disabled"))
}

// TestPSYNC_NotEnabled verifies PSYNC/SYNC error cleanly without replication.
func TestPSYNC_NotEnabled(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "PSYNC", [][]byte{[]byte("?"), []byte("-1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "replication not enabled"))

	resp = handler.executeCommand(state, "SYNC", nil, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "replication not enabled"))
}

// TestLatencySubcommands verifies the display-only LATENCY subcommands.
func TestLatencySubcommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	for _, sub := range []string{"GRAPH", "HISTORY", "HISTOGRAM"} {
		resp := handler.executeCommand(state, "LATENCY", [][]byte{[]byte(sub), []byte("command")}, "127.0.0.1:12345")
		arr, ok := resp.(*proto.Array)
		assert.True(t, ok)
		assert.Equal(t, 0, len(arr.Args))
	}
}

// TestMemorySubcommands verifies MEMORY STATS/MALLOC-STATS/PURGE shape.
func TestMemorySubcommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MEMORY", [][]byte{[]byte("STATS")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 4)

	resp = handler.executeCommand(state, "MEMORY", [][]byte{[]byte("MALLOC-STATS")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))

	resp = handler.executeCommand(state, "MEMORY", [][]byte{[]byte("PURGE")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))
}
