package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestExecuteCommand_CLIENT_ID_Coverage tests CLIENT ID command
func TestExecuteCommand_CLIENT_ID_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("ID")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0)
}

// TestExecuteCommand_CLIENT_KILL_Coverage tests CLIENT KILL command
func TestExecuteCommand_CLIENT_KILL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("KILL"), []byte("127.0.0.1:12345")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_CLIENT_PAUSE_Coverage tests CLIENT PAUSE command
func TestExecuteCommand_CLIENT_PAUSE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("PAUSE"), []byte("1000")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_CLIENT_UNPAUSE_Coverage tests CLIENT UNPAUSE command
func TestExecuteCommand_CLIENT_UNPAUSE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("UNPAUSE")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_CLIENT_INFO_Coverage tests CLIENT INFO command
func TestExecuteCommand_CLIENT_INFO_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("INFO")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(*bs) > 0)
}

// TestExecuteCommand_CLIENT_NOEVICT_Coverage tests CLIENT NOEVICT command
func TestExecuteCommand_CLIENT_NOEVICT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("NOEVICT"), []byte("ON")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("NOEVICT"), []byte("OFF")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_CLIENT_TRACKING_Coverage tests CLIENT TRACKING command
func TestExecuteCommand_CLIENT_TRACKING_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("TRACKING"), []byte("ON")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("TRACKING"), []byte("OFF")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_BITFIELD_Coverage tests BITFIELD command
func TestExecuteCommand_BITFIELD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key with known bit pattern
	handler.Db.Set("bitfieldkey", "A") // 0x41 = 01000001

	// GET i8 offset 0 - should return 65
	resp := handler.executeCommand(state, "BITFIELD", [][]byte{[]byte("bitfieldkey"), []byte("GET"), []byte("i8"), []byte("0")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(65), int64(*integer))
}

// TestExecuteCommand_BITPOS_Coverage tests BITPOS command
func TestExecuteCommand_BITPOS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("bitposkey", string([]byte{0xFF}))

	resp := handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bitposkey"), []byte("0")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// 0xFF = all bits are 1, so looking for bit 0 (first 0) returns -1 (not found)
	assert.Equal(t, int64(-1), int64(*integer))

	// Test finding a bit that exists
	handler.Db.Set("bitposkey2", string([]byte{0x00}))
	resp2 := handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bitposkey2"), []byte("1")}, "127.0.0.1:12345")
	integer2, ok := resp2.(*proto.Integer)
	assert.True(t, ok)
	// 0x00 = all bits are 0, looking for bit 1 returns -1 (not found)
	assert.Equal(t, int64(-1), int64(*integer2))

	// Test with mixed value
	handler.Db.Set("bitposkey3", string([]byte{0x80})) // 10000000
	resp3 := handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bitposkey3"), []byte("1")}, "127.0.0.1:12345")
	integer3, ok := resp3.(*proto.Integer)
	assert.True(t, ok)
	// Bit 0 of 0x80 is 1, so first 1 bit is at position 0
	assert.Equal(t, int64(0), int64(*integer3))
}

// TestExecuteCommand_BITOP_Coverage tests BITOP command
func TestExecuteCommand_BITOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("bitopkey1", "A") // 0x41 = 01000001
	handler.Db.Set("bitopkey2", "B") // 0x42 = 01000010

	resp := handler.executeCommand(state, "BITOP", [][]byte{[]byte("AND"), []byte("bitopresult"), []byte("bitopkey1"), []byte("bitopkey2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify result: AND of 0x41 and 0x42 = 0x40 = "@"
	val, _ := handler.Db.Get("bitopresult")
	assert.Equal(t, "@", val)
}

// TestExecuteCommand_SCAN_Coverage tests SCAN command
func TestExecuteCommand_SCAN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("scankey1", "value1")
	handler.Db.Set("scankey2", "value2")

	resp := handler.executeCommand(state, "SCAN", [][]byte{[]byte("0")}, "127.0.0.1:12345")
	// SCAN returns a RawString with pre-serialized RESP array [cursor, [keys...]]
	assert.NotNil(t, resp)
	s := resp.String()
	// Verify the serialized response contains both keys
	assert.True(t, strings.Contains(s, "scankey1"))
	assert.True(t, strings.Contains(s, "scankey2"))
	assert.True(t, strings.Contains(s, "0"))

	// Also verify via Store directly for completeness
	result, err := handler.Db.Scan(0, "*", 10)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.Keys))
	assert.Equal(t, uint64(0), result.Cursor)
}

// TestExecuteCommand_SORT_Coverage tests SORT command
func TestExecuteCommand_SORT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sortlist", "3")
	handler.Db.LPush("sortlist", "1")
	handler.Db.LPush("sortlist", "2")

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// SORT without modifiers returns items in ascending numeric order: 1, 2, 3
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "1", string(arr.Args[0]))
		assert.Equal(t, "2", string(arr.Args[1]))
		assert.Equal(t, "3", string(arr.Args[2]))
	}
}

// TestExecuteCommand_SORT_ASC_Coverage tests SORT with ASC
func TestExecuteCommand_SORT_ASC_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sortlist2", "3")
	handler.Db.LPush("sortlist2", "1")
	handler.Db.LPush("sortlist2", "2")

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist2"), []byte("ASC")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "1", string(arr.Args[0]))
		assert.Equal(t, "2", string(arr.Args[1]))
		assert.Equal(t, "3", string(arr.Args[2]))
	}
}

// TestExecuteCommand_SORT_DESC_Coverage tests SORT with DESC
func TestExecuteCommand_SORT_DESC_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sortlist3", "3")
	handler.Db.LPush("sortlist3", "1")
	handler.Db.LPush("sortlist3", "2")

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist3"), []byte("DESC")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "3", string(arr.Args[0]))
		assert.Equal(t, "2", string(arr.Args[1]))
		assert.Equal(t, "1", string(arr.Args[2]))
	}
}

// TestExecuteCommand_SORT_LIMIT_Coverage tests SORT with LIMIT
func TestExecuteCommand_SORT_LIMIT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sortlist4", "5")
	handler.Db.LPush("sortlist4", "3")
	handler.Db.LPush("sortlist4", "1")
	handler.Db.LPush("sortlist4", "4")
	handler.Db.LPush("sortlist4", "2")

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist4"), []byte("LIMIT"), []byte("1"), []byte("3")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// LIMIT 1 3 should return items at offset 1, count 3: [2, 3, 4]
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "2", string(arr.Args[0]))
		assert.Equal(t, "3", string(arr.Args[1]))
		assert.Equal(t, "4", string(arr.Args[2]))
	}
}

// TestExecuteCommand_SORT_ALPHA_Coverage tests SORT with ALPHA
func TestExecuteCommand_SORT_ALPHA_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sortlist5", "c")
	handler.Db.LPush("sortlist5", "a")
	handler.Db.LPush("sortlist5", "b")

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist5"), []byte("ALPHA")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// ALPHA sorts lexicographically: a, b, c
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "a", string(arr.Args[0]))
		assert.Equal(t, "b", string(arr.Args[1]))
		assert.Equal(t, "c", string(arr.Args[2]))
	}
}

// TestExecuteCommand_SORT_STORE_Coverage tests SORT with STORE
func TestExecuteCommand_SORT_STORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sortlist6", "3")
	handler.Db.LPush("sortlist6", "1")
	handler.Db.LPush("sortlist6", "2")

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist6"), []byte("STORE"), []byte("sortedlist")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))

	// Verify sortedlist contains [1, 2, 3]
	sortedVal, _ := handler.Db.LRange("sortedlist", 0, -1)
	assert.Equal(t, []string{"1", "2", "3"}, sortedVal)
}

// TestExecuteCommand_SORT_BY_Coverage tests SORT with BY
func TestExecuteCommand_SORT_BY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.LPush("sortlist7", "1")
	handler.Db.LPush("sortlist7", "2")
	handler.Db.Set("weight_1", "3")
	handler.Db.Set("weight_2", "1")

	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist7"), []byte("BY"), []byte("weight_*")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// BY weight_*: weight_1=3, weight_2=1. Ascending order: 2 (weight 1), 1 (weight 3)
	assert.Equal(t, 2, len(arr.Args))
	if len(arr.Args) == 2 {
		assert.Equal(t, "2", string(arr.Args[0]))
		assert.Equal(t, "1", string(arr.Args[1]))
	}
}

// TestExecuteCommand_CONFIG_GET_Coverage tests CONFIG GET command
func TestExecuteCommand_CONFIG_GET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CONFIG", [][]byte{[]byte("GET"), []byte("maxmemory")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// CONFIG GET returns [name, value]
	assert.Equal(t, 2, len(arr.Args))
	if len(arr.Args) == 2 {
		assert.Equal(t, "maxmemory", string(arr.Args[0]))
	}
}

// TestExecuteCommand_CONFIG_SET_Coverage tests CONFIG SET command
func TestExecuteCommand_CONFIG_SET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CONFIG", [][]byte{[]byte("SET"), []byte("maxmemory"), []byte("0")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_SLOWLOG_RESET_Coverage tests SLOWLOG RESET command
func TestExecuteCommand_SLOWLOG_RESET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SLOWLOG", [][]byte{[]byte("RESET")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_MEMORY_USAGE_Coverage tests MEMORY USAGE command
func TestExecuteCommand_MEMORY_USAGE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("memkey", "somevalue")

	resp := handler.executeCommand(state, "MEMORY", [][]byte{[]byte("USAGE"), []byte("memkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	memUsage := int64(*integer)
	assert.True(t, memUsage > 0)
	// Memory usage should be at least the size of key + value
	assert.True(t, memUsage >= int64(len("memkey")+len("somevalue")))
}

// TestExecuteCommand_ZRANGESTORE_Coverage tests ZRANGESTORE command
func TestExecuteCommand_ZRANGESTORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrangestore1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})

	// ZRANGESTORE dst src 0 1 stores indices 0 and 1 (a and b) -> returns 2
	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrangestore_result"), []byte("zrangestore1"), []byte("0"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify destination contains members a and b (indices 0,1)
	destMembers, _ := handler.Db.ZRange("zrangestore_result", 0, -1)
	assert.Equal(t, 2, len(destMembers))
	members := make([]string, len(destMembers))
	for i, m := range destMembers {
		members[i] = m.Member
	}
	assert.Equal(t, []string{"a", "b"}, members)
}

// TestExecuteCommand_ZRANGESTORE_BYSCORE_Coverage tests ZRANGESTORE with BYSCORE
func TestExecuteCommand_ZRANGESTORE_BYSCORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrangestore2", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 5}, {Member: "c", Score: 10}})

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrangestore_result2"), []byte("zrangestore2"), []byte("0"), []byte("5"), []byte("BYSCORE")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify destination contains a and b (scores 1 and 5, both <= 5)
	destMembers, _ := handler.Db.ZRange("zrangestore_result2", 0, -1)
	assert.Equal(t, 2, len(destMembers))
	members := make([]string, len(destMembers))
	for i, m := range destMembers {
		members[i] = m.Member
	}
	assert.Equal(t, []string{"a", "b"}, members)
}

// TestExecuteCommand_ZRANGESTORE_BYLEX_Coverage tests ZRANGESTORE with BYLEX
func TestExecuteCommand_ZRANGESTORE_BYLEX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrangestore3", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrangestore_result3"), []byte("zrangestore3"), []byte("-"), []byte("+"), []byte("BYLEX")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))

	// Verify all three members are stored
	destMembers, _ := handler.Db.ZRange("zrangestore_result3", 0, -1)
	assert.Equal(t, 3, len(destMembers))
	members := make([]string, len(destMembers))
	for i, m := range destMembers {
		members[i] = m.Member
	}
	assert.Equal(t, []string{"a", "b", "c"}, members)
}

// TestExecuteCommand_ZRANGESTORE_REV_Coverage tests ZRANGESTORE with REV
func TestExecuteCommand_ZRANGESTORE_REV_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrangestore4", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrangestore_result4"), []byte("zrangestore4"), []byte("2"), []byte("0"), []byte("REV")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))

	// REV from 2 to 0 gives indices 2,1,0 - but ZRANGESTORE stores values, not order
	// The stored zset has members with their scores (a:1, b:2, c:3)
	destMembers, _ := handler.Db.ZRange("zrangestore_result4", 0, -1)
	assert.Equal(t, 3, len(destMembers))
	// Verify all members exist (order is by score ascending)
	memberSet := make(map[string]bool)
	for _, m := range destMembers {
		memberSet[m.Member] = true
	}
	assert.True(t, memberSet["a"])
	assert.True(t, memberSet["b"])
	assert.True(t, memberSet["c"])
}

// TestExecuteCommand_ZRANGESTORE_LIMIT_Coverage tests ZRANGESTORE with LIMIT
func TestExecuteCommand_ZRANGESTORE_LIMIT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrangestore5", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}, {Member: "d", Score: 4}, {Member: "e", Score: 5}})

	resp := handler.executeCommand(state, "ZRANGESTORE", [][]byte{[]byte("zrangestore_result5"), []byte("zrangestore5"), []byte("0"), []byte("+inf"), []byte("BYSCORE"), []byte("LIMIT"), []byte("0"), []byte("2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// LIMIT 0 2 with BYSCORE should return first 2 members by score: a, b
	destMembers, _ := handler.Db.ZRange("zrangestore_result5", 0, -1)
	assert.Equal(t, 2, len(destMembers))
	members := make([]string, len(destMembers))
	for i, m := range destMembers {
		members[i] = m.Member
	}
	assert.Equal(t, []string{"a", "b"}, members)
}

// TestExecuteCommand_LATENCY_RESET_Coverage tests LATENCY RESET command
func TestExecuteCommand_LATENCY_RESET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LATENCY", [][]byte{[]byte("RESET")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_LATENCY_HELP_Coverage tests LATENCY HELP command
func TestExecuteCommand_LATENCY_HELP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LATENCY", [][]byte{[]byte("HELP")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0) // HELP returns array of help strings
}

// TestExecuteCommand_BLMOVE_Coverage tests BLMOVE command
func TestExecuteCommand_BLMOVE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("blmovetest", "value")

	resp := handler.executeCommand(state, "BLMOVE", [][]byte{[]byte("blmovetest"), []byte("blmovetest2"), []byte("RIGHT"), []byte("LEFT"), []byte("0")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*bs))

	// Verify value was moved from blmovetest to blmovetest2
	// Source list should be empty
	srcVals, _ := handler.Db.LRange("blmovetest", 0, -1)
	assert.Equal(t, 0, len(srcVals))
	// Dest list should contain value
	destVals, _ := handler.Db.LRange("blmovetest2", 0, -1)
	assert.Equal(t, []string{"value"}, destVals)
}

// TestExecuteCommand_BLPOP_Coverage tests BLPOP command
func TestExecuteCommand_BLPOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("blpoptest", "value")

	resp := handler.executeCommand(state, "BLPOP", [][]byte{[]byte("blpoptest"), []byte("1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "blpoptest", string(arr.Args[0]))
	assert.Equal(t, "value", string(arr.Args[1]))
}

// TestExecuteCommand_BRPOP_Coverage tests BRPOP command
func TestExecuteCommand_BRPOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("brpoptest", "value")

	resp := handler.executeCommand(state, "BRPOP", [][]byte{[]byte("brpoptest"), []byte("1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "brpoptest", string(arr.Args[0]))
	assert.Equal(t, "value", string(arr.Args[1]))
}

// TestExecuteCommand_BRPOPLPUSH_Coverage tests BRPOPLPUSH command
func TestExecuteCommand_BRPOPLPUSH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("brpoplpushsource", "value")

	resp := handler.executeCommand(state, "BRPOPLPUSH", [][]byte{[]byte("brpoplpushsource"), []byte("brpoplpushdest"), []byte("1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*bs))

	// Verify value was moved from source to dest
	destVals, _ := handler.Db.LRange("brpoplpushdest", 0, -1)
	assert.Equal(t, []string{"value"}, destVals)
	srcVals, _ := handler.Db.LRange("brpoplpushsource", 0, -1)
	assert.Equal(t, 0, len(srcVals))
}

// TestExecuteCommand_BZPOPMAX_Coverage tests BZPOPMAX command
func TestExecuteCommand_BZPOPMAX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("bzpopmaxtest", []store.ZSetMember{{Member: "member", Score: 1}})

	resp := handler.executeCommand(state, "BZPOPMAX", [][]byte{[]byte("bzpopmaxtest"), []byte("1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	assert.Equal(t, "bzpopmaxtest", string(arr.Args[0]))
	assert.Equal(t, "member", string(arr.Args[1]))
	assert.Equal(t, "1", string(arr.Args[2]))
}

// TestExecuteCommand_BZPOPMIN_Coverage tests BZPOPMIN command
func TestExecuteCommand_BZPOPMIN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("bzpopmintest", []store.ZSetMember{{Member: "member", Score: 1}})

	resp := handler.executeCommand(state, "BZPOPMIN", [][]byte{[]byte("bzpopmintest"), []byte("1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	assert.Equal(t, "bzpopmintest", string(arr.Args[0]))
	assert.Equal(t, "member", string(arr.Args[1]))
	assert.Equal(t, "1", string(arr.Args[2]))
}

// TestExecuteCommand_LPUSHX_Coverage tests LPUSHX command
func TestExecuteCommand_LPUSHX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("lpushxtest", "existing")

	resp := handler.executeCommand(state, "LPUSHX", [][]byte{[]byte("lpushxtest"), []byte("newvalue")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify list now contains both values
	vals, _ := handler.Db.LRange("lpushxtest", 0, -1)
	assert.Equal(t, []string{"newvalue", "existing"}, vals) // LPUSHX prepends
}

// TestExecuteCommand_LPUSHX_NotExists_Coverage tests LPUSHX when key doesn't exist
func TestExecuteCommand_LPUSHX_NotExists_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LPUSHX", [][]byte{[]byte("nonexistent"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_RPUSHX_Coverage tests RPUSHX command
func TestExecuteCommand_RPUSHX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("rpushxtest", "existing")

	resp := handler.executeCommand(state, "RPUSHX", [][]byte{[]byte("rpushxtest"), []byte("newvalue")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify list contains both values
	vals, _ := handler.Db.LRange("rpushxtest", 0, -1)
	assert.Equal(t, []string{"existing", "newvalue"}, vals) // RPUSHX appends
}

// TestExecuteCommand_RPUSHX_NotExists_Coverage tests RPUSHX when key doesn't exist
func TestExecuteCommand_RPUSHX_NotExists_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "RPUSHX", [][]byte{[]byte("nonexistent"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_PEXPIRE_Coverage tests PEXPIRE command
func TestExecuteCommand_PEXPIRE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("pexpirekey", "value")

	resp := handler.executeCommand(state, "PEXPIRE", [][]byte{[]byte("pexpirekey"), []byte("1000")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify TTL is set (PTTL should return positive value)
	pttlResp := handler.executeCommand(state, "PTTL", [][]byte{[]byte("pexpirekey")}, "127.0.0.1:12345")
	pttlInt, _ := pttlResp.(*proto.Integer)
	assert.True(t, int64(*pttlInt) > 0 && int64(*pttlInt) <= 1000)
}

// TestExecuteCommand_PEXPIREAT_Coverage tests PEXPIREAT command
func TestExecuteCommand_PEXPIREAT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("pexpireatkey", "value")

	resp := handler.executeCommand(state, "PEXPIREAT", [][]byte{[]byte("pexpireatkey"), []byte("9999999999999")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify TTL is set
	pttlResp := handler.executeCommand(state, "PTTL", [][]byte{[]byte("pexpireatkey")}, "127.0.0.1:12345")
	pttlInt, _ := pttlResp.(*proto.Integer)
	assert.True(t, int64(*pttlInt) > 0)
}

// TestExecuteCommand_EXPIREAT_Coverage tests EXPIREAT command
func TestExecuteCommand_EXPIREAT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("expireatkey", "value")

	resp := handler.executeCommand(state, "EXPIREAT", [][]byte{[]byte("expireatkey"), []byte("9999999999")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify TTL is set
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("expireatkey")}, "127.0.0.1:12345")
	ttlInt, _ := ttlResp.(*proto.Integer)
	assert.True(t, int64(*ttlInt) > 0)
}

// TestExecuteCommand_XREAD_BLOCK_Coverage tests XREAD with BLOCK
func TestExecuteCommand_XREAD_BLOCK_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "XADD", [][]byte{[]byte("blockstream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XREAD", [][]byte{[]byte("BLOCK"), []byte("1000"), []byte("STREAMS"), []byte("blockstream"), []byte("0")}, "127.0.0.1:12345")
	narr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(narr.Elems))
	streamResult, ok := narr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(streamResult.Elems))
	streamKey, ok := streamResult.Elems[0].(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "blockstream", string(*streamKey))
	entries, ok := streamResult.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(entries.Elems))
	entry, ok := entries.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(entry.Elems))
	entryID, ok := entry.Elems[0].(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "1", string(*entryID))
	fields, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(fields.Elems))
	fieldName, ok := fields.Elems[0].(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "field", string(*fieldName))
	fieldVal, ok := fields.Elems[1].(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*fieldVal))
}

// TestExecuteCommand_LINSERT_BEFORE_Coverage tests LINSERT with BEFORE
func TestExecuteCommand_LINSERT_BEFORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("linserttest", "a")
	handler.Db.RPush("linserttest", "c")

	resp := handler.executeCommand(state, "LINSERT", [][]byte{[]byte("linserttest"), []byte("BEFORE"), []byte("c"), []byte("b")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))

	// Verify list: a, b, c
	vals, _ := handler.Db.LRange("linserttest", 0, -1)
	assert.Equal(t, []string{"a", "b", "c"}, vals)
}

// TestExecuteCommand_LINSERT_AFTER_Coverage tests LINSERT with AFTER
func TestExecuteCommand_LINSERT_AFTER_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("linserttest2", "a")
	handler.Db.RPush("linserttest2", "c")

	resp := handler.executeCommand(state, "LINSERT", [][]byte{[]byte("linserttest2"), []byte("AFTER"), []byte("a"), []byte("b")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))

	// Verify list: a, b, c
	vals, _ := handler.Db.LRange("linserttest2", 0, -1)
	assert.Equal(t, []string{"a", "b", "c"}, vals)
}

// TestExecuteCommand_LSET_Coverage tests LSET command
func TestExecuteCommand_LSET_Coverage(t *testing.T) {
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("lsettest", "a")
	handler.Db.RPush("lsettest", "b")
	handler.Db.RPush("lsettest", "c")

	resp := handler.executeCommand(state, "LSET", [][]byte{[]byte("lsettest"), []byte("1"), []byte("newvalue")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify element was changed
	vals, _ := handler.Db.LRange("lsettest", 0, -1)
	assert.Equal(t, []string{"a", "newvalue", "c"}, vals)
}

// TestExecuteCommand_LREM_Coverage tests LREM command
func TestExecuteCommand_LREM_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("lremtest", "a")
	handler.Db.RPush("lremtest", "b")
	handler.Db.RPush("lremtest", "a")

	resp := handler.executeCommand(state, "LREM", [][]byte{[]byte("lremtest"), []byte("1"), []byte("a")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_RPOPLPUSH_Coverage tests RPOPLPUSH command
func TestExecuteCommand_RPOPLPUSH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("rpoplpushtest", "value")

	resp := handler.executeCommand(state, "RPOPLPUSH", [][]byte{[]byte("rpoplpushtest"), []byte("destlist")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*bs))
}

// TestExecuteCommand_GETBIT_Coverage tests GETBIT command
func TestExecuteCommand_GETBIT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("getbittest", "A")

	resp := handler.executeCommand(state, "GETBIT", [][]byte{[]byte("getbittest"), []byte("0")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_SETBIT_Coverage tests SETBIT command
func TestExecuteCommand_SETBIT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SETBIT", [][]byte{[]byte("setbittest"), []byte("7"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_BITLEN_Coverage tests BITLEN command
func TestExecuteCommand_BITLEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("bitlentest", "A")

	resp := handler.executeCommand(state, "BITLEN", [][]byte{[]byte("bitlentest")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(8), int64(*integer))
}

// TestExecuteCommand_ZMSCORE_Coverage tests ZMSCORE command
func TestExecuteCommand_ZMSCORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zmscoretest", []store.ZSetMember{{Member: "member", Score: 1.5}})

	resp := handler.executeCommand(state, "ZMSCORE", [][]byte{[]byte("zmscoretest"), []byte("member")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	if len(arr.Args) == 1 {
		// Score should be "1.5"
		assert.Equal(t, "1.5", string(arr.Args[0]))
	}
}

// TestExecuteCommand_ZRANK_Coverage tests ZRANK command
func TestExecuteCommand_ZRANK_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zranktest", []store.ZSetMember{{Member: "member1", Score: 1}, {Member: "member2", Score: 2}})

	resp := handler.executeCommand(state, "ZRANK", [][]byte{[]byte("zranktest"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_ZREVRANK_Coverage tests ZREVRANK command
func TestExecuteCommand_ZREVRANK_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrevranktest", []store.ZSetMember{{Member: "member1", Score: 1}, {Member: "member2", Score: 2}})

	resp := handler.executeCommand(state, "ZREVRANK", [][]byte{[]byte("zrevranktest"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_ZPOPMAX_Coverage tests ZPOPMAX command
func TestExecuteCommand_ZPOPMAX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zpopmaxtest", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})

	resp := handler.executeCommand(state, "ZPOPMAX", [][]byte{[]byte("zpopmaxtest")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "b", string(arr.Args[0]))
	assert.Equal(t, "2", string(arr.Args[1]))
	// Verify b was removed from zset
	members, _ := handler.Db.ZRange("zpopmaxtest", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "a", members[0].Member)
}

// TestExecuteCommand_ZPOPMIN_Coverage tests ZPOPMIN command
func TestExecuteCommand_ZPOPMIN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zpopmintest", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})

	resp := handler.executeCommand(state, "ZPOPMIN", [][]byte{[]byte("zpopmintest")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "a", string(arr.Args[0]))
	assert.Equal(t, "1", string(arr.Args[1]))
	// Verify a was removed from zset
	members, _ := handler.Db.ZRange("zpopmintest", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "b", members[0].Member)
}

// TestExecuteCommand_ZUNIONSTORE_Coverage tests ZUNIONSTORE command
func TestExecuteCommand_ZUNIONSTORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zunion1", []store.ZSetMember{{Member: "a", Score: 1}})
	handler.Db.ZAdd("zunion2", []store.ZSetMember{{Member: "b", Score: 2}})

	resp := handler.executeCommand(state, "ZUNIONSTORE", [][]byte{[]byte("zunionresult"), []byte("2"), []byte("zunion1"), []byte("zunion2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify union result contains both a and b
	members, _ := handler.Db.ZRange("zunionresult", 0, -1)
	assert.Equal(t, 2, len(members))
	hasA := false
	hasB := false
	for _, m := range members {
		if m.Member == "a" {
			hasA = true
		}
		if m.Member == "b" {
			hasB = true
		}
	}
	assert.True(t, hasA && hasB)
}

// TestExecuteCommand_ZUNIONSTORE_WEIGHTS_Coverage tests ZUNIONSTORE with WEIGHTS
func TestExecuteCommand_ZUNIONSTORE_WEIGHTS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zunion3", []store.ZSetMember{{Member: "a", Score: 1}})
	handler.Db.ZAdd("zunion4", []store.ZSetMember{{Member: "b", Score: 1}})

	resp := handler.executeCommand(state, "ZUNIONSTORE", [][]byte{[]byte("zunionresult2"), []byte("2"), []byte("zunion3"), []byte("zunion4"), []byte("WEIGHTS"), []byte("2"), []byte("3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_ZUNIONSTORE_AGGREGATE_Coverage tests ZUNIONSTORE with AGGREGATE
func TestExecuteCommand_ZUNIONSTORE_AGGREGATE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zunion5", []store.ZSetMember{{Member: "a", Score: 1}})
	handler.Db.ZAdd("zunion6", []store.ZSetMember{{Member: "b", Score: 1}})

	resp := handler.executeCommand(state, "ZUNIONSTORE", [][]byte{[]byte("zunionresult3"), []byte("2"), []byte("zunion5"), []byte("zunion6"), []byte("AGGREGATE"), []byte("MIN")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_ZINTERSTORE_Coverage tests ZINTERSTORE command
func TestExecuteCommand_ZINTERSTORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zinter1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})
	handler.Db.ZAdd("zinter2", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "c", Score: 3}})

	resp := handler.executeCommand(state, "ZINTERSTORE", [][]byte{[]byte("zinterresult"), []byte("2"), []byte("zinter1"), []byte("zinter2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_ZDIFFSTORE_Coverage tests ZDIFFSTORE command
func TestExecuteCommand_ZDIFFSTORE_Coverage(t *testing.T) {
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zdiff1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})
	handler.Db.ZAdd("zdiff2", []store.ZSetMember{{Member: "b", Score: 2}})

	resp := handler.executeCommand(state, "ZDIFFSTORE", [][]byte{[]byte("zdifffresult"), []byte("2"), []byte("zdiff1"), []byte("zdiff2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_ZDIFF_Coverage tests ZDIFF command
func TestExecuteCommand_ZDIFF_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zdiff1", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})
	handler.Db.ZAdd("zdiff2", []store.ZSetMember{{Member: "b", Score: 2}})

	resp := handler.executeCommand(state, "ZDIFF", [][]byte{[]byte("2"), []byte("zdiff1"), []byte("zdiff2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))

	resp = handler.executeCommand(state, "ZDIFF", [][]byte{[]byte("2"), []byte("zdiff1"), []byte("zdiff2"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr.Args))

	resp = handler.executeCommand(state, "ZDIFF", [][]byte{[]byte("2"), []byte("empty"), []byte("zdiff1")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestExecuteCommand_ZINTER_Coverage tests ZINTER command (Redis 7.0+)
func TestExecuteCommand_ZINTER_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zinter1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zinter2"), []byte("1"), []byte("a"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	// Basic ZINTER
	resp := handler.executeCommand(state, "ZINTER", [][]byte{[]byte("2"), []byte("zinter1"), []byte("zinter2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args)) // only 'a' is in both
	assert.Equal(t, "a", string(arr.Args[0]))

	// ZINTER WITHSCORES
	resp = handler.executeCommand(state, "ZINTER", [][]byte{[]byte("2"), []byte("zinter1"), []byte("zinter2"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args)) // member + score
	assert.Equal(t, "a", string(arr.Args[0]))

	// ZINTER with WEIGHTS
	resp = handler.executeCommand(state, "ZINTER", [][]byte{[]byte("2"), []byte("zinter1"), []byte("zinter2"), []byte("WEIGHTS"), []byte("2"), []byte("1")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))

	// ZINTER with AGGREGATE
	resp = handler.executeCommand(state, "ZINTER", [][]byte{[]byte("2"), []byte("zinter1"), []byte("zinter2"), []byte("AGGREGATE"), []byte("MAX")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))

	// ZINTER empty intersection
	resp = handler.executeCommand(state, "ZINTER", [][]byte{[]byte("2"), []byte("zinter1"), []byte("empty")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestExecuteCommand_ZUNION_Coverage tests ZUNION command (Redis 7.0+)
func TestExecuteCommand_ZUNION_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zun1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zun2"), []byte("1"), []byte("a"), []byte("3"), []byte("c")}, "127.0.0.1:12345")

	// Basic ZUNION
	resp := handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("zun1"), []byte("zun2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args)) // a, b, c

	// ZUNION WITHSCORES
	resp = handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("zun1"), []byte("zun2"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 6, len(arr.Args)) // 3 * (member + score)

	// ZUNION with WEIGHTS
	resp = handler.executeCommand(state, "ZUNION", [][]byte{[]byte("2"), []byte("zun1"), []byte("zun2"), []byte("WEIGHTS"), []byte("2"), []byte("1")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)

	// ZUNION empty
	resp = handler.executeCommand(state, "ZUNION", [][]byte{[]byte("1"), []byte("empty")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestExecuteCommand_ZLEXCOUNT_Coverage tests ZLEXCOUNT command
func TestExecuteCommand_ZLEXCOUNT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zlexcounttest", []store.ZSetMember{{Member: "a", Score: 0}, {Member: "b", Score: 0}, {Member: "c", Score: 0}})

	resp := handler.executeCommand(state, "ZLEXCOUNT", [][]byte{[]byte("zlexcounttest"), []byte("-"), []byte("+")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))
}

// TestExecuteCommand_ZREMRANGEBYLEX_Coverage tests ZREMRANGEBYLEX command
func TestExecuteCommand_ZREMRANGEBYLEX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrembylextest", []store.ZSetMember{{Member: "a", Score: 0}, {Member: "b", Score: 0}, {Member: "c", Score: 0}})

	resp := handler.executeCommand(state, "ZREMRANGEBYLEX", [][]byte{[]byte("zrembylextest"), []byte("-"), []byte("b")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_ZSCAN_Coverage tests ZSCAN command
func TestExecuteCommand_ZSCAN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zscantest", []store.ZSetMember{{Member: "member", Score: 1}})

	resp := handler.executeCommand(state, "ZSCAN", [][]byte{[]byte("zscantest"), []byte("0")}, "127.0.0.1:12345")
	_, isError := resp.(*proto.Error)
	assert.False(t, isError)
}

// TestExecuteCommand_SSCAN_Coverage tests SSCAN command
func TestExecuteCommand_SSCAN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sscantest", "member")

	resp := handler.executeCommand(state, "SSCAN", [][]byte{[]byte("sscantest"), []byte("0")}, "127.0.0.1:12345")
	_, isError := resp.(*proto.Error)
	assert.False(t, isError)
}

// TestExecuteCommand_HSTRLEN_Coverage tests HSTRLEN command
func TestExecuteCommand_HSTRLEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hstrlentest", "field", "value")

	resp := handler.executeCommand(state, "HSTRLEN", [][]byte{[]byte("hstrlentest"), []byte("field")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(5), int64(*integer))
}

// TestExecuteCommand_HINCRBY_Coverage tests HINCRBY command
func TestExecuteCommand_HINCRBY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hincrbytest", "field", "5")

	resp := handler.executeCommand(state, "HINCRBY", [][]byte{[]byte("hincrbytest"), []byte("field"), []byte("3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(8), int64(*integer))
}

// TestExecuteCommand_HINCRBYFLOAT_Coverage tests HINCRBYFLOAT command
func TestExecuteCommand_HINCRBYFLOAT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hincrbyfloattest", "field", "5.5")

	resp := handler.executeCommand(state, "HINCRBYFLOAT", [][]byte{[]byte("hincrbyfloattest"), []byte("field"), []byte("1.5")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	// Should return "7" or "7.0"
	assert.True(t, string(*bs) == "7" || string(*bs) == "7.0")

	// Verify field value
	val, _ := handler.Db.HGet("hincrbyfloattest", "field")
	assert.Equal(t, []byte("7"), val)
}

// TestExecuteCommand_SMISMEMBER_Coverage tests SMISMEMBER command
func TestExecuteCommand_SMISMEMBER_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("smismembertest", "member1")
	handler.Db.SAdd("smismembertest", "member2")

	resp := handler.executeCommand(state, "SMISMEMBER", [][]byte{[]byte("smismembertest"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")
	nestedArr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 3, len(nestedArr.Elems))
}

// TestExecuteCommand_SINTERSTORE_Coverage tests SINTERSTORE command
func TestExecuteCommand_SINTERSTORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sinter1", "a")
	handler.Db.SAdd("sinter1", "b")
	handler.Db.SAdd("sinter2", "b")
	handler.Db.SAdd("sinter2", "c")

	// SINTERSTORE dest key1 key2 [key3 ...]
	resp := handler.executeCommand(state, "SINTERSTORE", [][]byte{[]byte("sinterresult"), []byte("sinter1"), []byte("sinter2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify sinterresult contains exactly "b" (intersection)
	destVals, _ := handler.Db.SMembers("sinterresult")
	assert.Equal(t, []string{"b"}, destVals)
}

// TestExecuteCommand_SUNIONSTORE_Coverage tests SUNIONSTORE command
func TestExecuteCommand_SUNIONSTORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sunion1", "a")
	handler.Db.SAdd("sunion2", "b")

	// SUNIONSTORE dest key1 key2 [key3 ...]
	resp := handler.executeCommand(state, "SUNIONSTORE", [][]byte{[]byte("sunionresult"), []byte("sunion1"), []byte("sunion2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify union contains both a and b
	destVals, _ := handler.Db.SMembers("sunionresult")
	assert.Equal(t, 2, len(destVals))
	assert.True(t, len(destVals) == 2 && ((destVals[0] == "a" && destVals[1] == "b") || (destVals[0] == "b" && destVals[1] == "a")))
}

// TestExecuteCommand_SDIFFSTORE2_Coverage tests SDIFFSTORE (variant)
func TestExecuteCommand_SDIFFSTORE2_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sdiff1", "a")
	handler.Db.SAdd("sdiff1", "b")
	handler.Db.SAdd("sdiff2", "b")

	// SDIFFSTORE dest key1 key2 [key3 ...]
	resp := handler.executeCommand(state, "SDIFFSTORE", [][]byte{[]byte("sdifffresult"), []byte("sdiff1"), []byte("sdiff2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify sdifffresult contains "a" only (sdiff1 - sdiff2)
	destVals, _ := handler.Db.SMembers("sdifffresult")
	assert.Equal(t, []string{"a"}, destVals)
}

// TestExecuteCommand_SINTER_Coverage tests SINTER command
func TestExecuteCommand_SINTER_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sintercmd1", "a")
	handler.Db.SAdd("sintercmd1", "b")
	handler.Db.SAdd("sintercmd2", "b")
	handler.Db.SAdd("sintercmd2", "c")

	resp := handler.executeCommand(state, "SINTER", [][]byte{[]byte("sintercmd1"), []byte("sintercmd2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	// Verify intersection contains "b"
	if len(arr.Args) > 0 {
		assert.Equal(t, "b", string(arr.Args[0]))
	}
}

// TestExecuteCommand_SUNION_Coverage tests SUNION command
func TestExecuteCommand_SUNION_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sunioncmd1", "a")
	handler.Db.SAdd("sunioncmd2", "b")

	resp := handler.executeCommand(state, "SUNION", [][]byte{[]byte("sunioncmd1"), []byte("sunioncmd2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	// Verify union contains both a and b
	vals := make(map[string]bool)
	for _, arg := range arr.Args {
		vals[string(arg)] = true
	}
	assert.True(t, vals["a"])
	assert.True(t, vals["b"])
}

// TestExecuteCommand_SDIFF_Coverage tests SDIFF command
func TestExecuteCommand_SDIFF_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sdiffcmd1", "a")
	handler.Db.SAdd("sdiffcmd1", "b")
	handler.Db.SAdd("sdiffcmd2", "b")

	resp := handler.executeCommand(state, "SDIFF", [][]byte{[]byte("sdiffcmd1"), []byte("sdiffcmd2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	// Verify difference is "a" (sdiffcmd1 - sdiffcmd2)
	if len(arr.Args) > 0 {
		assert.Equal(t, "a", string(arr.Args[0]))
	}
}

// TestExecuteCommand_SPOP_Coverage tests SPOP command
func TestExecuteCommand_SPOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("spoptest", "member1")
	handler.Db.SAdd("spoptest", "member2")

	resp := handler.executeCommand(state, "SPOP", [][]byte{[]byte("spoptest")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(*bs) > 0)
	popped := string(*bs)
	assert.True(t, popped == "member1" || popped == "member2")

	// Verify set size decreased by 1
	cardResp := handler.executeCommand(state, "SCARD", [][]byte{[]byte("spoptest")}, "127.0.0.1:12345")
	cardInt, _ := cardResp.(*proto.Integer)
	assert.Equal(t, int64(1), int64(*cardInt))

	// Verify popped member is gone
	members, _ := handler.Db.SMembers("spoptest")
	assert.Equal(t, 1, len(members))
	assert.NotEqual(t, popped, members[0])
}

// TestExecuteCommand_SRANDMEMBER_Count_Coverage tests SRANDMEMBER with count
func TestExecuteCommand_SRANDMEMBER_Count_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("srandmembercount", "a")
	handler.Db.SAdd("srandmembercount", "b")
	handler.Db.SAdd("srandmembercount", "c")

	resp := handler.executeCommand(state, "SRANDMEMBER", [][]byte{[]byte("srandmembercount"), []byte("2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	// Verify returned members are actually in the set
	for _, member := range arr.Args {
		exists, _ := handler.Db.SIsMember("srandmembercount", string(member))
		assert.True(t, exists)
	}
}

// TestExecuteCommand_SMOVE_Coverage tests SMOVE command
func TestExecuteCommand_SMOVE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("smovetest", "member")

	resp := handler.executeCommand(state, "SMOVE", [][]byte{[]byte("smovetest"), []byte("smovedest"), []byte("member")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify member was moved: source is empty, destination contains member
	srcVals, _ := handler.Db.SMembers("smovetest")
	assert.Equal(t, 0, len(srcVals))
	destVals, _ := handler.Db.SMembers("smovedest")
	assert.Equal(t, []string{"member"}, destVals)
}

// TestExecuteCommand_ZREMRANGEBYRANK_Coverage tests ZREMRANGEBYRANK command
func TestExecuteCommand_ZREMRANGEBYRANK_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrembyranktest", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})

	resp := handler.executeCommand(state, "ZREMRANGEBYRANK", [][]byte{[]byte("zrembyranktest"), []byte("0"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify only "c" remains (ranks 0-1 removed)
	members, _ := handler.Db.ZRange("zrembyranktest", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "c", members[0].Member)
}

// TestExecuteCommand_ZREMRANGEBYSCORE_Coverage tests ZREMRANGEBYSCORE command
func TestExecuteCommand_ZREMRANGEBYSCORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrembyscoretest", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})

	resp := handler.executeCommand(state, "ZREMRANGEBYSCORE", [][]byte{[]byte("zrembyscoretest"), []byte("-inf"), []byte("2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify only "c" remains (scores <= 2 removed)
	members, _ := handler.Db.ZRange("zrembyscoretest", 0, -1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "c", members[0].Member)
}

// TestExecuteCommand_ZINCRBY_Coverage tests ZINCRBY command
func TestExecuteCommand_ZINCRBY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zincrbytest", []store.ZSetMember{{Member: "member", Score: 1}})

	resp := handler.executeCommand(state, "ZINCRBY", [][]byte{[]byte("zincrbytest"), []byte("2"), []byte("member")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "3", string(*bs))
	score, _, err := handler.Db.ZScore("zincrbytest", "member")
	assert.NoError(t, err)
	assert.Equal(t, 3.0, score)
}

// TestExecuteCommand_ZREVRANGEBYSCORE_Coverage tests ZREVRANGEBYSCORE command
func TestExecuteCommand_ZREVRANGEBYSCORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrevrangebyscore", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})

	resp := handler.executeCommand(state, "ZREVRANGEBYSCORE", [][]byte{[]byte("zrevrangebyscore"), []byte("+inf"), []byte("-inf")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 1)
	// Verify reverse order: c (3), b (2), a (1)
	assert.Equal(t, 3, len(arr.Args))
	if len(arr.Args) == 3 {
		assert.Equal(t, "c", string(arr.Args[0]))
		assert.Equal(t, "b", string(arr.Args[1]))
		assert.Equal(t, "a", string(arr.Args[2]))
	}
}

// TestExecuteCommand_ZRANGEBYLEX_Coverage tests ZRANGEBYLEX command
func TestExecuteCommand_ZRANGEBYLEX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrangebylex", []store.ZSetMember{{Member: "a", Score: 0}, {Member: "b", Score: 0}, {Member: "c", Score: 0}})

	resp := handler.executeCommand(state, "ZRANGEBYLEX", [][]byte{[]byte("zrangebylex"), []byte("-"), []byte("+")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	// Verify lexicographical order
	assert.Equal(t, "a", string(arr.Args[0]))
	assert.Equal(t, "b", string(arr.Args[1]))
	assert.Equal(t, "c", string(arr.Args[2]))
}

// TestExecuteCommand_ZREVRANGEBYLEX_Coverage tests ZREVRANGEBYLEX command
func TestExecuteCommand_ZREVRANGEBYLEX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zrevrangebylex", []store.ZSetMember{{Member: "a", Score: 0}, {Member: "b", Score: 0}, {Member: "c", Score: 0}})

	resp := handler.executeCommand(state, "ZREVRANGEBYLEX", [][]byte{[]byte("zrevrangebylex"), []byte("+"), []byte("-")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	// Verify reverse lexicographical order: c, b, a
	assert.Equal(t, "c", string(arr.Args[0]))
	assert.Equal(t, "b", string(arr.Args[1]))
	assert.Equal(t, "a", string(arr.Args[2]))
}

// TestExecuteCommand_ZCARD_Coverage tests ZCARD command
func TestExecuteCommand_ZCARD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zcardtest", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})

	resp := handler.executeCommand(state, "ZCARD", [][]byte{[]byte("zcardtest")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_ZCOUNT_Coverage tests ZCOUNT command
func TestExecuteCommand_ZCOUNT2_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.ZAdd("zcounttest", []store.ZSetMember{{Member: "a", Score: 1}, {Member: "b", Score: 5}, {Member: "c", Score: 10}})

	resp := handler.executeCommand(state, "ZCOUNT", [][]byte{[]byte("zcounttest"), []byte("1"), []byte("5")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_SISMEMBER_Coverage tests SISMEMBER command
func TestExecuteCommand_SISMEMBER_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sismembertest", "member")

	resp := handler.executeCommand(state, "SISMEMBER", [][]byte{[]byte("sismembertest"), []byte("member")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_SCARD_Coverage tests SCARD command
func TestExecuteCommand_SCARD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("scardtest", "a")
	handler.Db.SAdd("scardtest", "b")

	resp := handler.executeCommand(state, "SCARD", [][]byte{[]byte("scardtest")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_SREM_Coverage tests SREM command
func TestExecuteCommand_SREM2_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.SAdd("sremtest", "member1")
	handler.Db.SAdd("sremtest", "member2")

	resp := handler.executeCommand(state, "SREM", [][]byte{[]byte("sremtest"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify member1 was removed
	members, _ := handler.Db.SMembers("sremtest")
	assert.Equal(t, []string{"member2"}, members)
}

// TestExecuteCommand_HKEYS_Coverage tests HKEYS command
func TestExecuteCommand_HKEYS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hkeystest", "field1", "value1")
	handler.Db.HSet("hkeystest", "field2", "value2")

	resp := handler.executeCommand(state, "HKEYS", [][]byte{[]byte("hkeystest")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	// Verify field names
	fields := make(map[string]bool)
	for _, arg := range arr.Args {
		fields[string(arg)] = true
	}
	assert.True(t, fields["field1"])
	assert.True(t, fields["field2"])
}

// TestExecuteCommand_HVALS_Coverage tests HVALS command
func TestExecuteCommand_HVALS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hvalstest", "field1", "value1")
	handler.Db.HSet("hvalstest", "field2", "value2")

	resp := handler.executeCommand(state, "HVALS", [][]byte{[]byte("hvalstest")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	// Verify values
	vals := make(map[string]bool)
	for _, arg := range arr.Args {
		vals[string(arg)] = true
	}
	assert.True(t, vals["value1"])
	assert.True(t, vals["value2"])
}

// TestExecuteCommand_HEXISTS_Coverage tests HEXISTS command
func TestExecuteCommand_HEXISTS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hexiststest", "field", "value")

	resp := handler.executeCommand(state, "HEXISTS", [][]byte{[]byte("hexiststest"), []byte("field")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_HLEN_Coverage tests HLEN command
func TestExecuteCommand_HLEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hlentest", "field1", "value1")
	handler.Db.HSet("hlentest", "field2", "value2")

	resp := handler.executeCommand(state, "HLEN", [][]byte{[]byte("hlentest")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_HMGET_Coverage tests HMGET command
func TestExecuteCommand_HMGET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hmgettest", "field1", "value1")
	handler.Db.HSet("hmgettest", "field2", "value2")

	resp := handler.executeCommand(state, "HMGET", [][]byte{[]byte("hmgettest"), []byte("field1"), []byte("field2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	// Verify returned values
	assert.Equal(t, "value1", string(arr.Args[0]))
	assert.Equal(t, "value2", string(arr.Args[1]))
}

// TestExecuteCommand_HMSET_Coverage tests HMSET command
func TestExecuteCommand_HMSET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HMSET", [][]byte{[]byte("hmsettest"), []byte("field1"), []byte("value1"), []byte("field2"), []byte("value2")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify fields were stored
	val1, _ := handler.Db.HGet("hmsettest", "field1")
	assert.Equal(t, []byte("value1"), val1)
	val2, _ := handler.Db.HGet("hmsettest", "field2")
	assert.Equal(t, []byte("value2"), val2)
}

// TestExecuteCommand_HSETNX_Coverage tests HSETNX command
func TestExecuteCommand_HSETNX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.HSet("hsetnextest", "field", "value")

	resp := handler.executeCommand(state, "HSETNX", [][]byte{[]byte("hsetnextest"), []byte("field"), []byte("newvalue")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_LRANGE_Negative_Coverage tests LRANGE with negative indices
func TestExecuteCommand_LRANGE_Negative_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("lrangeneg", "a")
	handler.Db.RPush("lrangeneg", "b")
	handler.Db.RPush("lrangeneg", "c")

	resp := handler.executeCommand(state, "LRANGE", [][]byte{[]byte("lrangeneg"), []byte("-2"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
}

// TestExecuteCommand_LINDEX_Coverage tests LINDEX command
func TestExecuteCommand_LINDEX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("lindextest", "a")
	handler.Db.RPush("lindextest", "b")

	resp := handler.executeCommand(state, "LINDEX", [][]byte{[]byte("lindextest"), []byte("0")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "a", string(*bs))
}

// TestExecuteCommand_LTRIM_Coverage tests LTRIM command
func TestExecuteCommand_LTRIM_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("ltrimtest", "a")
	handler.Db.RPush("ltrimtest", "b")
	handler.Db.RPush("ltrimtest", "c")

	resp := handler.executeCommand(state, "LTRIM", [][]byte{[]byte("ltrimtest"), []byte("0"), []byte("1")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify only first two elements remain
	vals, _ := handler.Db.LRange("ltrimtest", 0, -1)
	assert.Equal(t, []string{"a", "b"}, vals)
}

// TestExecuteCommand_LLEN_Coverage tests LLEN command
func TestExecuteCommand_LLEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("llentest", "a")
	handler.Db.RPush("llentest", "b")

	resp := handler.executeCommand(state, "LLEN", [][]byte{[]byte("llentest")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_LPOP_Coverage tests LPOP command
func TestExecuteCommand_LPOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("lpoptest", "a")
	handler.Db.RPush("lpoptest", "b")

	resp := handler.executeCommand(state, "LPOP", [][]byte{[]byte("lpoptest")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "a", string(*bs))
}

// TestExecuteCommand_RPOP_Coverage tests RPOP command
func TestExecuteCommand_RPOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("rpoptest", "a")
	handler.Db.RPush("rpoptest", "b")

	resp := handler.executeCommand(state, "RPOP", [][]byte{[]byte("rpoptest")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "b", string(*bs))
}

// TestExecuteCommand_LPUSH_Coverage tests LPUSH command
func TestExecuteCommand_LPUSH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LPUSH", [][]byte{[]byte("lpushtest"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify value was pushed to head
	vals, _ := handler.Db.LRange("lpushtest", 0, -1)
	assert.Equal(t, []string{"value"}, vals)
}

// TestExecuteCommand_RPUSH_Coverage tests RPUSH command
func TestExecuteCommand_RPUSH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "RPUSH", [][]byte{[]byte("rpushtest"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify value was pushed to tail
	vals, _ := handler.Db.LRange("rpushtest", 0, -1)
	assert.Equal(t, []string{"value"}, vals)
}

// TestExecuteCommand_MSET_Coverage tests MSET command
func TestExecuteCommand_MSET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MSET", [][]byte{[]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify both keys were set
	val1, _ := handler.Db.Get("key1")
	assert.Equal(t, "value1", val1)
	val2, _ := handler.Db.Get("key2")
	assert.Equal(t, "value2", val2)
}

// TestExecuteCommand_MGET_Coverage tests MGET command
func TestExecuteCommand_MGET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mgetkey1", "value1")
	handler.Db.Set("mgetkey2", "value2")

	resp := handler.executeCommand(state, "MGET", [][]byte{[]byte("mgetkey1"), []byte("mgetkey2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	// Verify returned values
	assert.Equal(t, "value1", string(arr.Args[0]))
	assert.Equal(t, "value2", string(arr.Args[1]))
}

// TestExecuteCommand_INCR_Coverage tests INCR command
func TestExecuteCommand_INCR_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("incrkey", "10")

	resp := handler.executeCommand(state, "INCR", [][]byte{[]byte("incrkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))
}

// TestExecuteCommand_INCRBY_Coverage tests INCRBY command
func TestExecuteCommand_INCRBY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("incrbykey", "10")

	resp := handler.executeCommand(state, "INCRBY", [][]byte{[]byte("incrbykey"), []byte("5")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(15), int64(*integer))
}

// TestExecuteCommand_DECR_Coverage tests DECR command
func TestExecuteCommand_DECR_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("decrkey", "10")

	resp := handler.executeCommand(state, "DECR", [][]byte{[]byte("decrkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(9), int64(*integer))
}

// TestExecuteCommand_DECRBY_Coverage tests DECRBY command
func TestExecuteCommand_DECRBY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("decrbykey", "10")

	resp := handler.executeCommand(state, "DECRBY", [][]byte{[]byte("decrbykey"), []byte("3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(7), int64(*integer))
}

// TestExecuteCommand_INCRBYFLOAT_Coverage tests INCRBYFLOAT command
func TestExecuteCommand_INCRBYFLOAT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("incrbyfloatkey", "10.5")

	resp := handler.executeCommand(state, "INCRBYFLOAT", [][]byte{[]byte("incrbyfloatkey"), []byte("2.5")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "13", string(*bs))
}

// TestExecuteCommand_GETSET_Coverage tests GETSET command
func TestExecuteCommand_GETSET2_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("getsetskey", "oldvalue")

	resp := handler.executeCommand(state, "GETSET", [][]byte{[]byte("getsetskey"), []byte("newvalue")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "oldvalue", string(*bs))
}

// TestExecuteCommand_STRLEN_Coverage tests STRLEN command
func TestExecuteCommand_STRLEN2_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("strlenkey", "hello")

	resp := handler.executeCommand(state, "STRLEN", [][]byte{[]byte("strlenkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(5), int64(*integer))
}

// TestExecuteCommand_EXISTS_Coverage tests EXISTS command
func TestExecuteCommand_EXISTS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("existskey", "value")

	resp := handler.executeCommand(state, "EXISTS", [][]byte{[]byte("existskey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_DEL_Coverage tests DEL command
func TestExecuteCommand_DEL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("delkey", "value")

	resp := handler.executeCommand(state, "DEL", [][]byte{[]byte("delkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_RANDOMKEY_Coverage tests RANDOMKEY command
func TestExecuteCommand_RANDOMKEY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("randomkey1", "value1")

	resp := handler.executeCommand(state, "RANDOMKEY", nil, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_KEYS_Coverage tests KEYS command
func TestExecuteCommand_KEYS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("key1", "value1")

	resp := handler.executeCommand(state, "KEYS", [][]byte{[]byte("key*")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
}

// TestExecuteCommand_SWAPDB_Coverage tests SWAPDB command
func TestExecuteCommand_SWAPDB_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SWAPDB", [][]byte{[]byte("0"), []byte("1")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_SELECT_Coverage tests SELECT command
func TestExecuteCommand_SELECT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SELECT", [][]byte{[]byte("0")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_TIME_Coverage tests TIME command
func TestExecuteCommand_TIME_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TIME", nil, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
}

// TestExecuteCommand_AUTH_Coverage tests AUTH command
func TestExecuteCommand_AUTH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "AUTH", [][]byte{[]byte("password")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_MULTI_Coverage tests MULTI command
func TestExecuteCommand_MULTI_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MULTI", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_DISCARD_Coverage tests DISCARD command
func TestExecuteCommand_DISCARD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "MULTI", nil, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "DISCARD", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_UNWATCH_Coverage tests UNWATCH command
func TestExecuteCommand_UNWATCH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "UNWATCH", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}
