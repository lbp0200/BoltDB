package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestBoundary_EmptyCommandName verifies that an empty command name returns an error.
func TestBoundary_EmptyCommandName(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "", nil, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	// Empty command dispatched as unknown command by executeCommand
	// (the RESP parser path returns "ERR no command" at handler_core.go:499)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// TestBoundary_UnknownCommand verifies that an unknown command returns an error.
func TestBoundary_UnknownCommand(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "NONEXISTENT_CMD", [][]byte{[]byte("arg")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR unknown command"))
}

// TestBoundary_INCR_NonExistentKey verifies INCR on a nonexistent key starts from 0.
func TestBoundary_INCR_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "INCR", [][]byte{[]byte("new_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestBoundary_INCR_NonIntegerValue verifies INCR on a non-integer string returns error.
func TestBoundary_INCR_NonIntegerValue(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("str_key"), []byte("hello")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "INCR", [][]byte{[]byte("str_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

// TestBoundary_INCR_WrongType verifies INCR on a list key returns WRONGTYPE.
func TestBoundary_INCR_WrongType(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("mylist"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "INCR", [][]byte{[]byte("mylist")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestBoundary_SET_EmptyKey verifies SET with an empty key succeeds (Redis allows it).
func TestBoundary_SET_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SET", [][]byte{[]byte(""), []byte("val")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))
}

// TestBoundary_SET_EmptyValue verifies SET with an empty value succeeds.
func TestBoundary_SET_EmptyValue(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("key"), []byte("")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))

	// GET should return empty string
	resp = handler.executeCommand(state, "GET", [][]byte{[]byte("key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestBoundary_SET_MissingArgs verifies SET with no args returns error.
func TestBoundary_SET_MissingArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SET with no args
	resp := handler.executeCommand(state, "SET", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))

	// SET with only key (missing value)
	resp = handler.executeCommand(state, "SET", [][]byte{[]byte("k")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestBoundary_GET_NonExistentKey verifies GET on nonexistent key returns nil BulkString.
func TestBoundary_GET_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("no_such_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestBoundary_DEL_NonExistentKey verifies DEL on nonexistent key returns 0.
func TestBoundary_DEL_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "DEL", [][]byte{[]byte("ghost")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestBoundary_EXISTS_NonExistentKey verifies EXISTS on nonexistent key returns 0.
func TestBoundary_EXISTS_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "EXISTS", [][]byte{[]byte("ghost")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestBoundary_TYPE_NonExistentKey verifies TYPE on nonexistent key returns "none".
func TestBoundary_TYPE_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TYPE", [][]byte{[]byte("ghost")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "none", string(*ss))
}

// TestBoundary_TTL_NonExistentKey verifies TTL on nonexistent key returns -2.
func TestBoundary_TTL_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TTL", [][]byte{[]byte("ghost")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-2), int64(*integer))
}

// TestBoundary_ZADD_MissingScore verifies ZADD with member but no score returns error.
func TestBoundary_ZADD_MissingScore(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// ZADD key member (missing score)
	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("member1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestBoundary_ZADD_InvalidScore verifies ZADD with non-numeric score returns error.
func TestBoundary_ZADD_InvalidScore(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("not_a_number"), []byte("member1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR value is not a valid float"))
}

// TestBoundary_ZADD_EmptyKey verifies ZADD with empty key succeeds.
func TestBoundary_ZADD_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte(""), []byte("1.5"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestBoundary_ZRANK_NonMember verifies ZRANK on nonexistent member returns nil/empty.
func TestBoundary_ZRANK_NonMember(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Empty zset
	resp := handler.executeCommand(state, "ZRANK", [][]byte{[]byte("myzset"), []byte("nobody")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestBoundary_HSET_EmptyKey verifies HSET with empty key succeeds.
func TestBoundary_HSET_EmptyKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte(""), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestBoundary_HSET_OddArgs verifies HSET with odd field/value args returns error.
func TestBoundary_HSET_OddArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// HSET key field (missing value)
	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte("h"), []byte("f1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestBoundary_HGET_NonExistentField verifies HGET on nonexistent field returns nil.
func TestBoundary_HGET_NonExistentField(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a hash with one field
	handler.executeCommand(state, "HSET", [][]byte{[]byte("h"), []byte("f1"), []byte("v1")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HGET", [][]byte{[]byte("h"), []byte("nonexistent")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestBoundary_SADD_MissingMember verifies SADD with no member returns error.
func TestBoundary_SADD_MissingMember(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("myset")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestBoundary_LINDEX_OutOfRange verifies LINDEX on out-of-range index returns nil.
func TestBoundary_LINDEX_OutOfRange(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("mylist"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	// Index way beyond length
	resp := handler.executeCommand(state, "LINDEX", [][]byte{[]byte("mylist"), []byte("99999")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestBoundary_LINDEX_NegativeIndex verifies LINDEX with negative index.
func TestBoundary_LINDEX_NegativeIndex(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("mylist"), []byte("first"), []byte("last")}, "127.0.0.1:12345")

	// -1 should be last element
	resp := handler.executeCommand(state, "LINDEX", [][]byte{[]byte("mylist"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "last", string(*bs))
}

// TestBoundary_LRANGE_EmptyList verifies LRANGE on empty list returns empty array.
func TestBoundary_LRANGE_EmptyList(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LRANGE", [][]byte{[]byte("empty"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestBoundary_Append_NonStringKey verifies APPEND on a list key returns WRONGTYPE.
func TestBoundary_Append_NonStringKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("mylist"), []byte("a")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "APPEND", [][]byte{[]byte("mylist"), []byte("extra")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestBoundary_MultipleWrongType verifies WRONGTYPE errors for various cross-type operations.
func TestBoundary_MultipleWrongType(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a string key
	handler.executeCommand(state, "SET", [][]byte{[]byte("strkey"), []byte("val")}, "127.0.0.1:12345")

	// HGET on string key
	resp := handler.executeCommand(state, "HGET", [][]byte{[]byte("strkey"), []byte("f")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// LRANGE on string key
	resp = handler.executeCommand(state, "LRANGE", [][]byte{[]byte("strkey"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))

	// SMEMBERS on string key
	resp = handler.executeCommand(state, "SMEMBERS", [][]byte{[]byte("strkey")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestBoundary_VeryLongKeyName verifies operations with a 256-byte key name.
func TestBoundary_VeryLongKeyName(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	longKey := strings.Repeat("k", 256)

	resp := handler.executeCommand(state, "SET", [][]byte{[]byte(longKey), []byte("val")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))

	resp = handler.executeCommand(state, "GET", [][]byte{[]byte(longKey)}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "val", string(*bs))
}

// TestBoundary_VeryLongValue verifies SET/GET with a 64KB value.
func TestBoundary_VeryLongValue(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	bigVal := strings.Repeat("x", 64*1024) // 64KB
	handler.executeCommand(state, "SET", [][]byte{[]byte("bigval"), []byte(bigVal)}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("bigval")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, bigVal, string(*bs))
}

// TestBoundary_LPushNoArgs verifies LPUSH with no elements returns error.
func TestBoundary_LPushNoArgs(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LPUSH", [][]byte{[]byte("mylist")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestBoundary_ZCard_EmptySet verifies ZCARD on nonexistent sorted set returns 0.
func TestBoundary_ZCard_EmptySet(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZCARD", [][]byte{[]byte("nozset")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestBoundary_SCard_EmptySet verifies SCARD on nonexistent set returns 0.
func TestBoundary_SCard_EmptySet(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SCARD", [][]byte{[]byte("noset")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestBoundary_HLen_EmptyHash verifies HLEN on nonexistent hash returns 0.
func TestBoundary_HLen_EmptyHash(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HLEN", [][]byte{[]byte("nohash")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestBoundary_SpecialCharsInValues verifies SET/GET with binary and special characters.
func TestBoundary_SpecialCharsInValues(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	specialVals := []string{
		"\x00\x01\x02\xff",           // binary
		"中文测试",                      // unicode
		"line1\nline2\ttab",           // control chars
		"",                             // empty
		strings.Repeat("A", 1024*1024), // 1MB
	}

	for i, val := range specialVals {
		key := fmt.Sprintf("special_%d", i)
		handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(val)}, "127.0.0.1:12345")
		resp := handler.executeCommand(state, "GET", [][]byte{[]byte(key)}, "127.0.0.1:12345")
		bs, ok := resp.(*proto.BulkString)
		assert.True(t, ok)
		assert.Equal(t, val, string(*bs))
	}
}

// TestBoundary_DEL_MultipleKeys verifies DEL with multiple keys returns correct count.
func TestBoundary_DEL_MultipleKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("k1"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("k2"), []byte("v2")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("k3"), []byte("v3")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "DEL", [][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k_nonexist")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))
}

// TestBoundary_EXISTS_MultipleKeys verifies EXISTS with multiple keys.
func TestBoundary_EXISTS_MultipleKeys(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("e1"), []byte("v")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("e3"), []byte("v")}, "127.0.0.1:12345")

	// 2 exist, 1 does not
	resp := handler.executeCommand(state, "EXISTS", [][]byte{[]byte("e1"), []byte("e2"), []byte("e3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestBoundary_INCRBY_NonIntegerArg verifies INCRBY with non-integer increment.
func TestBoundary_INCRBY_NonIntegerArg(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "INCRBY", [][]byte{[]byte("k"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR value is not an integer or out of range"))
}

// TestBoundary_INCRBY_WrongType verifies INCRBY on a hash key returns WRONGTYPE.
func TestBoundary_INCRBY_WrongType(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("f"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "INCRBY", [][]byte{[]byte("myhash"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestBoundary_ZSCORE_EmptySet verifies ZSCORE on nonexistent member returns nil.
func TestBoundary_ZSCORE_EmptySet(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("zset"), []byte("no_member")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestBoundary_RENAME_SameKey verifies RENAME with same source and dest.
func TestBoundary_RENAME_SameKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("k"), []byte("v")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "RENAME", [][]byte{[]byte("k"), []byte("k")}, "127.0.0.1:12345")
	// Redis accepts RENAME to the same key (returns +OK)
	ss, ok := resp.(*proto.SimpleString)
	if !ok {
		t.Fatalf("RENAME same key should return +OK, got: %v", resp)
	}
	assert.Equal(t, "OK", string(*ss))
}

// TestBoundary_RENAME_NonExistentKey verifies RENAME on nonexistent key returns error.
func TestBoundary_RENAME_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "RENAME", [][]byte{[]byte("ghost"), []byte("new")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR no such key"))
}

// TestBoundary_PING_WithArg verifies PING returns PONG even with an argument.
func TestBoundary_PING_WithArg(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "PING", [][]byte{[]byte("hello")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "PONG", string(*ss))
}

// TestBoundary_ECHO_Verifies ECHO returns the argument.
func TestBoundary_ECHO_Verifies(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ECHO", [][]byte{[]byte("test_message")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "test_message", string(*bs))
}

// TestBoundary_GetSet_Atomicity verifies GETSET returns old value and sets new.
func TestBoundary_GetSet_Atomicity(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// First GETSET on nonexistent key
	resp := handler.executeCommand(state, "GETSET", [][]byte{[]byte("gskey"), []byte("first")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs)) // old value was empty

	// Second GETSET should return "first"
	resp = handler.executeCommand(state, "GETSET", [][]byte{[]byte("gskey"), []byte("second")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "first", string(*bs))
}

// TestBoundary_INCRBYFLOAT_NonFloatArg verifies INCRBYFLOAT with non-numeric arg.
func TestBoundary_INCRBYFLOAT_NonFloatArg(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "INCRBYFLOAT", [][]byte{[]byte("k"), []byte("not_a_float")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR value is not a valid float"))
}

// TestBoundary_Expire_NonExistentKey verifies EXPIRE on nonexistent key returns 0.
func TestBoundary_Expire_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("ghost"), []byte("100")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestBoundary_SET_InvalidExpiry verifies SET with invalid EX/PX values.
func TestBoundary_SET_InvalidExpiry(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SET with EX that is not a number
	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("k"), []byte("v"), []byte("EX"), []byte("abc")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR value is not an integer or out of range"))
}

// TestBoundary_ZRANGE_EmptySet verifies ZRANGE on empty sorted set returns empty array.
func TestBoundary_ZRANGE_EmptySet(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("nozset"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestBoundary_SMembers_EmptySet verifies SMEMBERS on empty set returns empty array.
func TestBoundary_SMembers_EmptySet(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SMEMBERS", [][]byte{[]byte("noset")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestBoundary_HGetAll_EmptyHash verifies HGETALL on empty hash returns empty array.
func TestBoundary_HGetAll_EmptyHash(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HGETALL", [][]byte{[]byte("nohash")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestBoundary_STRLEN_NonExistentKey verifies STRLEN on nonexistent key returns 0.
func TestBoundary_STRLEN_NonExistentKey(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "STRLEN", [][]byte{[]byte("ghost")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestBoundary_GetRange_BeyondEnd verifies GETRANGE beyond string end.
func TestBoundary_GetRange_BeyondEnd(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("k"), []byte("abc")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GETRANGE", [][]byte{[]byte("k"), []byte("0"), []byte("100")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "abc", string(*bs))
}

// TestBoundary_GetRange_NegativeIndices verifies GETRANGE with negative indices.
func TestBoundary_GetRange_NegativeIndices(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("k"), []byte("hello")}, "127.0.0.1:12345")

	// -3 to -1 should be "llo"
	resp := handler.executeCommand(state, "GETRANGE", [][]byte{[]byte("k"), []byte("-3"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "llo", string(*bs))
}

// TestBoundary_SetEX_ZeroTTL verifies SETEX with 0 TTL returns error.
func TestBoundary_SetEX_ZeroTTL(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SETEX", [][]byte{[]byte("k"), []byte("0"), []byte("v")}, "127.0.0.1:12345")
	// Redis rejects TTL <= 0 for SETEX
	errResp, isErr := resp.(*proto.Error)
	if !isErr {
		t.Fatalf("SETEX with TTL=0 should return error, got: %v", resp)
	}
	assert.True(t, strings.Contains(string(*errResp), "ERR invalid expire time"))
}

// TestBoundary_IncrBy_ManyGoroutines verifies INCRBY with large increments.
func TestBoundary_IncrBy_LargeIncrement(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("big"), []byte("0")}, "127.0.0.1:12345")

	// Add max safe int64-ish value (keep well under overflow)
	handler.executeCommand(state, "INCRBY", [][]byte{[]byte("big"), []byte("1000000000")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("big")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "1000000000", string(*bs))
}

// TestBoundary_ZRangeByScore_InvalidRange verifies ZRANGEBYSCORE with invalid args.
func TestBoundary_ZRangeByScore_EmptyResult(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create zset with limited scores
	handler.Db.ZAdd("zrbs", []store.ZSetMember{
		{Member: "a", Score: 1.0},
		{Member: "b", Score: 2.0},
		{Member: "c", Score: 3.0},
	})

	// Query range that excludes all members
	resp := handler.executeCommand(state, "ZRANGEBYSCORE", [][]byte{[]byte("zrbs"), []byte("10"), []byte("20")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}
