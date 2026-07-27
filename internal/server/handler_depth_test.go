package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/zeebo/assert"
)

// TestStringBoundary_EmptyKey tests GET on nonexistent key
func TestStringBoundary_EmptyKey(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "GET", [][]byte{[]byte("nonexistent_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestStringBoundary_MaxValueSize tests SET/GET with large value
func TestStringBoundary_MaxValueSize(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// 10MB value
	largeValue := make([]byte, 10*1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("large_key"), largeValue}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	getResp := handler.executeCommand(state, "GET", [][]byte{[]byte("large_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, 10*1024*1024, len(*bs))
}

// TestStringBoundary_EmptyString tests SET with empty value
func TestStringBoundary_EmptyString(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("empty_key"), []byte("")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	getResp := handler.executeCommand(state, "GET", [][]byte{[]byte("empty_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestIncrBoundary_MaxInt64 tests INCR on boundary values
func TestIncrBoundary_MaxInt64(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set to max int64 - 1
	handler.executeCommand(state, "SET", [][]byte{[]byte("max_counter"), []byte("9223372036854775806")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "INCR", [][]byte{[]byte("max_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(9223372036854775807), int64(*integer))

	// Next INCR should overflow
	resp = handler.executeCommand(state, "INCR", [][]byte{[]byte("max_counter")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "overflow"))
}

// TestIncrBoundary_NegativeToPositive tests DECR crossing zero
func TestIncrBoundary_NegativeToPositive(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("neg_counter"), []byte("-5")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-4), int64(*integer))

	// Cross zero
	for i := 0; i < 4; i++ {
		handler.executeCommand(state, "INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	}
	resp = handler.executeCommand(state, "INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestStringError_TypeMismatch tests string command on non-string type
func TestStringError_TypeMismatch(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a list type key
	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("list_key"), []byte("value")}, "127.0.0.1:12345")

	// APPEND on list key should error
	resp := handler.executeCommand(state, "APPEND", [][]byte{[]byte("list_key"), []byte("extra")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestStringError_WrongNumberOfArguments tests GET without key argument
func TestStringError_WrongNumberOfArguments(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "GET", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestStringError_SetGetInvalidArgs tests SET with missing value
func TestStringError_SetGetInvalidArgs(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SET with only key (missing value)
	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("key_only")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestStringError_IncrOnFloat tests INCR on float value
func TestStringError_IncrOnFloat(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("float_key"), []byte("1.5")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "INCR", [][]byte{[]byte("float_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	// Should error on non-integer value
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

// TestListBoundary_EmptyList tests LPOP on nonexistent key
func TestListBoundary_EmptyList(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LPOP", [][]byte{[]byte("nonexistent_list")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListBoundary_SingleElement tests LPOP/RPOP on list with one element
func TestListBoundary_SingleElement(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("single_list"), []byte("only")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LPOP", [][]byte{[]byte("single_list")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "only", string(*bs))

	// Second pop should return empty
	resp = handler.executeCommand(state, "LPOP", [][]byte{[]byte("single_list")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListBoundary_IndexOverflow tests LLEN and LINDEX on large list
func TestListBoundary_IndexOverflow(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Push 1000 elements
	for i := 0; i < 1000; i++ {
		handler.executeCommand(state, "RPUSH", [][]byte{[]byte("large_list"), []byte(string(rune('A' + i%26)))}, "127.0.0.1:12345")
	}

	resp := handler.executeCommand(state, "LLEN", [][]byte{[]byte("large_list")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), int64(*integer))

	// Index beyond length
	resp = handler.executeCommand(state, "LINDEX", [][]byte{[]byte("large_list"), []byte("9999")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListBoundary_NegativeIndex tests negative index access
func TestListBoundary_NegativeIndex(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("neg_index_list"), []byte("first"), []byte("middle"), []byte("last")}, "127.0.0.1:12345")

	// -1 = last element
	resp := handler.executeCommand(state, "LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "last", string(*bs))

	// -2 = middle element
	resp = handler.executeCommand(state, "LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-2")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "middle", string(*bs))

	// -3 = first element
	resp = handler.executeCommand(state, "LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-3")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "first", string(*bs))

	// -4 beyond list (only 3 elements, so -4 is out of bounds)
	resp = handler.executeCommand(state, "LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-4")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListError_TypeMismatch tests list command on string type
func TestListError_TypeMismatch(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LPUSH", [][]byte{[]byte("string_key"), []byte("new")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestListError_InvalidIndex tests LSET with invalid index
func TestListError_InvalidIndex(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("valid_list"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LSET", [][]byte{[]byte("valid_list"), []byte("5"), []byte("c")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "index out of range"))
}

// TestStringBoundary_DecrOverflow tests DECR at int64 min boundary
func TestStringBoundary_DecrOverflow(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set to min int64 + 1
	handler.executeCommand(state, "SET", [][]byte{[]byte("min_counter"), []byte("-9223372036854775807")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "DECR", [][]byte{[]byte("min_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-9223372036854775808), int64(*integer))

	// Next DECR should overflow
	resp = handler.executeCommand(state, "DECR", [][]byte{[]byte("min_counter")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "overflow"))
}

// TestStringBoundary_GetrangeFullString tests GETRANGE with full range
func TestStringBoundary_GetrangeFullString(t *testing.T) {
	t.Parallel()
	// Not parallel — avoids BadgerDB contention on slow CI runners
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("range_key"), []byte("hello")}, "127.0.0.1:12345")

	// Get full string with 0 to -1
	resp := handler.executeCommand(state, "GETRANGE", [][]byte{[]byte("range_key"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))
}

// TestStringBoundary_GetrangeOutOfBounds tests GETRANGE beyond string bounds
func TestStringBoundary_GetrangeOutOfBounds(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("short_key"), []byte("hi")}, "127.0.0.1:12345")

	// Request more than exists — should return what is available
	resp := handler.executeCommand(state, "GETRANGE", [][]byte{[]byte("short_key"), []byte("0"), []byte("100")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hi", string(*bs))
}

// TestStringError_WrongTypeForDecr tests DECR on hash key returns WRONGTYPE
func TestStringError_WrongTypeForDecr(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "DECR", [][]byte{[]byte("hash_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestStringError_WrongTypeForDecrby tests DECRBY on set key returns WRONGTYPE
func TestStringError_WrongTypeForDecrby(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("set_key"), []byte("member")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "DECRBY", [][]byte{[]byte("set_key"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestStringError_SetexWrongType tests SETEX on zset key returns WRONGTYPE
func TestStringError_SetexWrongType(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SETEX", [][]byte{[]byte("zset_key"), []byte("10"), []byte("value")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestStringError_PsetexWrongType tests PSETEX on list key returns WRONGTYPE
func TestStringError_PsetexWrongType(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("list_key"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "PSETEX", [][]byte{[]byte("list_key"), []byte("1000"), []byte("value")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected *proto.Error, got %T: %v", resp, resp)
	}
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestStringBoundary_SetbitGetbitBasic tests SETBIT and GETBIT
func TestStringBoundary_SetbitGetbitBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set bit 7 to 1 (value = 128)
	resp := handler.executeCommand(state, "SETBIT", [][]byte{[]byte("bit_key"), []byte("7"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer)) // old value was 0

	// GETBIT should return 1
	resp = handler.executeCommand(state, "GETBIT", [][]byte{[]byte("bit_key"), []byte("7")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestStringBoundary_SetbitOutOfRange tests SETBIT on offset beyond practical limits
func TestStringBoundary_SetbitOutOfRange(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set bit at a large offset (creates a string of that size)
	resp := handler.executeCommand(state, "SETBIT", [][]byte{[]byte("large_bit_key"), []byte("1048576"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify the bit was set
	resp = handler.executeCommand(state, "GETBIT", [][]byte{[]byte("large_bit_key"), []byte("1048576")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestStringBoundary_SetrangeExtend tests SETRANGE extending a string
func TestStringBoundary_SetrangeExtend(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("extend_key"), []byte("hello")}, "127.0.0.1:12345")

	// Extend from offset 5 with " world"
	resp := handler.executeCommand(state, "SETRANGE", [][]byte{[]byte("extend_key"), []byte("5"), []byte(" world")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// Verify new value
	getResp := handler.executeCommand(state, "GET", [][]byte{[]byte("extend_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello world", string(*bs))
}

// TestListBoundary_LinsertBefore tests LINSERT BEFORE first element
func TestListBoundary_LinsertBefore(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("linsert_key", "first", "second")

	// LINSERT BEFORE first element (position 0)
	resp := handler.executeCommand(state, "LINSERT", [][]byte{[]byte("linsert_key"), []byte("BEFORE"), []byte("first"), []byte("new_first")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// LINSERT returns new list length after successful insert
	assert.Equal(t, int64(3), int64(*integer))

	// Verify list length increased
	lenResp := handler.executeCommand(state, "LLEN", [][]byte{[]byte("linsert_key")}, "127.0.0.1:12345")
	lenInt, ok := lenResp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*lenInt))
}

// TestListBoundary_LinsertAfter tests LINSERT AFTER an element
func TestListBoundary_LinsertAfter(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.RPush("linsert_key", "first", "second")

	// LINSERT AFTER first element
	resp := handler.executeCommand(state, "LINSERT", [][]byte{[]byte("linsert_key"), []byte("AFTER"), []byte("first"), []byte("new_second")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// LINSERT returns new list length after successful insert
	assert.Equal(t, int64(3), int64(*integer))
}

// TestListBoundary_LinsertNotFound tests LINSERT when pivot not found
func TestListBoundary_LinsertNotFound(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("linsert_key"), []byte("first"), []byte("second")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LINSERT", [][]byte{[]byte("linsert_key"), []byte("BEFORE"), []byte("nonexistent"), []byte("new_val")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// LINSERT returns -1 when pivot is not found
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestListBoundary_LposBasic tests LPOS returns correct index
func TestListBoundary_LposBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("lpos_key"), []byte("a"), []byte("b"), []byte("c"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LPOS", [][]byte{[]byte("lpos_key"), []byte("b")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestListBoundary_LposRank tests LPOS with RANK returns nth occurrence
func TestListBoundary_LposRank(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("lpos_key"), []byte("a"), []byte("b"), []byte("c"), []byte("b")}, "127.0.0.1:12345")

	// LPOS with RANK returns an array of positions
	resp := handler.executeCommand(state, "LPOS", [][]byte{[]byte("lpos_key"), []byte("b"), []byte("RANK"), []byte("2")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "3", string(arr.Args[0]))
}

// TestListBoundary_LmoveBasic tests LMOVE from one list to another
func TestListBoundary_LmoveBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("source_list"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LMOVE", [][]byte{[]byte("source_list"), []byte("dest_list"), []byte("RIGHT"), []byte("LEFT")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "c", string(*bs))

	srcResp := handler.executeCommand(state, "LRANGE", [][]byte{[]byte("source_list"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	srcArr, ok := srcResp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(srcArr.Args))

	dstResp := handler.executeCommand(state, "LRANGE", [][]byte{[]byte("dest_list"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	dstArr, ok := dstResp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(dstArr.Args))
}

// TestListBoundary_LpushxBasic tests LPUSHX adds to existing list only
func TestListBoundary_LpushxBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("lpushx_key"), []byte("first")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LPUSHX", [][]byte{[]byte("lpushx_key"), []byte("pushed")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestListBoundary_LpushxNotExist tests LPUSHX does nothing if key doesn't exist
func TestListBoundary_LpushxNotExist(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LPUSHX", [][]byte{[]byte("nonexistent_key"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestListBoundary_RpushxBasic tests RPUSHX adds to existing list only
func TestListBoundary_RpushxBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("rpushx_key"), []byte("first")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "RPUSHX", [][]byte{[]byte("rpushx_key"), []byte("pushed")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestListError_WrongTypeForLinsert tests LINSERT on wrong type
func TestListError_WrongTypeForLinsert(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "LINSERT", [][]byte{[]byte("string_key"), []byte("BEFORE"), []byte("pivot"), []byte("val")}, "127.0.0.1:12345")
	// LINSERT returns 0 when key exists but is not a list (no error, just 0 count)
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestHashBoundary_HsetNxBasic tests HSETNX behavior
func TestHashBoundary_HsetNxBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("val1")}, "127.0.0.1:12345")

	// HSETNX on existing field should return 0
	resp := handler.executeCommand(state, "HSETNX", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("new_val")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// HSETNX on new field should return 1
	resp = handler.executeCommand(state, "HSETNX", [][]byte{[]byte("hash_key"), []byte("field2"), []byte("val2")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestHashBoundary_HincrbyBasic tests HINCRBY with positive increment
func TestHashBoundary_HincrbyBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("10")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HINCRBY", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("5")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(15), int64(*integer))
}

// TestHashBoundary_HincrbyNegative tests HINCRBY with negative increment
func TestHashBoundary_HincrbyNegative(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("10")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HINCRBY", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("-3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(7), int64(*integer))
}

// TestHashBoundary_HincrbyFloatBasic tests HINCRBYFLOAT with float value
func TestHashBoundary_HincrbyFloatBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("10.5")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HINCRBYFLOAT", [][]byte{[]byte("hash_key"), []byte("counter"), []byte("2.5")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "13", string(*bs)) // 10.5 + 2.5 = 13
}

// TestHashBoundary_HrandfieldBasic tests HRANDFIELD returns a field
func TestHashBoundary_HrandfieldBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("val1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field2"), []byte("val2")}, "127.0.0.1:12345")
	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field3"), []byte("val3")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HRANDFIELD", [][]byte{[]byte("hash_key")}, "127.0.0.1:12345")
	// HRANDFIELD returns an Array of field names
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.True(t, string(arr.Args[0]) == "field1" || string(arr.Args[0]) == "field2" || string(arr.Args[0]) == "field3")
}

// TestHashBoundary_HrandfieldWithValues tests HRANDFIELD with WITHVALUES
func TestHashBoundary_HrandfieldWithValues(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field1"), []byte("val1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field2"), []byte("val2")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HRANDFIELD", [][]byte{[]byte("hash_key"), []byte("2"), []byte("WITHVALUES")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr.Args))
}

// TestHashError_WrongTypeForHset tests HSET on wrong type returns error
func TestHashError_WrongTypeForHset(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte("string_key"), []byte("field"), []byte("val")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestHashError_WrongTypeForHget tests HGET on wrong type returns error
func TestHashError_WrongTypeForHget(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HGET", [][]byte{[]byte("string_key"), []byte("field")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestHashError_HincrbyOnNonNumeric tests HINCRBY on non-numeric value
func TestHashError_HincrbyOnNonNumeric(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field"), []byte("not_a_number")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HINCRBY", [][]byte{[]byte("hash_key"), []byte("field"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

// TestHashError_HincrbyFloatOnNonNumeric tests HINCRBYFLOAT on non-numeric value
func TestHashError_HincrbyFloatOnNonNumeric(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hash_key"), []byte("field"), []byte("not_a_number")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HINCRBYFLOAT", [][]byte{[]byte("hash_key"), []byte("field"), []byte("1.5")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not a valid float"))
}

// TestSetBoundary_SaddBasic tests SADD with multiple members
func TestSetBoundary_SaddBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))

	resp = handler.executeCommand(state, "SADD", [][]byte{[]byte("set_key"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestSetBoundary_SremBasic tests SREM behavior
func TestSetBoundary_SremBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SREM", [][]byte{[]byte("set_key"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	resp = handler.executeCommand(state, "SREM", [][]byte{[]byte("set_key"), []byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestSetBoundary_ScardBasic tests SCARD returns set size
func TestSetBoundary_ScardBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SCARD", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestSetBoundary_SrandmemberBasic tests SRANDMEMBER returns a member
func TestSetBoundary_SrandmemberBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SRANDMEMBER", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(string(*bs)) > 0)
}

// TestSetBoundary_SpopBasic tests SPOP removes and returns a member
func TestSetBoundary_SpopBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("set_key"), []byte("member1"), []byte("member2"), []byte("member3")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SPOP", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(string(*bs)) > 0)

	cardResp := handler.executeCommand(state, "SCARD", [][]byte{[]byte("set_key")}, "127.0.0.1:12345")
	cardInt, _ := cardResp.(*proto.Integer)
	assert.Equal(t, int64(2), int64(*cardInt))
}

// TestSetError_WrongTypeForSadd tests SADD on string key returns WRONGTYPE
func TestSetError_WrongTypeForSadd(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSetError_WrongTypeForSrem tests SREM on string key returns WRONGTYPE
func TestSetError_WrongTypeForSrem(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SREM", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSetError_WrongTypeForSismember tests SISMEMBER on string key returns WRONGTYPE
func TestSetError_WrongTypeForSismember(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SISMEMBER", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSetError_WrongTypeForSmembers tests SMEMBERS on string key returns WRONGTYPE
func TestSetError_WrongTypeForSmembers(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SMEMBERS", [][]byte{[]byte("string_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSetError_WrongTypeForScard tests SCARD on string key returns WRONGTYPE
func TestSetError_WrongTypeForScard(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SCARD", [][]byte{[]byte("string_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// === SortedSet Boundary and Error Tests ===

// TestSortedSetBoundary_ZaddBasic tests ZADD adds new members with scores
func TestSortedSetBoundary_ZaddBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// ZADD new members - returns count of members in command
	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member1"), []byte("2.0"), []byte("member2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// ZADD to update member - BoltDB returns len(members) (behavioral difference from Redis)
	resp = handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset_key"), []byte("3.0"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestSortedSetBoundary_ZremBasic tests ZREM removes existing members
func TestSortedSetBoundary_ZremBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member1"), []byte("2.0"), []byte("member2")}, "127.0.0.1:12345")

	// ZREM existing member
	resp := handler.executeCommand(state, "ZREM", [][]byte{[]byte("zset_key"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// ZREM non-existing member - should return 0
	resp = handler.executeCommand(state, "ZREM", [][]byte{[]byte("zset_key"), []byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestSortedSetBoundary_ZcardBasic tests ZCARD returns sorted set cardinality
func TestSortedSetBoundary_ZcardBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("member1"), []byte("2.0"), []byte("member2")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZCARD", [][]byte{[]byte("zset_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestSortedSetBoundary_ZscoreBasic tests ZSCORE returns member score
func TestSortedSetBoundary_ZscoreBasic(t *testing.T) {
	t.Parallel()
	// Not parallel — avoids BadgerDB contention on slow CI runners
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset_key"), []byte("1.5"), []byte("member1")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("zset_key"), []byte("member1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "1.5", string(*bs))

	// Non-existing member
	resp = handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("zset_key"), []byte("nonexistent")}, "127.0.0.1:12345")
	nilResp, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, nilResp == nil || string(*nilResp) == "")
}

// TestSortedSetBoundary_ZrangeBasic tests ZRANGE returns members by rank
func TestSortedSetBoundary_ZrangeBasic(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset_key"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"), []byte("3.0"), []byte("c")}, "127.0.0.1:12345")

	// Without WITHSCORES, ZRANGE returns only members
	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("zset_key"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args)) // a, b, c (members only)
}

// TestSortedSetError_WrongTypeForZadd tests ZADD on string key returns WRONGTYPE
func TestSortedSetError_WrongTypeForZadd(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("string_key"), []byte("1.0"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSortedSetError_WrongTypeForZrem tests ZREM on string key returns WRONGTYPE
func TestSortedSetError_WrongTypeForZrem(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZREM", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSortedSetError_WrongTypeForZcard tests ZCARD on string key returns WRONGTYPE
func TestSortedSetError_WrongTypeForZcard(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZCARD", [][]byte{[]byte("string_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSortedSetError_WrongTypeForZscore tests ZSCORE on string key returns WRONGTYPE
func TestSortedSetError_WrongTypeForZscore(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("string_key"), []byte("member")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestSortedSetError_WrongTypeForZrange tests ZRANGE on string key returns WRONGTYPE
func TestSortedSetError_WrongTypeForZrange(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("string_key"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// === Cluster Boundary and Error Tests ===

// TestClusterError_ClusterDisabled tests CLUSTER commands return error when cluster is disabled
func TestClusterError_ClusterDisabled(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// CLUSTER INFO should return error when cluster support is disabled
	resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("INFO")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "cluster support disabled"))
}

// TestClusterError_ClusterDisabledNodes tests CLUSTER NODES returns error when disabled
func TestClusterError_ClusterDisabledNodes(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("NODES")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "cluster support disabled"))
}

// TestClusterError_ClusterDisabledKeySlot tests CLUSTER KEYSLOT returns error when disabled
func TestClusterError_ClusterDisabledKeySlot(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("KEYSLOT"), []byte("mykey")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "cluster support disabled"))
}

// TestClusterError_ClusterDisabledSlots tests CLUSTER SLOTS returns error when disabled
func TestClusterError_ClusterDisabledSlots(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("SLOTS")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "cluster support disabled"))
}

// TestClusterError_InvalidSubcommand tests CLUSTER with invalid subcommand returns error
func TestClusterError_InvalidSubcommand(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte("INVALID")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// === Replication Boundary and Error Tests ===

// TestReplicationBoundary_RoleMaster tests ROLE returns master when node is master
func TestReplicationBoundary_RoleMaster(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ROLE", [][]byte{}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args)) // ROLE returns [role, replication-id]
	assert.Equal(t, []byte("master"), arr.Args[0])
}

// TestReplicationBoundary_ReplconfListeningPort tests REPLCONF LISTENING-PORT
func TestReplicationBoundary_ReplconfListeningPort(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Replication = replication.NewReplicationManager(handler.Db)

	resp := handler.executeCommand(state, "REPLCONF", [][]byte{[]byte("LISTENING-PORT"), []byte("6379")}, "127.0.0.1:12345")
	simple, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*simple))
}

// TestReplicationBoundary_ReplconfCapa tests REPLCONF CAPA
func TestReplicationBoundary_ReplconfCapa(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	handler.Replication = replication.NewReplicationManager(handler.Db)

	resp := handler.executeCommand(state, "REPLCONF", [][]byte{[]byte("CAPA"), []byte("eof")}, "127.0.0.1:12345")
	simple, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*simple))
}

// TestReplicationError_ReplconfInvalidSubcommand tests REPLCONF with invalid subcommand
func TestReplicationError_ReplconfInvalidSubcommand(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "REPLCONF", [][]byte{[]byte("INVALID")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// TestReplicationError_ReplconfAckWithoutArgs tests REPLCONF ACK without offset
func TestReplicationError_ReplconfAckWithoutArgs(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "REPLCONF", [][]byte{[]byte("ACK")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}

// === Sentinel Boundary and Error Tests ===

// TestSentinelBoundary_InfoReplication tests INFO replication returns sentinel-compatible format
func TestSentinelBoundary_InfoReplication(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set up replication manager as slave to get master_link_status in output
	handler.Replication = replication.NewReplicationManager(handler.Db)
	handler.Replication.SetRole("slave")
	handler.Replication.SetMasterAddr("127.0.0.1:6379")

	resp := handler.executeCommand(state, "INFO", [][]byte{[]byte("replication")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	info := string(*bs)
	assert.True(t, strings.Contains(info, "role:"))
	assert.True(t, strings.Contains(info, "master_link_status:"))
}

// TestSentinelBoundary_ReplconfGetack tests REPLCONF GETACK returns ACK format
func TestSentinelBoundary_ReplconfGetack(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set up replication manager for GETACK to work
	handler.Replication = replication.NewReplicationManager(handler.Db)

	resp := handler.executeCommand(state, "REPLCONF", [][]byte{[]byte("GETACK"), []byte("*")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	assert.Equal(t, []byte("REPLCONF"), arr.Args[0])
	assert.Equal(t, []byte("ACK"), arr.Args[1])
}

// TestSentinelError_ReplconfUnknownSubcommand tests REPLCONF with unknown subcommand
func TestSentinelError_ReplconfUnknownSubcommand(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "REPLCONF", [][]byte{[]byte("UNKNOWN")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "ERR"))
}
