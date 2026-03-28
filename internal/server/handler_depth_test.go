package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestStringBoundary_EmptyKey tests GET on nonexistent key
func TestStringBoundary_EmptyKey(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("GET", [][]byte{[]byte("nonexistent_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestStringBoundary_MaxValueSize tests SET/GET with large value
func TestStringBoundary_MaxValueSize(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// 10MB value
	largeValue := make([]byte, 10*1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	resp := handler.executeCommand("SET", [][]byte{[]byte("large_key"), largeValue}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	getResp := handler.executeCommand("GET", [][]byte{[]byte("large_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, 10*1024*1024, len(*bs))
}

// TestStringBoundary_EmptyString tests SET with empty value
func TestStringBoundary_EmptyString(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("SET", [][]byte{[]byte("empty_key"), []byte("")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	getResp := handler.executeCommand("GET", [][]byte{[]byte("empty_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestIncrBoundary_MaxInt64 tests INCR on boundary values
func TestIncrBoundary_MaxInt64(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set to max int64 - 1
	handler.executeCommand("SET", [][]byte{[]byte("max_counter"), []byte("9223372036854775806")}, "127.0.0.1:12345")

	resp := handler.executeCommand("INCR", [][]byte{[]byte("max_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(9223372036854775807), int64(*integer))

	// Next INCR should overflow
	resp = handler.executeCommand("INCR", [][]byte{[]byte("max_counter")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "overflow"))
}

// TestIncrBoundary_NegativeToPositive tests DECR crossing zero
func TestIncrBoundary_NegativeToPositive(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("neg_counter"), []byte("-5")}, "127.0.0.1:12345")

	resp := handler.executeCommand("INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-4), int64(*integer))

	// Cross zero
	for i := 0; i < 4; i++ {
		handler.executeCommand("INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	}
	resp = handler.executeCommand("INCR", [][]byte{[]byte("neg_counter")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestStringError_TypeMismatch tests string command on non-string type
func TestStringError_TypeMismatch(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a list type key
	handler.executeCommand("LPUSH", [][]byte{[]byte("list_key"), []byte("value")}, "127.0.0.1:12345")

	// APPEND on list key should error
	resp := handler.executeCommand("APPEND", [][]byte{[]byte("list_key"), []byte("extra")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestStringError_WrongNumberOfArguments tests GET without key argument
func TestStringError_WrongNumberOfArguments(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("GET", [][]byte{}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestStringError_SetGetInvalidArgs tests SET with missing value
func TestStringError_SetGetInvalidArgs(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// SET with only key (missing value)
	resp := handler.executeCommand("SET", [][]byte{[]byte("key_only")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestStringError_IncrOnFloat tests INCR on float value
func TestStringError_IncrOnFloat(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("float_key"), []byte("1.5")}, "127.0.0.1:12345")

	resp := handler.executeCommand("INCR", [][]byte{[]byte("float_key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	// Should error on non-integer value
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
}

// TestListBoundary_EmptyList tests LPOP on nonexistent key
func TestListBoundary_EmptyList(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("LPOP", [][]byte{[]byte("nonexistent_list")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListBoundary_SingleElement tests LPOP/RPOP on list with one element
func TestListBoundary_SingleElement(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("LPUSH", [][]byte{[]byte("single_list"), []byte("only")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LPOP", [][]byte{[]byte("single_list")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "only", string(*bs))

	// Second pop should return empty
	resp = handler.executeCommand("LPOP", [][]byte{[]byte("single_list")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListBoundary_IndexOverflow tests LLEN and LINDEX on large list
func TestListBoundary_IndexOverflow(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Push 1000 elements
	for i := 0; i < 1000; i++ {
		handler.executeCommand("RPUSH", [][]byte{[]byte("large_list"), []byte(string(rune('A' + i%26)))}, "127.0.0.1:12345")
	}

	resp := handler.executeCommand("LLEN", [][]byte{[]byte("large_list")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), int64(*integer))

	// Index beyond length
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("large_list"), []byte("9999")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListBoundary_NegativeIndex tests negative index access
func TestListBoundary_NegativeIndex(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("RPUSH", [][]byte{[]byte("neg_index_list"), []byte("first"), []byte("middle"), []byte("last")}, "127.0.0.1:12345")

	// -1 = last element
	resp := handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "last", string(*bs))

	// -2 = middle element
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-2")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "middle", string(*bs))

	// -3 = first element
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-3")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "first", string(*bs))

	// -4 beyond list (only 3 elements, so -4 is out of bounds)
	resp = handler.executeCommand("LINDEX", [][]byte{[]byte("neg_index_list"), []byte("-4")}, "127.0.0.1:12345")
	bs, ok = resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestListError_TypeMismatch tests list command on string type
func TestListError_TypeMismatch(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LPUSH", [][]byte{[]byte("string_key"), []byte("new")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestListError_InvalidIndex tests LSET with invalid index
func TestListError_InvalidIndex(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("RPUSH", [][]byte{[]byte("valid_list"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LSET", [][]byte{[]byte("valid_list"), []byte("5"), []byte("c")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "index out of range"))
}

// TestStringBoundary_SetbitGetbitBasic tests SETBIT and GETBIT
func TestStringBoundary_SetbitGetbitBasic(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set bit 7 to 1 (value = 128)
	resp := handler.executeCommand("SETBIT", [][]byte{[]byte("bit_key"), []byte("7"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer)) // old value was 0

	// GETBIT should return 1
	resp = handler.executeCommand("GETBIT", [][]byte{[]byte("bit_key"), []byte("7")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestStringBoundary_SetbitOutOfRange tests SETBIT on offset beyond practical limits
func TestStringBoundary_SetbitOutOfRange(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set bit at a large offset (creates a string of that size)
	resp := handler.executeCommand("SETBIT", [][]byte{[]byte("large_bit_key"), []byte("1048576"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify the bit was set
	resp = handler.executeCommand("GETBIT", [][]byte{[]byte("large_bit_key"), []byte("1048576")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestStringBoundary_SetrangeExtend tests SETRANGE extending a string
func TestStringBoundary_SetrangeExtend(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("extend_key"), []byte("hello")}, "127.0.0.1:12345")

	// Extend from offset 5 with " world"
	resp := handler.executeCommand("SETRANGE", [][]byte{[]byte("extend_key"), []byte("5"), []byte(" world")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// Verify new value
	getResp := handler.executeCommand("GET", [][]byte{[]byte("extend_key")}, "127.0.0.1:12345")
	bs, ok := getResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello world", string(*bs))
}
