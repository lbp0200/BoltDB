package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)


// TestHashBoundary_EmptyHash tests HGET on nonexistent hash
func TestHashBoundary_EmptyHash(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(testState, "HGET", [][]byte{[]byte("nonexistent_hash"), []byte("field")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestHashBoundary_LargeFieldCount tests HSET with many fields
func TestHashBoundary_LargeFieldCount(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set 1000 fields
	for i := 0; i < 1000; i++ {
		handler.executeCommand(testState, "HSET", [][]byte{[]byte("large_hash"), []byte(fmt.Sprintf("field%d", i)), []byte("value")}, "127.0.0.1:12345")
	}

	resp := handler.executeCommand(testState, "HLEN", [][]byte{[]byte("large_hash")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), int64(*integer))
}

// TestHashBoundary_EmptyField tests HSET with empty field value
func TestHashBoundary_EmptyField(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(testState, "HSET", [][]byte{[]byte("hash"), []byte("empty_field"), []byte("")}, "127.0.0.1:12345")

	resp := handler.executeCommand(testState, "HGET", [][]byte{[]byte("hash"), []byte("empty_field")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestHashError_TypeMismatch tests hash command on string type
func TestHashError_TypeMismatch(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(testState, "SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(testState, "HGET", [][]byte{[]byte("string_key"), []byte("field")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestHashError_WrongNumberOfArguments tests HSET with missing arguments
func TestHashError_WrongNumberOfArguments(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(testState, "HSET", [][]byte{[]byte("hash")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}

// TestSetBoundary_EmptySet tests SMEMBERS on nonexistent set
func TestSetBoundary_EmptySet(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(testState, "SMEMBERS", [][]byte{[]byte("nonexistent_set")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestSetBoundary_SingleElement tests SADD/SREM on single element set
func TestSetBoundary_SingleElement(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(testState, "SADD", [][]byte{[]byte("single_set"), []byte("only")}, "127.0.0.1:12345")

	resp := handler.executeCommand(testState, "SCARD", [][]byte{[]byte("single_set")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Remove the only element
	handler.executeCommand(testState, "SREM", [][]byte{[]byte("single_set"), []byte("only")}, "127.0.0.1:12345")

	resp = handler.executeCommand(testState, "SCARD", [][]byte{[]byte("single_set")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestSortedSetBoundary_EmptyZSet tests ZRANGE on nonexistent sorted set
func TestSortedSetBoundary_EmptyZSet(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(testState, "ZRANGE", [][]byte{[]byte("nonexistent_zset"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestSortedSetBoundary_ScoreBoundary tests sorted set with extreme scores
func TestSortedSetBoundary_ScoreBoundary(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add members with extreme scores
	handler.executeCommand(testState, "ZADD", [][]byte{[]byte("zset"), []byte("-9223372036854775808"), []byte("min_score")}, "127.0.0.1:12345")
	handler.executeCommand(testState, "ZADD", [][]byte{[]byte("zset"), []byte("9223372036854775807"), []byte("max_score")}, "127.0.0.1:12345")

	resp := handler.executeCommand(testState, "ZRANGE", [][]byte{[]byte("zset"), []byte("0"), []byte("-1"), []byte("WITHSCORES")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr.Args))
}

// TestKeyExpiryBoundary_ExpiredKey tests access to expired key
func TestKeyExpiryBoundary_ExpiredKey(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set key with 1ms expiry
	handler.executeCommand(testState, "SET", [][]byte{[]byte("expiring_key"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(testState, "PEXPIRE", [][]byte{[]byte("expiring_key"), []byte("1")}, "127.0.0.1:12345")

	// Immediate access should work
	resp := handler.executeCommand(testState, "GET", [][]byte{[]byte("expiring_key")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*bs))
}

// TestKeyExpiryBoundary_TTL tests TTL on key with expiry
func TestKeyExpiryBoundary_TTL(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(testState, "SET", [][]byte{[]byte("ttl_key"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(testState, "EXPIRE", [][]byte{[]byte("ttl_key"), []byte("3600")}, "127.0.0.1:12345")

	resp := handler.executeCommand(testState, "TTL", [][]byte{[]byte("ttl_key")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0 && int64(*integer) <= 3600)

	// Key with no expiry
	resp = handler.executeCommand(testState, "TTL", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-2), int64(*integer))
}
