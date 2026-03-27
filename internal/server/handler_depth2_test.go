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

	resp := handler.executeCommand("HGET", [][]byte{[]byte("nonexistent_hash"), []byte("field")}, "127.0.0.1:12345")
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
		handler.executeCommand("HSET", [][]byte{[]byte("large_hash"), []byte(fmt.Sprintf("field%d", i)), []byte("value")}, "127.0.0.1:12345")
	}

	resp := handler.executeCommand("HLEN", [][]byte{[]byte("large_hash")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), int64(*integer))
}

// TestHashBoundary_EmptyField tests HSET with empty field value
func TestHashBoundary_EmptyField(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("hash"), []byte("empty_field"), []byte("")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HGET", [][]byte{[]byte("hash"), []byte("empty_field")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

// TestHashError_TypeMismatch tests hash command on string type
func TestHashError_TypeMismatch(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SET", [][]byte{[]byte("string_key"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HGET", [][]byte{[]byte("string_key"), []byte("field")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "WRONGTYPE"))
}

// TestHashError_WrongNumberOfArguments tests HSET with missing arguments
func TestHashError_WrongNumberOfArguments(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("HSET", [][]byte{[]byte("hash")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
}
