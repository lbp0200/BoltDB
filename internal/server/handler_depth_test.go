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
