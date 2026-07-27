package server

import (
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

// TestExecuteCommand_CLIENT_NOEVICT2_Coverage tests CLIENT NOEVICT command
func TestExecuteCommand_CLIENT_NOEVICT2_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// CLIENT NOEVICT ON
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("NOEVICT"), []byte("ON")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// CLIENT NOEVICT OFF
	resp = handler.executeCommand(state, "CLIENT", [][]byte{[]byte("NOEVICT"), []byte("OFF")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
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
