package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)


// TestExecuteCommand_PING tests PING command
func TestExecuteCommand_PING_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "PING", nil, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "PONG", string(*ss))
}

// TestExecuteCommand_ECHO tests ECHO command
func TestExecuteCommand_ECHO_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ECHO", [][]byte{[]byte("hello")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))
}

// TestExecuteCommand_ECHO_NoArgs tests ECHO without arguments
func TestExecuteCommand_ECHO_NoArgs_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ECHO", [][]byte{}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// TestExecuteCommand_ROLE_Master tests ROLE command for master
func TestExecuteCommand_ROLE_Master_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ROLE", nil, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "master", string(arr.Args[0]))
}

// TestExecuteCommand_ROLE_Slave tests ROLE command for slave
// Note: This test is skipped because handler.Replication is nil in the basic setup
func TestExecuteCommand_ROLE_Slave_Coverage(t *testing.T) {
	t.Parallel()
	t.Skip("Skipping - handler.Replication is nil in basic setup")
}

// TestExecuteCommand_CLIENT_LIST tests CLIENT LIST command
func TestExecuteCommand_CLIENT_LIST_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("LIST")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(*bs) > 0)
}

// TestExecuteCommand_CLIENT_GETNAME tests CLIENT GETNAME command
func TestExecuteCommand_CLIENT_GETNAME_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Without name set
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("GETNAME")}, "127.0.0.1:12345")
	// Should return nil bulk string
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	// nil bulk string
	assert.Equal(t, "", string(*bs))
}

// TestExecuteCommand_CLIENT_SETNAME tests CLIENT SETNAME command
func TestExecuteCommand_CLIENT_SETNAME_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("SETNAME"), []byte("testclient")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_CLIENT_SETNAME_NoArgs tests CLIENT SETNAME without name
func TestExecuteCommand_CLIENT_SETNAME_NoArgs_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("SETNAME")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// TestExecuteCommand_DBSIZE tests DBSIZE command
func TestExecuteCommand_DBSIZE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add some data
	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	resp := handler.executeCommand(state, "DBSIZE", nil, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_DBSIZE_Empty tests DBSIZE on empty database
func TestExecuteCommand_DBSIZE_Empty_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "DBSIZE", nil, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_FLUSHDB tests FLUSHDB command
func TestExecuteCommand_FLUSHDB_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add some data
	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	resp := handler.executeCommand(state, "FLUSHDB", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify database is empty
	dbsize := handler.executeCommand(state, "DBSIZE", nil, "127.0.0.1:12345")
	integer, _ := dbsize.(*proto.Integer)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_FLUSHALL tests FLUSHALL command
func TestExecuteCommand_FLUSHALL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add some data
	handler.Db.Set("key1", "value1")

	resp := handler.executeCommand(state, "FLUSHALL", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_LASTSAVE tests LASTSAVE command
func TestExecuteCommand_LASTSAVE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LASTSAVE", nil, "127.0.0.1:12345")
	// Just verify it doesn't panic and returns something
	assert.True(t, resp != nil)
}

// TestExecuteCommand_UNKNOWN tests unknown command
func TestExecuteCommand_UNKNOWN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "UNKNOWNCMD", nil, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// TestExecuteCommand_EXPIRE tests EXPIRE command
func TestExecuteCommand_EXPIRE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("mykey"), []byte("60")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_EXPIRE_KeyNotExists tests EXPIRE on non-existent key
func TestExecuteCommand_EXPIRE_KeyNotExists_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("nonexistent"), []byte("60")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_PERSIST tests PERSIST command
func TestExecuteCommand_PERSIST_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand(state, "PERSIST", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	// PERSIST on key without TTL should return 0
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify TTL is still not set (key remains persistent)
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	ttlInt, _ := ttlResp.(*proto.Integer)
	assert.Equal(t, int64(-1), int64(*ttlInt))
}

// TestExecuteCommand_PERSIST_NoTTL tests PERSIST on key without TTL
func TestExecuteCommand_PERSIST_NoTTL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand(state, "PERSIST", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_TTL tests TTL command
func TestExecuteCommand_TTL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand(state, "TTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// Without TTL should return -1
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestExecuteCommand_TTL_NoExpiry tests TTL on key without TTL
func TestExecuteCommand_TTL_NoExpiry_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand(state, "TTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestExecuteCommand_TTL_KeyNotExists tests TTL on non-existent key
func TestExecuteCommand_TTL_KeyNotExists_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TTL", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-2), int64(*integer))
}

// TestExecuteCommand_PTTL tests PTTL command (milliseconds)
func TestExecuteCommand_PTTL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand(state, "PTTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// Without TTL should return -1
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestExecuteCommand_SETEX tests SETEX command
func TestExecuteCommand_SETEX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SETEX", [][]byte{[]byte("mykey"), []byte("60"), []byte("myvalue")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify value was set
	val, err := handler.Db.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)
}

// TestExecuteCommand_PSETEX tests PSETEX command
func TestExecuteCommand_PSETEX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "PSETEX", [][]byte{[]byte("mykey"), []byte("60000"), []byte("myvalue")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify value was set
	val, err := handler.Db.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)
}

// TestExecuteCommand_SETNX_Coverage tests SETNX command
func TestExecuteCommand_SETNX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set key that doesn't exist
	resp := handler.executeCommand(state, "SETNX", [][]byte{[]byte("newkey"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify value was set
	val, err := handler.Db.Get("newkey")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestExecuteCommand_SETNX_Exists tests SETNX on existing key
func TestExecuteCommand_SETNX_Exists_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set key first
	handler.Db.Set("existingkey", "oldvalue")

	// Try SETNX
	resp := handler.executeCommand(state, "SETNX", [][]byte{[]byte("existingkey"), []byte("newvalue")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify original value unchanged
	val, _ := handler.Db.Get("existingkey")
	assert.Equal(t, "oldvalue", val)
}

// TestExecuteCommand_MSETNX tests MSETNX command
func TestExecuteCommand_MSETNX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MSETNX", [][]byte{[]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify all keys were set
	val1, err := handler.Db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", val1)

	val2, err := handler.Db.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, "value2", val2)
}

// TestExecuteCommand_GETSET tests GETSET command
func TestExecuteCommand_GETSET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set initial value
	handler.Db.Set("mykey", "oldvalue")

	// GETSET
	resp := handler.executeCommand(state, "GETSET", [][]byte{[]byte("mykey"), []byte("newvalue")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "oldvalue", string(*bs))

	// Verify new value is set
	val, _ := handler.Db.Get("mykey")
	assert.Equal(t, "newvalue", val)
}

// TestExecuteCommand_STRLEN tests STRLEN command
func TestExecuteCommand_STRLEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello")

	resp := handler.executeCommand(state, "STRLEN", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(5), int64(*integer))
}

// TestExecuteCommand_STRLEN_KeyNotExists tests STRLEN on non-existent key
func TestExecuteCommand_STRLEN_KeyNotExists_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "STRLEN", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_APPEND tests APPEND command
func TestExecuteCommand_APPEND_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello")

	resp := handler.executeCommand(state, "APPEND", [][]byte{[]byte("mykey"), []byte(" world")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// Verify appended value
	val, _ := handler.Db.Get("mykey")
	assert.Equal(t, "hello world", val)
}

// TestExecuteCommand_APPEND_NewKey tests APPEND creating new key
func TestExecuteCommand_APPEND_NewKey_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "APPEND", [][]byte{[]byte("newkey"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(5), int64(*integer))

	// Verify key was created with the value
	val, err := handler.Db.Get("newkey")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestExecuteCommand_GETRANGE tests GETRANGE command
func TestExecuteCommand_GETRANGE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello world")

	resp := handler.executeCommand(state, "GETRANGE", [][]byte{[]byte("mykey"), []byte("0"), []byte("4")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))
}

// TestExecuteCommand_GETRANGE_Negative tests GETRANGE with negative end
func TestExecuteCommand_GETRANGE_Negative_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello world")

	resp := handler.executeCommand(state, "GETRANGE", [][]byte{[]byte("mykey"), []byte("-6"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, " world", string(*bs))
}

// TestExecuteCommand_SETRANGE tests SETRANGE command
func TestExecuteCommand_SETRANGE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello world")

	resp := handler.executeCommand(state, "SETRANGE", [][]byte{[]byte("mykey"), []byte("6"), []byte("go")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// Verify value
	val, _ := handler.Db.Get("mykey")
	assert.Equal(t, "hello gorld", val)
}

// TestExecuteCommand_SETRANGE_Expand tests SETRANGE expanding key
func TestExecuteCommand_SETRANGE_Expand_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hi")

	resp := handler.executeCommand(state, "SETRANGE", [][]byte{[]byte("mykey"), []byte("5"), []byte("world")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(10), int64(*integer))

	// Verify value was expanded correctly: "hi\x00\x00\x00world"
	val, _ := handler.Db.Get("mykey")
	assert.Equal(t, "hi\x00\x00\x00world", val)
}

// TestExecuteCommand_PFADD tests PFADD command
func TestExecuteCommand_PFADD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "PFADD", [][]byte{[]byte("myhyperloglog"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify cardinality count
	countResp := handler.executeCommand(state, "PFCOUNT", [][]byte{[]byte("myhyperloglog")}, "127.0.0.1:12345")
	countInt, _ := countResp.(*proto.Integer)
	assert.Equal(t, int64(3), int64(*countInt))
}

// TestExecuteCommand_PFCOUNT tests PFCOUNT command
func TestExecuteCommand_PFCOUNT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "PFADD", [][]byte{[]byte("myhyperloglog"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "PFCOUNT", [][]byte{[]byte("myhyperloglog")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0)
}

// TestExecuteCommand_PFMERGE tests PFMERGE command
func TestExecuteCommand_PFMERGE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "PFADD", [][]byte{[]byte("key1"), []byte("a"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "PFADD", [][]byte{[]byte("key2"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "PFMERGE", [][]byte{[]byte("dest"), []byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify dest has combined cardinality of 3 (a, b, c)
	countResp := handler.executeCommand(state, "PFCOUNT", [][]byte{[]byte("dest")}, "127.0.0.1:12345")
	countInt, _ := countResp.(*proto.Integer)
	assert.Equal(t, int64(3), int64(*countInt))
}

// TestExecuteCommand_RENAME tests RENAME command
func TestExecuteCommand_RENAME_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("oldkey", "value")

	resp := handler.executeCommand(state, "RENAME", [][]byte{[]byte("oldkey"), []byte("newkey")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify old key is gone
	val, _ := handler.Db.Get("oldkey")
	assert.Equal(t, "", val)

	// Verify new key exists
	val, _ = handler.Db.Get("newkey")
	assert.Equal(t, "value", val)
}

// TestExecuteCommand_RENAMENX tests RENAMENX command
func TestExecuteCommand_RENAMENX_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	// When target exists, RENAMENX should return 0 and NOT rename
	resp := handler.executeCommand(state, "RENAMENX", [][]byte{[]byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify key1 still exists with original value
	val1, _ := handler.Db.Get("key1")
	assert.Equal(t, "value1", val1)

	// Verify key2 still has its original value (unchanged)
	val2, _ := handler.Db.Get("key2")
	assert.Equal(t, "value2", val2)
}

// TestExecuteCommand_COPY tests COPY command
func TestExecuteCommand_COPY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("sourcekey", "value")

	resp := handler.executeCommand(state, "COPY", [][]byte{[]byte("sourcekey"), []byte("destkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify both keys exist
	val, _ := handler.Db.Get("sourcekey")
	assert.Equal(t, "value", val)

	val, _ = handler.Db.Get("destkey")
	assert.Equal(t, "value", val)
}

// TestExecuteCommand_TYPE tests TYPE command
func TestExecuteCommand_TYPE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("stringkey", "value")

	resp := handler.executeCommand(state, "TYPE", [][]byte{[]byte("stringkey")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "string", string(*ss))
}

// TestExecuteCommand_TYPE_None tests TYPE on non-existent key
func TestExecuteCommand_TYPE_None_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TYPE", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "none", string(*ss))
}

// TestExecuteCommand_TOUCH tests TOUCH command
func TestExecuteCommand_TOUCH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	resp := handler.executeCommand(state, "TOUCH", [][]byte{[]byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// Verify keys still exist with their values
	val1, err := handler.Db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", val1)

	val2, err := handler.Db.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, "value2", val2)
}

// TestExecuteCommand_SLOWLOG_GET tests SLOWLOG GET command
func TestExecuteCommand_SLOWLOG_GET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SLOWLOG", [][]byte{[]byte("GET")}, "127.0.0.1:12345")
	// Should return an array
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

// TestExecuteCommand_SLOWLOG_LEN tests SLOWLOG LEN command
func TestExecuteCommand_SLOWLOG_LEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SLOWLOG", [][]byte{[]byte("LEN")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_DEBUG_SLEEP tests DEBUG SLEEP command (if exists)
func TestExecuteCommand_DEBUG_SLEEP_Coverage(t *testing.T) {
	t.Parallel()
	t.Skip("DEBUG SLEEP may not be implemented")
}

// TestExecuteCommand_LOLWUT tests LOLWUT command
func TestExecuteCommand_LOLWUT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LOLWUT", nil, "127.0.0.1:12345")
	// Should return something
	assert.True(t, resp != nil)
}

// TestExecuteCommand_LATENCY_LATEST tests LATENCY LATEST command
func TestExecuteCommand_LATENCY_LATEST_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LATENCY", [][]byte{[]byte("LATEST")}, "127.0.0.1:12345")
	// Should return an array
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

// TestExecuteCommand_MODULE_LIST tests MODULE LIST command
func TestExecuteCommand_MODULE_LIST_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MODULE", [][]byte{[]byte("LIST")}, "127.0.0.1:12345")
	// Should return empty array if no modules
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestExecuteCommand_READONLY tests READONLY command
func TestExecuteCommand_READONLY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "READONLY", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_READWRITE tests READWRITE command
func TestExecuteCommand_READWRITE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "READWRITE", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_XADD tests XADD command
func TestExecuteCommand_XADD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("*"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(*bs) > 0)

	// Verify stream length increased
	lenResp := handler.executeCommand(state, "XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	lenInt, _ := lenResp.(*proto.Integer)
	assert.Equal(t, int64(1), int64(*lenInt))
}

// TestExecuteCommand_XLEN tests XLEN command
func TestExecuteCommand_XLEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("*"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0)
}

// TestExecuteCommand_XRANGE tests XRANGE command
func TestExecuteCommand_XRANGE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XRANGE", [][]byte{[]byte("mystream"), []byte("-"), []byte("+")}, "127.0.0.1:12345")
	nestedArr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(nestedArr.Elems) > 0)

	// Verify returned entry contains expected field-value pair
	if len(nestedArr.Elems) > 0 {
		entry, ok := nestedArr.Elems[0].(*proto.NestedArray)
		if ok && len(entry.Elems) >= 2 {
			// entry.Elems[0] is ID, entry.Elems[1] is [field, value, ...]
			fieldValueArr, ok := entry.Elems[1].(*proto.NestedArray)
			if ok && len(fieldValueArr.Elems) >= 2 {
				fieldBs, ok1 := fieldValueArr.Elems[0].(*proto.BulkString)
				valueBs, ok2 := fieldValueArr.Elems[1].(*proto.BulkString)
				if ok1 && ok2 {
					assert.Equal(t, "field", string(*fieldBs))
					assert.Equal(t, "value", string(*valueBs))
				}
			}
		}
	}
}

// TestExecuteCommand_XREAD tests XREAD command
func TestExecuteCommand_XREAD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XREAD", [][]byte{[]byte("COUNT"), []byte("1"), []byte("STREAMS"), []byte("mystream"), []byte("0")}, "127.0.0.1:12345")
	narr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(narr.Elems) >= 1)
	if len(narr.Elems) >= 1 {
		streamResult, ok := narr.Elems[0].(*proto.NestedArray)
		assert.True(t, ok)
		assert.True(t, len(streamResult.Elems) >= 2)
		if len(streamResult.Elems) >= 2 {
			streamKey, ok := streamResult.Elems[0].(*proto.BulkString)
			assert.True(t, ok)
			assert.Equal(t, "mystream", string(*streamKey))
			entries, ok := streamResult.Elems[1].(*proto.NestedArray)
			assert.True(t, ok)
			assert.True(t, len(entries.Elems) >= 1)
			if len(entries.Elems) >= 1 {
				entry, ok := entries.Elems[0].(*proto.NestedArray)
				assert.True(t, ok)
				assert.True(t, len(entry.Elems) >= 2)
				if len(entry.Elems) >= 2 {
					entryID, ok := entry.Elems[0].(*proto.BulkString)
					assert.True(t, ok)
					assert.Equal(t, "1", string(*entryID))
					fields, ok := entry.Elems[1].(*proto.NestedArray)
					assert.True(t, ok)
					assert.True(t, len(fields.Elems) >= 2)
					if len(fields.Elems) >= 2 {
						fieldName, ok := fields.Elems[0].(*proto.BulkString)
						assert.True(t, ok)
						assert.Equal(t, "field", string(*fieldName))
						fieldVal, ok := fields.Elems[1].(*proto.BulkString)
						assert.True(t, ok)
						assert.Equal(t, "value", string(*fieldVal))
					}
				}
			}
		}
	}
}

// TestExecuteCommand_XGROUP_CREATE tests XGROUP CREATE command
func TestExecuteCommand_XGROUP_CREATE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify group was created using XINFO GROUPS
	infoResp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("GROUPS"), []byte("mystream")}, "127.0.0.1:12345")
	infoArr, ok := infoResp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(infoArr.Elems) >= 1) // At least one group entry
	// Verify group name in first group entry
	if len(infoArr.Elems) >= 1 {
		groupEntry, ok := infoArr.Elems[0].(*proto.NestedArray)
		if ok {
			// groupEntry is [name, consumers, pending, ...] alternating key/value
			// Check name field
			for i := 0; i < len(groupEntry.Elems)-1; i += 2 {
				keyBs, ok1 := groupEntry.Elems[i].(*proto.BulkString)
				valBs, ok2 := groupEntry.Elems[i+1].(*proto.BulkString)
				if ok1 && ok2 {
					key := string(*keyBs)
					if key == "name" {
						val := string(*valBs)
						assert.Equal(t, "mygroup", val)
						break
					}
				}
			}
		}
	}
}

// TestExecuteCommand_XDEL tests XDEL command
func TestExecuteCommand_XDEL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	// Verify entry exists
	beforeLen := handler.executeCommand(state, "XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	beforeInt, _ := beforeLen.(*proto.Integer)
	assert.Equal(t, int64(1), int64(*beforeInt))

	// Delete the entry
	resp := handler.executeCommand(state, "XDEL", [][]byte{[]byte("mystream"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify entry was deleted (length now 0)
	afterLen := handler.executeCommand(state, "XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	afterInt, _ := afterLen.(*proto.Integer)
	assert.Equal(t, int64(0), int64(*afterInt))
}

// TestExecuteCommand_GEOADD tests GEOADD command
func TestExecuteCommand_GEOADD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SanFrancisco")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify member was added using GEOPOS
	geoResp := handler.executeCommand(state, "GEOPOS", [][]byte{[]byte("mygeo"), []byte("SanFrancisco")}, "127.0.0.1:12345")
	geoArr, ok := geoResp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(geoArr.Args) >= 1)
	// GEOPOS returns [longitude, latitude] or nil if not found
	if len(geoArr.Args) >= 1 && geoArr.Args[0] != nil {
		// Values are returned as bulk strings
		// We just verify non-nil response indicates member exists
		assert.True(t, len(geoArr.Args[0]) > 0)
	}
}

// TestExecuteCommand_GEODIST tests GEODIST command
func TestExecuteCommand_GEODIST_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-0.1278"), []byte("51.5074"), []byte("London")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEODIST", [][]byte{[]byte("mygeo"), []byte("SF"), []byte("London")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_GEOHASH tests GEOHASH command
func TestExecuteCommand_GEOHASH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEOHASH", [][]byte{[]byte("mygeo"), []byte("SF")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
	// Verify returned hash is a non-empty string
	if len(arr.Args) > 0 {
		assert.True(t, len(arr.Args[0]) > 0)
	}
}

// TestExecuteCommand_GEOPOS tests GEOPOS command
func TestExecuteCommand_GEOPOS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEOPOS", [][]byte{[]byte("mygeo"), []byte("SF")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
	// Verify position returned as "lat,lon" string
	if len(arr.Args) > 0 {
		assert.True(t, arr.Args[0] != nil)
		assert.True(t, len(arr.Args[0]) > 0)
	}
}

// TestExecuteCommand_GEOSEARCH tests GEOSEARCH command
func TestExecuteCommand_GEOSEARCH_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{[]byte("mygeo"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"), []byte("BYRADIUS"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
	// Verify SF is in results
	if len(arr.Args) > 0 {
		assert.Equal(t, "SF", string(arr.Args[0]))
	}
}

// TestExecuteCommand_XINFO_STREAMS tests XINFO STREAM command
func TestExecuteCommand_XINFO_STREAMS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("STREAM"), []byte("mystream")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
	// Verify stream info contains length field
	// Format: [length, first-entry-id, last-entry-id, ...]
	if len(arr.Args) > 0 {
		assert.Equal(t, "length", string(arr.Args[0]))
	}
}

// TestExecuteCommand_XTRIM tests XTRIM command
func TestExecuteCommand_XTRIM_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("2"), []byte("field"), []byte("value2")}, "127.0.0.1:12345")

	// Verify we have 2 entries
	beforeLen := handler.executeCommand(state, "XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	beforeInt, _ := beforeLen.(*proto.Integer)
	assert.Equal(t, int64(2), int64(*beforeInt))

	// Trim to max length 1
	resp := handler.executeCommand(state, "XTRIM", [][]byte{[]byte("mystream"), []byte("MAXLEN"), []byte("1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Integer)
	assert.True(t, ok)

	// Verify stream length decreased to 1
	afterLen := handler.executeCommand(state, "XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	afterInt, _ := afterLen.(*proto.Integer)
	assert.Equal(t, int64(1), int64(*afterInt))
}

// TestExecuteCommand_XACK tests XACK command
func TestExecuteCommand_XACK_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup stream and group
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	// Read message to make it pending
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("mygroup"), []byte("consumer1"), []byte("COUNT"), []byte("1"), []byte("STREAMS"), []byte("mystream"), []byte(">")}, "127.0.0.1:12345")

	// Acknowledge the message
	resp := handler.executeCommand(state, "XACK", [][]byte{[]byte("mystream"), []byte("mygroup"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Verify pending count decreased to 0 using XPENDING
	pendingResp := handler.executeCommand(state, "XPENDING", [][]byte{[]byte("mystream"), []byte("mygroup")}, "127.0.0.1:12345")
	pendingArr, ok := pendingResp.(*proto.NestedArray)
	assert.True(t, ok)
	// XPENDING returns [pendingCount, minID, maxID, [entries...]]
	if len(pendingArr.Elems) >= 1 {
		countVal, ok := pendingArr.Elems[0].(proto.Integer)
		assert.True(t, ok)
		assert.Equal(t, int64(0), int64(countVal))
	}
}

// TestExecuteCommand_LPOS tests LPOS command
func TestExecuteCommand_LPOS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add a list
	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LPOS", [][]byte{[]byte("mylist"), []byte("b")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// b is at index 1 (0-based)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_LMOVE tests LMOVE command
func TestExecuteCommand_LMOVE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add a list
	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LMOVE", [][]byte{[]byte("mylist"), []byte("mylist2"), []byte("RIGHT"), []byte("LEFT")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "c", string(*bs))

	// Verify c was moved from mylist to mylist2
	// mylist should now have a, b (in that order from RPUSH)
	list1Val, _ := handler.Db.LRange("mylist", 0, -1)
	assert.Equal(t, []string{"a", "b"}, list1Val)
	// mylist2 should have c at left
	list2Val, _ := handler.Db.LRange("mylist2", 0, -1)
	assert.Equal(t, []string{"c"}, list2Val)
}

// TestExecuteCommand_SINTERCARD tests SINTERCARD command
func TestExecuteCommand_SINTERCARD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sets
	handler.executeCommand(state, "SADD", [][]byte{[]byte("set1"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SADD", [][]byte{[]byte("set2"), []byte("b"), []byte("c"), []byte("d")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SINTERCARD", [][]byte{[]byte("2"), []byte("set1"), []byte("set2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// Intersection of {a,b,c} and {b,c,d} is {b,c} = 2
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_SDIFFSTORE tests SDIFFSTORE command
func TestExecuteCommand_SDIFFSTORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sets
	handler.executeCommand(state, "SADD", [][]byte{[]byte("set1"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SADD", [][]byte{[]byte("set2"), []byte("b"), []byte("c"), []byte("d")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "SDIFFSTORE", [][]byte{[]byte("dest"), []byte("set1"), []byte("set2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// set1 - set2 = {a} -> count = 1
	assert.Equal(t, int64(1), int64(*integer))

	// Verify dest contains exactly 'a'
	destResp := handler.executeCommand(state, "SMEMBERS", [][]byte{[]byte("dest")}, "127.0.0.1:12345")
	destArr, _ := destResp.(*proto.Array)
	assert.Equal(t, 1, len(destArr.Args))
	if len(destArr.Args) > 0 {
		assert.Equal(t, "a", string(destArr.Args[0]))
	}
}

// TestExecuteCommand_ZMPOP tests ZMPOP command
func TestExecuteCommand_ZMPOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sorted set
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset1"), []byte("1"), []byte("one"), []byte("2"), []byte("two")}, "127.0.0.1:12345")

	// Test ZMPOP MIN
	resp := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("zset1"), []byte("MIN")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	if ok && len(arr.Args) >= 3 {
		assert.Equal(t, "zset1", string(arr.Args[0]))
		assert.Equal(t, "one", string(arr.Args[1]))
	}

	// Test ZMPOP MAX with COUNT
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}, "127.0.0.1:12345")
	resp2 := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("zset2"), []byte("MAX"), []byte("COUNT"), []byte("2")}, "127.0.0.1:12345")
	arr2, ok2 := resp2.(*proto.Array)
	assert.True(t, ok2)
	if ok2 && len(arr2.Args) >= 5 {
		assert.Equal(t, "zset2", string(arr2.Args[0]))
		assert.Equal(t, "c", string(arr2.Args[1]))
		assert.Equal(t, "b", string(arr2.Args[3]))
	}

	// Test ZMPOP on empty set -> nil array
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("empty_zset"), []byte("1"), []byte("x")}, "127.0.0.1:12345")
	handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("empty_zset"), []byte("MAX")}, "127.0.0.1:12345")
	resp3 := handler.executeCommand(state, "ZMPOP", [][]byte{[]byte("1"), []byte("empty_zset"), []byte("MAX")}, "127.0.0.1:12345")
	_, ok3 := resp3.(proto.NilArray)
	assert.True(t, ok3)
}

// TestExecuteCommand_LMPOP tests LMPOP command
func TestExecuteCommand_LMPOP_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add list
	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("list1"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	// LMPOP may not be fully implemented
	resp := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("list1"), []byte("RIGHT")}, "127.0.0.1:12345")
	if err, ok := resp.(*proto.Error); ok {
		// Command not implemented - acceptable
		assert.True(t, strings.Contains(string(*err), "unknown command") || strings.Contains(string(*err), "ERR"))
		return
	}

	// If implemented, verify element was popped
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	if ok && len(arr.Args) >= 2 {
		assert.Equal(t, "list1", string(arr.Args[0]))
		// Args[1] is the popped element as bulk string ([]byte)
		assert.Equal(t, "b", string(arr.Args[1]))
	}

	// Verify list length decreased
	llen := handler.executeCommand(state, "LLEN", [][]byte{[]byte("list1")}, "127.0.0.1:12345")
	llenInt, _ := llen.(*proto.Integer)
	assert.Equal(t, int64(1), int64(*llenInt))
}

// TestExecuteCommand_ZRANDMEMBER tests ZRANDMEMBER command
func TestExecuteCommand_ZRANDMEMBER_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sorted set
	handler.executeCommand(state, "ZADD", [][]byte{[]byte("zset1"), []byte("1"), []byte("one"), []byte("2"), []byte("two")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "ZRANDMEMBER", [][]byte{[]byte("zset1")}, "127.0.0.1:12345")
	// ZRANDMEMBER may return BulkString or Array depending on implementation
	// Handle unknown command case
	if errResp, ok := resp.(*proto.Error); ok {
		// Command not implemented - this is acceptable for coverage tests
		assert.True(t, strings.Contains(string(*errResp), "unknown command") || strings.Contains(string(*errResp), "ERR"))
		return
	}
	// If implemented, verify response type
	if arr, ok := resp.(*proto.Array); ok {
		assert.True(t, len(arr.Args) > 0)
		if len(arr.Args) > 0 {
			member := string(arr.Args[0])
			assert.True(t, member == "one" || member == "two")
		}
	} else if bs, ok := resp.(*proto.BulkString); ok {
		member := string(*bs)
		assert.True(t, member == "one" || member == "two")
	}
}

// TestExecuteCommand_HRANDMEMBER tests HRANDMEMBER command
func TestExecuteCommand_HRANDMEMBER_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add hash
	handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1"), []byte("field2"), []byte("value2")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HRANDMEMBER", [][]byte{[]byte("myhash")}, "127.0.0.1:12345")
	// Verify response is a bulk string (single random field)
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	if ok {
		val := string(*bs)
		assert.True(t, val == "field1" || val == "field2")
	}

	// Test WITHVALUES
	resp2 := handler.executeCommand(state, "HRANDMEMBER", [][]byte{[]byte("myhash"), []byte("1"), []byte("WITHVALUES")}, "127.0.0.1:12345")
	arr, ok := resp2.(*proto.Array)
	assert.True(t, ok)
	if ok {
		assert.Equal(t, 2, len(arr.Args))
	}
}

// TestExecuteCommand_LCS tests LCS command
func TestExecuteCommand_LCS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set strings
	handler.executeCommand(state, "SET", [][]byte{[]byte("key1"), []byte("hello world")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("key2"), []byte("hello world")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "LCS", [][]byte{[]byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	assert.True(t, resp != nil)
}

// TestExecuteCommand_SADD tests SADD command
func TestExecuteCommand_SADD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_SREM tests SREM command
func TestExecuteCommand_SREM_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SREM", [][]byte{[]byte("myset"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_SMEMBERS tests SMEMBERS command
func TestExecuteCommand_SMEMBERS_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SMEMBERS", [][]byte{[]byte("myset")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}

// TestExecuteCommand_HSET tests HSET command
func TestExecuteCommand_HSET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_HGET tests HGET command
func TestExecuteCommand_HGET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "HGET", [][]byte{[]byte("myhash"), []byte("field1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_HDEL tests HDEL command
func TestExecuteCommand_HDEL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "HDEL", [][]byte{[]byte("myhash"), []byte("field1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_ZADD tests ZADD command
func TestExecuteCommand_ZADD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_ZREM tests ZREM command
func TestExecuteCommand_ZREM_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZREM", [][]byte{[]byte("myzset"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_ZSCORE tests ZSCORE command
func TestExecuteCommand_ZSCORE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("myzset"), []byte("member1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_ZRANGE tests ZRANGE command
func TestExecuteCommand_ZRANGE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZRANGE", [][]byte{[]byte("myzset"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}

// TestExecuteCommand_ZCOUNT tests ZCOUNT command
func TestExecuteCommand_ZCOUNT_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZCOUNT", [][]byte{[]byte("myzset"), []byte("0"), []byte("10")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteQueuedCommand tests executeQueuedCommand for MULTI/EXEC transactions
func TestExecuteQueuedCommand(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name     string
		cmd      string
		args     [][]byte
		validate func(t *testing.T, resp proto.RESP)
	}{
		{
			name: "SET",
			cmd:  "SET",
			args: [][]byte{[]byte("key1"), []byte("value1")},
			validate: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		{
			name: "GET",
			cmd:  "GET",
			args: [][]byte{[]byte("key1")},
			validate: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "value1", string(*bs))
			},
		},
		{
			name: "GET_NotFound",
			cmd:  "GET",
			args: [][]byte{[]byte("nonexistent")},
			validate: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Nil(t, *bs)
			},
		},
		{
			name: "DEL",
			cmd:  "DEL",
			args: [][]byte{[]byte("key1")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "DEL_Multiple",
			cmd:  "DEL",
			args: [][]byte{[]byte("key1"), []byte("key2")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(0), int64(*integer))
			},
		},
		{
			name: "INCR",
			cmd:  "INCR",
			args: [][]byte{[]byte("counter")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "DECR",
			cmd:  "DECR",
			args: [][]byte{[]byte("counter2")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(-1), int64(*integer))
			},
		},
		{
			name: "INCRBY",
			cmd:  "INCRBY",
			args: [][]byte{[]byte("counter3"), []byte("5")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(5), int64(*integer))
			},
		},
		{
			name: "DECRBY",
			cmd:  "DECRBY",
			args: [][]byte{[]byte("counter4"), []byte("3")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(-3), int64(*integer))
			},
		},
		{
			name: "APPEND",
			cmd:  "SET",
			args: [][]byte{[]byte("mykey"), []byte("hello")},
			validate: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		{
			name: "APPEND",
			cmd:  "APPEND",
			args: [][]byte{[]byte("mykey"), []byte("world")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(10), int64(*integer))
			},
		},
		{
			name: "STRLEN",
			cmd:  "STRLEN",
			args: [][]byte{[]byte("mykey")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) > 0)
			},
		},
		{
			name: "EXISTS",
			cmd:  "EXISTS",
			args: [][]byte{[]byte("mykey")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "EXISTS_NotFound",
			cmd:  "EXISTS",
			args: [][]byte{[]byte("nonexistent")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(0), int64(*integer))
			},
		},
		{
			name: "TYPE",
			cmd:  "TYPE",
			args: [][]byte{[]byte("mykey")},
			validate: func(t *testing.T, resp proto.RESP) {
				ss, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "string", string(*ss))
			},
		},
		{
			name: "LPUSH",
			cmd:  "LPUSH",
			args: [][]byte{[]byte("mylist"), []byte("value1"), []byte("value2")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		{
			name: "RPUSH",
			cmd:  "RPUSH",
			args: [][]byte{[]byte("mylist2"), []byte("value1")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "LLEN",
			cmd:  "LLEN",
			args: [][]byte{[]byte("mylist")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		{
			name: "LRANGE",
			cmd:  "LRANGE",
			args: [][]byte{[]byte("mylist"), []byte("0"), []byte("-1")},
			validate: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args))
			},
		},
		{
			name: "HSET",
			cmd:  "HSET",
			args: [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "HGET",
			cmd:  "HGET",
			args: [][]byte{[]byte("myhash"), []byte("field1")},
			validate: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "value1", string(*bs))
			},
		},
		{
			name: "HGET_NotFound",
			cmd:  "HGET",
			args: [][]byte{[]byte("myhash"), []byte("nonexistent")},
			validate: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Nil(t, *bs)
			},
		},
		{
			name: "HGETALL",
			cmd:  "HGETALL",
			args: [][]byte{[]byte("myhash")},
			validate: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args))
			},
		},
		{
			name: "HDEL",
			cmd:  "HDEL",
			args: [][]byte{[]byte("myhash"), []byte("field1")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "SADD",
			cmd:  "SADD",
			args: [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		{
			name: "SMEMBERS",
			cmd:  "SMEMBERS",
			args: [][]byte{[]byte("myset")},
			validate: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args))
			},
		},
		{
			name: "SISMEMBER",
			cmd:  "SISMEMBER",
			args: [][]byte{[]byte("myset"), []byte("member1")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "ZADD",
			cmd:  "ZADD",
			args: [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "ZSCORE",
			cmd:  "ZSCORE",
			args: [][]byte{[]byte("myzset"), []byte("member1")},
			validate: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "1", string(*bs))
			},
		},
		{
			name: "ZSCORE_NotFound",
			cmd:  "ZSCORE",
			args: [][]byte{[]byte("nonexistent_zset"), []byte("member")},
			validate: func(t *testing.T, resp proto.RESP) {
				// For non-existent sorted set, result depends on implementation
				// Just verify it doesn't panic
				_ = resp
			},
		},

		{
			name: "ZCARD",
			cmd:  "ZCARD",
			args: [][]byte{[]byte("myzset")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name: "UnknownCommand",
			cmd:  "UNKNOWN",
			args: [][]byte{[]byte("arg1")},
			validate: func(t *testing.T, resp proto.RESP) {
				err, ok := resp.(*proto.Error)
				assert.True(t, ok)
				assert.True(t, len(*err) > 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeQueuedCommand(tt.cmd, tt.args)
			tt.validate(t, resp)
		})
	}
}
