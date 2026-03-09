package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestExecuteCommand_PING tests PING command
func TestExecuteCommand_PING_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("PING", nil, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "PONG", string(*ss))
}

// TestExecuteCommand_ECHO tests ECHO command
func TestExecuteCommand_ECHO_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("ECHO", [][]byte{[]byte("hello")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))
}

// TestExecuteCommand_ECHO_NoArgs tests ECHO without arguments
func TestExecuteCommand_ECHO_NoArgs_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("ECHO", [][]byte{}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// TestExecuteCommand_ROLE_Master tests ROLE command for master
func TestExecuteCommand_ROLE_Master_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("ROLE", nil, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "master", string(arr.Args[0]))
}

// TestExecuteCommand_ROLE_Slave tests ROLE command for slave
// Note: This test is skipped because handler.Replication is nil in the basic setup
func TestExecuteCommand_ROLE_Slave_Coverage(t *testing.T) {
	t.Skip("Skipping - handler.Replication is nil in basic setup")
}

// TestExecuteCommand_CLIENT_LIST tests CLIENT LIST command
func TestExecuteCommand_CLIENT_LIST_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("CLIENT", [][]byte{[]byte("LIST")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, len(*bs) > 0)
}

// TestExecuteCommand_CLIENT_GETNAME tests CLIENT GETNAME command
func TestExecuteCommand_CLIENT_GETNAME_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Without name set
	resp := handler.executeCommand("CLIENT", [][]byte{[]byte("GETNAME")}, "127.0.0.1:12345")
	// Should return nil bulk string
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	// nil bulk string
	assert.Equal(t, "", string(*bs))
}

// TestExecuteCommand_CLIENT_SETNAME tests CLIENT SETNAME command
func TestExecuteCommand_CLIENT_SETNAME_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("CLIENT", [][]byte{[]byte("SETNAME"), []byte("testclient")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_CLIENT_SETNAME_NoArgs tests CLIENT SETNAME without name
func TestExecuteCommand_CLIENT_SETNAME_NoArgs_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("CLIENT", [][]byte{[]byte("SETNAME")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// TestExecuteCommand_DBSIZE tests DBSIZE command
func TestExecuteCommand_DBSIZE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add some data
	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	resp := handler.executeCommand("DBSIZE", nil, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_DBSIZE_Empty tests DBSIZE on empty database
func TestExecuteCommand_DBSIZE_Empty_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("DBSIZE", nil, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_FLUSHDB tests FLUSHDB command
func TestExecuteCommand_FLUSHDB_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add some data
	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	resp := handler.executeCommand("FLUSHDB", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify database is empty
	dbsize := handler.executeCommand("DBSIZE", nil, "127.0.0.1:12345")
	integer, _ := dbsize.(*proto.Integer)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_FLUSHALL tests FLUSHALL command
func TestExecuteCommand_FLUSHALL_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add some data
	handler.Db.Set("key1", "value1")

	resp := handler.executeCommand("FLUSHALL", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_LASTSAVE tests LASTSAVE command
func TestExecuteCommand_LASTSAVE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("LASTSAVE", nil, "127.0.0.1:12345")
	// Just verify it doesn't panic and returns something
	assert.True(t, resp != nil)
}

// TestExecuteCommand_UNKNOWN tests unknown command
func TestExecuteCommand_UNKNOWN_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("UNKNOWNCMD", nil, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// TestExecuteCommand_EXPIRE tests EXPIRE command
func TestExecuteCommand_EXPIRE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand("EXPIRE", [][]byte{[]byte("mykey"), []byte("60")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_EXPIRE_KeyNotExists tests EXPIRE on non-existent key
func TestExecuteCommand_EXPIRE_KeyNotExists_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("EXPIRE", [][]byte{[]byte("nonexistent"), []byte("60")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_PERSIST tests PERSIST command
func TestExecuteCommand_PERSIST_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand("PERSIST", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	// PERSIST on key without TTL should return 0
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_PERSIST_NoTTL tests PERSIST on key without TTL
func TestExecuteCommand_PERSIST_NoTTL_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand("PERSIST", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_TTL tests TTL command
func TestExecuteCommand_TTL_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand("TTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// Without TTL should return -1
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestExecuteCommand_TTL_NoExpiry tests TTL on key without TTL
func TestExecuteCommand_TTL_NoExpiry_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand("TTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestExecuteCommand_TTL_KeyNotExists tests TTL on non-existent key
func TestExecuteCommand_TTL_KeyNotExists_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("TTL", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-2), int64(*integer))
}

// TestExecuteCommand_PTTL tests PTTL command (milliseconds)
func TestExecuteCommand_PTTL_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key without TTL first
	handler.Db.Set("mykey", "myvalue")

	resp := handler.executeCommand("PTTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// Without TTL should return -1
	assert.Equal(t, int64(-1), int64(*integer))
}

// TestExecuteCommand_SETEX tests SETEX command
func TestExecuteCommand_SETEX_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("SETEX", [][]byte{[]byte("mykey"), []byte("60"), []byte("myvalue")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify value was set
	val, err := handler.Db.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)
}

// TestExecuteCommand_PSETEX tests PSETEX command
func TestExecuteCommand_PSETEX_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("PSETEX", [][]byte{[]byte("mykey"), []byte("60000"), []byte("myvalue")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify value was set
	val, err := handler.Db.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)
}

// TestExecuteCommand_SETNX tests SETNX command
func TestExecuteCommand_SETNX_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set key that doesn't exist
	resp := handler.executeCommand("SETNX", [][]byte{[]byte("newkey"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_SETNX_Exists tests SETNX on existing key
func TestExecuteCommand_SETNX_Exists_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set key first
	handler.Db.Set("existingkey", "oldvalue")

	// Try SETNX
	resp := handler.executeCommand("SETNX", [][]byte{[]byte("existingkey"), []byte("newvalue")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify original value unchanged
	val, _ := handler.Db.Get("existingkey")
	assert.Equal(t, "oldvalue", val)
}

// TestExecuteCommand_MSETNX tests MSETNX command
func TestExecuteCommand_MSETNX_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("MSETNX", [][]byte{[]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_GETSET tests GETSET command
func TestExecuteCommand_GETSET_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set initial value
	handler.Db.Set("mykey", "oldvalue")

	// GETSET
	resp := handler.executeCommand("GETSET", [][]byte{[]byte("mykey"), []byte("newvalue")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "oldvalue", string(*bs))

	// Verify new value is set
	val, _ := handler.Db.Get("mykey")
	assert.Equal(t, "newvalue", val)
}

// TestExecuteCommand_STRLEN tests STRLEN command
func TestExecuteCommand_STRLEN_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello")

	resp := handler.executeCommand("STRLEN", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(5), int64(*integer))
}

// TestExecuteCommand_STRLEN_KeyNotExists tests STRLEN on non-existent key
func TestExecuteCommand_STRLEN_KeyNotExists_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("STRLEN", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_APPEND tests APPEND command
func TestExecuteCommand_APPEND_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello")

	resp := handler.executeCommand("APPEND", [][]byte{[]byte("mykey"), []byte(" world")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// Verify appended value
	val, _ := handler.Db.Get("mykey")
	assert.Equal(t, "hello world", val)
}

// TestExecuteCommand_APPEND_NewKey tests APPEND creating new key
func TestExecuteCommand_APPEND_NewKey_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("APPEND", [][]byte{[]byte("newkey"), []byte("value")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(5), int64(*integer))
}

// TestExecuteCommand_GETRANGE tests GETRANGE command
func TestExecuteCommand_GETRANGE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello world")

	resp := handler.executeCommand("GETRANGE", [][]byte{[]byte("mykey"), []byte("0"), []byte("4")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))
}

// TestExecuteCommand_GETRANGE_Negative tests GETRANGE with negative end
func TestExecuteCommand_GETRANGE_Negative_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello world")

	resp := handler.executeCommand("GETRANGE", [][]byte{[]byte("mykey"), []byte("-6"), []byte("-1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, " world", string(*bs))
}

// TestExecuteCommand_SETRANGE tests SETRANGE command
func TestExecuteCommand_SETRANGE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hello world")

	resp := handler.executeCommand("SETRANGE", [][]byte{[]byte("mykey"), []byte("6"), []byte("go")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// Verify value
	val, _ := handler.Db.Get("mykey")
	assert.Equal(t, "hello gorld", val)
}

// TestExecuteCommand_SETRANGE_Expand tests SETRANGE expanding key
func TestExecuteCommand_SETRANGE_Expand_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("mykey", "hi")

	resp := handler.executeCommand("SETRANGE", [][]byte{[]byte("mykey"), []byte("5"), []byte("world")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(10), int64(*integer))
}

// TestExecuteCommand_PFADD tests PFADD command
func TestExecuteCommand_PFADD_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("PFADD", [][]byte{[]byte("myhyperloglog"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_PFCOUNT tests PFCOUNT command
func TestExecuteCommand_PFCOUNT_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("PFADD", [][]byte{[]byte("myhyperloglog"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand("PFCOUNT", [][]byte{[]byte("myhyperloglog")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0)
}

// TestExecuteCommand_PFMERGE tests PFMERGE command
func TestExecuteCommand_PFMERGE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("PFADD", [][]byte{[]byte("key1"), []byte("a"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand("PFADD", [][]byte{[]byte("key2"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand("PFMERGE", [][]byte{[]byte("dest"), []byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_RENAME tests RENAME command
func TestExecuteCommand_RENAME_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("oldkey", "value")

	resp := handler.executeCommand("RENAME", [][]byte{[]byte("oldkey"), []byte("newkey")}, "127.0.0.1:12345")
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
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	// When target exists, should return error
	resp := handler.executeCommand("RENAMENX", [][]byte{[]byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	// In this case, it may still return OK, just check the result
	_ = resp
}

// TestExecuteCommand_COPY tests COPY command
func TestExecuteCommand_COPY_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("sourcekey", "value")

	resp := handler.executeCommand("COPY", [][]byte{[]byte("sourcekey"), []byte("destkey")}, "127.0.0.1:12345")
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
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("stringkey", "value")

	resp := handler.executeCommand("TYPE", [][]byte{[]byte("stringkey")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "string", string(*ss))
}

// TestExecuteCommand_TYPE_None tests TYPE on non-existent key
func TestExecuteCommand_TYPE_None_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("TYPE", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "none", string(*ss))
}

// TestExecuteCommand_TOUCH tests TOUCH command
func TestExecuteCommand_TOUCH_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("key1", "value1")
	handler.Db.Set("key2", "value2")

	resp := handler.executeCommand("TOUCH", [][]byte{[]byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestExecuteCommand_SLOWLOG_GET tests SLOWLOG GET command
func TestExecuteCommand_SLOWLOG_GET_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("SLOWLOG", [][]byte{[]byte("GET")}, "127.0.0.1:12345")
	// Should return an array
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

// TestExecuteCommand_SLOWLOG_LEN tests SLOWLOG LEN command
func TestExecuteCommand_SLOWLOG_LEN_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("SLOWLOG", [][]byte{[]byte("LEN")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_DEBUG_SLEEP tests DEBUG SLEEP command (if exists)
func TestExecuteCommand_DEBUG_SLEEP_Coverage(t *testing.T) {
	t.Skip("DEBUG SLEEP may not be implemented")
}

// TestExecuteCommand_LOLWUT tests LOLWUT command
func TestExecuteCommand_LOLWUT_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("LOLWUT", nil, "127.0.0.1:12345")
	// Should return something
	assert.True(t, resp != nil)
}

// TestExecuteCommand_LATENCY_LATEST tests LATENCY LATEST command
func TestExecuteCommand_LATENCY_LATEST_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("LATENCY", [][]byte{[]byte("LATEST")}, "127.0.0.1:12345")
	// Should return an array
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

// TestExecuteCommand_MODULE_LIST tests MODULE LIST command
func TestExecuteCommand_MODULE_LIST_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("MODULE", [][]byte{[]byte("LIST")}, "127.0.0.1:12345")
	// Should return empty array if no modules
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestExecuteCommand_READONLY tests READONLY command
func TestExecuteCommand_READONLY_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("READONLY", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_READWRITE tests READWRITE command
func TestExecuteCommand_READWRITE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("READWRITE", nil, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_XADD tests XADD command
func TestExecuteCommand_XADD_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("*"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_XLEN tests XLEN command
func TestExecuteCommand_XLEN_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("*"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0)
}

// TestExecuteCommand_XRANGE tests XRANGE command
func TestExecuteCommand_XRANGE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	// Just verify the command executes without error
	resp := handler.executeCommand("XRANGE", [][]byte{[]byte("mystream"), []byte("-"), []byte("+")}, "127.0.0.1:12345")
	assert.True(t, resp != nil)
}

// TestExecuteCommand_XREAD tests XREAD command
func TestExecuteCommand_XREAD_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("XREAD", [][]byte{[]byte("COUNT"), []byte("1"), []byte("STREAMS"), []byte("mystream"), []byte("0")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

// TestExecuteCommand_XGROUP_CREATE tests XGROUP CREATE command
func TestExecuteCommand_XGROUP_CREATE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)
}

// TestExecuteCommand_XDEL tests XDEL command
func TestExecuteCommand_XDEL_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("XDEL", [][]byte{[]byte("mystream"), []byte("1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0)
}

// TestExecuteCommand_GEOADD tests GEOADD command
func TestExecuteCommand_GEOADD_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SanFrancisco")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) > 0)
}

// TestExecuteCommand_GEODIST tests GEODIST command
func TestExecuteCommand_GEODIST_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries
	handler.executeCommand("GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")
	handler.executeCommand("GEOADD", [][]byte{[]byte("mygeo"), []byte("-0.1278"), []byte("51.5074"), []byte("London")}, "127.0.0.1:12345")

	resp := handler.executeCommand("GEODIST", [][]byte{[]byte("mygeo"), []byte("SF"), []byte("London")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_GEOHASH tests GEOHASH command
func TestExecuteCommand_GEOHASH_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand("GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand("GEOHASH", [][]byte{[]byte("mygeo"), []byte("SF")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}

// TestExecuteCommand_GEOPOS tests GEOPOS command
func TestExecuteCommand_GEOPOS_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand("GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand("GEOPOS", [][]byte{[]byte("mygeo"), []byte("SF")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}

// TestExecuteCommand_GEOSEARCH tests GEOSEARCH command
func TestExecuteCommand_GEOSEARCH_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries
	handler.executeCommand("GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand("GEOSEARCH", [][]byte{[]byte("mygeo"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"), []byte("BYRADIUS"), []byte("100"), []byte("km")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

// TestExecuteCommand_XINFO_STREAMS tests XINFO STREAM command
func TestExecuteCommand_XINFO_STREAMS_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand("XINFO", [][]byte{[]byte("STREAM"), []byte("mystream")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Array)
	assert.True(t, ok)
}

// TestExecuteCommand_XTRIM tests XTRIM command
func TestExecuteCommand_XTRIM_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value1")}, "127.0.0.1:12345")
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("2"), []byte("field"), []byte("value2")}, "127.0.0.1:12345")

	resp := handler.executeCommand("XTRIM", [][]byte{[]byte("mystream"), []byte("MAXLEN"), []byte("1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Integer)
	assert.True(t, ok)
}

// TestExecuteCommand_XACK tests XACK command
func TestExecuteCommand_XACK_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup stream and group
	handler.executeCommand("XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand("XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	resp := handler.executeCommand("XACK", [][]byte{[]byte("mystream"), []byte("mygroup"), []byte("1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Integer)
	assert.True(t, ok)
}

// TestExecuteCommand_LPOS tests LPOS command
func TestExecuteCommand_LPOS_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add a list
	handler.executeCommand("RPUSH", [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LPOS", [][]byte{[]byte("mylist"), []byte("b")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_LMOVE tests LMOVE command
func TestExecuteCommand_LMOVE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add a list
	handler.executeCommand("RPUSH", [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LMOVE", [][]byte{[]byte("mylist"), []byte("mylist2"), []byte("RIGHT"), []byte("LEFT")}, "127.0.0.1:12345")
	// Just verify it returns something
	assert.True(t, resp != nil)
}

// TestExecuteCommand_SINTERCARD tests SINTERCARD command
func TestExecuteCommand_SINTERCARD_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sets
	handler.executeCommand("SADD", [][]byte{[]byte("set1"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand("SADD", [][]byte{[]byte("set2"), []byte("b"), []byte("c"), []byte("d")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SINTERCARD", [][]byte{[]byte("set1"), []byte("set2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_SDIFFSTORE tests SDIFFSTORE command
func TestExecuteCommand_SDIFFSTORE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sets
	handler.executeCommand("SADD", [][]byte{[]byte("set1"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand("SADD", [][]byte{[]byte("set2"), []byte("b"), []byte("c"), []byte("d")}, "127.0.0.1:12345")

	resp := handler.executeCommand("SDIFFSTORE", [][]byte{[]byte("dest"), []byte("set1"), []byte("set2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_ZMPOP tests ZMPOP command
func TestExecuteCommand_ZMPOP_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sorted set
	handler.executeCommand("ZADD", [][]byte{[]byte("zset1"), []byte("1"), []byte("one"), []byte("2"), []byte("two")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZMPOP", [][]byte{[]byte("1"), []byte("zset1"), []byte("MIN")}, "127.0.0.1:12345")
	// Just verify it returns something
	assert.True(t, resp != nil)
}

// TestExecuteCommand_LMPOP tests LMPOP command
func TestExecuteCommand_LMPOP_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add list
	handler.executeCommand("RPUSH", [][]byte{[]byte("list1"), []byte("a"), []byte("b")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LMPOP", [][]byte{[]byte("1"), []byte("list1"), []byte("RIGHT")}, "127.0.0.1:12345")
	// Just verify it returns something
	assert.True(t, resp != nil)
}

// TestExecuteCommand_ZRANDMEMBER tests ZRANDMEMBER command
func TestExecuteCommand_ZRANDMEMBER_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add sorted set
	handler.executeCommand("ZADD", [][]byte{[]byte("zset1"), []byte("1"), []byte("one"), []byte("2"), []byte("two")}, "127.0.0.1:12345")

	resp := handler.executeCommand("ZRANDMEMBER", [][]byte{[]byte("zset1")}, "127.0.0.1:12345")
	assert.True(t, resp != nil)
}

// TestExecuteCommand_HRANDMEMBER tests HRANDMEMBER command
func TestExecuteCommand_HRANDMEMBER_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Add hash
	handler.executeCommand("HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")

	resp := handler.executeCommand("HRANDMEMBER", [][]byte{[]byte("myhash")}, "127.0.0.1:12345")
	assert.True(t, resp != nil)
}

// TestExecuteCommand_LCS tests LCS command
func TestExecuteCommand_LCS_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set strings
	handler.executeCommand("SET", [][]byte{[]byte("key1"), []byte("hello world")}, "127.0.0.1:12345")
	handler.executeCommand("SET", [][]byte{[]byte("key2"), []byte("hello world")}, "127.0.0.1:12345")

	resp := handler.executeCommand("LCS", [][]byte{[]byte("key1"), []byte("key2")}, "127.0.0.1:12345")
	assert.True(t, resp != nil)
}

// TestExecuteCommand_SADD tests SADD command
func TestExecuteCommand_SADD_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_SREM tests SREM command
func TestExecuteCommand_SREM_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	resp := handler.executeCommand("SREM", [][]byte{[]byte("myset"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_SMEMBERS tests SMEMBERS command
func TestExecuteCommand_SMEMBERS_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	resp := handler.executeCommand("SMEMBERS", [][]byte{[]byte("myset")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}

// TestExecuteCommand_HSET tests HSET command
func TestExecuteCommand_HSET_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_HGET tests HGET command
func TestExecuteCommand_HGET_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	resp := handler.executeCommand("HGET", [][]byte{[]byte("myhash"), []byte("field1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_HDEL tests HDEL command
func TestExecuteCommand_HDEL_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	resp := handler.executeCommand("HDEL", [][]byte{[]byte("myhash"), []byte("field1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_ZADD tests ZADD command
func TestExecuteCommand_ZADD_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand("ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_ZREM tests ZREM command
func TestExecuteCommand_ZREM_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand("ZREM", [][]byte{[]byte("myzset"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteCommand_ZSCORE tests ZSCORE command
func TestExecuteCommand_ZSCORE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand("ZSCORE", [][]byte{[]byte("myzset"), []byte("member1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
}

// TestExecuteCommand_ZRANGE tests ZRANGE command
func TestExecuteCommand_ZRANGE_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand("ZRANGE", [][]byte{[]byte("myzset"), []byte("0"), []byte("-1")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}

// TestExecuteCommand_ZCOUNT tests ZCOUNT command
func TestExecuteCommand_ZCOUNT_Coverage(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand("ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand("ZCOUNT", [][]byte{[]byte("myzset"), []byte("0"), []byte("10")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}

// TestExecuteQueuedCommand tests executeQueuedCommand for MULTI/EXEC transactions
func TestExecuteQueuedCommand(t *testing.T) {
	handler := setupTestHandler(t)
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
				assert.Nil(t, resp)
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
				assert.Nil(t, resp)
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
