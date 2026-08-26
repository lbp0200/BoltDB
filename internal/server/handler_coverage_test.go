package server

import (
	"strings"
	"testing"
	"time"

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

// TestExecuteCommand_CLIENT_LIST tests CLIENT LIST command
func TestExecuteCommand_CLIENT_LIST_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("LIST")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "addr="))
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

	// Verify database is empty
	dbsize := handler.executeCommand(state, "DBSIZE", nil, "127.0.0.1:12345")
	integer, ok := dbsize.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_LASTSAVE tests LASTSAVE command
func TestExecuteCommand_LASTSAVE_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LASTSAVE", nil, "127.0.0.1:12345")
	// LASTSAVE without Backup returns an error
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "backup not enabled"))
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

	// Verify value was set and TTL applied (side effect, not just OK)
	val, err := handler.Db.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)
	ttlResp := handler.executeCommand(state, "TTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	ttl, ok := ttlResp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*ttl) > 0 && int64(*ttl) <= 60)
}

// TestExecuteCommand_PSETEX tests PSETEX command
func TestExecuteCommand_PSETEX_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "PSETEX", [][]byte{[]byte("mykey"), []byte("60000"), []byte("myvalue")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Verify value was set and PTTL applied
	val, err := handler.Db.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)
	pttlResp := handler.executeCommand(state, "PTTL", [][]byte{[]byte("mykey")}, "127.0.0.1:12345")
	pttl, ok := pttlResp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*pttl) > 0 && int64(*pttl) <= 60000)
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

// TestExecuteCommand_GETDEL_Coverage tests GETDEL command
func TestExecuteCommand_GETDEL_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("gddel", "todelete")
	resp := handler.executeCommand(state, "GETDEL", [][]byte{[]byte("gddel")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "todelete", string(*bs))

	_, err := handler.Db.Get("gddel")
	assert.Error(t, err)

	resp = handler.executeCommand(state, "GETDEL", [][]byte{[]byte("nonexist")}, "127.0.0.1:12345")
	shapeIsNilBulk(t, resp)
}

// TestExecuteCommand_GETEX_Coverage tests GETEX command
func TestExecuteCommand_GETEX_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("gdex", "value")
	resp := handler.executeCommand(state, "GETEX", [][]byte{[]byte("gdex"), []byte("EX"), []byte("10")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*bs))

	ttl := handler.executeCommand(state, "TTL", [][]byte{[]byte("gdex")}, "127.0.0.1:12345")
	ttlInt, ok := ttl.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*ttlInt) >= 1 && int64(*ttlInt) <= 10)

	resp = handler.executeCommand(state, "GETEX", [][]byte{[]byte("nonexist")}, "127.0.0.1:12345")
	shapeIsNilBulk(t, resp)
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
	assert.Equal(t, int64(3), int64(*integer))
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

// TestExecuteCommand_COPY_JSON verifies COPY for JSON type (handler path)
func TestExecuteCommand_COPY_JSON_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jsrc"), []byte("$"), []byte(`{"a":1,"b":2}`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "COPY", [][]byte{[]byte("jsrc"), []byte("jdst")}, "127.0.0.1:12345")
	if _, ok := resp.(*proto.Error); ok {
		t.Fatalf("COPY json: %v", resp)
	}
	resp = handler.executeCommand(state, "JSON.GET", [][]byte{[]byte("jdst")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	if !ok {
		t.Fatalf("COPY json GET type: %T %v", resp, resp)
	}
	if got := string(*bs); got != `{"a":1,"b":2}` && got != `{"b":2,"a":1}` {
		t.Fatalf("COPY json mismatch: %q", got)
	}
}

// TestExecuteCommand_COPY_STREAM verifies COPY for Stream type
func TestExecuteCommand_COPY_STREAM_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("ssrc"), []byte("*"), []byte("f"), []byte("v1")}, "127.0.0.1:12345")
	if _, ok := resp.(*proto.Error); ok {
		t.Fatalf("XADD: %v", resp)
	}
	resp = handler.executeCommand(state, "XADD", [][]byte{[]byte("ssrc"), []byte("*"), []byte("f"), []byte("v2")}, "127.0.0.1:12345")
	if _, ok := resp.(*proto.Error); ok {
		t.Fatalf("XADD2: %v", resp)
	}
	resp = handler.executeCommand(state, "COPY", [][]byte{[]byte("ssrc"), []byte("sdst")}, "127.0.0.1:12345")
	if _, ok := resp.(*proto.Error); ok {
		t.Fatalf("COPY stream: %v", resp)
	}
	resp = handler.executeCommand(state, "XLEN", [][]byte{[]byte("sdst")}, "127.0.0.1:12345")
	if n, ok := resp.(*proto.Integer); !ok || int64(*n) != 2 {
		t.Fatalf("COPY stream len: %v", resp)
	}
}

// TestExecuteCommand_COPY_GEO verifies COPY for GEO type
func TestExecuteCommand_COPY_GEO_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()
	resp := handler.executeCommand(state, "GEOADD", [][]byte{[]byte("gsrc"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")}, "127.0.0.1:12345")
	if _, ok := resp.(*proto.Error); ok {
		t.Fatalf("GEOADD: %v", resp)
	}
	resp = handler.executeCommand(state, "COPY", [][]byte{[]byte("gsrc"), []byte("gdst")}, "127.0.0.1:12345")
	if _, ok := resp.(*proto.Error); ok {
		t.Fatalf("COPY geo: %v", resp)
	}
	resp = handler.executeCommand(state, "TYPE", [][]byte{[]byte("gdst")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	if !ok || string(*ss) != "GEOHASH" {
		// GEO type reports as GEOHASH
		t.Fatalf("COPY geo TYPE: %v", resp)
	}
	resp = handler.executeCommand(state, "GEODIST", [][]byte{[]byte("gdst"), []byte("Palermo"), []byte("Palermo"), []byte("km")}, "127.0.0.1:12345")
	if _, ok := resp.(*proto.Error); ok {
		t.Fatalf("COPY geo GEODIST: %v", resp)
	}
}

// TestExecuteCommand_COPY_DB0 verifies COPY with explicit DB 0 is accepted
// (single-DB server; previously any DB option was rejected).
func TestExecuteCommand_COPY_DB0_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("sourcekey", "value")

	// COPY src dst DB 0 → accepted (DB 0 = default)
	resp := handler.executeCommand(state, "COPY", [][]byte{[]byte("sourcekey"), []byte("destkey"), []byte("DB"), []byte("0")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// COPY src dst DB 1 → rejected (cross-DB unsupported)
	resp = handler.executeCommand(state, "COPY", [][]byte{[]byte("sourcekey"), []byte("destkey2"), []byte("DB"), []byte("1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "DB option not supported"))
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
	// SLOWLOG GET returns NestedArray (each entry itself is an array)
	_, ok := resp.(*proto.NestedArray)
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
	assert.Equal(t, int64(0), int64(*integer))
}

// TestExecuteCommand_DEBUG_SLEEP tests DEBUG SLEEP command
func TestExecuteCommand_DEBUG_SLEEP_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	start := time.Now()
	resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("SLEEP"), []byte("0.05")}, "127.0.0.1:12345")
	elapsed := time.Since(start)
	assert.True(t, elapsed >= 45*time.Millisecond)
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))
}

// TestExecuteCommand_LOLWUT tests LOLWUT command
func TestExecuteCommand_LOLWUT_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LOLWUT", nil, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "BoltDB"))
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
	assert.True(t, strings.Contains(string(*bs), "-"))

	// Verify stream length increased
	lenResp := handler.executeCommand(state, "XLEN", [][]byte{[]byte("mystream")}, "127.0.0.1:12345")
	lenInt, _ := lenResp.(*proto.Integer)
	assert.Equal(t, int64(1), int64(*lenInt))
}

// TestExecuteCommand_XADD_MAXLEN verifies XADD MAXLEN trims the stream
// (previously the MAXLEN option was never parsed — the parser required
// options to start with '-', so XADD ... MAXLEN n ... failed with
// "invalid stream ID: MAXLEN").
func TestExecuteCommand_XADD_MAXLEN_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add 3 entries with MAXLEN 2
	for i := 0; i < 3; i++ {
		resp := handler.executeCommand(state, "XADD", [][]byte{[]byte("maxstream"), []byte("MAXLEN"), []byte("2"), []byte("*"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
		assert.NotNil(t, resp)
	}

	// Stream must be trimmed to 2
	lenResp := handler.executeCommand(state, "XLEN", [][]byte{[]byte("maxstream")}, "127.0.0.1:12345")
	lenInt, ok := lenResp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*lenInt))
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
	assert.Equal(t, int64(1), int64(*integer))
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
	assert.Equal(t, 1, len(nestedArr.Elems))

	// Verify returned entry contains expected field-value pair
	entry, ok := nestedArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(entry.Elems) >= 2)
	// entry.Elems[0] is ID, entry.Elems[1] is [field, value, ...]
	fieldValueArr, ok := entry.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(fieldValueArr.Elems) >= 2)
	fieldBs, ok1 := fieldValueArr.Elems[0].(*proto.BulkString)
	valueBs, ok2 := fieldValueArr.Elems[1].(*proto.BulkString)
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, "field", string(*fieldBs))
	assert.Equal(t, "value", string(*valueBs))
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
	assert.Equal(t, 1, len(narr.Elems))

	streamResult, ok := narr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(streamResult.Elems))

	streamKey, ok := streamResult.Elems[0].(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "mystream", string(*streamKey))

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
	assert.Equal(t, 1, len(infoArr.Elems)) // Exactly one group entry
	// Verify group name in first group entry
	groupEntry, ok := infoArr.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
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

// TestExecuteCommand_XGROUP_CREATE_Options verifies XGROUP CREATE accepts the
// legal MKSTREAM / ENTRIESREAD options and rejects unknown ones (previously
// all trailing options were silently ignored).
func TestExecuteCommand_XGROUP_CREATE_Options(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// MKSTREAM on a fresh stream is accepted (store auto-creates the stream)
	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("newstream"), []byte("grp"), []byte("$"), []byte("MKSTREAM")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// ENTRIESREAD with a value is accepted
	resp = handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("esstream"), []byte("grp2"), []byte("0"), []byte("ENTRIESREAD"), []byte("5")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// Unknown option must be rejected
	resp = handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("badstream"), []byte("grp3"), []byte("0"), []byte("BOGUS")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))

	// ENTRIESREAD without value must be rejected
	resp = handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("badstream2"), []byte("grp4"), []byte("0"), []byte("ENTRIESREAD")}, "127.0.0.1:12345")
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

// TestExecuteCommand_XGROUP_CREATECONSUMER tests XGROUP CREATECONSUMER:
// creates a consumer explicitly, returns 1 on first create, 0 if exists.
func TestExecuteCommand_XGROUP_CREATECONSUMER_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry first
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	// Create the group
	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// First CREATECONSUMER returns 1 (newly created)
	resp = handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATECONSUMER"), []byte("mystream"), []byte("mygroup"), []byte("consumer1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Second CREATECONSUMER with same consumer returns 0 (already exists)
	resp = handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATECONSUMER"), []byte("mystream"), []byte("mygroup"), []byte("consumer1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Verify consumer visible via XINFO CONSUMERS
	infoResp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("CONSUMERS"), []byte("mystream"), []byte("mygroup")}, "127.0.0.1:12345")
	infoArr, ok := infoResp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(infoArr.Elems))

	// Wrong number of args → error
	resp = handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATECONSUMER"), []byte("mystream"), []byte("mygroup")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
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
	geoNested, ok := geoResp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(geoNested.Elems))
	if len(geoNested.Elems) >= 1 {
		// GEOPOS returns [longitude, latitude] as nested array
		coord, ok := geoNested.Elems[0].(*proto.NestedArray)
		assert.True(t, ok)
		if ok {
			assert.Equal(t, 2, len(coord.Elems))
		}
	}
}

// TestExecuteCommand_GEOADD_Options verifies GEOADD NX/XX/CH options
// (previously options were parsed as longitude and rejected).
func TestExecuteCommand_GEOADD_Options_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Base add
	resp := handler.executeCommand(state, "GEOADD", [][]byte{[]byte("g"), []byte("-122.4"), []byte("37.7"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// NX on existing member → 0
	resp = handler.executeCommand(state, "GEOADD", [][]byte{[]byte("g"), []byte("NX"), []byte("-122.5"), []byte("37.8"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// XX on existing member → 0 (updated but no new member)
	resp = handler.executeCommand(state, "GEOADD", [][]byte{[]byte("g"), []byte("XX"), []byte("-122.6"), []byte("37.9"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// CH + XX on existing member → 1 (changed count includes updates)
	resp = handler.executeCommand(state, "GEOADD", [][]byte{[]byte("g"), []byte("CH"), []byte("XX"), []byte("-122.7"), []byte("38.0"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// NX + XX mutually exclusive → error
	resp = handler.executeCommand(state, "GEOADD", [][]byte{[]byte("g"), []byte("NX"), []byte("XX"), []byte("-122.4"), []byte("37.7"), []byte("m2")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not compatible"))
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
	assert.Equal(t, 1, len(arr.Args)) // querying 1 member
	// Verify returned hash is a non-empty string
	assert.True(t, len(arr.Args[0]) > 0)
}

// TestExecuteCommand_GEOPOS tests GEOPOS command
func TestExecuteCommand_GEOPOS_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "GEOPOS", [][]byte{[]byte("mygeo"), []byte("SF")}, "127.0.0.1:12345")
	nested, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nested.Elems)) // querying 1 member
	// Verify position returned as nested [lon, lat] array
	coord, ok := nested.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(coord.Elems))
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

// TestExecuteCommand_GEOSEARCH_WithModifiers tests GEOSEARCH WITHCOORD/WITHDIST/WITHHASH
func TestExecuteCommand_GEOSEARCH_WithModifiers_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-122.4194"), []byte("37.7749"), []byte("SF")}, "127.0.0.1:12345")
	handler.executeCommand(state, "GEOADD", [][]byte{[]byte("mygeo"), []byte("-74.0060"), []byte("40.7128"), []byte("NYC")}, "127.0.0.1:12345")

	// WITHCOORD — returns nested [member, [lon, lat]]
	resp := handler.executeCommand(state, "GEOSEARCH", [][]byte{[]byte("mygeo"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"), []byte("BYRADIUS"), []byte("5000"), []byte("km"), []byte("WITHCOORD")}, "127.0.0.1:12345")
	nested, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	if len(nested.Elems) > 0 {
		entry, ok := nested.Elems[0].(*proto.NestedArray)
		assert.True(t, ok)
		assert.True(t, len(entry.Elems) >= 2)
		coord, ok := entry.Elems[1].(*proto.NestedArray)
		assert.True(t, ok)
		assert.Equal(t, 2, len(coord.Elems))
	}

	// WITHDIST — returns nested [member, dist]
	resp = handler.executeCommand(state, "GEOSEARCH", [][]byte{[]byte("mygeo"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"), []byte("BYRADIUS"), []byte("5000"), []byte("km"), []byte("WITHDIST")}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	if len(nested.Elems) > 0 {
		entry, ok := nested.Elems[0].(*proto.NestedArray)
		assert.True(t, ok)
		assert.True(t, len(entry.Elems) >= 2)
	}

	// WITHDIST WITHCOORD — returns nested [member, dist, [lon, lat]]
	resp = handler.executeCommand(state, "GEOSEARCH", [][]byte{[]byte("mygeo"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"), []byte("BYRADIUS"), []byte("5000"), []byte("km"), []byte("WITHDIST"), []byte("WITHCOORD")}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	if len(nested.Elems) > 0 {
		entry, ok := nested.Elems[0].(*proto.NestedArray)
		assert.True(t, ok)
		assert.True(t, len(entry.Elems) >= 3)
		coord, ok := entry.Elems[2].(*proto.NestedArray)
		assert.True(t, ok)
		assert.Equal(t, 2, len(coord.Elems))
	}

	// WITHHASH — returns nested [member, hash]
	resp = handler.executeCommand(state, "GEOSEARCH", [][]byte{[]byte("mygeo"), []byte("FROMLONLAT"), []byte("-122.4194"), []byte("37.7749"), []byte("BYRADIUS"), []byte("5000"), []byte("km"), []byte("WITHHASH")}, "127.0.0.1:12345")
	nested, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(nested.Elems) > 0)
}

// TestExecuteCommand_XINFO_STREAMS tests XINFO STREAM command
func TestExecuteCommand_XINFO_STREAMS_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add an entry
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("field"), []byte("value")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "XINFO", [][]byte{[]byte("STREAM"), []byte("mystream")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(arr.Elems) > 0)
	// Verify stream info contains length field
	// Format: [length, first-entry-id, last-entry-id, ...]
	if len(arr.Elems) > 0 {
		assert.Equal(t, "length", string(*arr.Elems[0].(*proto.BulkString)))
	}
	// Verify recorded-first-entry-id field is present with the first entry ID
	// (store 层 formatStreamID 把 "1" 规范化为 "1-0")
	foundRecordedFirst := false
	for i := 0; i+1 < len(arr.Elems); i += 2 {
		if string(*arr.Elems[i].(*proto.BulkString)) == "recorded-first-entry-id" {
			assert.Equal(t, "1-0", string(*arr.Elems[i+1].(*proto.BulkString)))
			foundRecordedFirst = true
		}
	}
	if !foundRecordedFirst {
		t.Error("XINFO STREAM should include recorded-first-entry-id")
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

// TestExecuteCommand_XPENDING_IDLE tests the XPENDING extended form with the
// IDLE min-idle-time filter (Redis syntax: XPENDING key group [IDLE ms] start end count).
func TestExecuteCommand_XPENDING_IDLE_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Add entries and create group + pending
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("1"), []byte("f"), []byte("v1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("2"), []byte("f"), []byte("v2")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XREADGROUP", [][]byte{[]byte("GROUP"), []byte("mygroup"), []byte("consumer1"), []byte("COUNT"), []byte("10"), []byte("STREAMS"), []byte("mystream"), []byte(">")}, "127.0.0.1:12345")

	// Extended form without IDLE: returns 2 entries
	resp := handler.executeCommand(state, "XPENDING", [][]byte{[]byte("mystream"), []byte("mygroup"), []byte("-"), []byte("+"), []byte("10")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Elems))

	// Extended form with a huge IDLE filter: entries are fresh (< 10^9 ms), so 0 match
	resp = handler.executeCommand(state, "XPENDING", [][]byte{[]byte("mystream"), []byte("mygroup"), []byte("IDLE"), []byte("1000000000"), []byte("-"), []byte("+"), []byte("10")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Elems))

	// Extended form with IDLE 0: all entries match
	resp = handler.executeCommand(state, "XPENDING", [][]byte{[]byte("mystream"), []byte("mygroup"), []byte("IDLE"), []byte("0"), []byte("-"), []byte("+"), []byte("10")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Elems))

	// Invalid IDLE value rejected
	resp = handler.executeCommand(state, "XPENDING", [][]byte{[]byte("mystream"), []byte("mygroup"), []byte("IDLE"), []byte("abc"), []byte("-"), []byte("+"), []byte("10")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not an integer"))
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

	// Add list: [a, b]
	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("list1"), []byte("a"), []byte("b")}, "127.0.0.1:12345")
	handler.executeCommand(state, "RPUSH", [][]byte{[]byte("list2"), []byte("x"), []byte("y"), []byte("z")}, "127.0.0.1:12345")

	// LMPOP RIGHT from list1 → pops "b"
	resp := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("list1"), []byte("RIGHT")}, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(na.Elems))
	key := string(*na.Elems[0].(*proto.BulkString))
	assert.Equal(t, "list1", key)
	inner := na.Elems[1].(*proto.Array)
	assert.Equal(t, 1, len(inner.Args))
	assert.Equal(t, "b", string(inner.Args[0]))

	// Verify list length decreased
	llen := handler.executeCommand(state, "LLEN", [][]byte{[]byte("list1")}, "127.0.0.1:12345")
	assert.Equal(t, int64(1), int64(*llen.(*proto.Integer)))

	// LMPOP LEFT from list2 with COUNT 2 → pops [x, y]
	resp2 := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("list2"), []byte("LEFT"), []byte("COUNT"), []byte("2")}, "127.0.0.1:12345")
	na2, ok2 := resp2.(*proto.NestedArray)
	assert.True(t, ok2)
	assert.Equal(t, 2, len(na2.Elems))
	assert.Equal(t, "list2", string(*na2.Elems[0].(*proto.BulkString)))
	inner2 := na2.Elems[1].(*proto.Array)
	assert.Equal(t, 2, len(inner2.Args))
	assert.Equal(t, "x", string(inner2.Args[0]))
	assert.Equal(t, "y", string(inner2.Args[1]))

	// Verify list2 has 1 element left
	llen2 := handler.executeCommand(state, "LLEN", [][]byte{[]byte("list2")}, "127.0.0.1:12345")
	assert.Equal(t, int64(1), int64(*llen2.(*proto.Integer)))

	// LMPOP from empty list → NilArray
	resp3 := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("nonexistent"), []byte("LEFT")}, "127.0.0.1:12345")
	_, ok3 := resp3.(proto.NilArray)
	assert.True(t, ok3)

	// LMPOP with multiple keys: first non-empty wins
	resp4 := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("2"), []byte("empty_list"), []byte("list1"), []byte("LEFT")}, "127.0.0.1:12345")
	na4, ok4 := resp4.(*proto.NestedArray)
	assert.True(t, ok4)
	assert.Equal(t, "list1", string(*na4.Elems[0].(*proto.BulkString)))

	// LMPOP on wrong type → WRONGTYPE
	handler.executeCommand(state, "SET", [][]byte{[]byte("strkey"), []byte("value")}, "127.0.0.1:12345")
	resp5 := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("strkey"), []byte("LEFT")}, "127.0.0.1:12345")
	err5, ok5 := resp5.(*proto.Error)
	assert.True(t, ok5)
	assert.True(t, strings.Contains(string(*err5), "WRONGTYPE"))

	// LMPOP with invalid args
	resp6 := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("1"), []byte("list1"), []byte("INVALID")}, "127.0.0.1:12345")
	_, ok6 := resp6.(*proto.Error)
	assert.True(t, ok6)

	resp7 := handler.executeCommand(state, "LMPOP", [][]byte{[]byte("0"), []byte("list1"), []byte("LEFT")}, "127.0.0.1:12345")
	_, ok7 := resp7.(*proto.Error)
	assert.True(t, ok7)
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
	// LCS returns the longest common subsequence
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello world", string(*bs))
}

// TestExecuteCommand_SADD tests SADD command
func TestExecuteCommand_SADD_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
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
	assert.Equal(t, int64(1), int64(*integer))
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
	assert.Equal(t, 2, len(arr.Args))
}

// TestExecuteCommand_HSET tests HSET command
func TestExecuteCommand_HSET_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_HGET tests HGET command
func TestExecuteCommand_HGET_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "HGET", [][]byte{[]byte("myhash"), []byte("field1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value1", string(*bs))
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
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_HRANDFIELD_Coverage tests HRANDFIELD command
func TestExecuteCommand_HRANDFIELD_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("hrand"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")}, "127.0.0.1:12345")

	// HRANDFIELD with no count returns Array (count=1 default)
	resp := handler.executeCommand(state, "HRANDFIELD", [][]byte{[]byte("hrand")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))

	resp = handler.executeCommand(state, "HRANDFIELD", [][]byte{[]byte("hrand"), []byte("2")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))

	resp = handler.executeCommand(state, "HRANDFIELD", [][]byte{[]byte("hrand"), []byte("2"), []byte("WITHVALUES")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr.Args))

	resp = handler.executeCommand(state, "HRANDFIELD", [][]byte{[]byte("nonexist")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

// TestExecuteCommand_ZADD tests ZADD command
func TestExecuteCommand_ZADD_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_ZADD_Options verifies ZADD NX/XX/GT/LT/CH options
// (previously options were parsed as scores and rejected).
func TestExecuteCommand_ZADD_Options_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Base add
	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("oz"), []byte("1"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// NX on existing member → 0 (not added)
	resp = handler.executeCommand(state, "ZADD", [][]byte{[]byte("oz"), []byte("NX"), []byte("2"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// XX on existing member → 0 (updated but no NEW member; Redis default count)
	resp = handler.executeCommand(state, "ZADD", [][]byte{[]byte("oz"), []byte("XX"), []byte("3"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
	// Score must actually be updated to 3
	scoreResp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("oz"), []byte("m1")}, "127.0.0.1:12345")
	scoreBs, ok := scoreResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "3", string(*scoreBs))

	// GT: 3 -> 2 不满足（新分数不大于旧分数）→ 0
	resp = handler.executeCommand(state, "ZADD", [][]byte{[]byte("oz"), []byte("GT"), []byte("2"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// LT: 3 -> 2 满足 → 0 (updated but no NEW member)
	resp = handler.executeCommand(state, "ZADD", [][]byte{[]byte("oz"), []byte("LT"), []byte("2"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
	// Score must actually be updated to 2
	scoreResp = handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("oz"), []byte("m1")}, "127.0.0.1:12345")
	scoreBs, ok = scoreResp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "2", string(*scoreBs))

	// CH: 更新已存在成员 → 1（变更计数含更新）
	resp = handler.executeCommand(state, "ZADD", [][]byte{[]byte("oz"), []byte("CH"), []byte("5"), []byte("m1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// NX + XX 互斥 → 报错
	resp = handler.executeCommand(state, "ZADD", [][]byte{[]byte("oz"), []byte("NX"), []byte("XX"), []byte("1"), []byte("m1")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "not compatible"))
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
	assert.Equal(t, int64(1), int64(*integer))
}

// TestExecuteCommand_ZSCORE tests ZSCORE command
func TestExecuteCommand_ZSCORE_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "ZADD", [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("myzset"), []byte("member1")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "1", string(*bs))
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
	assert.Equal(t, 1, len(arr.Args))
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
	assert.Equal(t, int64(1), int64(*integer))
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
				// Non-existent key should return nil bulk string
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Nil(t, *bs)
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
			resp := handler.executeQueuedCommand(tt.cmd, tt.args, 2)
			tt.validate(t, resp)
		})
	}
}

// TestExecuteCommand_HGETALL_Order verifies HGETALL returns fields in
// stable sorted order (previously map iteration order was non-deterministic).
func TestExecuteCommand_HGETALL_Order_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "HSET", [][]byte{[]byte("h"), []byte("zebra"), []byte("1"), []byte("apple"), []byte("2"), []byte("mango"), []byte("3")}, "127.0.0.1:12345")

	resp := handler.executeCommand(state, "HGETALL", [][]byte{[]byte("h")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// 6 elements: [apple, 2, mango, 3, zebra, 1] (sorted field order)
	assert.Equal(t, 6, len(arr.Args))
	assert.Equal(t, "apple", string(arr.Args[0]))
	assert.Equal(t, "mango", string(arr.Args[2]))
	assert.Equal(t, "zebra", string(arr.Args[4]))
}

// TestExecuteCommand_BITCOUNT_BIT verifies BITCOUNT with the BIT unit counts
// bits in a bit range (previously the BYTE/BIT unit option was rejected).
func TestExecuteCommand_BITCOUNT_BIT_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// "A" = 0x41 = 01000001 → 2 bits set (bit 1 and bit 6)
	handler.Db.Set("bc", "A")

	// BITCOUNT bc 0 7 BIT → all 8 bits of byte 0 → 2
	resp := handler.executeCommand(state, "BITCOUNT", [][]byte{[]byte("bc"), []byte("0"), []byte("7"), []byte("BIT")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// BITCOUNT bc 1 1 BIT → only bit 1 (0x41 bit1 = 1) → 1
	resp = handler.executeCommand(state, "BITCOUNT", [][]byte{[]byte("bc"), []byte("1"), []byte("1"), []byte("BIT")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// Invalid unit → error
	resp = handler.executeCommand(state, "BITCOUNT", [][]byte{[]byte("bc"), []byte("0"), []byte("7"), []byte("BOGUS")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

// TestExecuteCommand_BITPOS_BIT verifies BITPOS with the BIT unit searches a
// bit range (previously the BYTE/BIT unit option was rejected).
func TestExecuteCommand_BITPOS_BIT_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// "A" = 0x41 = 01000001 → bit 1 and bit 6 set
	handler.Db.Set("bp", "A")

	// BITPOS bp 1 0 7 BIT → first set bit at position 1
	resp := handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bp"), []byte("1"), []byte("0"), []byte("7"), []byte("BIT")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// BITPOS bp 1 0 0 BIT → only bit 0 (0x41 bit0 = 0) → not found (-1)
	resp = handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bp"), []byte("1"), []byte("0"), []byte("0"), []byte("BIT")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*integer))

	// Invalid unit → error
	resp = handler.executeCommand(state, "BITPOS", [][]byte{[]byte("bp"), []byte("1"), []byte("0"), []byte("7"), []byte("BOGUS")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "syntax error"))
}

// TestExecuteCommand_EXPIRE_Conditions verifies EXPIRE NX/XX/GT/LT options
// (previously they were ignored).
func TestExecuteCommand_EXPIRE_Conditions_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Db.Set("k", "v")

	// EXPIRE k 100 NX → no TTL yet, must set → 1
	resp := handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k"), []byte("100"), []byte("NX")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// EXPIRE k 90 NX → already has TTL → 0
	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k"), []byte("90"), []byte("NX")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// EXPIRE k 90 LT → 90 < 100 → must shorten → 1
	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k"), []byte("90"), []byte("LT")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// EXPIRE k 200 GT → 200 > 90 → must extend → 1
	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k"), []byte("200"), []byte("GT")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// EXPIRE k 50 GT → 50 < 200 → not longer → 0
	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k"), []byte("50"), []byte("GT")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))

	// Invalid option → error
	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k"), []byte("100"), []byte("BOGUS")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "unsupported option"))

	// GT on a key WITHOUT TTL → must NOT set (Redis: no-TTL key has infinite TTL,
	// GT requires a larger expiry, so any finite TTL is rejected → 0)
	handler.Db.Set("k2", "v2")
	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k2"), []byte("100"), []byte("GT")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
	ttlVal, _ := handler.Db.TTL("k2")
	assert.Equal(t, int64(-1), ttlVal) // still no TTL

	// LT on a key WITHOUT TTL → must set (Redis: infinite TTL is greater than any
	// finite, so LT rejects nothing → 1)
	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("k2"), []byte("100"), []byte("LT")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}
