package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

func TestExecuteCommand_JSON_SET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jsonkey"), []byte("$"), []byte(`{"name":"test"}`)}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))
}

func TestExecuteCommand_JSON_SET_WrongArgs_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("key")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, len(string(*errResp)) > 0)
}

func TestExecuteCommand_JSON_GET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jget"), []byte("$"), []byte(`{"a":1}`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.GET", [][]byte{[]byte("jget")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, `{"a":1}`, string(*bs))
}

func TestExecuteCommand_JSON_GET_WrongArgs_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "JSON.GET", nil, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, len(string(*errResp)) > 0)
}

func TestExecuteCommand_JSON_DEL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jdel"), []byte("$"), []byte(`{"a":1}`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.DEL", [][]byte{[]byte("jdel")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

func TestExecuteCommand_JSON_TYPE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jtype"), []byte("$"), []byte(`{"a":1}`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.TYPE", [][]byte{[]byte("jtype"), []byte("$.a")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "number", string(*bs))
}

func TestExecuteCommand_JSON_TYPE_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "JSON.TYPE", [][]byte{[]byte("nonexistent")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

func TestExecuteCommand_JSON_MGET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jm1"), []byte("$"), []byte(`{"x":1}`)}, "127.0.0.1:12345")
	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jm2"), []byte("$"), []byte(`{"x":2}`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.MGET", [][]byte{[]byte("jm1"), []byte("jm2"), []byte("$")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
}

func TestExecuteCommand_JSON_MGET_WrongArgs_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "JSON.MGET", [][]byte{[]byte("onlykey")}, "127.0.0.1:12345")
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, len(string(*errResp)) > 0)
}

func TestExecuteCommand_JSON_ARRAPPEND_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jarr"), []byte("$"), []byte(`[]`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.ARRAPPEND", [][]byte{[]byte("jarr"), []byte("$"), []byte(`1`)}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

func TestExecuteCommand_JSON_NUMINCRBY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jnum"), []byte("$"), []byte(`5`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.NUMINCRBY", [][]byte{[]byte("jnum"), []byte("$"), []byte("3")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "8", string(*bs))
}

func TestExecuteCommand_JSON_NUMMULTBY_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jmult"), []byte("$"), []byte(`10`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.NUMMULTBY", [][]byte{[]byte("jmult"), []byte("$"), []byte("2")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "20", string(*bs))
}

func TestExecuteCommand_JSON_ARRLEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jalen"), []byte("$"), []byte(`[1,2,3]`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.ARRLEN", [][]byte{[]byte("jalen")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))
}

func TestExecuteCommand_JSON_CLEAR_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "JSON.SET", [][]byte{[]byte("jclr"), []byte("$"), []byte(`{"a":[1,2,3]}`)}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "JSON.CLEAR", [][]byte{[]byte("jclr"), []byte("$")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

func TestExecuteCommand_TS_CREATE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.CREATE", [][]byte{[]byte("ts1")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))
}

func TestExecuteCommand_TS_CREATE_WithOptions_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.CREATE", [][]byte{[]byte("tsopt"), []byte("RETENTION"), []byte("3600000")}, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))
}

func TestExecuteCommand_TS_ADD_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tsadd"), []byte("*"), []byte("42.5")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, *integer > 0)
}

func TestExecuteCommand_TS_GET_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tsget"), []byte("1000"), []byte("3.14")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "TS.GET", [][]byte{[]byte("tsget")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
}

func TestExecuteCommand_TS_GET_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "TS.GET", [][]byte{[]byte("noexist")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "", string(*bs))
}

func TestExecuteCommand_TS_LEN_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.CREATE", [][]byte{[]byte("tslen")}, "127.0.0.1:12345")
	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tslen"), []byte("100"), []byte("1.0")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "TS.LEN", [][]byte{[]byte("tslen")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

func TestExecuteCommand_TS_DEL_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tsdel"), []byte("100"), []byte("1.0")}, "127.0.0.1:12345")
	handler.executeCommand(state, "TS.ADD", [][]byte{[]byte("tsdel"), []byte("200"), []byte("2.0")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "TS.DEL", [][]byte{[]byte("tsdel"), []byte("100"), []byte("150")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}
