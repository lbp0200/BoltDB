package server

import (
	"net"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// setupHandlerWithConns creates a handler with registered connections for testing
func setupHandlerWithConns(t *testing.T, states []*connState) *Handler {
	handler, _ := setupTestHandler(t)
	for _, st := range states {
		if st.ctx == nil {
			st.ctx, st.cancel = handler.Ctx, func() {}
		}
		handler.registerConnection(st, &mockConn{}, "127.0.0.1:12345")
	}
	return handler
}

type mockConn struct{ net.Conn }

func (m *mockConn) RemoteAddr() net.Addr { return &mockAddr{addr: "127.0.0.1:12345"} }
func (m *mockConn) Close() error         { return nil }

type mockAddr struct{ addr string }

func (m *mockAddr) Network() string { return "tcp" }
func (m *mockAddr) String() string  { return m.addr }

// metrics.go coverage: 5 functions at 0%

func TestHandler_ActiveClientCount_Coverage(t *testing.T) {
	t.Parallel()

	handler := setupHandlerWithConns(t, []*connState{{}})
	defer handler.Db.Close()
	assert.Equal(t, 1, handler.ActiveClientCount())
}

func TestHandler_BlockedClientCount_Coverage(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	assert.Equal(t, 0, handler.BlockedClientCount())

	handlerNil := &Handler{}
	assert.Equal(t, 0, handlerNil.BlockedClientCount())
}

func TestHandler_MonitorClientCount_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	assert.Equal(t, 0, handler.MonitorClientCount())
	handler.registerMonitorClient(state)
	assert.Equal(t, 1, handler.MonitorClientCount())
	handler.unregisterMonitorClient(state)
	assert.Equal(t, 0, handler.MonitorClientCount())
}

func TestHandler_PubSubClientCount_Coverage(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	assert.Equal(t, 0, handler.PubSubClientCount())
}

func TestHandler_TotalOutputBytes_Coverage(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()
	assert.Equal(t, int64(0), handler.TotalOutputBytes())
}

// clientListRESP coverage: 12.5% -> 100%

func TestClientListRESP_Empty_Coverage(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.clientListRESP()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "id=1"))
}

func TestClientListRESP_WithConnections_Coverage(t *testing.T) {
	t.Parallel()

	state := &connState{}
	handler := setupHandlerWithConns(t, []*connState{state})
	defer handler.Db.Close()

	resp := handler.clientListRESP()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "127.0.0.1:12345"))
}

func TestClientListRESP_Subscribed_Coverage(t *testing.T) {
	t.Parallel()

	state := &connState{subscriber: store.NewSubscriber("test")}
	handler := setupHandlerWithConns(t, []*connState{state})
	defer handler.Db.Close()

	resp := handler.clientListRESP()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "flags=P"))
}

func TestClientListRESP_NoEvict_Coverage(t *testing.T) {
	t.Parallel()

	// 普通连接 + NOEVICT ON → flags=O（Redis 语义：N 与 O 互斥）
	state := &connState{}
	state.noEvict.Store(true)
	handler := setupHandlerWithConns(t, []*connState{state})
	defer handler.Db.Close()

	resp := handler.clientListRESP()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "flags=O"))

	// Subscriber + NOEVICT ON → flags=PO（组合标志）
	state2 := &connState{subscriber: store.NewSubscriber("test2")}
	state2.noEvict.Store(true)
	handler2 := setupHandlerWithConns(t, []*connState{state2})
	defer handler2.Db.Close()

	resp2 := handler2.clientListRESP()
	bs2, ok := resp2.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs2), "flags=PO"))
}

func TestClientListRESP_InTransaction_Coverage(t *testing.T) {
	t.Parallel()

	state := &connState{
		inTransaction: true,
		commands:      []TransactionCommand{{Command: "SET", Args: [][]byte{[]byte("k"), []byte("v")}}},
	}
	handler := setupHandlerWithConns(t, []*connState{state})
	defer handler.Db.Close()

	resp := handler.clientListRESP()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "flags=t"))
	assert.True(t, strings.Contains(string(*bs), "multi=1"))
}

func TestClientListRESP_OutputBufferOverLimit_Coverage(t *testing.T) {
	t.Parallel()

	state := &connState{}
	handler := setupHandlerWithConns(t, []*connState{state})
	defer handler.Db.Close()

	handler.OutputBufferLimit = 100
	handler.connsMu.Lock()
	if meta, ok := handler.conns[state]; ok {
		meta.outputBytes = 60
	}
	handler.connsMu.Unlock()

	resp := handler.clientListRESP()
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*bs), "oFlags=>"))
}

// markDirtyKeys coverage: 50% -> 100%

func TestMarkDirtyKeys_WithWatchers_Coverage(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	watcher := &connState{}
	handler.watchMu.Lock()
	handler.watchMonitors = map[string]map[*connState]struct{}{
		"testkey": {watcher: {}},
	}
	handler.watchMu.Unlock()

	handler.markDirtyKeys(&connState{}, "testkey")
	handler.watchMu.Lock()
	_, exists := watcher.dirtyKeys["testkey"]
	handler.watchMu.Unlock()
	assert.True(t, exists)
}

// Signal types: PubSubQuitSignal.String() 0%, MultiResponse.String() 0%

func TestPubSubQuitSignal_String_Coverage(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "+OK\r\n", PubSubQuitSignal{}.String())
}

func TestMultiResponse_String_Coverage(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "", (&MultiResponse{Responses: nil}).String())
	assert.True(t, len((&MultiResponse{Responses: []proto.RESP{proto.NewSimpleString("OK")}}).String()) > 0)
}

// processRequest coverage: 50% (test empty args path)

func TestProcessRequest_EmptyArgs_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	emptyReq := &proto.Array{Args: [][]byte{}}
	resp := handler.processRequest(emptyReq, nil, "127.0.0.1:12345", nil, nil, state)
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "no command"))
}

// executeCommand error boundary paths

func TestExecuteCommand_NilState_Coverage(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(nil, "PING", nil, "127.0.0.1:12345")
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "nil connState"))
}

func TestExecuteCommand_EmptyCommand_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "", nil, "127.0.0.1:12345")
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "unknown command"))
}

func TestExecuteCommand_WAIT_NoRepl_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "WAIT", [][]byte{[]byte("1"), []byte("1000")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

func TestExecuteCommand_TTL_PersistentKey_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("persistkey"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "TTL", [][]byte{[]byte("persistkey")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), int64(*integer))
}

func TestExecuteCommand_SORT_Store_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("sortlist"), []byte("3"), []byte("1"), []byte("2")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "SORT", [][]byte{[]byte("sortlist"), []byte("STORE"), []byte("sortedresult")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(3), int64(*integer))
}

func TestExecuteCommand_GETEX_Persist_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("getexkey"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("getexkey"), []byte("100")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "GETEX", [][]byte{[]byte("getexkey"), []byte("PERSIST")}, "127.0.0.1:12345")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "value", string(*bs))
}

func TestExecuteCommand_EXPIRETIME_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("ekey"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "EXPIRETIME", [][]byte{[]byte("ekey")}, "127.0.0.1:12345")
	// EXPIRETIME returns absolute Unix timestamp in seconds
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// Key exists without TTL → -1
	assert.Equal(t, int64(-1), int64(*integer))
}

func TestExecuteCommand_PEXPIRETIME_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.executeCommand(state, "SET", [][]byte{[]byte("pekey"), []byte("value")}, "127.0.0.1:12345")
	resp := handler.executeCommand(state, "PEXPIRETIME", [][]byte{[]byte("pekey")}, "127.0.0.1:12345")
	// PEXPIRETIME returns absolute Unix timestamp in milliseconds
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	// Key exists without TTL → -1
	assert.Equal(t, int64(-1), int64(*integer))
}

func TestExecuteCommand_HELLO_Resp3_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "HELLO", [][]byte{[]byte("3")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Map)
	assert.True(t, ok)
	assert.Equal(t, 3, state.respVersion)
}

func TestExecuteCommand_ACL_NotSupported_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ACL", nil, "127.0.0.1:12345")
	// ACL with no args returns wrong number of arguments
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "wrong number of arguments"))
}

func TestExecuteCommand_ACL_CAT_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ACL", [][]byte{[]byte("CAT")}, "127.0.0.1:12345")
	// ACL CAT returns an Array of command categories
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}

func TestExecuteCommand_ACL_SETUSER_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ACL", [][]byte{[]byte("SETUSER"), []byte("test")}, "127.0.0.1:12345")
	// ACL SETUSER is not implemented — returns unknown subcommand error
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "unknown subcommand"))
}

func TestExecuteCommand_RENAME_NonExistent_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "RENAME", [][]byte{[]byte("nonexistent"), []byte("newkey")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestExecuteCommand_WrongArity_MSETNX_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MSETNX", [][]byte{[]byte("key1")}, "127.0.0.1:12345")
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "wrong number of arguments"))
}

func TestExecuteCommand_CLIENT_UNBLOCK_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("UNBLOCK"), []byte("1")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestExecuteCommand_CLIENT_LIST_Multiple_Coverage(t *testing.T) {
	t.Parallel()

	subState := &connState{subscriber: store.NewSubscriber("test")}
	handler := setupHandlerWithConns(t, []*connState{subState})
	defer handler.Db.Close()

	// Register a transaction state
	txnState := &connState{
		inTransaction: true,
		commands:      []TransactionCommand{{Command: "SET", Args: [][]byte{[]byte("k"), []byte("v")}}},
	}
	handler.registerConnection(txnState, &mockConn{}, "127.0.0.1:12347")

	// Use a non-transaction caller
	callerState := &connState{}
	handler.registerConnection(callerState, &mockConn{}, "127.0.0.1:12346")

	resp := handler.executeCommand(callerState, "CLIENT", [][]byte{[]byte("LIST")}, "127.0.0.1:12346")
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	body := string(*bs)
	assert.True(t, strings.Contains(body, "flags=P"))
	assert.True(t, strings.Contains(body, "flags=t"))
}

func TestExecuteCommand_CLIENT_KILL_ByAddr_Coverage(t *testing.T) {
	t.Parallel()

	state1 := &connState{}
	handler := setupHandlerWithConns(t, []*connState{state1})
	defer handler.Db.Close()

	// KILL <addr> kills by matching remoteAddr
	resp := handler.executeCommand(state1, "CLIENT",
		[][]byte{[]byte("KILL"), []byte("127.0.0.1:12345")},
		"127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

func TestExecuteCommand_CLIENT_KILL_NoMatch_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// KILL <nonexistent-addr> returns Integer(0) — no matching client
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("KILL"), []byte("1.2.3.4:5678")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(0), int64(*integer))
}

func TestExecuteCommand_CLIENT_KILL_InvalidFilter_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// No args returns Error
	resp := handler.executeCommand(state, "CLIENT", [][]byte{[]byte("KILL")}, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestExecuteCommand_XGROUP_HELP_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "XGROUP", [][]byte{[]byte("HELP")}, "127.0.0.1:12345")
	// XGROUP HELP is not a valid subcommand — returns syntax error
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "syntax error"))
}

func TestExecuteCommand_LATENCY_HISTORY_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "LATENCY", [][]byte{[]byte("HISTORY"), []byte("command")}, "127.0.0.1:12345")
	// LATENCY HISTORY — no samples recorded, returns empty array
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 0, len(arr.Args))
}

func TestExecuteCommand_MEMORY_STATS_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "MEMORY", [][]byte{[]byte("STATS")}, "127.0.0.1:12345")
	// MEMORY STATS — flat key/value pairs (minimal shape)
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) >= 4)
}
