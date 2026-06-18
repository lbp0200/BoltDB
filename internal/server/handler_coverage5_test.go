package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// buildPubSubPush coverage: 0% -> 100%
// Tests the "message" path (no pattern)

func TestBuildPubSubPush_Message_Coverage(t *testing.T) {
	t.Parallel()
	msg := &store.Message{
		Channel: "testchan",
		Data:    []byte("testdata"),
	}
	resp := buildPubSubPush(msg)
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	assert.Equal(t, "message", string(arr.Args[0]))
	assert.Equal(t, "testchan", string(arr.Args[1]))
	assert.Equal(t, "testdata", string(arr.Args[2]))
}

func TestBuildPubSubPush_PMessage_Coverage(t *testing.T) {
	t.Parallel()
	msg := &store.Message{
		Channel: "testchan",
		Pattern: "test*",
		Data:    []byte("testdata"),
	}
	resp := buildPubSubPush(msg)
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr.Args))
	assert.Equal(t, "pmessage", string(arr.Args[0]))
	assert.Equal(t, "test*", string(arr.Args[1]))
	assert.Equal(t, "testchan", string(arr.Args[2]))
	assert.Equal(t, "testdata", string(arr.Args[3]))
}

// runPubSubLoop coverage: 39.2% — test early return when subscriber is nil

func TestRunPubSubLoop_NilSubscriber_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	state.subscriber = nil
	handler.runPubSubLoop(nil, nil, nil, nil, state, "127.0.0.1:12345")
}

// broadcastToMonitors coverage: 80% — test with active monitor clients

func TestBroadcastToMonitors_WithClients_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	ch := make(chan []byte, 1)
	handler.monitorMu.Lock()
	if handler.monitorClients == nil {
		handler.monitorClients = make(map[*connState]chan []byte)
	}
	handler.monitorClients[state] = ch
	handler.monitorMu.Unlock()

	handler.broadcastToMonitors("SET", [][]byte{[]byte("key"), []byte("val")}, "127.0.0.1:12345")
	assert.Equal(t, 1, len(ch))
}

// copyHash coverage: 80% — test HGetAll error path (wrong type)

func TestCopyHash_WrongType_Coverage(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a string key (not a hash)
	handler.Db.Set("notahash", "stringvalue")

	result := handler.copyHash("notahash", "destkey")
	assert.False(t, result)
}

// processPubSubCommand coverage: 0% -> 100%
// Error paths first

func TestProcessPubSubCommand_NoArgs_Coverage(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{}}
	resp := handler.processPubSubCommand(nil, req, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestProcessPubSubCommand_PubSubDisabled_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	state.subscriber = store.NewSubscriber("test")
	handler.PubSub = nil

	// All commands should return "ERR pubsub not enabled" when PubSub is nil
	req := &proto.Array{Args: [][]byte{[]byte("SUBSCRIBE"), []byte("ch")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestProcessPubSubCommand_SubscribeNoChannels_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	req := &proto.Array{Args: [][]byte{[]byte("SUBSCRIBE")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestProcessPubSubCommand_Subscribe_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	req := &proto.Array{Args: [][]byte{[]byte("SUBSCRIBE"), []byte("ch1"), []byte("ch2")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	mr, ok := resp.(*MultiResponse)
	assert.True(t, ok)
	assert.Equal(t, 2, len(mr.Responses))
}

func TestProcessPubSubCommand_PSUBSCRIBE_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	req := &proto.Array{Args: [][]byte{[]byte("PSUBSCRIBE"), []byte("ch*"), []byte("foo*")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	mr, ok := resp.(*MultiResponse)
	assert.True(t, ok)
	assert.Equal(t, 2, len(mr.Responses))
}

func TestProcessPubSubCommand_PSUBSCRIBE_NoChannels_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	req := &proto.Array{Args: [][]byte{[]byte("PSUBSCRIBE")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestProcessPubSubCommand_Unsubscribe_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	// Unsubscribe without channels — returns NestedArray for empty subscriber
	req := &proto.Array{Args: [][]byte{[]byte("UNSUBSCRIBE")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
}

func TestProcessPubSubCommand_UnsubscribeSpecific_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")
	handler.PubSub.Subscribe(state.subscriber, "ch1")

	req := &proto.Array{Args: [][]byte{[]byte("UNSUBSCRIBE"), []byte("ch1")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	mr, ok := resp.(*MultiResponse)
	assert.True(t, ok)
	assert.Equal(t, 1, len(mr.Responses))
}

func TestProcessPubSubCommand_PUnsubscribe_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	req := &proto.Array{Args: [][]byte{[]byte("PUNSUBSCRIBE")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
}

func TestProcessPubSubCommand_PUnsubscribeSpecific_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")
	handler.PubSub.PSubscribe(state.subscriber, "ch*")

	req := &proto.Array{Args: [][]byte{[]byte("PUNSUBSCRIBE"), []byte("ch*")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	mr, ok := resp.(*MultiResponse)
	assert.True(t, ok)
	assert.Equal(t, 1, len(mr.Responses))
}

func TestProcessPubSubCommand_Ping_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{[]byte("PING")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "PONG", string(*ss))
}

func TestProcessPubSubCommand_Quit_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{[]byte("QUIT")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*PubSubQuitSignal)
	assert.True(t, ok)
}

func TestProcessPubSubCommand_Default_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{[]byte("GET"), []byte("key")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// processMonitorCommand coverage: 0% -> 100%

func TestProcessMonitorCommand_EmptyArgs_Coverage(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{}}
	resp := handler.processMonitorCommand(req, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestProcessMonitorCommand_Quit_Coverage(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{[]byte("QUIT")}}
	resp := handler.processMonitorCommand(req, "127.0.0.1:12345")
	_, ok := resp.(*PubSubQuitSignal)
	assert.True(t, ok)
}

func TestProcessMonitorCommand_Ping_Coverage(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{[]byte("PING")}}
	resp := handler.processMonitorCommand(req, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "PONG", string(*ss))
}

func TestProcessMonitorCommand_Default_Coverage(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{[]byte("GET")}}
	resp := handler.processMonitorCommand(req, "127.0.0.1:12345")
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

// Shutdown coverage: 0%
// Tests the graceful connection-close + wg.Wait() path

func TestHandler_Shutdown_NoConnections_Coverage(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Shutdown()
	// Verify shuttingDown flag is set
	assert.Equal(t, int32(1), handler.shuttingDown.Load())
}

func TestHandler_Shutdown_WithConnections_Coverage(t *testing.T) {
	t.Parallel()
	state := &connState{}
	handler := setupHandlerWithConns(t, []*connState{state})
	defer handler.Db.Close()

	handler.Shutdown()
	assert.Equal(t, int32(1), handler.shuttingDown.Load())
}

// runMonitorLoop coverage: 0% — test early return when monitorCh is nil

func TestRunMonitorLoop_NilChannel_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	state.monitorCh = nil
	handler.runMonitorLoop(nil, nil, state, "127.0.0.1:12345")
}

// processRequest coverage: 56.2% — test MONITOR command path (broadcast filter)

func TestProcessRequest_MONITOR_Command_Coverage(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	req := &proto.Array{Args: [][]byte{[]byte("MONITOR")}}
	resp := handler.processRequest(req, nil, "127.0.0.1:12345", nil, nil, state)
	_, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
}
