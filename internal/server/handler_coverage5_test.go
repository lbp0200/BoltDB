package server

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// buildPubSubPush coverage: message path, pmessage path, RESP2 and RESP3
func TestBuildPubSubPush_Message_Coverage(t *testing.T) {
	t.Parallel()

	msg := &store.Message{
		Channel: "testchan",
		Data:    []byte("testdata"),
	}
	// RESP2 path
	resp := buildPubSubPush(msg, 2)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 3, len(na.Elems))
	assert.Equal(t, "message", string(*na.Elems[0].(*proto.BulkString)))
	assert.Equal(t, "testchan", string(*na.Elems[1].(*proto.BulkString)))
	assert.Equal(t, "testdata", string(*na.Elems[2].(*proto.BulkString)))

	// RESP3 path
	resp3 := buildPubSubPush(msg, 3)
	p, ok := resp3.(*proto.Push)
	assert.True(t, ok)
	assert.Equal(t, 3, len(p.Elems))
}

func TestBuildPubSubPush_PMessage_Coverage(t *testing.T) {
	t.Parallel()

	msg := &store.Message{
		Channel: "testchan",
		Pattern: "test*",
		Data:    []byte("testdata"),
	}
	resp := buildPubSubPush(msg, 2)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 4, len(na.Elems))
	assert.Equal(t, "pmessage", string(*na.Elems[0].(*proto.BulkString)))
	assert.Equal(t, "test*", string(*na.Elems[1].(*proto.BulkString)))
	assert.Equal(t, "testchan", string(*na.Elems[2].(*proto.BulkString)))
	assert.Equal(t, "testdata", string(*na.Elems[3].(*proto.BulkString)))
}

// runPubSubLoop coverage: test early return when subscriber is nil

func TestRunPubSubLoop_NilSubscriber_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	state.subscriber = nil
	done := make(chan struct{})
	go func() {
		handler.runPubSubLoop(nil, nil, nil, nil, state, "127.0.0.1:12345")
		close(done)
	}()

	select {
	case <-done:
		// runPubSubLoop returned promptly — nil subscriber handled correctly
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runPubSubLoop did not return within 500ms with nil subscriber — possible deadlock")
	}
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

func TestHandlePSyncWithRDB_WrongArgs(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.handlePSyncWithRDB(
		[][]byte{[]byte("only-replid")},
		"127.0.0.1:6379", nil, nil, nil,
	)
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestHandlePSyncWithRDB_InvalidOffset(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.handlePSyncWithRDB(
		[][]byte{[]byte("replid"), []byte("not-a-number")},
		"127.0.0.1:6379", nil, nil, nil,
	)
	_, ok := resp.(*proto.Error)
	assert.True(t, ok)
}

func TestHandleSlaveReplicationConnection_NilContext(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Replication = replication.NewReplicationManager(handler.Db)

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	slaveConn := replication.NewSlaveConnection(clientEnd)
	handler.wg.Add(1)
	go handler.handleSlaveReplicationConnection(nil, slaveConn)
	serverEnd.Close()
	slaveConn.Close()
}

func TestHandleSlaveReplicationConnection_CancelledContext(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Replication = replication.NewReplicationManager(handler.Db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	slaveConn := replication.NewSlaveConnection(clientEnd)
	handler.wg.Add(1)
	done := make(chan struct{})
	go func() {
		handler.handleSlaveReplicationConnection(ctx, slaveConn)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleSlaveReplicationConnection did not return after context cancellation")
	}
}

// TestProcessPubSubCommand_SSUBSCRIBE_Coverage tests SSUBSCRIBE: subscribes
// to shard channels with "ssubscribe" confirmations, isolated from regular
// SUBSCRIBE channels.
func TestProcessPubSubCommand_SSUBSCRIBE_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	req := &proto.Array{Args: [][]byte{[]byte("SSUBSCRIBE"), []byte("{shard}ch1"), []byte("{shard}ch2")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	mr, ok := resp.(*MultiResponse)
	assert.True(t, ok)
	assert.Equal(t, 2, len(mr.Responses))

	// Confirmation carries the "ssubscribe" type
	first, ok := mr.Responses[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, "ssubscribe", string(*first.Elems[0].(*proto.BulkString)))

	// SPUBLISH delivers to shard subscribers...
	count := handler.PubSub.SPublish("{shard}ch1", []byte("hello"))
	assert.Equal(t, 1, count)

	// ...but regular PUBLISH does not reach shard subscribers, and SPUBLISH
	// does not reach regular subscribers (isolated namespaces).
	count = handler.PubSub.Publish("{shard}ch1", []byte("regular"))
	assert.Equal(t, 0, count)

	// Message received by the shard subscriber has the shard flag
	select {
	case msg := <-state.subscriber.MessageCh:
		assert.True(t, msg.Shard)
		assert.Equal(t, "{shard}ch1", msg.Channel)
		assert.Equal(t, "hello", string(msg.Data))
	default:
		t.Fatal("shard subscriber did not receive the message")
	}
}

// TestProcessPubSubCommand_SUNSUBSCRIBE_Coverage tests SUNSUBSCRIBE.
func TestProcessPubSubCommand_SUNSUBSCRIBE_Coverage(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	// Subscribe then unsubscribe
	req := &proto.Array{Args: [][]byte{[]byte("SSUBSCRIBE"), []byte("{s}ch")}}
	resp := handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	_, ok := resp.(*MultiResponse)
	assert.True(t, ok)

	req = &proto.Array{Args: [][]byte{[]byte("SUNSUBSCRIBE"), []byte("{s}ch")}}
	resp = handler.processPubSubCommand(state, req, "127.0.0.1:12345")
	mr, ok := resp.(*MultiResponse)
	assert.True(t, ok)
	assert.Equal(t, 1, len(mr.Responses))
	first, ok := mr.Responses[0].(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, "sunsubscribe", string(*first.Elems[0].(*proto.BulkString)))

	// No more deliveries after unsubscribe
	count := handler.PubSub.SPublish("{s}ch", []byte("after"))
	assert.Equal(t, 0, count)
}

// TestBuildPubSubPush_Shard verifies shard messages assemble as "smessage".
func TestBuildPubSubPush_Shard(t *testing.T) {
	t.Parallel()

	msg := &store.Message{Channel: "{s}ch", Data: []byte("m"), Shard: true}
	resp := buildPubSubPush(msg, 3)
	push, ok := resp.(*proto.Push)
	assert.True(t, ok)
	assert.Equal(t, 3, len(push.Elems))
	assert.Equal(t, "smessage", string(*push.Elems[0].(*proto.BulkString)))
	assert.Equal(t, "{s}ch", string(*push.Elems[1].(*proto.BulkString)))
	assert.Equal(t, "m", string(*push.Elems[2].(*proto.BulkString)))

	// RESP2: NestedArray with same content
	resp = buildPubSubPush(msg, 2)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, "smessage", string(*na.Elems[0].(*proto.BulkString)))
}

// TestProcessPubSubCommand_PUBSUB_SHARDCHANNELS tests PUBSUB SHARDCHANNELS:
// lists active shard channels (isolated from regular channels).
func TestProcessPubSubCommand_PUBSUB_SHARDCHANNELS(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	handler.PubSub = store.NewPubSubManager()
	state.subscriber = store.NewSubscriber("test")

	// Regular channel must NOT appear in SHARDCHANNELS
	handler.PubSub.Subscribe(state.subscriber, "plain_ch")
	handler.PubSub.SSubscribe(state.subscriber, "{s}one", "{s}two")

	resp := handler.executeCommand(state, "PUBSUB", [][]byte{[]byte("SHARDCHANNELS")}, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	names := map[string]bool{}
	for _, ch := range arr.Args {
		names[string(ch)] = true
	}
	assert.True(t, names["{s}one"])
	assert.True(t, names["{s}two"])
	assert.True(t, !names["plain_ch"])

	// Pattern filter
	resp = handler.executeCommand(state, "PUBSUB", [][]byte{[]byte("SHARDCHANNELS"), []byte("{s}o*")}, "127.0.0.1:12345")
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "{s}one", string(arr.Args[0]))
}

// TestHandleSlaveReplicationConnection_RepliesToGetAck 守卫：从节点发
// REPLCONF GETACK * 时，主节点必须回复携带自身 offset + currentTS 的
// REPLCONF ACK（S2 ACK-ts 双轨 4 参：REPLCONF ACK <offset> <ts>——从节点
// readCommandLoop 据此检测投递停滞的尾巴缺口，见 TODO §1c）。
func TestHandleSlaveReplicationConnection_RepliesToGetAck(t *testing.T) {
	t.Parallel()

	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	handler.Replication = replication.NewReplicationManager(handler.Db)
	defer handler.Replication.Stop()
	handler.Replication.SetMasterReplOffset(12345)

	serverEnd, clientEnd := net.Pipe()
	defer serverEnd.Close()
	defer clientEnd.Close()

	slaveConn := replication.NewSlaveConnection(clientEnd)
	handler.wg.Add(1)
	done := make(chan struct{})
	go func() {
		handler.handleSlaveReplicationConnection(context.Background(), slaveConn)
		close(done)
	}()

	_, err := serverEnd.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
	assert.NoError(t, err)

	resp, err := proto.ReadRESP(bufio.NewReader(serverEnd))
	assert.NoError(t, err)
	// S2 ACK-ts 双轨（15d5f7b）：主侧 GETACK 回复为 4 参 REPLCONF ACK <offset> <ts>
	// ——第 4 参 = currentTS（本测试无写路径——fresh ReplicationManager → 0）。
	if len(resp.Args) != 4 {
		t.Fatalf("GETACK reply has %d args, want 4", len(resp.Args))
	}
	assert.Equal(t, "REPLCONF", string(resp.Args[0]))
	assert.Equal(t, "ACK", string(resp.Args[1]))
	assert.Equal(t, "12345", string(resp.Args[2]))
	assert.Equal(t, "0", string(resp.Args[3]))

	serverEnd.Close()
	slaveConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleSlaveReplicationConnection did not return after close")
	}
}
