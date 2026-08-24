package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// setupTestHandler 创建测试用的Handler
func setupTestHandler(t *testing.T) (*Handler, *connState) {
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	h := &Handler{
		Db:    db,
		conns: make(map[*connState]*connMeta),
		cmdCounters: map[string]*atomic.Int64{
			"ZRANK":    new(atomic.Int64),
			"ZREVRANK": new(atomic.Int64),
			"ZRANGE":   new(atomic.Int64),
		},
	}
	h.SetAuthPassword(os.Getenv("BOLTDB_PASSWORD"))
	return h, &connState{}
}

// TestExecuteCommand 单元测试：直接测试executeCommand函数
func TestExecuteCommand(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name     string
		cmd      string
		args     [][]byte
		validate func(t *testing.T, resp proto.RESP)
	}{
		{
			name: "PING",
			cmd:  "PING",
			args: nil,
			validate: func(t *testing.T, resp proto.RESP) {
				// PING should return "PONG"
				ss, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "PONG", string(*ss))
			},
		},
		{
			name: "SET and GET",
			cmd:  "SET",
			args: [][]byte{[]byte("key1"), []byte("value1")},
			validate: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
				// Test GET
				getResp := handler.executeCommand(state, "GET", [][]byte{[]byte("key1")}, "127.0.0.1:12345")
				bulk, ok := getResp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "value1", string(*bulk))
			},
		},
		{
			name: "SET with wrong args",
			cmd:  "SET",
			args: [][]byte{[]byte("key1")},
			validate: func(t *testing.T, resp proto.RESP) {
				err, ok := resp.(*proto.Error)
				assert.True(t, ok)
				assert.True(t, strings.Contains(string(*err), "wrong number of arguments"))
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
			name: "EXISTS",
			cmd:  "EXISTS",
			args: [][]byte{[]byte("nonexistent")},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(0), int64(*integer))
			},
		},
		{
			name: "Unknown command",
			cmd:  "UNKNOWN",
			args: nil,
			validate: func(t *testing.T, resp proto.RESP) {
				err, ok := resp.(*proto.Error)
				assert.True(t, ok)
				assert.True(t, strings.Contains(string(*err), "unknown command"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.validate(t, resp)
		})
	}
}

// readRESPResponse 辅助函数：读取并解析RESP响应
func readRESPResponse(reader *bufio.Reader) (proto.RESP, error) {
	// 读取第一行来确定响应类型
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// 去掉\r\n
	line = bytes.TrimRight(line, "\r\n")

	if len(line) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	switch line[0] {
	case '+': // Simple String
		return proto.NewSimpleString(string(line[1:])), nil
	case '-': // Error
		return proto.NewError(string(line[1:])), nil
	case ':': // Integer
		val, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return proto.NewInteger(val), nil
	case '$': // Bulk String
		length, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return proto.NewBulkString(nil), nil
		}
		data := make([]byte, length)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, err
		}
		// 读取\r\n
		_, _ = reader.ReadBytes('\n')
		return proto.NewBulkString(data), nil
	case '*': // Array
		return proto.ReadRESP(reader)
	default:
		return nil, fmt.Errorf("unknown RESP type: %c", line[0])
	}
}

// TestTCPIntegration 集成测试：通过TCP连接测试完整的请求-响应流程
func TestTCPIntegration(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	// 启动测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	// 在goroutine中运行服务器
	go func() {
		_ = handler.ServeTCP(listener)
	}()

	// 连接到服务器 (listener 在 net.Listen 返回时已就绪)
	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	reader := bufio.NewReader(conn)

	tests := []struct {
		name     string
		command  string
		args     []string
		validate func(t *testing.T, resp proto.RESP)
	}{
		{
			name:    "PING",
			command: "PING",
			args:    nil,
			validate: func(t *testing.T, resp proto.RESP) {
				simple, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "PONG", string(*simple))
			},
		},
		{
			name:    "SET",
			command: "SET",
			args:    []string{"testkey", "testvalue"},
			validate: func(t *testing.T, resp proto.RESP) {
				simple, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "OK", string(*simple))
			},
		},
		{
			name:    "GET",
			command: "GET",
			args:    []string{"testkey"},
			validate: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "testvalue", string(*bulk))
			},
		},
		{
			name:    "GET nonexistent",
			command: "GET",
			args:    []string{"nonexistent"},
			validate: func(t *testing.T, resp proto.RESP) {
				// GET nonexistent should return nil bulk string
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Nil(t, *bulk) // Check that the BulkString data is nil
			},
		},
		{
			name:    "INCR",
			command: "INCR",
			args:    []string{"counter"},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name:    "INCRBY",
			command: "INCRBY",
			args:    []string{"counter", "5"},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(6), int64(*integer))
			},
		},
		{
			name:    "EXISTS",
			command: "EXISTS",
			args:    []string{"testkey"},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		{
			name:    "TYPE",
			command: "TYPE",
			args:    []string{"testkey"},
			validate: func(t *testing.T, resp proto.RESP) {
				simple, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "string", string(*simple))
			},
		},
		{
			name:    "DEL",
			command: "DEL",
			args:    []string{"testkey"},
			validate: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构建RESP命令
			cmdArgs := make([][]byte, 1+len(tt.args))
			cmdArgs[0] = []byte(tt.command)
			for i, arg := range tt.args {
				cmdArgs[i+1] = []byte(arg)
			}
			req := &proto.Array{Args: cmdArgs}

			// 发送命令
			err := proto.WriteRESP(conn, req)
			assert.NoError(t, err)

			// 读取并解析响应
			resp, err := readRESPResponse(reader)
			assert.NoError(t, err)

			// 验证响应
			tt.validate(t, resp)
		})
	}
}

// TestListCommands 测试List相关命令
func TestListCommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// LPUSH
	resp := handler.executeCommand(state, "LPUSH", [][]byte{[]byte("mylist"), []byte("world"), []byte("hello")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// LLEN
	resp = handler.executeCommand(state, "LLEN", [][]byte{[]byte("mylist")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// LPOP
	resp = handler.executeCommand(state, "LPOP", [][]byte{[]byte("mylist")}, "127.0.0.1:12345")
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bulk))
}

// TestHashCommands 测试Hash相关命令
func TestHashCommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// HSET
	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte("user:1"), []byte("name"), []byte("Alice"), []byte("age"), []byte("30")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// HGET
	resp = handler.executeCommand(state, "HGET", [][]byte{[]byte("user:1"), []byte("name")}, "127.0.0.1:12345")
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "Alice", string(*bulk))

	// HLEN
	resp = handler.executeCommand(state, "HLEN", [][]byte{[]byte("user:1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))
}

// TestSetCommands 测试Set相关命令
func TestSetCommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// SADD
	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("member1"), []byte("member2")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// SCARD
	resp = handler.executeCommand(state, "SCARD", [][]byte{[]byte("myset")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// SISMEMBER
	resp = handler.executeCommand(state, "SISMEMBER", [][]byte{[]byte("myset"), []byte("member1")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))
}

// TestSortedSetCommands 测试SortedSet相关命令
func TestSortedSetCommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// ZADD
	resp := handler.executeCommand(state, "ZADD", [][]byte{
		[]byte("zset"),
		[]byte("1.0"),
		[]byte("member1"),
		[]byte("2.0"),
		[]byte("member2"),
	}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// ZCARD
	resp = handler.executeCommand(state, "ZCARD", [][]byte{[]byte("zset")}, "127.0.0.1:12345")
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(2), int64(*integer))

	// ZSCORE
	resp = handler.executeCommand(state, "ZSCORE", [][]byte{[]byte("zset"), []byte("member1")}, "127.0.0.1:12345")
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "1", string(*bulk))
}

// TestErrorHandling 测试错误处理
func TestErrorHandling(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name    string
		cmd     string
		args    [][]byte
		wantErr bool
		errMsg  string
	}{
		{
			name:    "SET with insufficient args",
			cmd:     "SET",
			args:    [][]byte{[]byte("key")},
			wantErr: true,
			errMsg:  "wrong number of arguments",
		},
		{
			name:    "GET with no args",
			cmd:     "GET",
			args:    nil,
			wantErr: true,
			errMsg:  "wrong number of arguments",
		},
		{
			name:    "INCRBY with invalid number",
			cmd:     "INCRBY",
			args:    [][]byte{[]byte("key"), []byte("notanumber")},
			wantErr: true,
			errMsg:  "value is not an integer",
		},
		{
			name:    "Unknown command",
			cmd:     "UNKNOWNCMD",
			args:    nil,
			wantErr: true,
			errMsg:  "unknown command",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			err, ok := resp.(*proto.Error)
			if tt.wantErr {
				assert.True(t, ok)
				assert.True(t, strings.Contains(string(*err), tt.errMsg))
			} else {
				assert.False(t, ok)
			}
		})
	}
}

// TestConcurrentConnections 测试并发连接
func TestConcurrentConnections(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	go func() {
		_ = handler.ServeTCP(listener)
	}()

	// 创建多个并发连接
	const numConnections = 10
	done := make(chan bool, numConnections)

	for i := 0; i < numConnections; i++ {
		go func(id int) {
			conn, err := net.Dial("tcp", listener.Addr().String())
			assert.NoError(t, err)
			defer conn.Close()

			// 每个连接执行SET和GET
			key := fmt.Sprintf("key%d", id)
			value := fmt.Sprintf("value%d", id)

			// SET
			req := &proto.Array{
				Args: [][]byte{[]byte("SET"), []byte(key), []byte(value)},
			}
			err = proto.WriteRESP(conn, req)
			assert.NoError(t, err)

			// 读取响应
			reader := bufio.NewReader(conn)
			resp, err := proto.ReadRESP(reader)
			assert.NoError(t, err)
			assert.Equal(t, "OK", string(resp.Args[0]))

			// GET
			req = &proto.Array{
				Args: [][]byte{[]byte("GET"), []byte(key)},
			}
			err = proto.WriteRESP(conn, req)
			assert.NoError(t, err)

			resp, err = proto.ReadRESP(reader)
			assert.NoError(t, err)
			assert.Equal(t, value, string(resp.Args[0]))

			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numConnections; i++ {
		<-done
	}
}

// BenchmarkExecuteCommand 性能测试
func BenchmarkExecuteCommand(b *testing.B) {
	handler, state := setupTestHandler(&testing.T{})
	defer handler.Db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(value)}, "127.0.0.1:12345")
		_ = handler.executeCommand(state, "GET", [][]byte{[]byte(key)}, "127.0.0.1:12345")
	}
}

// sendCommand 辅助函数：发送命令并读取响应
func sendCommand(conn net.Conn, reader *bufio.Reader, cmd string, args ...string) (proto.RESP, error) {
	cmdArgs := make([][]byte, 1+len(args))
	cmdArgs[0] = []byte(cmd)
	for i, arg := range args {
		cmdArgs[i+1] = []byte(arg)
	}
	req := &proto.Array{Args: cmdArgs}

	if err := proto.WriteRESP(conn, req); err != nil {
		return nil, err
	}

	return readRESPResponse(reader)
}

func setupTestHandlerWithPubSub(t *testing.T) (*Handler, *connState) {
	h, state := setupTestHandler(t)
	h.PubSub = store.NewPubSubManager()
	return h, state
}

// TestWatchCleanupOnDisconnect 验证 WATCH 连接断开后 watchMonitors 被正确清理
func TestWatchCleanupOnDisconnect(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	go func() { _ = handler.ServeTCP(listener) }()

	// 连接1：WATCH 一个 key
	conn1, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	reader1 := bufio.NewReader(conn1)

	resp, err := sendCommand(conn1, reader1, "WATCH", "mykey")
	assert.NoError(t, err)
	assert.Equal(t, proto.OK, resp)

	// 验证 watchMonitors 已创建并包含 mykey
	handler.watchMu.Lock()
	assert.NotNil(t, handler.watchMonitors)
	assert.Equal(t, 1, len(handler.watchMonitors["mykey"]))
	handler.watchMu.Unlock()

	// 断开连接
	conn1.Close()

	// 等待服务器 goroutine 完成 cleanup
	time.Sleep(50 * time.Millisecond)

	// 验证 watchMonitors 已被清理
	handler.watchMu.Lock()
	_, exists := handler.watchMonitors["mykey"]
	handler.watchMu.Unlock()
	assert.False(t, exists)
}

// TestWatchCleanupOnDisconnectMultipleKeys 验证多 key WATCH 断开后全部被清理
func TestWatchCleanupOnDisconnectMultipleKeys(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	go func() { _ = handler.ServeTCP(listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	reader := bufio.NewReader(conn)

	resp, err := sendCommand(conn, reader, "WATCH", "k1", "k2", "k3")
	assert.NoError(t, err)
	assert.Equal(t, proto.OK, resp)

	handler.watchMu.Lock()
	assert.Equal(t, 3, len(handler.watchMonitors))
	handler.watchMu.Unlock()

	conn.Close()
	time.Sleep(50 * time.Millisecond)

	handler.watchMu.Lock()
	assert.Equal(t, 0, len(handler.watchMonitors))
	handler.watchMu.Unlock()
}

// TestSubscriberCleanupOnDisconnect 验证 SUBSCRIBE 连接断开后 subscriber 被移除
func TestSubscriberCleanupOnDisconnect(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandlerWithPubSub(t)
	defer handler.Db.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	go func() { _ = handler.ServeTCP(listener) }()

	// 订阅前 subscriber 数量应为 0
	assert.Equal(t, 0, handler.PubSub.GetSubscriberCount("testchan"))

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	reader := bufio.NewReader(conn)

	// SUBSCRIBE 到一个频道
	req := &proto.Array{
		Args: [][]byte{[]byte("SUBSCRIBE"), []byte("testchan")},
	}
	err = proto.WriteRESP(conn, req)
	assert.NoError(t, err)

	// 读取 3 条确认消息 (subscribe, testchan, 1)
	for i := 0; i < 3; i++ {
		_, err := readRESPResponse(reader)
		assert.NoError(t, err)
	}

	// 验证 subscriber 已注册
	assert.Equal(t, 1, handler.PubSub.GetSubscriberCount("testchan"))

	// 断开连接
	conn.Close()
	time.Sleep(50 * time.Millisecond)

	// 验证 subscriber 已被移除
	assert.Equal(t, 0, handler.PubSub.GetSubscriberCount("testchan"))
}

// TestDisconnectMidTransaction 验证事务中断开后没有泄漏
func TestDisconnectMidTransaction(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	go func() { _ = handler.ServeTCP(listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	reader := bufio.NewReader(conn)

	// 开始事务但不 EXEC
	resp, err := sendCommand(conn, reader, "MULTI")
	assert.NoError(t, err)
	assert.Equal(t, proto.OK, resp)
	resp, err = sendCommand(conn, reader, "SET", "midkey", "midval")
	assert.NoError(t, err)
	// In transaction — returns QUEUED
	queuedResp := proto.NewSimpleString("QUEUED")
	assert.Equal(t, queuedResp, resp)
	resp, err = sendCommand(conn, reader, "INCR", "midkey")
	assert.NoError(t, err)
	assert.Equal(t, queuedResp, resp)

	// 断开连接（不 EXEC/DISCARD）
	conn.Close()
	time.Sleep(50 * time.Millisecond)

	// 验证 key 没有被设置（事务没提交）
	conn2, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	resp, err = sendCommand(conn2, reader2, "GET", "midkey")
	assert.NoError(t, err)
	bulk, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Nil(t, *bulk)
}

// TestDisconnectCleanupAfterPipeline 验证 pipeline 中段断开后状态清理
func TestDisconnectCleanupAfterPipeline(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandlerWithPubSub(t)
	defer handler.Db.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	go func() { _ = handler.ServeTCP(listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)

	// Pipeline: 发送多条命令但不读响应
	cmds := [][][]byte{
		{[]byte("SET"), []byte("k1"), []byte("v1")},
		{[]byte("SET"), []byte("k2"), []byte("v2")},
		{[]byte("WATCH"), []byte("k1")},
		{[]byte("SUBSCRIBE"), []byte("ch")},
	}
	for _, args := range cmds {
		req := &proto.Array{Args: args}
		_ = proto.WriteRESP(conn, req)
	}

	// 立即断开（不读取任何响应）
	conn.Close()
	time.Sleep(100 * time.Millisecond)

	// 验证没有泄露
	handler.watchMu.Lock()
	watchCount := len(handler.watchMonitors)
	handler.watchMu.Unlock()
	assert.Equal(t, 0, watchCount)
	assert.Equal(t, 0, handler.PubSub.GetSubscriberCount("ch"))
}

// TestRealWorldScenario 测试真实场景
func TestRealWorldScenario(t *testing.T) {
	t.Parallel()
	handler, _ := setupTestHandler(t)
	defer handler.Db.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	go func() {
		_ = handler.ServeTCP(listener)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 场景1：用户会话管理
	// SET session:user1 "token123"
	resp, err := sendCommand(conn, reader, "SET", "session:user1", "token123")
	assert.NoError(t, err)
	assert.Equal(t, proto.OK, resp)

	// EXPIRE session:user1 3600
	resp, err = sendCommand(conn, reader, "EXPIRE", "session:user1", "3600")
	assert.NoError(t, err)
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// GET session:user1
	resp, err = sendCommand(conn, reader, "GET", "session:user1")
	assert.NoError(t, err)
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "token123", string(*bs))

	// 场景2：计数器
	// INCR page:views
	resp, err = sendCommand(conn, reader, "INCR", "page:views")
	assert.NoError(t, err)
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// INCRBY page:views 10
	resp, err = sendCommand(conn, reader, "INCRBY", "page:views", "10")
	assert.NoError(t, err)
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(11), int64(*integer))

	// 场景3：购物车（使用Hash）
	// HSET cart:user1 item1 2
	resp, err = sendCommand(conn, reader, "HSET", "cart:user1", "item1", "2")
	assert.NoError(t, err)
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// HSET cart:user1 item2 1
	resp, err = sendCommand(conn, reader, "HSET", "cart:user1", "item2", "1")
	assert.NoError(t, err)
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(1), int64(*integer))

	// HGETALL cart:user1
	resp, err = sendCommand(conn, reader, "HGETALL", "cart:user1")
	assert.NoError(t, err)
}

// TestMaxInputBytesCutsConnection verifies that when MaxInputBytes is set,
// a client connection is cut after exceeding the cumulative input limit.
func TestMaxInputBytesCutsConnection(t *testing.T) {
	t.Parallel()
	db, err := store.NewBadgerStore(t.TempDir())
	assert.NoError(t, err)
	defer db.CloseWithTimeout(store.CloseTimeout)

	handler := &Handler{
		Db:            db,
		Ctx:           context.Background(),
		MaxInputBytes: 100, // very low limit — 100 bytes total
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	done := make(chan error, 1)
	go func() { done <- handler.ServeTCP(listener) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send a small SET command (~40 bytes in RESP wire format)
	resp, err := sendCommand(conn, reader, "SET", "key1", "value1")
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", resp.String())

	// Send a larger SET command (~50 bytes). Cumulative should now exceed 100.
	_, err = sendCommand(conn, reader, "SET", "key2", "value2345678901234567890")
	// After exceeding the limit, ReadRESP should fail → sendCommand returns error
	if err == nil {
		// The limit might be hit mid-write or on the *next* read boundary;
		// try one more command to verify the connection is unusable.
		_, err = sendCommand(conn, reader, "PING")
	}
	assert.Error(t, err)

	// Shutdown the handler
	listener.Close()
	handler.Shutdown()
	<-done
}
