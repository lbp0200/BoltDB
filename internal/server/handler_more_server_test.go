package server

import (
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestServerAdditionalCommands tests additional server commands
func TestServerAdditionalCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// SHUTDOWN
		{
			name: "SHUTDOWN",
			cmd:  "SHUTDOWN",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				// SHUTDOWN 已实现：返回 OK 并触发 OnShutdown 钩子
				// （此前是 NOREADONLY 错误占位）
				assert.Equal(t, proto.OK, resp)
			},
		},
		// FLUSHALL
		{
			name: "FLUSHALL",
			cmd:  "FLUSHALL",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// MEMORY USAGE
		{
			name: "MEMORY USAGE",
			cmd:  "MEMORY",
			args: [][]byte{[]byte("USAGE"), []byte("nonexistent")},
			check: func(t *testing.T, resp proto.RESP) {
				// Nonexistent key returns nil bulk string
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Nil(t, *bs)
			},
		},
		// MEMORY DOCTOR
		{
			name: "MEMORY DOCTOR",
			cmd:  "MEMORY",
			args: [][]byte{[]byte("DOCTOR")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) > 0)
			},
		},
		// MEMORY HELP
		{
			name: "MEMORY HELP",
			cmd:  "MEMORY",
			args: [][]byte{[]byte("HELP")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) > 0)
			},
		},
		// LATENCY LATEST
		{
			name: "LATENCY LATEST",
			cmd:  "LATENCY",
			args: [][]byte{[]byte("LATEST")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Array)
				assert.True(t, ok)
			},
		},
		// LATENCY HELP
		{
			name: "LATENCY HELP",
			cmd:  "LATENCY",
			args: [][]byte{[]byte("HELP")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) > 0)
			},
		},
		// LATENCY DOCTOR
		{
			name: "LATENCY DOCTOR",
			cmd:  "LATENCY",
			args: [][]byte{[]byte("DOCTOR")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) > 0)
			},
		},
		// READONLY
		{
			name: "READONLY",
			cmd:  "READONLY",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// READWRITE
		{
			name: "READWRITE",
			cmd:  "READWRITE",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// LASTSAVE
		{
			name: "LASTSAVE",
			cmd:  "LASTSAVE",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				// Backup not enabled — returns Error
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// SLOWLOG GET
		{
			name: "SLOWLOG GET",
			cmd:  "SLOWLOG",
			args: [][]byte{[]byte("GET"), []byte("10")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
			},
		},
		// SLOWLOG RESET
		{
			name: "SLOWLOG RESET",
			cmd:  "SLOWLOG",
			args: [][]byte{[]byte("RESET")},
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// SLOWLOG HELP
		{
			name: "SLOWLOG HELP",
			cmd:  "SLOWLOG",
			args: [][]byte{[]byte("HELP")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) > 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerPubSubCommands tests PUBSUB commands
func TestServerPubSubCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// PUBSUB CHANNELS
		{
			name: "PUBSUB CHANNELS",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("CHANNELS")},
			check: func(t *testing.T, resp proto.RESP) {
				// PubSub not enabled — returns Error
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// PUBSUB CHANNELS with pattern
		{
			name: "PUBSUB CHANNELS pattern",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("CHANNELS"), []byte("test*")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// PUBSUB NUMSUB
		{
			name: "PUBSUB NUMSUB",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("NUMSUB"), []byte("testchannel")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// PUBSUB NUMPAT
		{
			name: "PUBSUB NUMPAT",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("NUMPAT")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// PUBSUB HELP
		{
			name: "PUBSUB HELP",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("HELP")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerDebugCommands tests DEBUG commands
func TestServerDebugCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set up a key for testing
	handler.executeCommand(state, "SET", [][]byte{[]byte("debugkey"), []byte("debugvalue")}, "127.0.0.1:12345")

	t.Run("DEBUG SLEEP", func(t *testing.T) {
		start := time.Now()
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("SLEEP"), []byte("0.05")}, "127.0.0.1:12345")
		elapsed := time.Since(start)
		// Should have slept at least 50ms
		assert.True(t, elapsed >= 45*time.Millisecond)
		// Should return OK
		ss, ok := resp.(*proto.SimpleString)
		assert.True(t, ok)
		assert.Equal(t, "OK", string(*ss))
	})

	t.Run("DEBUG OBJECT", func(t *testing.T) {
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("OBJECT"), []byte("debugkey")}, "127.0.0.1:12345")
		bs, ok := resp.(*proto.BulkString)
		assert.True(t, ok)
		assert.True(t, strings.Contains(string(*bs), "Type: string"))
	})

	t.Run("DEBUG OBJECT nonexistent", func(t *testing.T) {
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("OBJECT"), []byte("nosuchkey")}, "127.0.0.1:12345")
		bs, ok := resp.(*proto.BulkString)
		assert.True(t, ok)
		assert.True(t, strings.Contains(string(*bs), "Type: none"))
	})

	t.Run("DEBUG SEGFAULT", func(t *testing.T) {
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("SEGFAULT")}, "127.0.0.1:12345")
		errResp, ok := resp.(*proto.Error)
		assert.True(t, ok)
		assert.True(t, strings.Contains(string(*errResp), "simulated"))
	})

	t.Run("DEBUG ERROR", func(t *testing.T) {
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("ERROR"), []byte("test error message")}, "127.0.0.1:12345")
		errResp, ok := resp.(*proto.Error)
		assert.True(t, ok)
		assert.True(t, strings.Contains(string(*errResp), "test error message"))
	})

	t.Run("DEBUG unknown subcommand", func(t *testing.T) {
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("NOSUCH")}, "127.0.0.1:12345")
		errResp, ok := resp.(*proto.Error)
		assert.True(t, ok)
		assert.True(t, strings.Contains(string(*errResp), "unknown DEBUG subcommand"))
	})

	t.Run("DEBUG GC", func(t *testing.T) {
		// On an almost-empty store this may rewrite 0 files, but must return
		// an integer without error.
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("GC")}, "127.0.0.1:12345")
		di, ok := resp.(*proto.Integer)
		assert.True(t, ok)
		assert.True(t, *di >= 0)
	})

	t.Run("DEBUG GC invalid ratio", func(t *testing.T) {
		resp := handler.executeCommand(state, "DEBUG", [][]byte{[]byte("GC"), []byte("1.5")}, "127.0.0.1:12345")
		errResp, ok := resp.(*proto.Error)
		assert.True(t, ok)
		assert.True(t, strings.Contains(string(*errResp), "invalid discard ratio"))
	})

	t.Run("DEBUG no args", func(t *testing.T) {
		resp := handler.executeCommand(state, "DEBUG", nil, "127.0.0.1:12345")
		errResp, ok := resp.(*proto.Error)
		assert.True(t, ok)
		assert.True(t, strings.Contains(string(*errResp), "wrong number of arguments"))
	})
}

// TestServerRoleCommand tests ROLE command
func TestServerRoleCommand(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(state, "ROLE", nil, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}
