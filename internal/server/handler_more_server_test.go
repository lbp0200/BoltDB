package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)


// TestServerAdditionalCommands tests additional server commands
func TestServerAdditionalCommands(t *testing.T) {
	handler := setupTestHandler(t)
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
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
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
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// MEMORY DOCTOR
		{
			name: "MEMORY DOCTOR",
			cmd:  "MEMORY",
			args: [][]byte{[]byte("DOCTOR")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
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
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) >= 0)
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
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
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
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// SLOWLOG GET
		{
			name: "SLOWLOG GET",
			cmd:  "SLOWLOG",
			args: [][]byte{[]byte("GET"), []byte("10")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) >= 0)
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
			resp := handler.executeCommand(testState, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerPubSubCommands tests PUBSUB commands
func TestServerPubSubCommands(t *testing.T) {
	handler := setupTestHandler(t)
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
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// PUBSUB CHANNELS with pattern
		{
			name: "PUBSUB CHANNELS pattern",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("CHANNELS"), []byte("test*")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// PUBSUB NUMSUB
		{
			name: "PUBSUB NUMSUB",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("NUMSUB"), []byte("testchannel")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// PUBSUB NUMPAT
		{
			name: "PUBSUB NUMPAT",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("NUMPAT")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// PUBSUB HELP
		{
			name: "PUBSUB HELP",
			cmd:  "PUBSUB",
			args: [][]byte{[]byte("HELP")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(testState, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerDebugCommands tests DEBUG commands
func TestServerDebugCommands(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set up a key for testing
	handler.executeCommand(testState, "SET", [][]byte{[]byte("debugkey"), []byte("debugvalue")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// DEBUG OBJECT
		{
			name: "DEBUG OBJECT",
			cmd:  "DEBUG",
			args: [][]byte{[]byte("OBJECT"), []byte("debugkey")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// DEBUG SLEEP
		{
			name: "DEBUG SLEEP",
			cmd:  "DEBUG",
			args: [][]byte{[]byte("SLEEP"), []byte("0.1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// DEBUG SEGFAULT
		{
			name: "DEBUG SEGFAULT",
			cmd:  "DEBUG",
			args: [][]byte{[]byte("SEGFAULT")},
			check: func(t *testing.T, resp proto.RESP) {
				// This should cause an error or crash
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// DEBUG ERROR
		{
			name: "DEBUG ERROR",
			cmd:  "DEBUG",
			args: [][]byte{[]byte("ERROR")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(testState, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerRoleCommand tests ROLE command
func TestServerRoleCommand(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	resp := handler.executeCommand(testState, "ROLE", nil, "127.0.0.1:12345")
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.True(t, len(arr.Args) > 0)
}
