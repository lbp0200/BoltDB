package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestServerInfoCommand tests INFO command
func TestServerInfoCommand(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		{
			name: "INFO default",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, len(string(*bulk)) > 0)
			},
		},
		{
			name: "INFO SERVER",
			args: [][]byte{[]byte("SERVER")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, len(string(*bulk)) > 0)
			},
		},
		{
			name: "INFO REPLICATION",
			args: [][]byte{[]byte("REPLICATION")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, len(string(*bulk)) > 0)
			},
		},
		{
			name: "INFO PERSISTENCE",
			args: [][]byte{[]byte("PERSISTENCE")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, len(string(*bulk)) > 0)
			},
		},
		{
			name: "INFO STATS",
			args: [][]byte{[]byte("STATS")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, len(string(*bulk)) > 0)
			},
		},
		{
			name: "INFO ALL",
			args: [][]byte{[]byte("ALL")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, len(string(*bulk)) > 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp proto.RESP
			if tt.args == nil {
				resp = handler.executeCommand(nil, "INFO", nil, "127.0.0.1:12345")
			} else {
				resp = handler.executeCommand(nil, "INFO", tt.args, "127.0.0.1:12345")
			}
			tt.check(t, resp)
		})
	}
}

// TestServerManagementCommands tests server management commands
func TestServerManagementCommands(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Set up some data first
	handler.executeCommand(nil, "SET", [][]byte{[]byte("key1"), []byte("value1")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// DBSIZE
		{
			name: "DBSIZE",
			cmd:  "DBSIZE",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) > 0)
			},
		},
		// FLUSHDB
		{
			name: "FLUSHDB",
			cmd:  "FLUSHDB",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// TIME
		{
			name: "TIME",
			cmd:  "TIME",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp proto.RESP
			if tt.args == nil {
				resp = handler.executeCommand(nil, tt.cmd, nil, "127.0.0.1:12345")
			} else {
				resp = handler.executeCommand(nil, tt.cmd, tt.args, "127.0.0.1:12345")
			}
			tt.check(t, resp)
		})
	}
}

// TestServerConnectionCommands tests connection-related commands
func TestServerConnectionCommands(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// ECHO
		{
			name: "ECHO",
			cmd:  "ECHO",
			args: [][]byte{[]byte("hello world")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "hello world", string(*bulk))
			},
		},
		// SELECT
		{
			name: "SELECT",
			cmd:  "SELECT",
			args: [][]byte{[]byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp proto.RESP
			if tt.args == nil {
				resp = handler.executeCommand(nil, tt.cmd, nil, "127.0.0.1:12345")
			} else {
				resp = handler.executeCommand(nil, tt.cmd, tt.args, "127.0.0.1:12345")
			}
			tt.check(t, resp)
		})
	}
}

// TestServerSlowLogCommands tests slowlog commands
func TestServerSlowLogCommands(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// SLOWLOG LEN
	resp := handler.executeCommand(nil, "SLOWLOG", [][]byte{[]byte("LEN")}, "127.0.0.1:12345")
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.True(t, int64(*integer) >= 0)
}
