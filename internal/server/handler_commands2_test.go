package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestServerListCommands2 tests more list commands
func TestServerListCommands2(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup list
	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("mylist"), []byte("value1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("mylist"), []byte("value2")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// LLEN
		{
			name: "LLEN",
			cmd:  "LLEN",
			args: [][]byte{[]byte("mylist")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		// LINDEX
		{
			name: "LINDEX",
			cmd:  "LINDEX",
			args: [][]byte{[]byte("mylist"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "value2", string(*bs))
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

// TestServerSetCommands2 tests more set commands
func TestServerSetCommands2(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup set
	handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("member1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SADD", [][]byte{[]byte("myset"), []byte("member2")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// SCARD
		{
			name: "SCARD",
			cmd:  "SCARD",
			args: [][]byte{[]byte("myset")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		// SISMEMBER
		{
			name: "SISMEMBER existing",
			cmd:  "SISMEMBER",
			args: [][]byte{[]byte("myset"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Integer)
				assert.True(t, ok)
			},
		},
		// SMEMBERS
		{
			name: "SMEMBERS",
			cmd:  "SMEMBERS",
			args: [][]byte{[]byte("myset")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args))
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

// TestServerSortedSetCommands2 tests more sorted set commands
func TestServerSortedSetCommands2(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup sorted set using store.ZSetMember
	handler.Db.ZAdd("myzset", []store.ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// ZCARD
		{
			name: "ZCARD",
			cmd:  "ZCARD",
			args: [][]byte{[]byte("myzset")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		// ZSCORE
		{
			name: "ZSCORE",
			cmd:  "ZSCORE",
			args: [][]byte{[]byte("myzset"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "1", string(*bs))
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

// TestServerHashCommands2 tests more hash commands
func TestServerHashCommands2(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup hash
	handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "HSET", [][]byte{[]byte("myhash"), []byte("field2"), []byte("value2")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// HLEN
		{
			name: "HLEN",
			cmd:  "HLEN",
			args: [][]byte{[]byte("myhash")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		// HEXISTS
		{
			name: "HEXISTS existing",
			cmd:  "HEXISTS",
			args: [][]byte{[]byte("myhash"), []byte("field1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Integer)
				assert.True(t, ok)
			},
		},
		// HKEYS
		{
			name: "HKEYS",
			cmd:  "HKEYS",
			args: [][]byte{[]byte("myhash")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args))
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
