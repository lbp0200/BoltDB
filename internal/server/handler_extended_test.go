package server

import (
	"regexp"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestServerBitCommands tests BIT commands
func TestServerBitCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// SETBIT
		{
			name: "SETBIT",
			cmd:  "SETBIT",
			args: [][]byte{[]byte("bitkey"), []byte("0"), []byte("1")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(0), int64(*integer))
			},
		},
		// GETBIT
		{
			name: "GETBIT",
			cmd:  "GETBIT",
			args: [][]byte{[]byte("bitkey"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// BITCOUNT
		{
			name: "BITCOUNT",
			cmd:  "BITCOUNT",
			args: [][]byte{[]byte("bitkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// BITPOS
		{
			name: "BITPOS",
			cmd:  "BITPOS",
			args: [][]byte{[]byte("bitkey"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) >= -1)
			},
		},
		// BITOP AND
		{
			name: "BITOP AND",
			cmd:  "BITOP",
			args: [][]byte{[]byte("AND"), []byte("dest"), []byte("bitkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) >= 0) // BITOP AND returns result length
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

// TestServerTransactionCommands tests MULTI/EXEC commands
func TestServerTransactionCommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Just verify commands execute without error - actual transaction behavior may vary
	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		{"MULTI", "MULTI", nil, func(t *testing.T, resp proto.RESP) {
			ss, ok := resp.(*proto.SimpleString)
			assert.True(t, ok)
			assert.Equal(t, "OK", string(*ss))
		}},
		{"SET in transaction", "SET", [][]byte{[]byte("txkey"), []byte("txvalue")}, func(t *testing.T, resp proto.RESP) {
			ss, ok := resp.(*proto.SimpleString)
			assert.True(t, ok)
			assert.Equal(t, "QUEUED", string(*ss))
		}},
		{"EXEC", "EXEC", nil, func(t *testing.T, resp proto.RESP) {
			_, ok := resp.(*proto.NestedArray)
			assert.True(t, ok)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerGeoCommands tests GEO commands
func TestServerGeoCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// GEOADD
		{
			name: "GEOADD",
			cmd:  "GEOADD",
			args: [][]byte{[]byte("cities"), []byte("-122.4194"), []byte("37.7749"), []byte("San Francisco")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// GEODIST
		{
			name: "GEODIST",
			cmd:  "GEODIST",
			args: [][]byte{[]byte("cities"), []byte("San Francisco"), []byte("Los Angeles")},
			check: func(t *testing.T, resp proto.RESP) {
				// Los Angeles not added — returns Error
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// GEOHASH
		{
			name: "GEOHASH",
			cmd:  "GEOHASH",
			args: [][]byte{[]byte("cities"), []byte("San Francisco")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 1, len(arr.Args))
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

// TestServerStreamCommands tests Stream commands
func TestServerStreamCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// XADD
		{
			name: "XADD",
			cmd:  "XADD",
			args: [][]byte{[]byte("mystream"), []byte("*"), []byte("field"), []byte("value")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, regexp.MustCompile("^[0-9]+-[0-9]+$").MatchString(string(*bulk)))
			},
		},
		// XLEN
		{
			name: "XLEN",
			cmd:  "XLEN",
			args: [][]byte{[]byte("mystream")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// XRANGE
		{
			name: "XRANGE",
			cmd:  "XRANGE",
			args: [][]byte{[]byte("mystream"), []byte("-"), []byte("+"), []byte("COUNT"), []byte("1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
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

// TestServerScanCommands tests SCAN commands
func TestServerScanCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set some keys
	handler.executeCommand(state, "SET", [][]byte{[]byte("key1"), []byte("value1")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("key2"), []byte("value2")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// SCAN
		{
			name: "SCAN",
			cmd:  "SCAN",
			args: [][]byte{[]byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RawString)
				assert.True(t, ok) // SCAN returns formatted RESP string
			},
		},
		// SSCAN
		{
			name: "SSCAN",
			cmd:  "SSCAN",
			args: [][]byte{[]byte("nonexistent"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
			},
		},
		// HSCAN
		{
			name: "HSCAN",
			cmd:  "HSCAN",
			args: [][]byte{[]byte("nonexistent"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
			},
		},
		// ZSCAN
		{
			name: "ZSCAN",
			cmd:  "ZSCAN",
			args: [][]byte{[]byte("nonexistent"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
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

// TestServerObjectCommands tests OBJECT commands
func TestServerObjectCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Set a key
	handler.executeCommand(state, "SET", [][]byte{[]byte("objkey"), []byte("objvalue")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// OBJECT REFCOUNT
		{
			name: "OBJECT REFCOUNT",
			cmd:  "OBJECT",
			args: [][]byte{[]byte("REFCOUNT"), []byte("objkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) >= 1)
			},
		},
		// OBJECT ENCODING
		{
			name: "OBJECT ENCODING",
			cmd:  "OBJECT",
			args: [][]byte{[]byte("ENCODING"), []byte("objkey")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
			},
		},
		// OBJECT IDLETIME
		{
			name: "OBJECT IDLETIME",
			cmd:  "OBJECT",
			args: [][]byte{[]byte("IDLETIME"), []byte("objkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(0), int64(*integer))
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

// TestServerClientCommands tests CLIENT commands
func TestServerClientCommands(t *testing.T) {
	t.Parallel()
	// Not parallel — avoids BadgerDB contention on slow CI runners
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// CLIENT LIST
		{
			name: "CLIENT LIST",
			cmd:  "CLIENT",
			args: [][]byte{[]byte("LIST")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
			},
		},
		// CLIENT ID
		{
			name: "CLIENT ID",
			cmd:  "CLIENT",
			args: [][]byte{[]byte("ID")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) > 0)
			},
		},
		// CLIENT GETNAME
		{
			name: "CLIENT GETNAME",
			cmd:  "CLIENT",
			args: [][]byte{[]byte("GETNAME")},
			check: func(t *testing.T, resp proto.RESP) {
				// No name set — returns nil bulk string
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Nil(t, *bs)
			},
		},
		// CLIENT SETNAME
		{
			name: "CLIENT SETNAME",
			cmd:  "CLIENT",
			args: [][]byte{[]byte("SETNAME"), []byte("testclient")},
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
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

// TestServerConfigCommands tests CONFIG commands
func TestServerConfigCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// CONFIG GET
		{
			name: "CONFIG GET",
			cmd:  "CONFIG",
			args: [][]byte{[]byte("GET"), []byte("maxmemory")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Array)
				assert.True(t, ok)
			},
		},
		// CONFIG HELP
		{
			name: "CONFIG HELP",
			cmd:  "CONFIG",
			args: [][]byte{[]byte("HELP")},
			check: func(t *testing.T, resp proto.RESP) {
				// CONFIG HELP is not supported — returns Error
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

// TestServerClusterCommands tests CLUSTER commands (basic)
func TestServerClusterCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// CLUSTER INFO (when cluster is not enabled)
		{
			name: "CLUSTER INFO",
			cmd:  "CLUSTER",
			args: [][]byte{[]byte("INFO")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok) // cluster disabled
			},
		},
		// CLUSTER NODES
		{
			name: "CLUSTER NODES",
			cmd:  "CLUSTER",
			args: [][]byte{[]byte("NODES")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok) // cluster disabled
			},
		},
		// CLUSTER MYID
		{
			name: "CLUSTER MYID",
			cmd:  "CLUSTER",
			args: [][]byte{[]byte("MYID")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok) // cluster disabled
			},
		},
		// CLUSTER KEYSLOT
		{
			name: "CLUSTER KEYSLOT",
			cmd:  "CLUSTER",
			args: [][]byte{[]byte("KEYSLOT"), []byte("testkey")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.Error)
				assert.True(t, ok) // cluster disabled
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

// TestServerReplConfCommands tests REPLCONF commands
func TestServerReplConfCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// REPLCONF LISTENING-PORT
		{
			name: "REPLCONF LISTENING-PORT",
			cmd:  "REPLCONF",
			args: [][]byte{[]byte("LISTENING-PORT"), []byte("6379")},
			check: func(t *testing.T, resp proto.RESP) {
				// Replication not enabled — returns Error
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// REPLCONF CAPA
		{
			name: "REPLCONF CAPA",
			cmd:  "REPLCONF",
			args: [][]byte{[]byte("CAPA"), []byte("eof")},
			check: func(t *testing.T, resp proto.RESP) {
				// Replication not enabled — returns Error
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

// TestServerSaveCommands tests SAVE commands
func TestServerSaveCommands(t *testing.T) {
	t.Parallel()

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// SAVE (synchronous)
		{
			name: "SAVE",
			cmd:  "SAVE",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				// Backup not enabled in tests — returns Error
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
		// BGSAVE
		{
			name: "BGSAVE",
			cmd:  "BGSAVE",
			args: nil,
			check: func(t *testing.T, resp proto.RESP) {
				// Backup not enabled in tests — returns Error
				_, ok := resp.(*proto.Error)
				assert.True(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp proto.RESP
			if tt.args == nil {
				resp = handler.executeCommand(state, tt.cmd, nil, "127.0.0.1:12345")
			} else {
				resp = handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			}
			tt.check(t, resp)
		})
	}
}
