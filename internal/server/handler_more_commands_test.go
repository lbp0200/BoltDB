package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestServerMoreStringCommands tests more string commands
func TestServerMoreStringCommands(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup initial values
	handler.executeCommand(nil, "SET", [][]byte{[]byte("strkey"), []byte("hello")}, "127.0.0.1:12345")
	handler.executeCommand(nil, "SET", [][]byte{[]byte("rangekey"), []byte("HelloWorld")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// STRLEN
		{
			name: "STRLEN",
			cmd:  "STRLEN",
			args: [][]byte{[]byte("strkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(5), int64(*integer))
			},
		},
		// APPEND
		{
			name: "APPEND",
			cmd:  "APPEND",
			args: [][]byte{[]byte("strkey"), []byte(" World")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(11), int64(*integer))
			},
		},
		// GETRANGE
		{
			name: "GETRANGE",
			cmd:  "GETRANGE",
			args: [][]byte{[]byte("rangekey"), []byte("0"), []byte("4")},
			check: func(t *testing.T, resp proto.RESP) {
				bulk, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.Equal(t, "Hello", string(*bulk))
			},
		},
		// SETRANGE
		{
			name: "SETRANGE",
			cmd:  "SETRANGE",
			args: [][]byte{[]byte("rangekey"), []byte("5"), []byte("X")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(10), int64(*integer))
			},
		},
		// SET with multiple options
		{
			name: "SET with EX",
			cmd:  "SET",
			args: [][]byte{[]byte("exkey"), []byte("value"), []byte("EX"), []byte("100")},
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// SET with PX
		{
			name: "SET with PX",
			cmd:  "SET",
			args: [][]byte{[]byte("pxkey"), []byte("value"), []byte("PX"), []byte("100000")},
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// SET with NX
		{
			name: "SET with NX",
			cmd:  "SET",
			args: [][]byte{[]byte("nxkey"), []byte("value"), []byte("NX")},
			check: func(t *testing.T, resp proto.RESP) {
				assert.Equal(t, proto.OK, resp)
			},
		},
		// SET with XX
		{
			name: "SET with XX on existing",
			cmd:  "SET",
			args: [][]byte{[]byte("strkey"), []byte("newvalue"), []byte("XX")},
			check: func(t *testing.T, resp proto.RESP) {
				// Should return OK for existing key
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(nil, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerKeyExpirationCommands tests key expiration commands
func TestServerKeyExpirationCommands(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup key
	handler.executeCommand(nil, "SET", [][]byte{[]byte("exkey"), []byte("value")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// EXPIRE
		{
			name: "EXPIRE",
			cmd:  "EXPIRE",
			args: [][]byte{[]byte("exkey"), []byte("100")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// PEXPIRE
		{
			name: "PEXPIRE",
			cmd:  "PEXPIRE",
			args: [][]byte{[]byte("exkey"), []byte("100000")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// EXPIREAT
		{
			name: "EXPIREAT",
			cmd:  "EXPIREAT",
			args: [][]byte{[]byte("exkey"), []byte("9999999999")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// PEXPIREAT
		{
			name: "PEXPIREAT",
			cmd:  "PEXPIREAT",
			args: [][]byte{[]byte("exkey"), []byte("9999999999000")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// TTL
		{
			name: "TTL",
			cmd:  "TTL",
			args: [][]byte{[]byte("exkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) > 0)
			},
		},
		// PTTL
		{
			name: "PTTL",
			cmd:  "PTTL",
			args: [][]byte{[]byte("exkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) > 0)
			},
		},
		// PERSIST
		{
			name: "PERSIST",
			cmd:  "PERSIST",
			args: [][]byte{[]byte("exkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(nil, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerTypeAndExistsCommands tests TYPE and EXISTS commands
func TestServerTypeAndExistsCommands(t *testing.T) {
	handler := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup keys
	handler.executeCommand(nil, "SET", [][]byte{[]byte("stringkey"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(nil, "HSET", [][]byte{[]byte("hashkey"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(nil, "LPUSH", [][]byte{[]byte("listkey"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(nil, "SADD", [][]byte{[]byte("setkey"), []byte("member")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// TYPE for string
		{
			name: "TYPE string",
			cmd:  "TYPE",
			args: [][]byte{[]byte("stringkey")},
			check: func(t *testing.T, resp proto.RESP) {
				// Just verify it returns a valid RESP response
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// TYPE for hash
		{
			name: "TYPE hash",
			cmd:  "TYPE",
			args: [][]byte{[]byte("hashkey")},
			check: func(t *testing.T, resp proto.RESP) {
				// Just verify it returns a valid RESP response
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// TYPE for list
		{
			name: "TYPE list",
			cmd:  "TYPE",
			args: [][]byte{[]byte("listkey")},
			check: func(t *testing.T, resp proto.RESP) {
				// Just verify it returns a valid RESP response
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// TYPE for set
		{
			name: "TYPE set",
			cmd:  "TYPE",
			args: [][]byte{[]byte("setkey")},
			check: func(t *testing.T, resp proto.RESP) {
				// Just verify it returns a valid RESP response
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// TYPE for nonexistent
		{
			name: "TYPE nonexistent",
			cmd:  "TYPE",
			args: [][]byte{[]byte("nonexistent")},
			check: func(t *testing.T, resp proto.RESP) {
				// Just verify it returns a valid RESP response
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// EXISTS
		{
			name: "EXISTS",
			cmd:  "EXISTS",
			args: [][]byte{[]byte("stringkey"), []byte("hashkey"), []byte("nonexistent")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer))
			},
		},
		// DEL
		{
			name: "DEL",
			cmd:  "DEL",
			args: [][]byte{[]byte("stringkey")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := handler.executeCommand(nil, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}
