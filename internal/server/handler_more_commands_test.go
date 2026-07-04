package server

import (
	"strconv"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// TestServerMoreStringCommands tests more string commands
func TestServerMoreStringCommands(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup initial values
	handler.executeCommand(state, "SET", [][]byte{[]byte("strkey"), []byte("hello")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SET", [][]byte{[]byte("rangekey"), []byte("HelloWorld")}, "127.0.0.1:12345")

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

// TestServerKeyExpirationCommands tests key expiration commands
func TestServerKeyExpirationCommands(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup key
	handler.executeCommand(state, "SET", [][]byte{[]byte("exkey"), []byte("value")}, "127.0.0.1:12345")

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
			resp := handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestServerTypeAndExistsCommands tests TYPE and EXISTS commands
func TestServerTypeAndExistsCommands(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup keys
	handler.executeCommand(state, "SET", [][]byte{[]byte("stringkey"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(state, "HSET", [][]byte{[]byte("hashkey"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(state, "LPUSH", [][]byte{[]byte("listkey"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SADD", [][]byte{[]byte("setkey"), []byte("member")}, "127.0.0.1:12345")

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
				ss, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "string", string(*ss))
			},
		},
		// TYPE for hash
		{
			name: "TYPE hash",
			cmd:  "TYPE",
			args: [][]byte{[]byte("hashkey")},
			check: func(t *testing.T, resp proto.RESP) {
				ss, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "hash", string(*ss))
			},
		},
		// TYPE for list
		{
			name: "TYPE list",
			cmd:  "TYPE",
			args: [][]byte{[]byte("listkey")},
			check: func(t *testing.T, resp proto.RESP) {
				ss, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "list", string(*ss))
			},
		},
		// TYPE for set
		{
			name: "TYPE set",
			cmd:  "TYPE",
			args: [][]byte{[]byte("setkey")},
			check: func(t *testing.T, resp proto.RESP) {
				ss, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "set", string(*ss))
			},
		},
		// TYPE for nonexistent
		{
			name: "TYPE nonexistent",
			cmd:  "TYPE",
			args: [][]byte{[]byte("nonexistent")},
			check: func(t *testing.T, resp proto.RESP) {
				ss, ok := resp.(*proto.SimpleString)
				assert.True(t, ok)
				assert.Equal(t, "none", string(*ss))
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
			resp := handler.executeCommand(state, tt.cmd, tt.args, "127.0.0.1:12345")
			tt.check(t, resp)
		})
	}
}

// TestSETModifiers 验证 SET 修饰符的实际效果（TTL、NX、XX、KEEPTTL）
func TestSETModifiers(t *testing.T) {

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	t.Run("EX sets TTL", func(t *testing.T) {
		resp := handler.executeCommand(state, "SET", [][]byte{[]byte("s:ex"), []byte("v"), []byte("EX"), []byte("100")}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)

		ttl, err := handler.Db.TTL("s:ex")
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 100)
	})

	t.Run("PX sets TTL", func(t *testing.T) {
		resp := handler.executeCommand(state, "SET", [][]byte{[]byte("s:px"), []byte("v"), []byte("PX"), []byte("50000")}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)

		ttl, err := handler.Db.TTL("s:px")
		assert.NoError(t, err)
		assert.True(t, ttl > 0)
	})

	t.Run("NX sets only if missing", func(t *testing.T) {
		resp := handler.executeCommand(state, "SET", [][]byte{[]byte("s:nx1"), []byte("v"), []byte("NX")}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)

		resp = handler.executeCommand(state, "SET", [][]byte{[]byte("s:nx1"), []byte("v2"), []byte("NX")}, "127.0.0.1:12345")
		// RESP2: nil BulkString ($-1); RESP3: Null (_) — both acceptable
		switch v := resp.(type) {
		case *proto.Null:
			// RESP3 Null is acceptable
		case *proto.BulkString:
			if v != nil && len(*v) > 0 {
				t.Errorf("expected nil response for NX on existing key, got %T: %v", resp, resp)
			}
		default:
			t.Errorf("expected nil response for NX on existing key, got %T: %v", resp, resp)
		}

		val, err := handler.Db.Get("s:nx1")
		assert.NoError(t, err)
		assert.Equal(t, "v", val)
	})

	t.Run("XX sets only if exists", func(t *testing.T) {
		resp := handler.executeCommand(state, "SET", [][]byte{[]byte("s:xx1"), []byte("v"), []byte("XX")}, "127.0.0.1:12345")
		// RESP2: nil BulkString ($-1); RESP3: Null (_) — both acceptable
		switch v := resp.(type) {
		case *proto.Null:
			// RESP3 Null is acceptable
		case *proto.BulkString:
			if v != nil && len(*v) > 0 {
				t.Errorf("expected nil response for XX on missing key, got %T: %v", resp, resp)
			}
		default:
			t.Errorf("expected nil response for XX on missing key, got %T: %v", resp, resp)
		}

		handler.executeCommand(state, "SET", [][]byte{[]byte("s:xx1"), []byte("orig")}, "127.0.0.1:12345")

		resp = handler.executeCommand(state, "SET", [][]byte{[]byte("s:xx1"), []byte("newval"), []byte("XX")}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)

		val, err := handler.Db.Get("s:xx1")
		assert.NoError(t, err)
		assert.Equal(t, "newval", val)
	})

	t.Run("KEEPTTL retains existing TTL", func(t *testing.T) {
		resp := handler.executeCommand(state, "SET", [][]byte{[]byte("s:kt"), []byte("first"), []byte("EX"), []byte("200")}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)

		ttlBefore, err := handler.Db.TTL("s:kt")
		assert.NoError(t, err)
		assert.True(t, ttlBefore > 0)

		resp = handler.executeCommand(state, "SET", [][]byte{[]byte("s:kt"), []byte("second"), []byte("KEEPTTL")}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)

		val, err := handler.Db.Get("s:kt")
		assert.NoError(t, err)
		assert.Equal(t, "second", val)

		ttlAfter, err := handler.Db.TTL("s:kt")
		assert.NoError(t, err)
		if ttlAfter <= 0 {
			t.Errorf("KEEPTTL should preserve TTL, got %d", ttlAfter)
		}
	})

	t.Run("GET returns old value", func(t *testing.T) {
		handler.executeCommand(state, "SET", [][]byte{[]byte("s:get"), []byte("oldval")}, "127.0.0.1:12345")

		resp := handler.executeCommand(state, "SET", [][]byte{[]byte("s:get"), []byte("newval"), []byte("GET")}, "127.0.0.1:12345")
		bs, ok := resp.(*proto.BulkString)
		assert.True(t, ok)
		if bs != nil {
			assert.Equal(t, "oldval", string(*bs))
		}
	})

	t.Run("EXAT sets absolute expiry", func(t *testing.T) {
		future := time.Now().Unix() + 300
		resp := handler.executeCommand(state, "SET", [][]byte{[]byte("s:exat"), []byte("v"), []byte("EXAT"), []byte(strconv.FormatInt(future, 10))}, "127.0.0.1:12345")
		assert.Equal(t, proto.OK, resp)

		ttl, err := handler.Db.TTL("s:exat")
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 300)
	})
}
