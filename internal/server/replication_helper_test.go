package server

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestIsWriteCommand(t *testing.T) {
	tests := []struct {
		cmd      string
		expected bool
	}{
		// String commands
		{"SET", true},
		{"SETEX", true},
		{"PSETEX", true},
		{"SETNX", true},
		{"GETSET", true},
		{"MSET", true},
		{"MSETNX", true},
		{"INCR", true},
		{"INCRBY", true},
		{"DECR", true},
		{"DECRBY", true},
		{"INCRBYFLOAT", true},
		{"APPEND", true},
		{"SETRANGE", true},
		{"GET", false},

		// Key commands
		{"DEL", true},
		{"EXPIRE", true},
		{"EXPIREAT", true},
		{"PEXPIRE", true},
		{"PEXPIREAT", true},
		{"PERSIST", true},
		{"RENAME", true},
		{"RENAMENX", true},
		{"EXISTS", false},
		{"TYPE", false},
		{"TTL", false},
		{"PTTL", false},

		// List commands
		{"LPUSH", true},
		{"RPUSH", true},
		{"LPOP", true},
		{"RPOP", true},
		{"LSET", true},
		{"LTRIM", true},
		{"LINSERT", true},
		{"LREM", true},
		{"RPOPLPUSH", true},
		{"LPUSHX", true},
		{"RPUSHX", true},
		{"LRANGE", false},
		{"LLEN", false},

		// Hash commands
		{"HSET", true},
		{"HDEL", true},
		{"HMSET", true},
		{"HSETNX", true},
		{"HINCRBY", true},
		{"HINCRBYFLOAT", true},
		{"HGET", false},
		{"HMGET", false},
		{"HGETALL", false},
		{"HLEN", false},

		// Set commands
		{"SADD", true},
		{"SREM", true},
		{"SPOP", true},
		{"SMOVE", true},
		{"SINTERSTORE", true},
		{"SUNIONSTORE", true},
		{"SDIFFSTORE", true},
		{"SMEMBERS", false},
		{"SISMEMBER", false},
		{"SCARD", false},

		// Sorted set commands
		{"ZADD", true},
		{"ZREM", true},
		{"ZINCRBY", true},
		{"ZRANGE", false},
		{"ZRANK", false},
		{"ZSCORE", false},

		// GEO commands
		{"GEOADD", true},
		{"GEOSEARCHSTORE", true},
		{"GEOPOS", false},
		{"GEODIST", false},

		// Stream commands
		{"XADD", true},
		{"XDEL", true},
		{"XACK", true},
		{"XCLAIM", true},
		{"XGROUP", true},
		{"XTRIM", true},
		{"XRANGE", false},
		{"XREAD", false},

		// Unknown commands
		{"UNKNOWN", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.cmd, func(t *testing.T) {
			result := isWriteCommand(tt.cmd)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsWriteCommand_CaseSensitivity(t *testing.T) {
	// isWriteCommand should be case sensitive
	assert.False(t, isWriteCommand("set"))
	assert.False(t, isWriteCommand("Set"))
	assert.False(t, isWriteCommand("get"))
}
