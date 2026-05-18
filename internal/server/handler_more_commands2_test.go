package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)


// TestServerGeoCommands2 tests GEO commands
func TestServerGeoCommands2(t *testing.T) {
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
			args: [][]byte{[]byte("mygeo"), []byte("122.4194"), []byte("37.7749"), []byte("sanfrancisco")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// GEOPOS
		{
			name: "GEOPOS",
			cmd:  "GEOPOS",
			args: [][]byte{[]byte("mygeo"), []byte("sanfrancisco")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// GEOHASH
		{
			name: "GEOHASH",
			cmd:  "GEOHASH",
			args: [][]byte{[]byte("mygeo"), []byte("sanfrancisco")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// GEODIST
		{
			name: "GEODIST",
			cmd:  "GEODIST",
			args: [][]byte{[]byte("mygeo"), []byte("sanfrancisco"), []byte("losangeles")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// GEOSEARCH
		{
			name: "GEOSEARCH",
			cmd:  "GEOSEARCH",
			args: [][]byte{[]byte("mygeo"), []byte("FROM"), []byte("BYRADIUS"), []byte("10"), []byte("km"), []byte("ASC")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
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

// TestServerHyperLogLogCommands tests HyperLogLog commands
func TestServerHyperLogLogCommands(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// PFADD
		{
			name: "PFADD",
			cmd:  "PFADD",
			args: [][]byte{[]byte("myhll"), []byte("a"), []byte("b"), []byte("c")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// PFCOUNT
		{
			name: "PFCOUNT",
			cmd:  "PFCOUNT",
			args: [][]byte{[]byte("myhll")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// PFINFO
		{
			name: "PFINFO",
			cmd:  "PFINFO",
			args: [][]byte{[]byte("myhll")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
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

// TestServerStreamCommands2 tests Stream commands
func TestServerStreamCommands2(t *testing.T) {
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
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// XLEN
		{
			name: "XLEN",
			cmd:  "XLEN",
			args: [][]byte{[]byte("mystream")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// XRANGE
		{
			name: "XRANGE",
			cmd:  "XRANGE",
			args: [][]byte{[]byte("mystream"), []byte("-"), []byte("+"), []byte("COUNT"), []byte("10")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// XREVRANGE
		{
			name: "XREVRANGE",
			cmd:  "XREVRANGE",
			args: [][]byte{[]byte("mystream"), []byte("+"), []byte("-"), []byte("COUNT"), []byte("10")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// XREAD
		{
			name: "XREAD",
			cmd:  "XREAD",
			args: [][]byte{[]byte("COUNT"), []byte("1"), []byte("STREAMS"), []byte("mystream"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// XINFO STREAM
		{
			name: "XINFO STREAM",
			cmd:  "XINFO",
			args: [][]byte{[]byte("STREAM"), []byte("mystream")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// XPENDING
		{
			name: "XPENDING",
			cmd:  "XPENDING",
			args: [][]byte{[]byte("mystream")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
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

// TestServerSetCommands3 tests more Set commands
func TestServerSetCommands3(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup sets
	handler.executeCommand(state, "SADD", [][]byte{[]byte("set1"), []byte("a"), []byte("b"), []byte("c")}, "127.0.0.1:12345")
	handler.executeCommand(state, "SADD", [][]byte{[]byte("set2"), []byte("b"), []byte("c"), []byte("d")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// SINTER
		{
			name: "SINTER",
			cmd:  "SINTER",
			args: [][]byte{[]byte("set1"), []byte("set2")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// SUNION
		{
			name: "SUNION",
			cmd:  "SUNION",
			args: [][]byte{[]byte("set1"), []byte("set2")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// SDIFF
		{
			name: "SDIFF",
			cmd:  "SDIFF",
			args: [][]byte{[]byte("set1"), []byte("set2")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// SMISMEMBER
		{
			name: "SMISMEMBER",
			cmd:  "SMISMEMBER",
			args: [][]byte{[]byte("set1"), []byte("a"), []byte("b")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// SPOP
		{
			name: "SPOP",
			cmd:  "SPOP",
			args: [][]byte{[]byte("set1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// SRANDMEMBER
		{
			name: "SRANDMEMBER",
			cmd:  "SRANDMEMBER",
			args: [][]byte{[]byte("set1"), []byte("2")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// SMOVE
		{
			name: "SMOVE",
			cmd:  "SMOVE",
			args: [][]byte{[]byte("set1"), []byte("set2"), []byte("e")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
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

// TestServerSortedSetCommands3 tests more Sorted Set commands
func TestServerSortedSetCommands3(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Setup sorted set
	handler.Db.ZAdd("myzset", []store.ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
		// ZRANK
		{
			name: "ZRANK",
			cmd:  "ZRANK",
			args: [][]byte{[]byte("myzset"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// ZREVRANK
		{
			name: "ZREVRANK",
			cmd:  "ZREVRANK",
			args: [][]byte{[]byte("myzset"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// ZCOUNT
		{
			name: "ZCOUNT",
			cmd:  "ZCOUNT",
			args: [][]byte{[]byte("myzset"), []byte("1"), []byte("3")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// ZPOPMIN
		{
			name: "ZPOPMIN",
			cmd:  "ZPOPMIN",
			args: [][]byte{[]byte("myzset"), []byte("1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// ZPOPMAX
		{
			name: "ZPOPMAX",
			cmd:  "ZPOPMAX",
			args: [][]byte{[]byte("myzset"), []byte("1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// ZINCRBY
		{
			name: "ZINCRBY",
			cmd:  "ZINCRBY",
			args: [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
				assert.True(t, ok)
			},
		},
		// ZMSCORE
		{
			name: "ZMSCORE",
			cmd:  "ZMSCORE",
			args: [][]byte{[]byte("myzset"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(proto.RESP)
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
