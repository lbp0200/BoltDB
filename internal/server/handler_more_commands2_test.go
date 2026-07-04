package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestServerGeoCommands2 tests GEO commands
func TestServerGeoCommands2(t *testing.T) {

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
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// GEOPOS
		{
			name: "GEOPOS",
			cmd:  "GEOPOS",
			args: [][]byte{[]byte("mygeo"), []byte("sanfrancisco")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
				assert.Equal(t, 1, len(arr.Elems))
			},
		},
		// GEOHASH
		{
			name: "GEOHASH",
			cmd:  "GEOHASH",
			args: [][]byte{[]byte("mygeo"), []byte("sanfrancisco")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 1, len(arr.Args))
			},
		},
		// GEODIST
		{
			name: "GEODIST",
			cmd:  "GEODIST",
			args: [][]byte{[]byte("mygeo"), []byte("sanfrancisco"), []byte("sanfrancisco")}, // same city = 0 dist
			check: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				assert.True(t, len(*bs) > 0)
			},
		},
		// GEOSEARCH
		{
			name: "GEOSEARCH",
			cmd:  "GEOSEARCH",
			args: [][]byte{[]byte("mygeo"), []byte("FROMLONLAT"), []byte("122.41"), []byte("37.77"), []byte("BYRADIUS"), []byte("100"), []byte("km")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) > 0) // sanfrancisco within 100km
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
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(1), int64(*integer))
			},
		},
		// PFCOUNT
		{
			name: "PFCOUNT",
			cmd:  "PFCOUNT",
			args: [][]byte{[]byte("myhll")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.True(t, int64(*integer) >= 0)
			},
		},
		// PFINFO
		{
			name: "PFINFO",
			cmd:  "PFINFO",
			args: [][]byte{[]byte("myhll")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) >= 2)
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

	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// Create a stream entry and consumer group for XPENDING test
	handler.executeCommand(state, "XADD", [][]byte{[]byte("mystream"), []byte("*"), []byte("field"), []byte("value")}, "127.0.0.1:12345")
	handler.executeCommand(state, "XGROUP", [][]byte{[]byte("CREATE"), []byte("mystream"), []byte("mygroup"), []byte("0")}, "127.0.0.1:12345")

	tests := []struct {
		name  string
		cmd   string
		args  [][]byte
		check func(t *testing.T, resp proto.RESP)
	}{
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
			args: [][]byte{[]byte("mystream"), []byte("-"), []byte("+")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
			},
		},
		// XREVRANGE
		{
			name: "XREVRANGE",
			cmd:  "XREVRANGE",
			args: [][]byte{[]byte("mystream"), []byte("+"), []byte("-")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
			},
		},
		// XREAD
		{
			name: "XREAD",
			cmd:  "XREAD",
			args: [][]byte{[]byte("STREAMS"), []byte("mystream"), []byte("0")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
			},
		},
		// XINFO STREAM
		{
			name: "XINFO STREAM",
			cmd:  "XINFO",
			args: [][]byte{[]byte("STREAM"), []byte("mystream")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.True(t, len(arr.Args) >= 2)
			},
		},
		// XPENDING
		{
			name: "XPENDING",
			cmd:  "XPENDING",
			args: [][]byte{[]byte("mystream"), []byte("mygroup")},
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

// TestServerSetCommands3 tests more Set commands
func TestServerSetCommands3(t *testing.T) {

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
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args)) // intersection of {a,b,c} and {b,c,d} = {b,c}
			},
		},
		// SUNION
		{
			name: "SUNION",
			cmd:  "SUNION",
			args: [][]byte{[]byte("set1"), []byte("set2")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 4, len(arr.Args)) // {a,b,c,d}
			},
		},
		// SDIFF
		{
			name: "SDIFF",
			cmd:  "SDIFF",
			args: [][]byte{[]byte("set1"), []byte("set2")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 1, len(arr.Args)) // {a}
			},
		},
		// SMISMEMBER
		{
			name: "SMISMEMBER",
			cmd:  "SMISMEMBER",
			args: [][]byte{[]byte("set1"), []byte("a"), []byte("b")},
			check: func(t *testing.T, resp proto.RESP) {
				nested, ok := resp.(*proto.NestedArray)
				assert.True(t, ok)
				assert.Equal(t, 2, len(nested.Elems))
			},
		},
		// SPOP
		{
			name: "SPOP",
			cmd:  "SPOP",
			args: [][]byte{[]byte("set1")},
			check: func(t *testing.T, resp proto.RESP) {
				_, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
			},
		},
		// SRANDMEMBER
		{
			name: "SRANDMEMBER",
			cmd:  "SRANDMEMBER",
			args: [][]byte{[]byte("set1"), []byte("2")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args))
			},
		},
		// SMOVE
		{
			name: "SMOVE",
			cmd:  "SMOVE",
			args: [][]byte{[]byte("set1"), []byte("set2"), []byte("e")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(0), int64(*integer)) // 'e' not in set1
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
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(0), int64(*integer)) // member1 is first
			},
		},
		// ZREVRANK
		{
			name: "ZREVRANK",
			cmd:  "ZREVRANK",
			args: [][]byte{[]byte("myzset"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(2), int64(*integer)) // member1 is last in reverse
			},
		},
		// ZCOUNT
		{
			name: "ZCOUNT",
			cmd:  "ZCOUNT",
			args: [][]byte{[]byte("myzset"), []byte("1"), []byte("3")},
			check: func(t *testing.T, resp proto.RESP) {
				integer, ok := resp.(*proto.Integer)
				assert.True(t, ok)
				assert.Equal(t, int64(3), int64(*integer)) // all 3 members in [1,3]
			},
		},
		// ZPOPMIN
		{
			name: "ZPOPMIN",
			cmd:  "ZPOPMIN",
			args: [][]byte{[]byte("myzset"), []byte("1")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args)) // [member, score]
			},
		},
		// ZPOPMAX
		{
			name: "ZPOPMAX",
			cmd:  "ZPOPMAX",
			args: [][]byte{[]byte("myzset"), []byte("1")},
			check: func(t *testing.T, resp proto.RESP) {
				arr, ok := resp.(*proto.Array)
				assert.True(t, ok)
				assert.Equal(t, 2, len(arr.Args)) // [member, score]
			},
		},
		// ZINCRBY
		{
			name: "ZINCRBY",
			cmd:  "ZINCRBY",
			args: [][]byte{[]byte("myzset"), []byte("1"), []byte("member1")},
			check: func(t *testing.T, resp proto.RESP) {
				bs, ok := resp.(*proto.BulkString)
				assert.True(t, ok)
				// member1 was popped by ZPOPMIN, so ZINCRBY creates it with score 1
				assert.Equal(t, "1", string(*bs))
			},
		},
		// ZMSCORE
		{
			name: "ZMSCORE",
			cmd:  "ZMSCORE",
			args: [][]byte{[]byte("myzset"), []byte("member1")},
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
