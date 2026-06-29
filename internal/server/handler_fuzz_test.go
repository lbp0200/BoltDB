package server

import (
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/stretchr/testify/assert"
)

// truncateFuzz limits fuzz input size.
const maxServerFuzzLen = 512

func truncFuzz(s string) string {
	if len(s) > maxServerFuzzLen {
		return s[:maxServerFuzzLen]
	}
	return s
}

// filterEmptyArgs builds a [][]byte from variadic strings, skipping empty ones.
func filterEmptyArgs(args ...string) [][]byte {
	var result [][]byte
	for _, a := range args {
		if a != "" {
			result = append(result, []byte(a))
		}
	}
	return result
}

// assertNotNil is a lightweight fuzz helper: verify the command returned a
// non-nil RESP value (i.e. didn't panic or produce an unexpected nil).
func assertNotNil(t *testing.T, resp proto.RESP) {
	t.Helper()
	assert.NotNil(t, resp)
}

// FuzzCommandDispatch fuzzes the server command handler with random command names.
// Tests for panics.
func FuzzCommandDispatch(f *testing.F) {
	f.Add("PING")
	f.Add("SET")
	f.Add("GET")
	f.Add("HSET")
	f.Add("SADD")
	f.Add("LPUSH")
	f.Add("ZADD")
	f.Add("")
	f.Add("UNKNOWN")
	f.Add("DEL")
	f.Add("INCR")
	f.Add("APPEND")

	f.Fuzz(func(t *testing.T, cmd string) {
		cmd = truncFuzz(strings.ToUpper(cmd))

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()

		resp := handler.executeCommand(state, cmd, nil, "127.0.0.1:12345")
		assertNotNil(t, resp)
	})
}

// FuzzKnownCmdRandomArgs fuzzes known commands with random argument names and values.
func FuzzKnownCmdRandomArgs(f *testing.F) {
	f.Add("SET", "key", "value")
	f.Add("GET", "key", "")
	f.Add("HSET", "hash", "field")
	f.Add("SADD", "set", "member")
	f.Add("LPUSH", "list", "elem")
	f.Add("ZADD", "zset", "member")
	f.Add("DEL", "key", "")
	f.Add("EXPIRE", "key", "")
	f.Add("APPEND", "key", "value")
	f.Add("INCR", "key", "")
	f.Add("LRANGE", "list", "0")
	f.Add("ZRANGE", "zset", "0")
	f.Add("SMEMBERS", "set", "")
	f.Add("HGETALL", "hash", "")

	f.Fuzz(func(t *testing.T, cmd, arg1, arg2 string) {
		cmd = truncFuzz(strings.ToUpper(cmd))
		arg1 = truncFuzz(arg1)
		arg2 = truncFuzz(arg2)

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()

		resp := handler.executeCommand(state, cmd, [][]byte{[]byte(arg1), []byte(arg2)}, "127.0.0.1:12345")
		assertNotNil(t, resp)

		if arg1 != "" {
			getResp := handler.executeCommand(state, "GET", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
			assertNotNil(t, getResp)

			typeResp := handler.executeCommand(state, "TYPE", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
			assertNotNil(t, typeResp)
			_, ok := typeResp.(*proto.SimpleString)
			assert.True(t, ok, "TYPE must return SimpleString, got %T", typeResp)

			existsResp := handler.executeCommand(state, "EXISTS", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
			assertNotNil(t, existsResp)
			_, isInt := existsResp.(*proto.Integer)
			assert.True(t, isInt, "EXISTS must return Integer, got %T", existsResp)

			delResp := handler.executeCommand(state, "DEL", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
			assertNotNil(t, delResp)
		}
	})
}

// FuzzCommandPipeline fuzzes 3-command sequences to test state corruption.
func FuzzCommandPipeline(f *testing.F) {
	f.Add("SET", "k1", "v1", "GET", "k1", "DEL", "k1", "", "")
	f.Add("PING", "", "", "PING", "", "", "DBSIZE", "", "")
	f.Add("HSET", "h", "f", "HGET", "h", "f", "HDEL", "h", "f")
	f.Add("SADD", "s", "a", "SISMEMBER", "s", "a", "SREM", "s", "a")
	f.Add("LPUSH", "l", "e", "LLEN", "l", "", "LPOP", "l", "")

	f.Fuzz(func(t *testing.T, cmd1, arg1a, arg1b, cmd2, arg2a, arg2b, cmd3, arg3a, arg3b string) {
		cmd1 = truncFuzz(strings.ToUpper(cmd1))
		cmd2 = truncFuzz(strings.ToUpper(cmd2))
		cmd3 = truncFuzz(strings.ToUpper(cmd3))

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()

		addr := "127.0.0.1:12345"

		resp1 := handler.executeCommand(state, cmd1, filterEmptyArgs(arg1a, arg1b), addr)
		assertNotNil(t, resp1)
		resp2 := handler.executeCommand(state, cmd2, filterEmptyArgs(arg2a, arg2b), addr)
		assertNotNil(t, resp2)
		resp3 := handler.executeCommand(state, cmd3, filterEmptyArgs(arg3a, arg3b), addr)
		assertNotNil(t, resp3)
		flushResp := handler.executeCommand(state, "FLUSHDB", nil, addr)
		assertNotNil(t, flushResp)
		_, isOK := flushResp.(*proto.SimpleString)
		assert.True(t, isOK, "FLUSHDB must return SimpleString, got %T", flushResp)
	})
}

// FuzzServerTypeConfusion tests the same key used as different types.
func FuzzServerTypeConfusion(f *testing.F) {
	f.Add("shared-key", "str-val", "hash-field", "set-member", "list-elem")
	f.Add("k", "v", "f", "m", "e")
	f.Add("t", "1", "2", "3", "4")

	f.Fuzz(func(t *testing.T, key, strVal, field, member, listElem string) {
		key = truncFuzz(key)
		strVal = truncFuzz(strVal)
		field = truncFuzz(field)
		member = truncFuzz(member)
		listElem = truncFuzz(listElem)

		if key == "" {
			return
		}

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		// Phase 1: String
		setResp := handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(strVal)}, addr)
		assertNotNil(t, setResp)
		_, isOK := setResp.(*proto.SimpleString)
		assert.True(t, isOK, "SET must return SimpleString, got %T", setResp)

		// Phase 2: Hash on string key
		assertNotNil(t, handler.executeCommand(state, "HSET", [][]byte{[]byte(key), []byte(field), []byte(strVal)}, addr))
		assertNotNil(t, handler.executeCommand(state, "HGET", [][]byte{[]byte(key), []byte(field)}, addr))

		// Phase 3: List on string key
		assertNotNil(t, handler.executeCommand(state, "LPUSH", [][]byte{[]byte(key), []byte(listElem)}, addr))
		assertNotNil(t, handler.executeCommand(state, "LLEN", [][]byte{[]byte(key)}, addr))

		// Phase 4: Set on string key
		assertNotNil(t, handler.executeCommand(state, "SADD", [][]byte{[]byte(key), []byte(member)}, addr))
		assertNotNil(t, handler.executeCommand(state, "SCARD", [][]byte{[]byte(key)}, addr))

		// Phase 5: ZSet on string key
		assertNotNil(t, handler.executeCommand(state, "ZADD", [][]byte{[]byte(key), []byte("1.0"), []byte(member)}, addr))
		assertNotNil(t, handler.executeCommand(state, "ZCARD", [][]byte{[]byte(key)}, addr))

		// Phase 6: Cleanup
		assertNotNil(t, handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr))

		typeResp := handler.executeCommand(state, "TYPE", [][]byte{[]byte(key)}, addr)
		assertNotNil(t, typeResp)
		ss, ok := typeResp.(*proto.SimpleString)
		assert.True(t, ok, "TYPE must return SimpleString, got %T", typeResp)
		if ok {
			assert.Equal(t, "none", string(*ss), "key should be deleted after DEL")
		}

		// Phase 7: Expire/Persist on non-existent key
		assertNotNil(t, handler.executeCommand(state, "EXPIRE", [][]byte{[]byte(key), []byte("100")}, addr))
		assertNotNil(t, handler.executeCommand(state, "TTL", [][]byte{[]byte(key)}, addr))
		assertNotNil(t, handler.executeCommand(state, "PERSIST", [][]byte{[]byte(key)}, addr))
	})
}

// FuzzSpecialCharKeys fuzzes keys/values with special characters.
func FuzzSpecialCharKeys(f *testing.F) {
	f.Add("key\x00null", "val\x00null")
	f.Add("key\r\nCRLF", "val\r\nCRLF")
	f.Add("中文-key", "中文-val")
	f.Add("key with spaces", "val with spaces")

	f.Fuzz(func(t *testing.T, key, value string) {
		key = truncFuzz(key)
		value = truncFuzz(value)

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		setResp := handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(value)}, addr)
		assertNotNil(t, setResp)

		getResp := handler.executeCommand(state, "GET", [][]byte{[]byte(key)}, addr)
		assertNotNil(t, getResp)
		bs, ok := getResp.(*proto.BulkString)
		if ok && *bs != nil {
			assert.Equal(t, value, string(*bs), "GET must return the value that was SET")
		}

		assertNotNil(t, handler.executeCommand(state, "HSET", [][]byte{[]byte(key), []byte("f"), []byte(value)}, addr))
		assertNotNil(t, handler.executeCommand(state, "SADD", [][]byte{[]byte(key), []byte(value)}, addr))
		assertNotNil(t, handler.executeCommand(state, "LPUSH", [][]byte{[]byte(key), []byte(value)}, addr))
		assertNotNil(t, handler.executeCommand(state, "ZADD", [][]byte{[]byte(key), []byte("1.0"), []byte(value)}, addr))
		assertNotNil(t, handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr))
	})
}

// FuzzTransactionOps fuzzes MULTI/DISCARD transactions (no EXEC to avoid ctx panics).
func FuzzTransactionOps(f *testing.F) {
	f.Add("SET", "k", "v")
	f.Add("INCR", "counter", "")
	f.Add("HSET", "h", "f")

	f.Fuzz(func(t *testing.T, cmd1, arg1a, arg1b string) {
		cmd1 = truncFuzz(strings.ToUpper(cmd1))

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		// MULTI + queue + DISCARD (safe, no EXEC)
		multiResp := handler.executeCommand(state, "MULTI", nil, addr)
		assertNotNil(t, multiResp)
		ss, ok := multiResp.(*proto.SimpleString)
		assert.True(t, ok, "MULTI must return SimpleString, got %T", multiResp)
		if ok {
			assert.Equal(t, "OK", string(*ss))
		}

		queueResp := handler.executeCommand(state, cmd1, filterEmptyArgs(arg1a, arg1b), addr)
		assertNotNil(t, queueResp)
		queuedStr, qok := queueResp.(*proto.SimpleString)
		assert.True(t, qok, "queued command must return SimpleString QUEUED, got %T", queueResp)
		if qok {
			assert.Equal(t, "QUEUED", string(*queuedStr))
		}

		discardResp := handler.executeCommand(state, "DISCARD", nil, addr)
		assertNotNil(t, discardResp)
		dss, dok := discardResp.(*proto.SimpleString)
		assert.True(t, dok, "DISCARD must return SimpleString, got %T", discardResp)
		if dok {
			assert.Equal(t, "OK", string(*dss))
		}
	})
}

// FuzzInfoCommand fuzzes INFO with random section names.
func FuzzInfoCommand(f *testing.F) {
	f.Add("all")
	f.Add("server")
	f.Add("clients")
	f.Add("memory")
	f.Add("stats")
	f.Add("replication")
	f.Add("persistence")
	f.Add("cpu")
	f.Add("")
	f.Add("nonexistent")

	f.Fuzz(func(t *testing.T, section string) {
		section = truncFuzz(section)
		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		resp := handler.executeCommand(state, "INFO", [][]byte{[]byte(section)}, "127.0.0.1:12345")
		assertNotNil(t, resp)
		_, ok := resp.(*proto.BulkString)
		assert.True(t, ok, "INFO must return BulkString, got %T", resp)
	})
}

// FuzzSlowlogFuzz fuzzes SLOWLOG with random arguments.
func FuzzSlowlogFuzz(f *testing.F) {
	f.Add("GET", "10")
	f.Add("LEN", "")
	f.Add("RESET", "")
	f.Add("GET", "0")

	f.Fuzz(func(t *testing.T, subcmd, arg string) {
		subcmd = truncFuzz(strings.ToUpper(subcmd))
		arg = truncFuzz(arg)
		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		args := [][]byte{[]byte(subcmd)}
		if arg != "" {
			args = append(args, []byte(arg))
		}
		resp := handler.executeCommand(state, "SLOWLOG", args, "127.0.0.1:12345")
		assertNotNil(t, resp)
	})
}

// FuzzClusterCommands fuzzes CLUSTER with random subcommands.
func FuzzClusterCommands(f *testing.F) {
	f.Add("INFO")
	f.Add("MYID")
	f.Add("NODES")
	f.Add("KEYSLOT")
	f.Add("COUNTKEYSINSLOT")
	f.Add("GETKEYSINSLOT")

	f.Fuzz(func(t *testing.T, subcmd string) {
		subcmd = truncFuzz(strings.ToUpper(subcmd))
		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		resp := handler.executeCommand(state, "CLUSTER", [][]byte{[]byte(subcmd)}, "127.0.0.1:12345")
		assertNotNil(t, resp)
	})
}

// FuzzStreamCommands fuzzes stream operations.
func FuzzStreamCommands(f *testing.F) {
	f.Add("mystream", "*", "field1", "value1")
	f.Add("mystream", "1-0", "f", "v")

	f.Fuzz(func(t *testing.T, stream, id, field, value string) {
		stream = truncFuzz(stream)
		id = truncFuzz(id)
		field = truncFuzz(field)
		value = truncFuzz(value)

		if stream == "" || field == "" || value == "" {
			return
		}

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		xaddResp := handler.executeCommand(state, "XADD", [][]byte{[]byte(stream), []byte(id), []byte(field), []byte(value)}, addr)
		assertNotNil(t, xaddResp)
		// XADD returns a BulkString with the entry ID
		bs, ok := xaddResp.(*proto.BulkString)
		assert.True(t, ok, "XADD must return BulkString, got %T", xaddResp)
		if ok {
			assert.NotEmpty(t, string(*bs), "XADD must return a non-empty entry ID")
		}

		xlenResp := handler.executeCommand(state, "XLEN", [][]byte{[]byte(stream)}, addr)
		assertNotNil(t, xlenResp)
		_, isInt := xlenResp.(*proto.Integer)
		assert.True(t, isInt, "XLEN must return Integer, got %T", xlenResp)

		assertNotNil(t, handler.executeCommand(state, "XRANGE", [][]byte{[]byte(stream), []byte("-"), []byte("+")}, addr))
		assertNotNil(t, handler.executeCommand(state, "XREVRANGE", [][]byte{[]byte(stream), []byte("+"), []byte("-")}, addr))
		assertNotNil(t, handler.executeCommand(state, "XINFO", [][]byte{[]byte("STREAM"), []byte(stream)}, addr))
		assertNotNil(t, handler.executeCommand(state, "DEL", [][]byte{[]byte(stream)}, addr))
	})
}

// FuzzGeospatialCommands fuzzes GEO commands with random coordinates.
func FuzzGeospatialCommands(f *testing.F) {
	f.Add("geokey", "116.40", "39.90", "Beijing")
	f.Add("geokey", "0.0", "0.0", "origin")
	f.Add("geokey", "abc", "def", "member")

	f.Fuzz(func(t *testing.T, key, lon, lat, member string) {
		key = truncFuzz(key)
		lon = truncFuzz(lon)
		lat = truncFuzz(lat)
		member = truncFuzz(member)

		if key == "" || member == "" {
			return
		}

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		geoaddResp := handler.executeCommand(state, "GEOADD", [][]byte{[]byte(key), []byte(lon), []byte(lat), []byte(member)}, addr)
		assertNotNil(t, geoaddResp)
		// GEOADD returns Integer on success, Error on invalid coords
		if _, isErr := geoaddResp.(*proto.Error); !isErr {
			_, isInt := geoaddResp.(*proto.Integer)
			assert.True(t, isInt, "GEOADD must return Integer or Error, got %T", geoaddResp)
		}

		assertNotNil(t, handler.executeCommand(state, "GEOPOS", [][]byte{[]byte(key), []byte(member)}, addr))
		assertNotNil(t, handler.executeCommand(state, "GEODIST", [][]byte{[]byte(key), []byte(member), []byte(member)}, addr))
		assertNotNil(t, handler.executeCommand(state, "GEOHASH", [][]byte{[]byte(key), []byte(member)}, addr))
		assertNotNil(t, handler.executeCommand(state, "GEOSEARCH", [][]byte{
			[]byte(key), []byte("FROMLONLAT"), []byte(lon), []byte(lat),
			[]byte("BYRADIUS"), []byte("100000"), []byte("km"),
		}, addr))
		assertNotNil(t, handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr))
	})
}

// FuzzHyperLogLog fuzzes HLL operations.
func FuzzHyperLogLog(f *testing.F) {
	f.Add("hll1", "a")
	f.Add("hll1", "b")
	f.Add("hll", "x")

	f.Fuzz(func(t *testing.T, key, value string) {
		key = truncFuzz(key)
		value = truncFuzz(value)

		if key == "" {
			return
		}

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		pfaddResp := handler.executeCommand(state, "PFADD", [][]byte{[]byte(key), []byte(value)}, addr)
		assertNotNil(t, pfaddResp)
		_, isInt := pfaddResp.(*proto.Integer)
		assert.True(t, isInt, "PFADD must return Integer, got %T", pfaddResp)

		pfcountResp := handler.executeCommand(state, "PFCOUNT", [][]byte{[]byte(key)}, addr)
		assertNotNil(t, pfcountResp)
		_, isInt2 := pfcountResp.(*proto.Integer)
		assert.True(t, isInt2, "PFCOUNT must return Integer, got %T", pfcountResp)

		key2 := key + "_merge"
		assertNotNil(t, handler.executeCommand(state, "PFADD", [][]byte{[]byte(key2), []byte(value)}, addr))
		assertNotNil(t, handler.executeCommand(state, "PFMERGE", [][]byte{[]byte(key), []byte(key2)}, addr))
		assertNotNil(t, handler.executeCommand(state, "DEL", [][]byte{[]byte(key), []byte(key2)}, addr))
	})
}

// FuzzEmptyAndNilArgs fuzzes commands with empty arguments.
func FuzzEmptyAndNilArgs(f *testing.F) {
	f.Add("SET")
	f.Add("GET")
	f.Add("DEL")
	f.Add("HSET")
	f.Add("SADD")
	f.Add("LPUSH")
	f.Add("ZADD")

	f.Fuzz(func(t *testing.T, cmd string) {
		cmd = truncFuzz(strings.ToUpper(cmd))
		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		// Empty args should produce errors, not panics
		assertNotNil(t, handler.executeCommand(state, cmd, nil, addr))
		assertNotNil(t, handler.executeCommand(state, cmd, [][]byte{[]byte("")}, addr))
		assertNotNil(t, handler.executeCommand(state, cmd, [][]byte{[]byte(""), []byte(""), []byte("")}, addr))
		assertNotNil(t, handler.executeCommand(state, cmd, [][]byte{[]byte(""), []byte("value")}, addr))
		assertNotNil(t, handler.executeCommand(state, cmd, [][]byte{[]byte("key"), []byte("")}, addr))

		manyEmpty := make([][]byte, 64)
		for i := range manyEmpty {
			manyEmpty[i] = []byte("")
		}
		assertNotNil(t, handler.executeCommand(state, cmd, manyEmpty, addr))

		flushResp := handler.executeCommand(state, "FLUSHDB", nil, addr)
		assertNotNil(t, flushResp)
		_, isOK := flushResp.(*proto.SimpleString)
		assert.True(t, isOK, "FLUSHDB must return SimpleString, got %T", flushResp)
	})
}

// FuzzLCSCommand fuzzes the LCS command.
func FuzzLCSCommand(f *testing.F) {
	f.Add("key1", "key2", "LEN")
	f.Add("key1", "key2", "")
	f.Add("key1", "key2", "IDX")
	f.Add("k", "k", "MINMATCHLEN")
	f.Add("k", "k", "WITHMATCHLEN")

	f.Fuzz(func(t *testing.T, key1, key2, modifier string) {
		key1 = truncFuzz(key1)
		key2 = truncFuzz(key2)
		modifier = truncFuzz(strings.ToUpper(modifier))

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		if key1 != "" {
			setResp := handler.executeCommand(state, "SET", [][]byte{[]byte(key1), []byte("hello world")}, addr)
			assertNotNil(t, setResp)
		}
		if key2 != "" {
			setResp := handler.executeCommand(state, "SET", [][]byte{[]byte(key2), []byte("hello there")}, addr)
			assertNotNil(t, setResp)
		}

		args := [][]byte{[]byte(key1), []byte(key2)}
		if modifier != "" {
			args = append(args, []byte(modifier))
		}
		resp := handler.executeCommand(state, "LCS", args, addr)
		assertNotNil(t, resp)
	})
}

// FuzzObjectCommands fuzzes OBJECT subcommands.
func FuzzObjectCommands(f *testing.F) {
	f.Add("REFCOUNT", "mykey")
	f.Add("ENCODING", "mykey")
	f.Add("IDLETIME", "mykey")
	f.Add("HELP", "")
	f.Add("FREQUENCY", "mykey")

	f.Fuzz(func(t *testing.T, subcmd, key string) {
		subcmd = truncFuzz(strings.ToUpper(subcmd))
		key = truncFuzz(key)

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		if key != "" {
			setResp := handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte("value")}, addr)
			assertNotNil(t, setResp)
		}
		resp := handler.executeCommand(state, "OBJECT", [][]byte{[]byte(subcmd), []byte(key)}, addr)
		assertNotNil(t, resp)
	})
}

// FuzzMemoryCommands fuzzes MEMORY subcommands.
func FuzzMemoryCommands(f *testing.F) {
	f.Add("DOCTOR", "")
	f.Add("HELP", "")
	f.Add("USAGE", "mykey")
	f.Add("MALLOC-STATS", "")

	f.Fuzz(func(t *testing.T, subcmd, key string) {
		subcmd = truncFuzz(strings.ToUpper(subcmd))
		key = truncFuzz(key)

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		if key != "" {
			setResp := handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte("value")}, addr)
			assertNotNil(t, setResp)
		}
		args := [][]byte{[]byte(subcmd)}
		if key != "" {
			args = append(args, []byte(key))
		}
		resp := handler.executeCommand(state, "MEMORY", args, addr)
		assertNotNil(t, resp)
	})
}

// FuzzSortCommand fuzzes SORT with random arguments.
func FuzzSortCommand(f *testing.F) {
	f.Add("mylist", "ASC", "ALPHA", "0", "10")
	f.Add("mylist", "DESC", "", "0", "-1")
	f.Add("mylist", "", "LIMIT", "5", "10")

	f.Fuzz(func(t *testing.T, key, order, modifier, offset, count string) {
		key = truncFuzz(key)
		order = truncFuzz(strings.ToUpper(order))
		modifier = truncFuzz(strings.ToUpper(modifier))
		offset = truncFuzz(offset)
		count = truncFuzz(count)

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		pushResp := handler.executeCommand(state, "RPUSH", [][]byte{[]byte(key), []byte("3"), []byte("1"), []byte("2")}, addr)
		assertNotNil(t, pushResp)
		_, isInt := pushResp.(*proto.Integer)
		assert.True(t, isInt, "RPUSH must return Integer, got %T", pushResp)

		args := [][]byte{[]byte(key)}
		if order == "ASC" || order == "DESC" {
			args = append(args, []byte(order))
		}
		if modifier == "ALPHA" {
			args = append(args, []byte("ALPHA"))
		}
		if modifier == "LIMIT" {
			args = append(args, []byte("LIMIT"), []byte(offset), []byte(count))
		}

		assertNotNil(t, handler.executeCommand(state, "SORT", args, addr))
		assertNotNil(t, handler.executeCommand(state, "SORT_RO", args, addr))
	})
}

// FuzzDbsizeCommand fuzzes DBSIZE with random data.
func FuzzDbsizeCommand(f *testing.F) {
	f.Add("key", "value")
	f.Add("", "")

	f.Fuzz(func(t *testing.T, key, value string) {
		key = truncFuzz(key)
		value = truncFuzz(value)

		handler, state := setupTestHandler(t)
		defer handler.Db.Close()
		addr := "127.0.0.1:12345"

		if key != "" {
			setResp := handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(value)}, addr)
			assertNotNil(t, setResp)
		}

		dbsizeResp := handler.executeCommand(state, "DBSIZE", nil, addr)
		assertNotNil(t, dbsizeResp)
		di, ok := dbsizeResp.(*proto.Integer)
		assert.True(t, ok, "DBSIZE must return Integer, got %T", dbsizeResp)
		if ok {
			assert.GreaterOrEqual(t, int64(*di), int64(0), "DBSIZE must be non-negative")
		}

		if key != "" {
			delResp := handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr)
			assertNotNil(t, delResp)
		}

		dbsizeAfter := handler.executeCommand(state, "DBSIZE", nil, addr)
		assertNotNil(t, dbsizeAfter)
		di2, ok2 := dbsizeAfter.(*proto.Integer)
		assert.True(t, ok2, "DBSIZE must return Integer, got %T", dbsizeAfter)
		if ok && ok2 {
			assert.LessOrEqual(t, int64(*di2), int64(*di), "DBSIZE should not increase after DEL")
		}
	})
}
