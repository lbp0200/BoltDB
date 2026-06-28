package server

import (
	"strings"
	"testing"
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

		_ = handler.executeCommand(state, cmd, nil, "127.0.0.1:12345")
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

		_ = handler.executeCommand(state, cmd, [][]byte{[]byte(arg1), []byte(arg2)}, "127.0.0.1:12345")

		if arg1 != "" {
			_ = handler.executeCommand(state, "GET", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
			_ = handler.executeCommand(state, "TYPE", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
			_ = handler.executeCommand(state, "EXISTS", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
			_ = handler.executeCommand(state, "DEL", [][]byte{[]byte(arg1)}, "127.0.0.1:12345")
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

		_ = handler.executeCommand(state, cmd1, filterEmptyArgs(arg1a, arg1b), addr)
		_ = handler.executeCommand(state, cmd2, filterEmptyArgs(arg2a, arg2b), addr)
		_ = handler.executeCommand(state, cmd3, filterEmptyArgs(arg3a, arg3b), addr)
		_ = handler.executeCommand(state, "FLUSHDB", nil, addr)
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
		_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(strVal)}, addr)
		// Phase 2: Hash on string key
		_ = handler.executeCommand(state, "HSET", [][]byte{[]byte(key), []byte(field), []byte(strVal)}, addr)
		_ = handler.executeCommand(state, "HGET", [][]byte{[]byte(key), []byte(field)}, addr)
		// Phase 3: List on string key
		_ = handler.executeCommand(state, "LPUSH", [][]byte{[]byte(key), []byte(listElem)}, addr)
		_ = handler.executeCommand(state, "LLEN", [][]byte{[]byte(key)}, addr)
		// Phase 4: Set on string key
		_ = handler.executeCommand(state, "SADD", [][]byte{[]byte(key), []byte(member)}, addr)
		_ = handler.executeCommand(state, "SCARD", [][]byte{[]byte(key)}, addr)
		// Phase 5: ZSet on string key
		_ = handler.executeCommand(state, "ZADD", [][]byte{[]byte(key), []byte("1.0"), []byte(member)}, addr)
		_ = handler.executeCommand(state, "ZCARD", [][]byte{[]byte(key)}, addr)
		// Phase 6: Cleanup
		_ = handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr)
		_ = handler.executeCommand(state, "TYPE", [][]byte{[]byte(key)}, addr)
		// Phase 7: Expire/Persist
		_ = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte(key), []byte("100")}, addr)
		_ = handler.executeCommand(state, "TTL", [][]byte{[]byte(key)}, addr)
		_ = handler.executeCommand(state, "PERSIST", [][]byte{[]byte(key)}, addr)
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

		_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(value)}, addr)
		_ = handler.executeCommand(state, "GET", [][]byte{[]byte(key)}, addr)
		_ = handler.executeCommand(state, "HSET", [][]byte{[]byte(key), []byte("f"), []byte(value)}, addr)
		_ = handler.executeCommand(state, "SADD", [][]byte{[]byte(key), []byte(value)}, addr)
		_ = handler.executeCommand(state, "LPUSH", [][]byte{[]byte(key), []byte(value)}, addr)
		_ = handler.executeCommand(state, "ZADD", [][]byte{[]byte(key), []byte("1.0"), []byte(value)}, addr)
		_ = handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr)
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
		_ = handler.executeCommand(state, "MULTI", nil, addr)
		_ = handler.executeCommand(state, cmd1, filterEmptyArgs(arg1a, arg1b), addr)
		_ = handler.executeCommand(state, "DISCARD", nil, addr)
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
		_ = handler.executeCommand(state, "INFO", [][]byte{[]byte(section)}, "127.0.0.1:12345")
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
		_ = handler.executeCommand(state, "SLOWLOG", args, "127.0.0.1:12345")
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
		_ = handler.executeCommand(state, "CLUSTER", [][]byte{[]byte(subcmd)}, "127.0.0.1:12345")
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

		_ = handler.executeCommand(state, "XADD", [][]byte{[]byte(stream), []byte(id), []byte(field), []byte(value)}, addr)
		_ = handler.executeCommand(state, "XLEN", [][]byte{[]byte(stream)}, addr)
		_ = handler.executeCommand(state, "XRANGE", [][]byte{[]byte(stream), []byte("-"), []byte("+")}, addr)
		_ = handler.executeCommand(state, "XREVRANGE", [][]byte{[]byte(stream), []byte("+"), []byte("-")}, addr)
		_ = handler.executeCommand(state, "XINFO", [][]byte{[]byte("STREAM"), []byte(stream)}, addr)
		_ = handler.executeCommand(state, "DEL", [][]byte{[]byte(stream)}, addr)
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

		_ = handler.executeCommand(state, "GEOADD", [][]byte{[]byte(key), []byte(lon), []byte(lat), []byte(member)}, addr)
		_ = handler.executeCommand(state, "GEOPOS", [][]byte{[]byte(key), []byte(member)}, addr)
		_ = handler.executeCommand(state, "GEODIST", [][]byte{[]byte(key), []byte(member), []byte(member)}, addr)
		_ = handler.executeCommand(state, "GEOHASH", [][]byte{[]byte(key), []byte(member)}, addr)
		_ = handler.executeCommand(state, "GEOSEARCH", [][]byte{
			[]byte(key), []byte("FROMLONLAT"), []byte(lon), []byte(lat),
			[]byte("BYRADIUS"), []byte("100000"), []byte("km"),
		}, addr)
		_ = handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr)
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

		_ = handler.executeCommand(state, "PFADD", [][]byte{[]byte(key), []byte(value)}, addr)
		_ = handler.executeCommand(state, "PFCOUNT", [][]byte{[]byte(key)}, addr)
		key2 := key + "_merge"
		_ = handler.executeCommand(state, "PFADD", [][]byte{[]byte(key2), []byte(value)}, addr)
		_ = handler.executeCommand(state, "PFMERGE", [][]byte{[]byte(key), []byte(key2)}, addr)
		_ = handler.executeCommand(state, "DEL", [][]byte{[]byte(key), []byte(key2)}, addr)
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

		_ = handler.executeCommand(state, cmd, nil, addr)
		_ = handler.executeCommand(state, cmd, [][]byte{[]byte("")}, addr)
		_ = handler.executeCommand(state, cmd, [][]byte{[]byte(""), []byte(""), []byte("")}, addr)
		_ = handler.executeCommand(state, cmd, [][]byte{[]byte(""), []byte("value")}, addr)
		_ = handler.executeCommand(state, cmd, [][]byte{[]byte("key"), []byte("")}, addr)

		manyEmpty := make([][]byte, 64)
		for i := range manyEmpty {
			manyEmpty[i] = []byte("")
		}
		_ = handler.executeCommand(state, cmd, manyEmpty, addr)
		_ = handler.executeCommand(state, "FLUSHDB", nil, addr)
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
			_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key1), []byte("hello world")}, addr)
		}
		if key2 != "" {
			_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key2), []byte("hello there")}, addr)
		}

		args := [][]byte{[]byte(key1), []byte(key2)}
		if modifier != "" {
			args = append(args, []byte(modifier))
		}
		_ = handler.executeCommand(state, "LCS", args, addr)
	})
}

// FuzzWaitCommand fuzzes WAIT command.
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
			_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte("value")}, addr)
		}
		_ = handler.executeCommand(state, "OBJECT", [][]byte{[]byte(subcmd), []byte(key)}, addr)
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
			_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte("value")}, addr)
		}
		args := [][]byte{[]byte(subcmd)}
		if key != "" {
			args = append(args, []byte(key))
		}
		_ = handler.executeCommand(state, "MEMORY", args, addr)
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

		_ = handler.executeCommand(state, "RPUSH", [][]byte{[]byte(key), []byte("3"), []byte("1"), []byte("2")}, addr)

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

		_ = handler.executeCommand(state, "SORT", args, addr)
		_ = handler.executeCommand(state, "SORT_RO", args, addr)
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
			_ = handler.executeCommand(state, "SET", [][]byte{[]byte(key), []byte(value)}, addr)
		}
		_ = handler.executeCommand(state, "DBSIZE", nil, addr)
		if key != "" {
			_ = handler.executeCommand(state, "DEL", [][]byte{[]byte(key)}, addr)
		}
		_ = handler.executeCommand(state, "DBSIZE", nil, addr)
	})
}
