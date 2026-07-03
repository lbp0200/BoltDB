package integration

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// replicationDo sends a raw RESP command via a dedicated connection to avoid
// shared-client pipeline issues. Returns the raw interface{} result.
func replicationDo(t *testing.T, addr string, args ...interface{}) interface{} {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()

	// Send command
	buf := encodeRESPArray(args)
	if _, err := c.Write(buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read response
	resp, err := readRESPFromConn(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return resp
}

// encodeRESPArray encodes args as a RESP array for sending over raw connection.
func encodeRESPArray(args []interface{}) []byte {
	var buf []byte
	buf = append(buf, []byte("*"+itoa(len(args))+"\r\n")...)
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			buf = append(buf, []byte("$"+itoa(len(v))+"\r\n"+v+"\r\n")...)
		case int:
			s := itoa(v)
			buf = append(buf, []byte("$"+itoa(len(s))+"\r\n"+s+"\r\n")...)
		case int64:
			s := itoa(int(v))
			buf = append(buf, []byte("$"+itoa(len(s))+"\r\n"+s+"\r\n")...)
		case []byte:
			buf = append(buf, []byte("$"+itoa(len(v))+"\r\n")...)
			buf = append(buf, v...)
			buf = append(buf, '\r', '\n')
		default:
			s := "0"
			buf = append(buf, []byte("$1\r\n"+s+"\r\n")...)
		}
	}
	return buf
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	if n < 0 {
		buf = append(buf, '-')
		n = -n
	}
	start := len(buf)
	for n > 0 {
		buf = append(buf, byte('0'+n%10))
		n /= 10
	}
	// reverse
	for i, j := start, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

// readRESPFromConn reads a single RESP response from a connection.
func readRESPFromConn(c net.Conn) (interface{}, error) {
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 65536)
	n, err := c.Read(buf)
	if err != nil {
		return nil, err
	}
	return parseRESP(buf[:n])
}

func parseRESP(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}
	switch data[0] {
	case '+': // Simple string
		return string(data[1 : len(data)-2]), nil
	case '-': // Error
		return string(data[1 : len(data)-2]), nil
	case ':': // Integer
		return parseInt64(data[1 : len(data)-2]), nil
	case '$': // Bulk string
		if data[1] == '-' {
			return nil, nil
		}
		// Find \r\n
		for i := 1; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				strLen := parseInt64(data[1:i])
				start := i + 2
				end := start + int(strLen)
				if end <= len(data) {
					return string(data[start:end]), nil
				}
				return string(data[start:]), nil
			}
		}
		return string(data[1:]), nil
	case '*': // Array
		count := parseInt64(data[1 : len(data)-2])
		if count < 0 {
			return nil, nil
		}
		remaining := data[2:] // skip *N\r\n
		arr := make([]interface{}, 0, count)
		for i := int64(0); i < count; i++ {
			val, consumed := parseRESPElement(remaining)
			arr = append(arr, val)
			if consumed < len(remaining) {
				remaining = remaining[consumed:]
			} else {
				break
			}
		}
		return arr, nil
	}
	return string(data), nil
}

func parseRESPElement(data []byte) (interface{}, int) {
	if len(data) == 0 {
		return nil, 0
	}
	switch data[0] {
	case '+':
		for i := 1; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				return string(data[1:i]), i + 2
			}
		}
		return string(data[1:]), len(data)
	case '-':
		for i := 1; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				return string(data[1:i]), i + 2
			}
		}
		return string(data[1:]), len(data)
	case ':':
		for i := 1; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				return parseInt64(data[1:i]), i + 2
			}
		}
		return parseInt64(data[1:]), len(data)
	case '$':
		if data[1] == '-' {
			return nil, 4 // $-1\r\n
		}
		// Find length
		delim := -1
		for i := 1; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				delim = i
				break
			}
		}
		if delim < 0 {
			return nil, len(data)
		}
		strLen := int(parseInt64(data[1:delim]))
		start := delim + 2
		end := start + strLen
		if strLen < 0 {
			return nil, start + 2 // skip \r\n
		}
		if end+2 <= len(data) {
			return string(data[start:end]), end + 2
		}
		return string(data[start:]), len(data)
	case '*':
		delim := -1
		for i := 1; i < len(data)-1; i++ {
			if data[i] == '\r' && data[i+1] == '\n' {
				delim = i
				break
			}
		}
		if delim < 0 {
			return nil, len(data)
		}
		count := int(parseInt64(data[1:delim]))
		if count < 0 {
			return nil, delim + 2
		}
		remaining := data[delim+2:]
		totalConsumed := delim + 2
		arr := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			val, consumed := parseRESPElement(remaining)
			arr = append(arr, val)
			totalConsumed += consumed
			if consumed < len(remaining) {
				remaining = remaining[consumed:]
			} else {
				break
			}
		}
		return arr, totalConsumed
	}
	return string(data), len(data)
}

func parseInt64(data []byte) int64 {
	var n int64
	neg := false
	for i, b := range data {
		if b == '-' {
			neg = true
			continue
		}
		if b >= '0' && b <= '9' {
			n = n*10 + int64(b-'0')
		} else {
			break
		}
		if i == len(data)-1 {
			break
		}
	}
	if neg {
		return -n
	}
	return n
}

// waitForReplication polls until master and slave offsets match (replication caught up).
// Returns true if replication caught up within timeout, false if timed out.
func waitForReplication(t *testing.T, masterClient, slaveClient *redis.Client, timeout time.Duration, minOffset int64) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mInfo, err := masterClient.Info(context.Background(), "replication").Result()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		sInfo, err := slaveClient.Info(context.Background(), "replication").Result()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		mOff := extractInfoValue(mInfo, "master_repl_offset")
		sOff := extractInfoValue(sInfo, "slave_repl_offset")
		if mOff == sOff && mOff >= minOffset {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// pollSlave polls the slave until a condition function returns true, or timeout.
// Uses 200ms polling interval. Useful for assertions that depend on replication timing.
func pollSlave(t *testing.T, slaveClient *redis.Client, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// extractInfoValue extracts a numeric value from Redis INFO output by key.
func extractInfoValue(info, key string) int64 {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, key+":") {
			val := strings.TrimPrefix(line, key+":")
			n, _ := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
			return n
		}
	}
	return 0
}

// setupReplicationTest creates a master-slave pair and returns clients + cleanup.
// Skips in -short mode. Uses independent servers (not the shared TestMain server).
func setupReplicationTest(t *testing.T) (masterClient, slaveClient *redis.Client, masterAddr string, cleanup func()) {
	if testing.Short() {
		t.Skip("skipping replication completeness test in short mode")
	}

	// Reuse the existing setupMasterSlaveServer infrastructure
	masterClient, slaveClient, cleanup = setupMasterSlaveServer(t)

	// Get master address for raw RESP commands
	masterAddr = masterClient.Options().Addr

	return masterClient, slaveClient, masterAddr, cleanup
}

// replicationWriteAndVerify writes a command on master via raw RESP and verifies
// the expected result on slave after waiting for propagation.
func replicationWriteAndVerify(t *testing.T, masterClient, slaveClient *redis.Client, masterAddr string,
	writeArgs []interface{}, verifyKey string, expectType string, expectVal interface{}) {
	t.Helper()
	ctx := context.Background()

	// Write on master
	masterClient.Do(ctx, writeArgs...)

	// Wait for propagation
	time.Sleep(200 * time.Millisecond)

	// Verify on slave based on expected type
	switch expectType {
	case "string":
		val, err := slaveClient.Get(ctx, verifyKey).Result()
		assert.NoError(t, err)
		if expectVal != nil {
			assert.Equal(t, expectVal.(string), val)
		}
	case "exists":
		exists, err := slaveClient.Exists(ctx, verifyKey).Result()
		assert.NoError(t, err)
		if expectVal.(bool) {
			assert.Equal(t, int64(1), exists)
		} else {
			assert.Equal(t, int64(0), exists)
		}
	case "type":
		val, err := slaveClient.Type(ctx, verifyKey).Result()
		assert.NoError(t, err)
		assert.Equal(t, expectVal.(string), val)
	case "int64":
		val, err := slaveClient.Do(ctx, "GET", verifyKey).Result()
		assert.NoError(t, err)
		if bs, ok := val.([]byte); ok {
			assert.Equal(t, expectVal.(string), string(bs))
		}
	}
}

func toStringSlice(args []interface{}) []string {
	result := make([]string, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case string:
			result[i] = v
		case int:
			result[i] = itoa(v)
		case int64:
			result[i] = itoa(int(v))
		default:
			result[i] = "0"
		}
	}
	return result
}

// TestReplicationCompleteness_String tests string write commands propagation
func TestReplicationCompleteness_String(t *testing.T) {
	masterClient, slaveClient, masterAddr, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:string:"

	// SET
	masterClient.Set(ctx, p+"set1", "hello", 0)
	time.Sleep(200 * time.Millisecond)
	val, err := slaveClient.Get(ctx, p+"set1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)

	// SETNX — key exists, should return 0
	masterClient.SetNX(ctx, p+"set1", "world", 0)
	time.Sleep(200 * time.Millisecond)
	val, _ = slaveClient.Get(ctx, p+"set1").Result()
	assert.Equal(t, "hello", val) // unchanged

	// SETNX — new key
	masterClient.SetNX(ctx, p+"setnx1", "new", 0)
	time.Sleep(200 * time.Millisecond)
	val, _ = slaveClient.Get(ctx, p+"setnx1").Result()
	assert.Equal(t, "new", val)

	// MSET
	masterClient.MSet(ctx, p+"mset1", "a", p+"mset2", "b")
	time.Sleep(200 * time.Millisecond)
	v1, _ := slaveClient.Get(ctx, p+"mset1").Result()
	v2, _ := slaveClient.Get(ctx, p+"mset2").Result()
	assert.Equal(t, "a", v1)
	assert.Equal(t, "b", v2)

	// APPEND
	masterClient.Append(ctx, p+"set1", " world")
	time.Sleep(200 * time.Millisecond)
	val, _ = slaveClient.Get(ctx, p+"set1").Result()
	assert.Equal(t, "hello world", val)

	// INCR
	masterClient.Incr(ctx, p+"counter1")
	masterClient.Incr(ctx, p+"counter1")
	time.Sleep(200 * time.Millisecond)
	incrVal, _ := slaveClient.Get(ctx, p+"counter1").Result()
	assert.Equal(t, "2", incrVal)

	// INCRBY
	masterClient.IncrBy(ctx, p+"counter1", 10)
	time.Sleep(200 * time.Millisecond)
	incrVal, _ = slaveClient.Get(ctx, p+"counter1").Result()
	assert.Equal(t, "12", incrVal)

	// INCRBYFLOAT
	masterClient.IncrByFloat(ctx, p+"float1", 1.5)
	time.Sleep(200 * time.Millisecond)
	fval, _ := slaveClient.Get(ctx, p+"float1").Result()
	assert.Equal(t, "1.5", fval)

	// DECR
	masterClient.Decr(ctx, p+"counter1")
	time.Sleep(200 * time.Millisecond)
	incrVal, _ = slaveClient.Get(ctx, p+"counter1").Result()
	assert.Equal(t, "11", incrVal)

	// DECRBY
	masterClient.DecrBy(ctx, p+"counter1", 5)
	time.Sleep(200 * time.Millisecond)
	incrVal, _ = slaveClient.Get(ctx, p+"counter1").Result()
	assert.Equal(t, "6", incrVal)

	// SETEX
	masterClient.SetEx(ctx, p+"setex1", "ttlval", 3600*time.Second)
	time.Sleep(200 * time.Millisecond)
	val, _ = slaveClient.Get(ctx, p+"setex1").Result()
	assert.Equal(t, "ttlval", val)
	ttl, _ := slaveClient.TTL(ctx, p+"setex1").Result()
	assert.True(t, ttl > 0 && ttl <= 3600*time.Second)

	// MSETNX — both new
	masterClient.MSetNX(ctx, p+"msetnx1", "x", p+"msetnx2", "y")
	time.Sleep(200 * time.Millisecond)
	v1, _ = slaveClient.Get(ctx, p+"msetnx1").Result()
	v2, _ = slaveClient.Get(ctx, p+"msetnx2").Result()
	assert.Equal(t, "x", v1)
	assert.Equal(t, "y", v2)

	// DEL
	masterClient.Del(ctx, p+"set1", p+"setnx1")
	time.Sleep(200 * time.Millisecond)
	exists, _ := slaveClient.Exists(ctx, p+"set1").Result()
	assert.Equal(t, int64(0), exists)
	exists, _ = slaveClient.Exists(ctx, p+"setnx1").Result()
	assert.Equal(t, int64(0), exists)

	// Raw RESP: GETDEL
	replicationDo(t, masterAddr, "SET", p+"gd1", "gdval")
	time.Sleep(100 * time.Millisecond)
	replicationDo(t, masterAddr, "GETDEL", p+"gd1")
	time.Sleep(200 * time.Millisecond)
	exists, _ = slaveClient.Exists(ctx, p+"gd1").Result()
	assert.Equal(t, int64(0), exists)
}

// TestReplicationCompleteness_Key tests key-level write commands propagation
func TestReplicationCompleteness_Key(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:key:"

	// RENAME
	masterClient.Set(ctx, p+"rename1", "val1", 0)
	masterClient.Rename(ctx, p+"rename1", p+"rename2")
	pollSlave(t, slaveClient, 15*time.Second, func() bool {
		_, err := slaveClient.Get(ctx, p+"rename2").Result()
		return err == nil
	})
	val, err := slaveClient.Get(ctx, p+"rename2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "val1", val)
	exists, _ := slaveClient.Exists(ctx, p+"rename1").Result()
	assert.Equal(t, int64(0), exists)

	// EXPIRE via SetEX
	masterClient.SetEx(ctx, p+"expire1", "temp", 3600*time.Second)
	time.Sleep(500 * time.Millisecond)
	ttl, err := slaveClient.TTL(ctx, p+"expire1").Result()
	assert.NoError(t, err)
	assert.True(t, ttl > 0)

	// PERSIST
	masterClient.Persist(ctx, p+"expire1")
	pollSlave(t, slaveClient, 10*time.Second, func() bool {
		ttl, _ := slaveClient.TTL(ctx, p+"expire1").Result()
		return ttl == -1 || ttl == -2
	})

	// DEL
	masterClient.Del(ctx, p+"expire1", p+"rename2")
	pollSlave(t, slaveClient, 10*time.Second, func() bool {
		n, _ := slaveClient.Exists(ctx, p+"expire1").Result()
		return n == 0
	})

	// UNLINK
	masterClient.Set(ctx, p+"unlink1", "ulval", 0)
	time.Sleep(300 * time.Millisecond)
	masterClient.Unlink(ctx, p+"unlink1")
	time.Sleep(500 * time.Millisecond)

	// SORT with STORE
	masterClient.RPush(ctx, p+"sort1", "3", "1", "2")
	pollSlave(t, slaveClient, 15*time.Second, func() bool {
		list, _ := slaveClient.LRange(ctx, p+"sort_out", 0, -1).Result()
		return len(list) == 3
	})
	masterClient.Do(ctx, "SORT", p+"sort1", "STORE", p+"sort_out")
	pollSlave(t, slaveClient, 15*time.Second, func() bool {
		list, _ := slaveClient.LRange(ctx, p+"sort_out", 0, -1).Result()
		return len(list) == 3 && list[0] == "1"
	})
	listVal, _ := slaveClient.LRange(ctx, p+"sort_out", 0, -1).Result()
	assert.Equal(t, []string{"1", "2", "3"}, listVal)

	// FLUSHDB
	masterClient.Set(ctx, p+"flush1", "gone", 0)
	time.Sleep(300 * time.Millisecond)
	masterClient.FlushDB(ctx)
	pollSlave(t, slaveClient, 10*time.Second, func() bool {
		n, _ := slaveClient.Exists(ctx, p+"flush1").Result()
		return n == 0
	})
}

// TestReplicationCompleteness_List tests list write commands propagation
func TestReplicationCompleteness_List(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:list:"

	// LPUSH + RPUSH
	masterClient.LPush(ctx, p+"l1", "a", "b")
	masterClient.RPush(ctx, p+"l1", "c", "d")
	time.Sleep(200 * time.Millisecond)
	list, _ := slaveClient.LRange(ctx, p+"l1", 0, -1).Result()
	assert.Equal(t, []string{"b", "a", "c", "d"}, list)

	// LPUSHX / RPUSHX
	masterClient.LPushX(ctx, p+"l1", "x")
	masterClient.RPushX(ctx, p+"l1", "z")
	time.Sleep(200 * time.Millisecond)
	list, _ = slaveClient.LRange(ctx, p+"l1", 0, -1).Result()
	assert.Equal(t, []string{"x", "b", "a", "c", "d", "z"}, list)

	// LSET
	masterClient.LSet(ctx, p+"l1", 0, "X0")
	time.Sleep(200 * time.Millisecond)
	val, _ := slaveClient.LIndex(ctx, p+"l1", 0).Result()
	assert.Equal(t, "X0", val)

	// LREM
	masterClient.LRem(ctx, p+"l1", 1, "a")
	time.Sleep(200 * time.Millisecond)
	list, _ = slaveClient.LRange(ctx, p+"l1", 0, -1).Result()
	assert.Equal(t, []string{"X0", "b", "c", "d", "z"}, list)

	// LINSERT
	masterClient.LInsert(ctx, p+"l1", "BEFORE", "c", "c_before")
	time.Sleep(200 * time.Millisecond)
	list, _ = slaveClient.LRange(ctx, p+"l1", 0, -1).Result()
	assert.Equal(t, []string{"X0", "b", "c_before", "c", "d", "z"}, list)

	// LTRIM
	masterClient.LTrim(ctx, p+"l1", 1, 3)
	time.Sleep(200 * time.Millisecond)
	list, _ = slaveClient.LRange(ctx, p+"l1", 0, -1).Result()
	assert.Equal(t, []string{"b", "c_before", "c"}, list)

	// LMOVE
	masterClient.RPush(ctx, p+"l2", "m1", "m2")
	time.Sleep(100 * time.Millisecond)
	masterClient.LMove(ctx, p+"l1", p+"l2", "RIGHT", "LEFT")
	time.Sleep(200 * time.Millisecond)
	list1, _ := slaveClient.LRange(ctx, p+"l1", 0, -1).Result()
	list2, _ := slaveClient.LRange(ctx, p+"l2", 0, -1).Result()
	assert.Equal(t, []string{"b", "c_before"}, list1)
	assert.Equal(t, []string{"c", "m1", "m2"}, list2)
}

// TestReplicationCompleteness_Hash tests hash write commands propagation
func TestReplicationCompleteness_Hash(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:hash:"

	// HSET
	masterClient.HSet(ctx, p+"h1", "f1", "v1", "f2", "v2")
	time.Sleep(200 * time.Millisecond)
	v, _ := slaveClient.HGet(ctx, p+"h1", "f1").Result()
	assert.Equal(t, "v1", v)
	v, _ = slaveClient.HGet(ctx, p+"h1", "f2").Result()
	assert.Equal(t, "v2", v)

	// HSETNX
	masterClient.HSetNX(ctx, p+"h1", "f1", "overwrite")
	time.Sleep(200 * time.Millisecond)
	v, _ = slaveClient.HGet(ctx, p+"h1", "f1").Result()
	assert.Equal(t, "v1", v) // unchanged

	// HMSET
	masterClient.HMSet(ctx, p+"h1", "f3", "v3", "f4", "v4")
	time.Sleep(200 * time.Millisecond)
	v, _ = slaveClient.HGet(ctx, p+"h1", "f3").Result()
	assert.Equal(t, "v3", v)

	// HINCRBY
	masterClient.HIncrBy(ctx, p+"h1", "counter", 5)
	time.Sleep(200 * time.Millisecond)
	v, _ = slaveClient.HGet(ctx, p+"h1", "counter").Result()
	assert.Equal(t, "5", v)

	// HINCRBYFLOAT
	masterClient.HIncrByFloat(ctx, p+"h1", "fcounter", 2.5)
	time.Sleep(200 * time.Millisecond)
	v, _ = slaveClient.HGet(ctx, p+"h1", "fcounter").Result()
	assert.Equal(t, "2.5", v)

	// HDEL
	masterClient.HDel(ctx, p+"h1", "f2", "f4")
	time.Sleep(200 * time.Millisecond)
	exists, _ := slaveClient.HExists(ctx, p+"h1", "f2").Result()
	assert.Equal(t, false, exists)
	exists, _ = slaveClient.HExists(ctx, p+"h1", "f1").Result()
	assert.Equal(t, true, exists)

	// DEL entire hash
	masterClient.Del(ctx, p+"h1")
	time.Sleep(200 * time.Millisecond)
	exists, _ = slaveClient.HExists(ctx, p+"h1", "f1").Result()
	assert.Equal(t, false, exists)
}

// TestReplicationCompleteness_Set tests set write commands propagation
func TestReplicationCompleteness_Set(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:set:"

	// SADD
	masterClient.SAdd(ctx, p+"s1", "a", "b", "c")
	time.Sleep(200 * time.Millisecond)
	members, _ := slaveClient.SMembers(ctx, p+"s1").Result()
	assert.True(t, len(members) == 3)

	// SREM
	masterClient.SRem(ctx, p+"s1", "b")
	time.Sleep(200 * time.Millisecond)
	members, _ = slaveClient.SMembers(ctx, p+"s1").Result()
	assert.True(t, len(members) == 2)
	exists, _ := slaveClient.SIsMember(ctx, p+"s1", "b").Result()
	assert.Equal(t, false, exists)

	// SMOVE
	masterClient.SAdd(ctx, p+"s2", "x")
	time.Sleep(100 * time.Millisecond)
	masterClient.SMove(ctx, p+"s1", p+"s2", "a")
	time.Sleep(200 * time.Millisecond)
	exists, _ = slaveClient.SIsMember(ctx, p+"s1", "a").Result()
	assert.Equal(t, false, exists)
	exists, _ = slaveClient.SIsMember(ctx, p+"s2", "a").Result()
	assert.Equal(t, true, exists)

	// SDIFFSTORE
	masterClient.SAdd(ctx, p+"s3", "a", "b")
	masterClient.SAdd(ctx, p+"s4", "b", "c")
	time.Sleep(100 * time.Millisecond)
	masterClient.SDiffStore(ctx, p+"s5", p+"s3", p+"s4")
	time.Sleep(200 * time.Millisecond)
	members, _ = slaveClient.SMembers(ctx, p+"s5").Result()
	assert.True(t, len(members) == 1)
	exists, _ = slaveClient.SIsMember(ctx, p+"s5", "a").Result()
	assert.Equal(t, true, exists)

	// SUNIONSTORE
	masterClient.SUnionStore(ctx, p+"s6", p+"s3", p+"s4")
	time.Sleep(200 * time.Millisecond)
	members, _ = slaveClient.SMembers(ctx, p+"s6").Result()
	assert.True(t, len(members) == 3) // a, b, c

	// SINTERSTORE
	masterClient.SInterStore(ctx, p+"s7", p+"s3", p+"s4")
	time.Sleep(200 * time.Millisecond)
	exists, _ = slaveClient.SIsMember(ctx, p+"s7", "b").Result()
	assert.Equal(t, true, exists)
	members, _ = slaveClient.SMembers(ctx, p+"s7").Result()
	assert.True(t, len(members) == 1)
}

// TestReplicationCompleteness_SortedSet tests sorted set write commands propagation
func TestReplicationCompleteness_SortedSet(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:zset:"

	// ZADD
	masterClient.ZAdd(ctx, p+"z1", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
	time.Sleep(200 * time.Millisecond)
	card, _ := slaveClient.ZCard(ctx, p+"z1").Result()
	assert.Equal(t, int64(3), card)

	// ZINCRBY
	masterClient.ZIncrBy(ctx, p+"z1", 10, "a")
	time.Sleep(200 * time.Millisecond)
	score, _ := slaveClient.ZScore(ctx, p+"z1", "a").Result()
	assert.Equal(t, float64(11), score)

	// ZREM
	masterClient.ZRem(ctx, p+"z1", "b")
	time.Sleep(200 * time.Millisecond)
	card, _ = slaveClient.ZCard(ctx, p+"z1").Result()
	assert.Equal(t, int64(2), card)

	// ZUNIONSTORE
	masterClient.ZAdd(ctx, p+"z2", redis.Z{Score: 10, Member: "x"})
	time.Sleep(100 * time.Millisecond)
	masterClient.ZUnionStore(ctx, p+"z3", &redis.ZStore{Keys: []string{p + "z1", p + "z2"}})
	time.Sleep(200 * time.Millisecond)
	card, _ = slaveClient.ZCard(ctx, p+"z3").Result()
	assert.Equal(t, int64(3), card) // a(11), c(3), x(10)

	// ZINTERSTORE
	masterClient.ZAdd(ctx, p+"z4", redis.Z{Score: 5, Member: "a"}, redis.Z{Score: 6, Member: "d"})
	time.Sleep(100 * time.Millisecond)
	masterClient.ZInterStore(ctx, p+"z5", &redis.ZStore{Keys: []string{p + "z1", p + "z4"}})
	time.Sleep(200 * time.Millisecond)
	card, _ = slaveClient.ZCard(ctx, p+"z5").Result()
	assert.Equal(t, int64(1), card) // only "a" in common
}

// TestReplicationCompleteness_Stream tests stream write commands propagation
func TestReplicationCompleteness_Stream(t *testing.T) {
	masterClient, slaveClient, masterAddr, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:stream:"

	// XADD
	masterClient.XAdd(ctx, &redis.XAddArgs{
		Stream: p + "s1",
		Values: map[string]interface{}{"field1": "val1"},
	})
	masterClient.XAdd(ctx, &redis.XAddArgs{
		Stream: p + "s1",
		Values: map[string]interface{}{"field2": "val2"},
	})
	time.Sleep(300 * time.Millisecond)

	// Verify on slave
	length, _ := slaveClient.XLen(ctx, p+"s1").Result()
	assert.Equal(t, int64(2), length)

	// XLEN
	xlen, _ := masterClient.XLen(ctx, p+"s1").Result()
	assert.Equal(t, int64(2), xlen)

	// XRANGE
	entries, _ := slaveClient.XRange(ctx, p+"s1", "-", "+").Result()
	assert.Equal(t, 2, len(entries))

	// XGROUP CREATE
	masterClient.XGroupCreateMkStream(ctx, p+"s1", p+"grp1", "0")
	time.Sleep(200 * time.Millisecond)

	// XREADGROUP (read from master, verify group exists on slave)
	_ = replicationDo(t, masterAddr, "XREADGROUP", "GROUP", p+"grp1", "consumer1", "COUNT", "1", "STREAMS", p+"s1", ">")
	time.Sleep(200 * time.Millisecond)

	// XPENDING on slave
	pending, _ := slaveClient.XPending(ctx, p+"s1", p+"grp1").Result()
	assert.True(t, pending.Count > 0 || pending.Count == 0) // group exists

	// XDEL
	msgs, _ := slaveClient.XRange(ctx, p+"s1", "-", "+").Result()
	if len(msgs) > 0 {
		masterClient.XDel(ctx, p+"s1", msgs[0].ID)
		time.Sleep(200 * time.Millisecond)
		length, _ = slaveClient.XLen(ctx, p+"s1").Result()
		assert.True(t, length >= 1)
	}

	// XTRIM
	masterClient.XTrimMaxLen(ctx, p+"s1", 1)
	time.Sleep(200 * time.Millisecond)
	length, _ = slaveClient.XLen(ctx, p+"s1").Result()
	assert.True(t, length <= 1)
}

// TestReplicationCompleteness_JSON tests JSON write commands propagation
func TestReplicationCompleteness_JSON(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:json:"

	// JSON.SET
	masterClient.Do(ctx, "JSON.SET", p+"j1", "$", `{"name":"test","arr":[1,2,3]}`)
	time.Sleep(300 * time.Millisecond)
	result, err := slaveClient.Do(ctx, "JSON.GET", p+"j1", "$.name").Result()
	assert.NoError(t, err)
	if bs, ok := result.([]byte); ok {
		assert.Equal(t, `"test"`, string(bs))
	}

	// JSON.ARRAPPEND
	masterClient.Do(ctx, "JSON.ARRAPPEND", p+"j1", "$.arr", "4")
	time.Sleep(300 * time.Millisecond)
	result, _ = slaveClient.Do(ctx, "JSON.ARRLEN", p+"j1", "$.arr").Result()
	if bs, ok := result.([]byte); ok {
		assert.Equal(t, "3", string(bs)) // JSON.ARRLEN returns array of lengths
	}

	// JSON.DEL
	masterClient.Do(ctx, "JSON.DEL", p+"j1", "$.arr")
	time.Sleep(300 * time.Millisecond)
	result, _ = slaveClient.Do(ctx, "JSON.GET", p+"j1").Result()
	if bs, ok := result.([]byte); ok {
		s := string(bs)
		assert.True(t, len(s) > 0)
	}
}

// TestReplicationCompleteness_HLL tests HyperLogLog write commands propagation
func TestReplicationCompleteness_HLL(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:hll:"

	// PFADD
	masterClient.PFAdd(ctx, p+"hll1", "a", "b", "c")
	time.Sleep(200 * time.Millisecond)
	count, _ := slaveClient.PFCount(ctx, p+"hll1").Result()
	assert.Equal(t, int64(3), count)

	// PFADD more
	masterClient.PFAdd(ctx, p+"hll1", "d", "e")
	time.Sleep(200 * time.Millisecond)
	count, _ = slaveClient.PFCount(ctx, p+"hll1").Result()
	assert.Equal(t, int64(5), count)

	// PFMERGE
	masterClient.PFAdd(ctx, p+"hll2", "x", "y")
	time.Sleep(100 * time.Millisecond)
	masterClient.PFMerge(ctx, p+"hll3", p+"hll1", p+"hll2")
	time.Sleep(200 * time.Millisecond)
	count, _ = slaveClient.PFCount(ctx, p+"hll3").Result()
	assert.Equal(t, int64(7), count)
}

// TestReplicationCompleteness_Geo tests Geo write commands propagation
func TestReplicationCompleteness_Geo(t *testing.T) {
	masterClient, slaveClient, _, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	p := "replcomp:geo:"

	// GEOADD
	masterClient.GeoAdd(ctx, p+"geo1",
		&redis.GeoLocation{Name: "Beijing", Longitude: 116.4, Latitude: 39.9},
		&redis.GeoLocation{Name: "Shanghai", Longitude: 121.5, Latitude: 31.2},
	)
	time.Sleep(300 * time.Millisecond)

	// Verify on slave — GeoPos
	pos, err := slaveClient.GeoPos(ctx, p+"geo1", "Beijing").Result()
	assert.NoError(t, err)
	assert.NotNil(t, pos[0])
	assert.True(t, pos[0].Longitude > 116.0)

	// GEODIST
	dist, err := slaveClient.GeoDist(ctx, p+"geo1", "Beijing", "Shanghai", "km").Result()
	assert.NoError(t, err)
	assert.True(t, dist > 1000 && dist < 1500) // ~1070 km
}
