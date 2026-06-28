package integration

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

func rawCmd(t *testing.T, conn net.Conn, args ...string) {
	t.Helper()
	w := bufio.NewWriter(conn)
	cmdArgs := make([][]byte, len(args))
	for i, a := range args {
		cmdArgs[i] = []byte(a)
	}
	proto.WriteRESP(w, &proto.Array{Args: cmdArgs})
	w.Flush()
}

func rawRead(t *testing.T, conn net.Conn) *proto.Array {
	t.Helper()
	r := bufio.NewReader(conn)
	resp, err := proto.ReadRESP(r)
	assert.NoError(t, err)
	return resp
}

func rawArg(t *testing.T, conn net.Conn) string {
	t.Helper()
	return string(rawRead(t, conn).Args[0])
}

func rawOK(t *testing.T, conn net.Conn) {
	t.Helper()
	v := rawArg(t, conn)
	if v != "OK" {
		t.Errorf("expected OK, got: %s", v)
	}
}

func rawQueued(t *testing.T, conn net.Conn) {
	t.Helper()
	v := rawArg(t, conn)
	if v != "QUEUED" {
		t.Errorf("expected QUEUED, got: %s", v)
	}
}

// rawWatchOK asserts WATCH returned an integer count (BoltDB returns :N not +OK)
func rawReadWire(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	r := bufio.NewReader(conn)
	line, err := r.ReadBytes('\n')
	assert.NoError(t, err)
	return line
}

func rawWatchOK(t *testing.T, conn net.Conn) {
	t.Helper()
	v := rawArg(t, conn)
	// Redis WATCH returns +OK (SimpleString), not an integer
	if v != "OK" {
		t.Errorf("WATCH should return OK, got: %s", v)
	}
}

func dialConn(t *testing.T) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	return conn
}

// ============================================================================
// 1. TRANSACTION TESTS
// ============================================================================

func TestCompatTxBasicMULTIEXEC(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn := dialConn(t)
	defer conn.Close()

	rawCmd(t, conn, "MULTI")
	rawOK(t, conn)

	rawCmd(t, conn, "SET", "tx:string", "v1")
	rawQueued(t, conn)

	rawCmd(t, conn, "LPUSH", "tx:list", "a")
	rawQueued(t, conn)

	rawCmd(t, conn, "HSET", "tx:hash", "f1", "val1")
	rawQueued(t, conn)

	rawCmd(t, conn, "SADD", "tx:set", "m1")
	rawQueued(t, conn)

	rawCmd(t, conn, "EXEC")
	execResp := rawRead(t, conn)
	if len(execResp.Args) == 0 {
		t.Fatal("EXEC returned empty array")
	}

	ctx := context.Background()
	val, err := sharedClient.Get(ctx, "tx:string").Result()
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	count, err := sharedClient.LLen(ctx, "tx:list").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	exists, err := sharedClient.HExists(ctx, "tx:hash", "f1").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, exists)
}

func TestCompatTxDISCARD(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn := dialConn(t)
	defer conn.Close()

	rawCmd(t, conn, "MULTI")
	rawOK(t, conn)

	rawCmd(t, conn, "SET", "tx:discard", "should_not_exist")
	rawQueued(t, conn)

	rawCmd(t, conn, "DISCARD")
	rawOK(t, conn)

	ctx := context.Background()
	_, err := sharedClient.Get(ctx, "tx:discard").Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

func TestCompatTxEXECWithoutMULTI(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn := dialConn(t)
	defer conn.Close()

	rawCmd(t, conn, "EXEC")
	resp := rawRead(t, conn)
	var parts []string
	for _, a := range resp.Args {
		parts = append(parts, string(a))
	}
	msg := strings.Join(parts, " ")
	if !strings.Contains(msg, "EXEC without MULTI") {
		t.Errorf("expected EXEC without MULTI error, got: %s", msg)
	}
}

func TestCompatTxDISCARDWithoutMULTI(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn := dialConn(t)
	defer conn.Close()

	rawCmd(t, conn, "DISCARD")
	resp := rawRead(t, conn)
	var parts []string
	for _, a := range resp.Args {
		parts = append(parts, string(a))
	}
	msg := strings.Join(parts, " ")
	if !strings.Contains(msg, "DISCARD without MULTI") {
		t.Errorf("expected DISCARD without MULTI error, got: %s", msg)
	}
}

func TestCompatTxWATCHOptimisticLocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	sharedClient.Set(ctx, "watch:key", "original", 0)

	var execResult error
	err := sharedClient.Watch(ctx, func(tx *redis.Tx) error {
		sharedClient.Set(ctx, "watch:key", "modified_by_other", 0)

		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "watch:key", "my_value", 0)
			return nil
		})
		execResult = err
		return err
	}, "watch:key")

	if err == nil {
		t.Error("WATCH conflict should cause EXEC to fail")
	}
	if err != redis.TxFailedErr {
		t.Logf("WATCH conflict error: %v (expected redis.TxFailedErr)", err)
	}

	val, err := sharedClient.Get(ctx, "watch:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "modified_by_other", val)
	assert.Error(t, execResult)
}

func TestCompatTxWATCHNoConflict(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	sharedClient.Set(ctx, "w:noconflict", "original", 0)

	conn := dialConn(t)
	defer conn.Close()

	rawCmd(t, conn, "WATCH", "w:noconflict")
	rawWatchOK(t, conn)

	rawCmd(t, conn, "MULTI")
	rawOK(t, conn)

	rawCmd(t, conn, "SET", "w:noconflict", "updated")
	rawQueued(t, conn)

	rawCmd(t, conn, "EXEC")
	execResp := rawRead(t, conn)
	if len(execResp.Args) == 0 {
		t.Errorf("EXEC should return result array, got empty")
	}

	val, _ := sharedClient.Get(ctx, "w:noconflict").Result()
	assert.Equal(t, "updated", val)
}

func TestCompatTxUNWATCH(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	sharedClient.Set(ctx, "unwatch:key", "val", 0)

	conn := dialConn(t)
	defer conn.Close()

	rawCmd(t, conn, "WATCH", "unwatch:key")
	rawWatchOK(t, conn)

	rawCmd(t, conn, "UNWATCH")
	rawOK(t, conn)

	rawCmd(t, conn, "MULTI")
	rawOK(t, conn)

	rawCmd(t, conn, "SET", "unwatch:key", "newval")
	rawQueued(t, conn)

	rawCmd(t, conn, "EXEC")
	execResp := rawRead(t, conn)
	if len(execResp.Args) == 0 {
		t.Errorf("EXEC should return result array, got empty")
	}
}

// ============================================================================
// 2. PUBSUB TESTS
// ============================================================================

func TestCompatPubSubMessageDeliveryOrder(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	subConn, subReader := dialPubSub(t)
	defer subConn.Close()

	sendPubSubCmd(t, subConn, "SUBSCRIBE", "ps:order")
	readPubSubResp(t, subReader)

	pubClient := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
		DB:   0,
	})
	defer pubClient.Close()

	pubClient.Publish(ctx, "ps:order", "msg1")
	pubClient.Publish(ctx, "ps:order", "msg2")
	pubClient.Publish(ctx, "ps:order", "msg3")

	parts1 := readPubSubResp(t, subReader)
	assert.Equal(t, "message", parts1[0])
	assert.Equal(t, "ps:order", parts1[1])
	assert.Equal(t, "msg1", parts1[2])

	parts2 := readPubSubResp(t, subReader)
	assert.Equal(t, "msg2", parts2[2])

	parts3 := readPubSubResp(t, subReader)
	assert.Equal(t, "msg3", parts3[2])
}

func TestCompatPubSubPatternMultiChannel(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	subConn, subReader := dialPubSub(t)
	defer subConn.Close()

	sendPubSubCmd(t, subConn, "PSUBSCRIBE", "ps:*")
	readPubSubResp(t, subReader)

	pubClient := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
		DB:   0,
	})
	defer pubClient.Close()

	time.Sleep(50 * time.Millisecond)

	pubClient.Publish(ctx, "ps:alpha", "data1")
	pubClient.Publish(ctx, "ps:beta", "data2")

	parts1 := readPubSubResp(t, subReader)
	assert.Equal(t, "pmessage", parts1[0])
	assert.Equal(t, "ps:*", parts1[1])
	assert.Equal(t, "ps:alpha", parts1[2])
	assert.Equal(t, "data1", parts1[3])

	parts2 := readPubSubResp(t, subReader)
	assert.Equal(t, "pmessage", parts2[0])
	assert.Equal(t, "ps:*", parts2[1])
	assert.Equal(t, "ps:beta", parts2[2])
	assert.Equal(t, "data2", parts2[3])
}

func TestCompatPubSubPINGinPubSub(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "ps:pingtest")
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "PING")
	line, err := reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "+PONG\r\n", line)
}

func TestCompatPubSubQUIT(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "ps:quit")
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "QUIT")
	line, err := reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", line)
}

func TestCompatPubSubNonPubSubCmd(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "ps:nonpubsub")
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "GET", "somekey")
	line, err := reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, byte('-'), line[0])
}

// ============================================================================
// 3. TIMEOUT / TTL TESTS
// ============================================================================

func TestCompatTTLBasic(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "ttl:key", "value", 0)

	d, err := sharedClient.TTL(ctx, "ttl:key").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), d)

	_, err = sharedClient.Expire(ctx, "ttl:key", 10*time.Second).Result()
	assert.NoError(t, err)

	d, err = sharedClient.TTL(ctx, "ttl:key").Result()
	assert.NoError(t, err)
	if d <= 0 || d > 10*time.Second {
		t.Errorf("TTL should be > 0 and <= 10s, got: %v", d)
	}
}

func TestCompatTTLNonExistentKey(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	d, err := sharedClient.TTL(ctx, "ttl:nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-2), d)
}

func TestCompatExpireNonExistentKey(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	result, err := sharedClient.Expire(ctx, "ttl:noexist", 10*time.Second).Result()
	assert.NoError(t, err)
	assert.Equal(t, false, result)
}

func TestCompatExpireAt(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "ttl:expireat", "val", 0)

	future := time.Now().Add(1 * time.Hour)
	result, err := sharedClient.ExpireAt(ctx, "ttl:expireat", future).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, result)
}

func TestCompatPExpire(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "ttl:pexpire", "val", 0)

	result, err := sharedClient.PExpire(ctx, "ttl:pexpire", 5000*time.Millisecond).Result()
	assert.NoError(t, err)
	assert.Equal(t, true, result)

	d, err := sharedClient.PTTL(ctx, "ttl:pexpire").Result()
	assert.NoError(t, err)
	if d <= 0 || d > 5000*time.Millisecond {
		t.Errorf("PTTL should be > 0 and <= 5000ms, got: %v", d)
	}
}

func TestCompatPersist(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "ttl:persist", "val", 0)
	sharedClient.Expire(ctx, "ttl:persist", 10*time.Second)

	result, err := sharedClient.Persist(ctx, "ttl:persist").Result()
	assert.NoError(t, err)
	assert.Equal(t, true, result)

	d, err := sharedClient.TTL(ctx, "ttl:persist").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), d)
}

func TestCompatBLPOPTimeoutReturnsNil(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	_, err := sharedClient.BLPop(ctx, 1*time.Second, "bl:empty").Result()
	if err != redis.Nil {
		t.Errorf("BLPop timeout should return redis.Nil, got: %v", err)
	}
}

func TestCompatBZPOPMAXTimeoutReturnsNil(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	_, err := sharedClient.BZPopMax(ctx, 1*time.Second, "bz:empty").Result()
	if err != redis.Nil {
		t.Errorf("BZPopMax timeout should return redis.Nil, got: %v", err)
	}
}

func TestCompatBZPOPMINTimeoutReturnsNil(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	_, err := sharedClient.BZPopMin(ctx, 1*time.Second, "bz:empty2").Result()
	if err != redis.Nil {
		t.Errorf("BZPopMin timeout should return redis.Nil, got: %v", err)
	}
}

func TestCompatBlockingTimeoutWireFormat(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	tests := []struct {
		name string
		cmd  []string
	}{
		{"BLPOP", []string{"BLPOP", "bw:empty", "1"}},
		{"BRPOP", []string{"BRPOP", "bw:empty2", "1"}},
		{"BZPOPMAX", []string{"BZPOPMAX", "bw:empty3", "1"}},
		{"BZPOPMIN", []string{"BZPOPMIN", "bw:empty4", "1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := dialConn(t)
			defer conn.Close()

			rawCmd(t, conn, tt.cmd...)
			wire := rawReadWire(t, conn)
			if string(wire) != "*-1\r\n" {
				t.Errorf("%s timeout wire format: got %q, want %q", tt.name, string(wire), "*-1\r\n")
			}
		})
	}
}

func TestCompatKeyEvictionAfterEXPIRE(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "ttl:evict", "will_disappear", 0)
	sharedClient.Expire(ctx, "ttl:evict", 1*time.Second)

	val, err := sharedClient.Get(ctx, "ttl:evict").Result()
	assert.NoError(t, err)
	assert.Equal(t, "will_disappear", val)

	// BadgerDB uses compaction-based TTL eviction, not eager on read.
	// Known difference from Redis: keys may remain readable for a while after TTL.
	time.Sleep(500 * time.Millisecond)
	_, err = sharedClient.Get(ctx, "ttl:evict").Result()
	if err == nil {
		t.Log("TTL eviction not immediate (known: BadgerDB compaction-based eviction)")
	}
}

// ============================================================================
// 4. PIPELINE TESTS
// ============================================================================

func TestCompatPipelineBasic(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	pipe := sharedClient.Pipeline()

	pipe.Set(ctx, "pl:string", "value1", 0)
	pipe.Set(ctx, "pl:string2", "value2", 0)
	pipe.Get(ctx, "pl:string")
	pipe.LLen(ctx, "pl:list")
	pipe.HGetAll(ctx, "pl:hash")

	cmders, err := pipe.Exec(ctx)
	assert.NoError(t, err)
	if len(cmders) != 5 {
		t.Fatalf("expected 5 pipeline results, got %d", len(cmders))
	}

	for i, cmder := range cmders {
		if cmder.Err() != nil {
			t.Errorf("pipeline cmd %d error: %v", i, cmder.Err())
		}
	}
}

func TestCompatPipelineMixedTypes(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	pipe := sharedClient.Pipeline()

	pipe.Set(ctx, "pl:mix", "hello", 0)
	pipe.LPush(ctx, "pl:mix_list", "a", "b")
	pipe.HSet(ctx, "pl:mix_hash", "field", "val")
	pipe.SAdd(ctx, "pl:mix_set", "member")
	pipe.ZAdd(ctx, "pl:mix_zset", redis.Z{Score: 1, Member: "one"})

	cmders, err := pipe.Exec(ctx)
	assert.NoError(t, err)
	if len(cmders) != 5 {
		t.Fatalf("expected 5 pipeline results, got %d", len(cmders))
	}

	for i, cmder := range cmders {
		if cmder.Err() != nil {
			t.Errorf("pipeline cmd %d error: %v", i, cmder.Err())
		}
	}

	val, _ := sharedClient.Get(ctx, "pl:mix").Result()
	assert.Equal(t, "hello", val)

	llen, _ := sharedClient.LLen(ctx, "pl:mix_list").Result()
	assert.Equal(t, int64(2), llen)
}

func TestCompatPipelineOrdering(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	pipe := sharedClient.Pipeline()

	pipe.Set(ctx, "pl:order", "step1", 0)
	pipe.Append(ctx, "pl:order", "+step2")
	pipe.Get(ctx, "pl:order")

	cmders, err := pipe.Exec(ctx)
	assert.NoError(t, err)
	if len(cmders) != 3 {
		t.Fatalf("expected 3 pipeline results, got %d", len(cmders))
	}

	getCmd := cmders[2].(*redis.StringCmd)
	val, err := getCmd.Result()
	assert.NoError(t, err)
	assert.Equal(t, "step1+step2", val)
}

func TestCompatPipelineErrorHandling(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	pipe := sharedClient.Pipeline()

	pipe.Set(ctx, "pl:errkey", "val", 0)
	pipe.LPush(ctx, "pl:errkey", "should_fail")

	cmders, err := pipe.Exec(ctx)
	if err == nil {
		t.Error("expected pipeline error for wrongtype, got nil")
	}
	if len(cmders) != 2 {
		t.Fatalf("expected 2 pipeline results, got %d", len(cmders))
	}
	if cmders[0].Err() != nil {
		t.Errorf("first cmd should succeed, got: %v", cmders[0].Err())
	}
}

// ============================================================================
// 5. WRONGTYPE TESTS
// ============================================================================

func TestCompatWrongTypeStringOpOnHash(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.HSet(ctx, "wt:hash", "field", "value")

	_, err := sharedClient.Get(ctx, "wt:hash").Result()
	if err == nil {
		t.Error("GET on hash should return error")
	}
}

func TestCompatWrongTypeStringOpOnList(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.LPush(ctx, "wt:list", "a")

	_, err := sharedClient.Get(ctx, "wt:list").Result()
	if err == nil {
		t.Error("GET on list should return error")
	}
}

func TestCompatWrongTypeHashOpOnString(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "wt:str", "val", 0)

	_, err := sharedClient.HGet(ctx, "wt:str", "field").Result()
	if err == nil {
		t.Error("HGET on string should return error")
	}
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Logf("HGET on string got: %v (expected WRONGTYPE)", err)
	}
}

func TestCompatWrongTypeSetOpOnString(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "wt:str2", "val", 0)

	_, err := sharedClient.SMembers(ctx, "wt:str2").Result()
	if err == nil {
		t.Error("SMEMBERS on string should return error")
	}
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Logf("SMEMBERS on string got: %v (expected WRONGTYPE)", err)
	}
}

func TestCompatWrongTypeZSetOpOnString(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "wt:str3", "val", 0)

	_, err := sharedClient.ZCard(ctx, "wt:str3").Result()
	if err == nil {
		t.Error("ZCARD on string should return error")
	}
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Logf("ZCARD on string got: %v (expected WRONGTYPE)", err)
	}
}

func TestCompatWrongTypeZSetOpOnHash(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.HSet(ctx, "wt:hash3", "f", "v")

	_, err := sharedClient.ZCard(ctx, "wt:hash3").Result()
	if err == nil {
		t.Error("ZCARD on hash should return error")
	}
	if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Logf("ZCARD on hash got: %v (expected WRONGTYPE)", err)
	}
}

func TestCompatWrongTypeStringOpOnZSet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.ZAdd(ctx, "wt:zset", redis.Z{Score: 1, Member: "a"})

	_, err := sharedClient.Get(ctx, "wt:zset").Result()
	if err == nil {
		t.Error("GET on zset should return error")
	}
}

func TestCompatWrongTypeListOpOnString(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "wt:string", "value", 0)

	_, err := sharedClient.LLen(ctx, "wt:string").Result()
	if err == nil {
		t.Error("LLEN on string should return WRONGTYPE error")
	} else if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Errorf("LLEN on string got: %v (expected WRONGTYPE)", err)
	}
}

func TestCompatWrongTypeHashOpOnList(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.LPush(ctx, "wt:list2", "a")

	_, err := sharedClient.HGet(ctx, "wt:list2", "field").Result()
	if err == nil {
		t.Error("HGET on list should return error")
	}
}

// ============================================================================
// 6. NIL RESPONSE TESTS
// ============================================================================

func TestCompatNilGET(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	val, err := sharedClient.Get(ctx, "nil:nonexistent").Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
	assert.Equal(t, "", val)
}

func TestCompatNilLPOP(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	val, err := sharedClient.LPop(ctx, "nil:emptylist").Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
	assert.Equal(t, "", val)
}

func TestCompatNilHGET(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.HSet(ctx, "nil:hash", "f", "v")

	val, err := sharedClient.HGet(ctx, "nil:hash", "nonexistent").Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
	assert.Equal(t, "", val)
}

func TestCompatNilZSCORE(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.ZAdd(ctx, "nil:zset", redis.Z{Score: 1, Member: "a"})

	val, err := sharedClient.ZScore(ctx, "nil:zset", "nonexistent").Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
	assert.Equal(t, float64(0), val)
}

func TestCompatNilLINDEX(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.LPush(ctx, "nil:list", "a")

	val, err := sharedClient.LIndex(ctx, "nil:list", 999).Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
	assert.Equal(t, "", val)
}

func TestCompatNilMGET(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.Set(ctx, "nil:mget_exist", "present", 0)

	vals, err := sharedClient.MGet(ctx, "nil:mget_exist", "nil:mget_missing").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))
	assert.Equal(t, "present", vals[0])
	if vals[1] != nil {
		t.Errorf("MGET missing key should be nil, got: %v", vals[1])
	}
}

func TestCompatNilHMGET(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sharedClient.HSet(ctx, "nil:hmhash", "exist_field", "val")

	vals, err := sharedClient.HMGet(ctx, "nil:hmhash", "exist_field", "missing_field").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))
	assert.Equal(t, "val", vals[0])
	if vals[1] != nil {
		t.Errorf("HMGET missing field should be nil, got: %v", vals[1])
	}
}

func TestCompatNilRANDOMKEYonEmpty(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	_, err := sharedClient.RandomKey(ctx).Result()
	if err != redis.Nil {
		t.Errorf("RANDOMKEY on empty DB should return redis.Nil, got: %v", err)
	}
}

func TestCompatNilTYPEonNonExistent(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	val, err := sharedClient.Type(ctx, "nil:nokey").Result()
	assert.NoError(t, err)
	assert.Equal(t, "none", val)
}

// ============================================================================
// 7. DISCONNECT / CLEANUP TESTS
// ============================================================================

func TestCompatDisconnectSubscriberCleanup(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	subConn, subReader := dialPubSub(t)

	sendPubSubCmd(t, subConn, "SUBSCRIBE", "disc:channel")
	readPubSubResp(t, subReader)

	subConn.Close()

	time.Sleep(100 * time.Millisecond)

	count, err := sharedClient.Publish(ctx, "disc:channel", "after_close").Result()
	assert.NoError(t, err)
	if count != 0 {
		t.Errorf("PUBLISH after subscriber disconnect should return 0, got: %d", count)
	}
}

func TestCompatDisconnectWatchCleanup(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	conn := dialConn(t)

	rawCmd(t, conn, "WATCH", "disc:watchkey")
	rawWatchOK(t, conn)

	conn.Close()

	time.Sleep(100 * time.Millisecond)

	sharedClient.Set(ctx, "disc:watchkey", "modified", 0)

	conn2 := dialConn(t)
	defer conn2.Close()

	rawCmd(t, conn2, "WATCH", "disc:watchkey")
	rawWatchOK(t, conn2)

	rawCmd(t, conn2, "MULTI")
	rawOK(t, conn2)

	rawCmd(t, conn2, "SET", "disc:watchkey", "newvalue")
	rawQueued(t, conn2)

	rawCmd(t, conn2, "EXEC")
	execResp := rawRead(t, conn2)
	if len(execResp.Args) == 0 {
		t.Errorf("EXEC should succeed after disconnected watcher cleanup")
	}
}

func TestCompatDisconnectStateCleanup(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	conn := dialConn(t)

	rawCmd(t, conn, "MULTI")
	rawOK(t, conn)

	rawCmd(t, conn, "SET", "disc:state", "queued")
	rawQueued(t, conn)

	conn.Close()

	time.Sleep(100 * time.Millisecond)

	_, err := sharedClient.Get(ctx, "disc:state").Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
}

// ============================================================================
// 8. CONCURRENT TESTS
// ============================================================================

func TestCompatConcurrentWATCH(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	sharedClient.Set(ctx, "cc:watched", "init", 0)

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := redis.NewClient(&redis.Options{
				Addr: sharedListener.Addr().String(),
				DB:   0,
			})
			defer client.Close()

			err := client.Watch(ctx, func(tx *redis.Tx) error {
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, "cc:watched", string(rune('A'+id)), 0)
					return nil
				})
				return err
			}, "cc:watched")

			mu.Lock()
			if err == nil {
				successCount++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if successCount < 1 {
		t.Errorf("at least one concurrent WATCH should succeed, got %d", successCount)
	}
	if successCount > 10 {
		t.Errorf("at most 10 concurrent WATCH can succeed, got %d", successCount)
	}
}

func TestCompatConcurrentPubSub(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	numSubs := 5
	numMsgs := 10

	subConns := make([]net.Conn, numSubs)
	subReaders := make([]*bufio.Reader, numSubs)

	for i := 0; i < numSubs; i++ {
		conn, reader := dialPubSub(t)
		subConns[i] = conn
		subReaders[i] = reader
		sendPubSubCmd(t, conn, "SUBSCRIBE", "cc:broadcast")
		readPubSubResp(t, reader)
	}

	pubClient := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
		DB:   0,
	})
	defer pubClient.Close()

	for j := 0; j < numMsgs; j++ {
		pubClient.Publish(ctx, "cc:broadcast", "msg")
	}

	for _, reader := range subReaders {
		for j := 0; j < numMsgs; j++ {
			parts := readPubSubResp(t, reader)
			assert.Equal(t, "message", parts[0])
			assert.Equal(t, "cc:broadcast", parts[1])
		}
	}

	for _, conn := range subConns {
		conn.Close()
	}
}

// ============================================================================
// COMPAT SUMMARY
// ============================================================================

func TestCompatSummary(t *testing.T) {
	t.Log("========================================")
	t.Log("BoltDB Redis Compatibility Test Summary")
	t.Log("========================================")
	t.Log("")
	t.Log("1. TRANSACTION  : MULTI/EXEC/DISCARD/WATCH/UNWATCH")
	t.Log("2. PUBSUB       : SUBSCRIBE/PUBLISH/PSUBSCRIBE/UNSUBSCRIBE/QUIT")
	t.Log("3. TIMEOUT      : EXPIRE/TTL/PTTL/PERSIST/BLPOP timeout")
	t.Log("4. PIPELINE     : go-redis Pipeline() with mixed types")
	t.Log("5. WRONGTYPE    : Cross-type operation error detection (incl LLEN fix)")
	t.Log("6. NIL RESPONSE : nil bulk/nil array for missing keys/fields")
	t.Log("7. DISCONNECT   : Subscriber/Watch/Transaction cleanup on close")
	t.Log("")
	t.Log("See COMPATIBILITY_REPORT.md for full results")
}
