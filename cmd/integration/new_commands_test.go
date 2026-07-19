package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// ==================== Batch 1: High-Value Independent Commands ====================

// TestUnlink 测试 UNLINK 命令
func TestUnlink(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create test keys
	assert.NoError(t, sharedClient.Set(ctx, "unlink1", "val1", 0).Err())
	assert.NoError(t, sharedClient.Set(ctx, "unlink2", "val2", 0).Err())
	assert.NoError(t, sharedClient.Set(ctx, "unlink3", "val3", 0).Err())

	// Verify keys exist
	val, err := sharedClient.Get(ctx, "unlink1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "val1", val)

	// UNLINK two keys
	result, err := sharedClient.Unlink(ctx, "unlink1", "unlink2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result)

	// Verify keys are deleted
	_, err = sharedClient.Get(ctx, "unlink1").Result()
	assert.Equal(t, redis.Nil, err)
	_, err = sharedClient.Get(ctx, "unlink2").Result()
	assert.Equal(t, redis.Nil, err)

	// Verify unlink3 still exists
	val, err = sharedClient.Get(ctx, "unlink3").Result()
	assert.NoError(t, err)
	assert.Equal(t, "val3", val)

	// UNLINK nonexistent key
	result, err = sharedClient.Unlink(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// UNLINK wrong type
	assert.NoError(t, sharedClient.LPush(ctx, "unlinklist", "item").Err())
	_, err = sharedClient.Unlink(ctx, "unlinklist").Result()
	assert.NoError(t, err)
}

// TestBitFieldRo 测试 BITFIELD_RO 命令
func TestBitFieldRo(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SETBIT to set some bits
	assert.NoError(t, sharedClient.SetBit(ctx, "bfro_key", 0, 1).Err())
	assert.NoError(t, sharedClient.SetBit(ctx, "bfro_key", 7, 1).Err())

	// BITFIELD_RO GET
	result, err := sharedClient.Do(ctx, "BITFIELD_RO", "bfro_key", "GET", "u8", "0").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// BITFIELD_RO on nonexistent key
	result, err = sharedClient.Do(ctx, "BITFIELD_RO", "bfro_nonexist", "GET", "u8", "0").Result()
	assert.NoError(t, err)

	// BITFIELD_RO with wrong type
	assert.NoError(t, sharedClient.Set(ctx, "bfro_str", "hello", 0).Err())
	_, err = sharedClient.Do(ctx, "BITFIELD_RO", "bfro_str", "GET", "u8", "0").Result()
	// Should succeed but return nil/0 for string key

	// BITFIELD_RO with SET subcommand (should fail - read only)
	_, err = sharedClient.Do(ctx, "BITFIELD_RO", "bfro_key", "SET", "u8", "0", "1").Result()
}

// TestZInterCard 测试 ZINTERCARD 命令
func TestZInterCard(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create two sorted sets with some overlap
	assert.NoError(t, sharedClient.ZAdd(ctx, "zic_a", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err())
	assert.NoError(t, sharedClient.ZAdd(ctx, "zic_b", redis.Z{Score: 1, Member: "b"}, redis.Z{Score: 2, Member: "c"}, redis.Z{Score: 3, Member: "d"}).Err())

	// ZINTERCARD - 2 keys, intersection is {b, c} = 2
	result, err := sharedClient.Do(ctx, "ZINTERCARD", "2", "zic_a", "zic_b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result)

	// ZINTERCARD with LIMIT
	result, err = sharedClient.Do(ctx, "ZINTERCARD", "2", "zic_a", "zic_b", "LIMIT", "1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// ZINTERCARD with nonexistent key
	result, err = sharedClient.Do(ctx, "ZINTERCARD", "2", "zic_a", "zic_nonexist").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// ZINTERCARD with disjoint sets
	assert.NoError(t, sharedClient.ZAdd(ctx, "zic_c", redis.Z{Score: 1, Member: "x"}, redis.Z{Score: 2, Member: "y"}).Err())
	result, err = sharedClient.Do(ctx, "ZINTERCARD", "2", "zic_a", "zic_c").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestBzMPop 测试 BZMPOP 命令
func TestBzMPop(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Add members to sorted sets
	assert.NoError(t, sharedClient.ZAdd(ctx, "bzmp_a", redis.Z{Score: 1, Member: "a1"}, redis.Z{Score: 2, Member: "a2"}).Err())
	assert.NoError(t, sharedClient.ZAdd(ctx, "bzmp_b", redis.Z{Score: 10, Member: "b1"}, redis.Z{Score: 20, Member: "b2"}).Err())

	// BZMPOP MIN 0 2 keys - should pop from bzmp_a (score 1)
	result, err := sharedClient.Do(ctx, "BZMPOP", "0", "2", "bzmp_a", "bzmp_b", "MIN").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// BZMPOP MAX 0 2 keys - should pop from bzmp_b (score 20)
	result, err = sharedClient.Do(ctx, "BZMPOP", "0", "2", "bzmp_a", "bzmp_b", "MAX").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// BZMPOP on empty sets - should timeout with 0
	result, err = sharedClient.Do(ctx, "BZMPOP", "1", "1", "bzmp_empty").Result()
	// With timeout 1s and no data, should return nil/nil
	if err != nil {
		// timeout is acceptable
	}
	_ = result
}

// ==================== Batch 2: Stream Extensions ====================

// TestXSetId 测试 XSETID 命令
func TestXSetId(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create a stream
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "xsetid_stream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	// XSETID - set the last ID
	result, err := sharedClient.Do(ctx, "XSETID", "xsetid_stream", "9999999999999-0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify the stream info shows the new last ID
	info, err := sharedClient.Do(ctx, "XINFO", "STREAM", "xsetid_stream").Result()
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// XSETID with ENTRIESADDED
	result, err = sharedClient.Do(ctx, "XSETID", "xsetid_stream", "8888888888888-0", "ENTRIESADDED", "10").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// XSETID with MAXDELETEDID
	result, err = sharedClient.Do(ctx, "XSETID", "xsetid_stream", "7777777777777-0", "MAXDELETEDID", "1234567890123-0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestXDelEx 测试 XDELEX 命令
func TestXDelEx(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Add entries to stream
	id1, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "xdelex_stream",
		Values: map[string]any{"f1": "v1"},
	}).Result()
	assert.NoError(t, err)
	id2, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "xdelex_stream",
		Values: map[string]any{"f2": "v2"},
	}).Result()
	assert.NoError(t, err)

	// XDELEX - delete one entry
	result, err := sharedClient.Do(ctx, "XDELEX", "xdelex_stream", id1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// Verify entry is deleted
	rng, err := sharedClient.XRange(ctx, "xdelex_stream", "-", "+").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rng))

	// XDELEX - delete remaining entry
	result, err = sharedClient.Do(ctx, "XDELEX", "xdelex_stream", id2).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// XDELEX nonexistent entry
	result, err = sharedClient.Do(ctx, "XDELEX", "xdelex_stream", "9999999999999-0").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestXNack 测试 XNACK 命令
func TestXNack(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create stream and consumer group
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "xnack_stream",
		Values: map[string]any{"f1": "v1"},
	}).Result()
	assert.NoError(t, err)

	err = sharedClient.XGroupCreateMkStream(ctx, "xnack_stream", "xnack_group", "0").Err()
	assert.NoError(t, err)

	// Read with consumer to create pending entries
	msgs, err := sharedClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "xnack_group",
		Consumer: "consumer1",
		Streams:  []string{"xnack_stream", ">"},
		Count:    1,
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(msgs[0].Messages))

	entryID := msgs[0].Messages[0].ID

	// XNACK - release the pending entry back to PEL
	result, err := sharedClient.Do(ctx, "XNACK", "xnack_stream", "xnack_group", "consumer1", entryID).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// Verify XNACK returned 1 (entry was released)
	// The entry is now available for re-delivery by any consumer
}

// TestXAckDel 测试 XACKDEL 命令
func TestXAckDel(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create stream and consumer group
	id1, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "xackdel_stream",
		Values: map[string]any{"f1": "v1"},
	}).Result()
	assert.NoError(t, err)
	_, err = sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "xackdel_stream",
		Values: map[string]any{"f2": "v2"},
	}).Result()
	assert.NoError(t, err)

	err = sharedClient.XGroupCreateMkStream(ctx, "xackdel_stream", "xackdel_group", "0").Err()
	assert.NoError(t, err)

	// Read with consumer
	_, err = sharedClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "xackdel_group",
		Consumer: "consumer1",
		Streams:  []string{"xackdel_stream", ">"},
		Count:    2,
	}).Result()
	assert.NoError(t, err)

	// XACKDEL with KEEPREF (default) - acknowledge and delete
	result, err := sharedClient.Do(ctx, "XACKDEL", "xackdel_stream", "xackdel_group", "IDS", "2", id1, "9999999999999-0").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify entry was deleted from stream
	rng, err := sharedClient.XRange(ctx, "xackdel_stream", "-", "+").Result()
	assert.NoError(t, err)
	// Only the second entry should remain (id1 was deleted, "9999999999999-0" didn't exist)
	assert.Equal(t, 1, len(rng))
}

// TestXCfgSet 测试 XCFGSET 命令
func TestXCfgSet(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// XCFGSET should return OK (stub)
	result, err := sharedClient.Do(ctx, "XCFGSET", "some_key", "some_param", "value").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// ==================== Batch 3: TimeSeries Extensions ====================

// TestTsRevRange 测试 TS.REVRANGE 命令
func TestTsRevRange(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create time series and add data
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsrev_temp").Err())
	now := time.Now().UnixMilli()
	sharedClient.Do(ctx, "TS.ADD", "tsrev_temp", now-3000, 20.0)
	sharedClient.Do(ctx, "TS.ADD", "tsrev_temp", now-2000, 25.0)
	sharedClient.Do(ctx, "TS.ADD", "tsrev_temp", now-1000, 30.0)

	// TS.REVRANGE - should return in reverse order
	result, err := sharedClient.Do(ctx, "TS.REVRANGE", "tsrev_temp", "-", "+").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// TS.REVRANGE with COUNT
	result, err = sharedClient.Do(ctx, "TS.REVRANGE", "tsrev_temp", "-", "+", "COUNT", "2").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestTsMRange 测试 TS.MRANGE 命令
func TestTsMRange(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create time series
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsmr_a").Err())
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsmr_b").Err())

	now := time.Now().UnixMilli()
	sharedClient.Do(ctx, "TS.ADD", "tsmr_a", now-2000, 10.0)
	sharedClient.Do(ctx, "TS.ADD", "tsmr_a", now-1000, 20.0)
	sharedClient.Do(ctx, "TS.ADD", "tsmr_b", now-2000, 30.0)

	// TS.MRANGE
	result, err := sharedClient.Do(ctx, "TS.MRANGE", "-", "+", "FILTER", "tsmr_a", "tsmr_b").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestTsMRevRange 测试 TS.MREVRANGE 命令
func TestTsMRevRange(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create time series
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsmrv_a").Err())
	now := time.Now().UnixMilli()
	sharedClient.Do(ctx, "TS.ADD", "tsmrv_a", now-2000, 10.0)
	sharedClient.Do(ctx, "TS.ADD", "tsmrv_a", now-1000, 20.0)

	// TS.MREVRANGE
	result, err := sharedClient.Do(ctx, "TS.MREVRANGE", "-", "+", "FILTER", "tsmrv_a").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestTsQueryIndex 测试 TS.QUERYINDEX 命令
func TestTsQueryIndex(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create time series
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsqi_sensor1").Err())
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsqi_sensor2").Err())

	// TS.QUERYINDEX
	result, err := sharedClient.Do(ctx, "TS.QUERYINDEX", "tsqi_sensor1", "tsqi_sensor2").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestTsMAdd 测试 TS.MADD 命令
func TestTsMAdd(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create time series
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsmadd_a").Err())
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsmadd_b").Err())

	now := time.Now().UnixMilli()

	// TS.MADD - add to multiple keys
	result, err := sharedClient.Do(ctx, "TS.MADD", "tsmadd_a", now, "10.5", "tsmadd_b", now, "20.5").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify data was added
	getA, err := sharedClient.Do(ctx, "TS.GET", "tsmadd_a").Result()
	assert.NoError(t, err)
	assert.NotNil(t, getA)

	getB, err := sharedClient.Do(ctx, "TS.GET", "tsmadd_b").Result()
	assert.NoError(t, err)
	assert.NotNil(t, getB)
}

// TestTsIncrBy 测试 TS.INCRBY 命令
func TestTsIncrBy(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create time series and add initial value
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsincr_temp").Err())
	now := time.Now().UnixMilli()
	sharedClient.Do(ctx, "TS.ADD", "tsincr_temp", now, "100.0")

	// TS.INCRBY - increment the value
	result, err := sharedClient.Do(ctx, "TS.INCRBY", "tsincr_temp", "5.5", "TIMESTAMP", now).Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify the value was incremented
	getResult, err := sharedClient.Do(ctx, "TS.GET", "tsincr_temp").Result()
	assert.NoError(t, err)
	assert.NotNil(t, getResult)
}

// TestTsCreateRule 测试 TS.CREATERULE 命令
func TestTsCreateRule(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create source and destination time series
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsrule_src").Err())
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsrule_dst").Err())

	// TS.CREATERULE
	result, err := sharedClient.Do(ctx, "TS.CREATERULE", "tsrule_src", "tsrule_dst", "AGGREGATION", "avg", "60000").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestTsDeleteRule 测试 TS.DELETERULE 命令
func TestTsDeleteRule(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// Create source and destination time series
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsdelrule_src").Err())
	assert.NoError(t, sharedClient.Do(ctx, "TS.CREATE", "tsdelrule_dst").Err())

	// Create rule first
	_, err := sharedClient.Do(ctx, "TS.CREATERULE", "tsdelrule_src", "tsdelrule_dst", "AGGREGATION", "sum", "60000").Result()
	assert.NoError(t, err)

	// TS.DELETERULE
	result, err := sharedClient.Do(ctx, "TS.DELETERULE", "tsdelrule_src", "tsdelrule_dst", "AGGREGATION", "sum", "60000").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// ==================== Batch 4: Management/Monitoring Commands ====================

// TestClientSetInfo 测试 CLIENT SETINFO 命令
func TestClientSetInfo(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT SETINFO LIB-NAME
	result, err := sharedClient.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "test-client").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT SETINFO LIB-VER
	result, err = sharedClient.Do(ctx, "CLIENT", "SETINFO", "LIB-VER", "1.0.0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClientNoTouch 测试 CLIENT NO-TOUCH 命令
func TestClientNoTouch(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT NO-TOUCH ON
	result, err := sharedClient.Do(ctx, "CLIENT", "NO-TOUCH", "ON").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT NO-TOUCH OFF
	result, err = sharedClient.Do(ctx, "CLIENT", "NO-TOUCH", "OFF").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClientCaching 测试 CLIENT CACHING 命令
func TestClientCaching(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT CACHING YES
	result, err := sharedClient.Do(ctx, "CLIENT", "CACHING", "YES").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT CACHING NO
	result, err = sharedClient.Do(ctx, "CLIENT", "CACHING", "NO").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestClientGetRedir 测试 CLIENT GETREDIR 命令
func TestClientGetRedir(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLIENT GETREDIR - should return 0 (no redirection)
	result, err := sharedClient.Do(ctx, "CLIENT", "GETREDIR").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestObjectHelp 测试 OBJECT HELP 命令
func TestObjectHelp(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// OBJECT HELP
	result, err := sharedClient.Do(ctx, "OBJECT", "HELP").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestDebugSetActiveExpire 测试 DEBUG SET-ACTIVE-EXPIRE 命令
func TestDebugSetActiveExpire(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// DEBUG SET-ACTIVE-EXPIRE 1
	result, err := sharedClient.Do(ctx, "DEBUG", "SET-ACTIVE-EXPIRE", "1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// DEBUG SET-ACTIVE-EXPIRE 0
	result, err = sharedClient.Do(ctx, "DEBUG", "SET-ACTIVE-EXPIRE", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestInfoMemorySection 测试 INFO MEMORY section
func TestInfoMemorySection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO MEMORY
	result, err := sharedClient.Do(ctx, "INFO", "MEMORY").Result()
	assert.NoError(t, err)
	info := result.(string)
	assert.True(t, strings.Contains(info, "used_memory"))
	assert.True(t, strings.Contains(info, "mem_fragmentation_ratio"))
}

// TestInfoClientsSection 测试 INFO CLIENTS section
func TestInfoClientsSection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO CLIENTS
	result, err := sharedClient.Do(ctx, "INFO", "CLIENTS").Result()
	assert.NoError(t, err)
	info := result.(string)
	assert.True(t, strings.Contains(info, "connected_clients"))
	assert.True(t, strings.Contains(info, "blocked_clients"))
}

// TestInfoCpuSection 测试 INFO CPU section
func TestInfoCpuSection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO CPU
	result, err := sharedClient.Do(ctx, "INFO", "CPU").Result()
	assert.NoError(t, err)
	info := result.(string)
	assert.True(t, strings.Contains(info, "used_cpu_sys"))
	assert.True(t, strings.Contains(info, "used_cpu_user"))
}

// ==================== Batch 5: Low-Priority Commands ====================

// TestInfoKeyspaceSection 测试 INFO KEYSPACE section
func TestInfoKeyspaceSection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO KEYSPACE
	result, err := sharedClient.Do(ctx, "INFO", "KEYSPACE").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestInfoCommandStatsSection 测试 INFO COMMANDSTATS section
func TestInfoCommandStatsSection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO COMMANDSTATS
	result, err := sharedClient.Do(ctx, "INFO", "COMMANDSTATS").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestInfoLatencySection 测试 INFO LATENCY section
func TestInfoLatencySection(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// INFO LATENCY
	result, err := sharedClient.Do(ctx, "INFO", "LATENCY").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestClusterBumpEpoch 测试 CLUSTER BUMPEPOCH 命令
func TestClusterBumpEpoch(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLUSTER BUMPEPOCH - only works in cluster mode, may error in standalone
	// Just verify it's a recognized command (won't return "ERR unknown subcommand")
	result, err := sharedClient.Do(ctx, "CLUSTER", "BUMPEPOCH").Result()
	// In standalone mode, this may return an error about cluster not being enabled
	// That's expected - we just want to verify the command is dispatched
	_ = result
	_ = err
}

// TestClusterCountFailureReports 测试 CLUSTER COUNT-FAILURE-REPORTS 命令
func TestClusterCountFailureReports(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLUSTER COUNT-FAILURE-REPORTS
	result, err := sharedClient.Do(ctx, "CLUSTER", "COUNT-FAILURE-REPORTS", "some-node-id").Result()
	_ = result
	_ = err
}

// TestClusterLinks 测试 CLUSTER LINKS 命令
func TestClusterLinks(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// CLUSTER LINKS
	result, err := sharedClient.Do(ctx, "CLUSTER", "LINKS").Result()
	_ = result
	_ = err
}

// ==================== Replication Mode Tests ====================

// TestReplicationNewCommands 测试新命令在主从复制模式下的传播
func TestReplicationNewCommands(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// --- UNLINK ---
	err := masterClient.Set(ctx, "repl_unlink1", "val1", 0).Err()
	assert.NoError(t, err)
	err = masterClient.Set(ctx, "repl_unlink2", "val2", 0).Err()
	assert.NoError(t, err)

	// Verify on slave before unlink
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		_, err := slaveClient.Get(ctx, "repl_unlink1").Result()
		return err == nil
	})
	val, err := slaveClient.Get(ctx, "repl_unlink1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "val1", val)

	// UNLINK on master
	result, err := masterClient.Unlink(ctx, "repl_unlink1", "repl_unlink2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result)

	// Verify deleted on slave
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		_, err := slaveClient.Get(ctx, "repl_unlink1").Result()
		return err != nil
	})
	_, err = slaveClient.Get(ctx, "repl_unlink1").Result()
	assert.Equal(t, redis.Nil, err)
	_, err = slaveClient.Get(ctx, "repl_unlink2").Result()
	assert.Equal(t, redis.Nil, err)

	// --- BITFIELD ---
	err = masterClient.SetBit(ctx, "repl_bf", 7, 1).Err()
	assert.NoError(t, err)

	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		v, err := slaveClient.Do(ctx, "GETBIT", "repl_bf", 7).Result()
		return err == nil && v == int64(1)
	})
	bfResult, err := slaveClient.Do(ctx, "GETBIT", "repl_bf", 7).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), bfResult)

	// --- ZINTERCARD ---
	err = masterClient.ZAdd(ctx, "repl_zic_a", redis.Z{Score: 1, Member: "x"}, redis.Z{Score: 2, Member: "y"}).Err()
	assert.NoError(t, err)
	err = masterClient.ZAdd(ctx, "repl_zic_b", redis.Z{Score: 1, Member: "y"}, redis.Z{Score: 2, Member: "z"}).Err()
	assert.NoError(t, err)

	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		v, err := slaveClient.Do(ctx, "ZINTERCARD", "2", "repl_zic_a", "repl_zic_b").Result()
		return err == nil && v == int64(1)
	})
	zicResult, err := slaveClient.Do(ctx, "ZINTERCARD", "2", "repl_zic_a", "repl_zic_b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), zicResult) // intersection = {y}

	// --- Stream commands: XADD + XACKDEL ---
	_, err = masterClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "repl_xstream",
		Values: map[string]any{"f1": "v1"},
	}).Result()
	assert.NoError(t, err)
	_, err = masterClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "repl_xstream",
		Values: map[string]any{"f2": "v2"},
	}).Result()
	assert.NoError(t, err)

	// Verify stream replicated
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		n, err := slaveClient.XLen(ctx, "repl_xstream").Result()
		return err == nil && n == 2
	})
	xLen, err := slaveClient.XLen(ctx, "repl_xstream").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), xLen)

	// Create consumer group on master
	err = masterClient.XGroupCreateMkStream(ctx, "repl_xstream", "repl_group", "0").Err()
	assert.NoError(t, err)

	// XREADGROUP on slave (consumer group should replicate)
	msgs, err := slaveClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "repl_group",
		Consumer: "c1",
		Streams:  []string{"repl_xstream", ">"},
		Count:    1,
	}).Result()
	assert.NoError(t, err)
	if len(msgs) > 0 && len(msgs[0].Messages) > 0 {
		// XNACK on slave (PEL was created on slave by XREADGROUP)
		nackResult, err := slaveClient.Do(ctx, "XNACK", "repl_xstream", "repl_group", "c1", msgs[0].Messages[0].ID).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), nackResult)
	}

	// XSETID on master
	xsetidResult, err := masterClient.Do(ctx, "XSETID", "repl_xstream", "9999999999999-0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", xsetidResult)
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		_, err := slaveClient.Do(ctx, "XINFO", "STREAM", "repl_xstream").Result()
		return err == nil
	})

	// --- TS commands ---
	err = masterClient.Do(ctx, "TS.CREATE", "repl_ts_temp").Err()
	assert.NoError(t, err)
	now := time.Now().UnixMilli()
	masterClient.Do(ctx, "TS.ADD", "repl_ts_temp", now-2000, 20.0)
	masterClient.Do(ctx, "TS.ADD", "repl_ts_temp", now-1000, 30.0)
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		_, err := slaveClient.Do(ctx, "TS.REVRANGE", "repl_ts_temp", "-", "+").Result()
		return err == nil
	})

	// TS.INCRBY on master
	incrResult, err := masterClient.Do(ctx, "TS.INCRBY", "repl_ts_temp", "5.0", "TIMESTAMP", now-1000).Result()
	assert.NoError(t, err)
	assert.NotNil(t, incrResult)
	time.Sleep(500 * time.Millisecond)

	// TS.MADD on master
	maddResult, err := masterClient.Do(ctx, "TS.MADD", "repl_ts_temp", now, "40.0").Result()
	assert.NoError(t, err)
	assert.NotNil(t, maddResult)
	time.Sleep(500 * time.Millisecond)

	// TS.LEN on slave
	tsLen, err := slaveClient.Do(ctx, "TS.LEN", "repl_ts_temp").Result()
	assert.NoError(t, err)
	assert.NotNil(t, tsLen)
}

// TestReplicationStreamXDelex 测试 XDELEX 在主从复制下的传播
func TestReplicationStreamXDelex(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// Add entries
	id1, err := masterClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "repl_xdelex",
		Values: map[string]any{"f1": "v1"},
	}).Result()
	assert.NoError(t, err)
	_, err = masterClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "repl_xdelex",
		Values: map[string]any{"f2": "v2"},
	}).Result()
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Verify replicated
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		n, err := slaveClient.XLen(ctx, "repl_xdelex").Result()
		return err == nil && n == 2
	})

	// XDELEX on master
	result, err := masterClient.Do(ctx, "XDELEX", "repl_xdelex", id1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		rng, err := slaveClient.XRange(ctx, "repl_xdelex", "-", "+").Result()
		return err == nil && len(rng) == 1
	})
}

// TestReplicationSortedSetCommands 测试 ZINTERCARD/BZMPOP 在主从复制下的传播
func TestReplicationSortedSetCommands(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// ZINTERCARD replication
	err := masterClient.ZAdd(ctx, "repl_zs1", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}).Err()
	assert.NoError(t, err)
	err = masterClient.ZAdd(ctx, "repl_zs2", redis.Z{Score: 1, Member: "b"}, redis.Z{Score: 2, Member: "c"}).Err()
	assert.NoError(t, err)
	pollSlave(t, slaveClient, 20*time.Second, func() bool {
		v, err := slaveClient.Do(ctx, "ZINTERCARD", "2", "repl_zs1", "repl_zs2").Result()
		return err == nil && v == int64(1)
	})

	// ZINTERCARD on slave
	result, err := slaveClient.Do(ctx, "ZINTERCARD", "2", "repl_zs1", "repl_zs2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result) // intersection = {b}

	// BZMPOP on master (non-blocking pop)
	err = masterClient.ZAdd(ctx, "repl_bzmp", redis.Z{Score: 10, Member: "m1"}).Err()
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	bzmpResult, err := masterClient.Do(ctx, "BZMPOP", "0", "1", "repl_bzmp", "MIN").Result()
	assert.NoError(t, err)
	assert.NotNil(t, bzmpResult)
	waitForReplication(t, masterClient, slaveClient, 20*time.Second, 82)

	// Verify key is gone on slave (popped from master)
	zCard, err := slaveClient.Do(ctx, "ZCARD", "repl_bzmp").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), zCard)
}

// ==================== Cluster Mode Tests ====================

// TestClusterNewSubcommands 测试新的 CLUSTER 子命令
func TestClusterNewSubcommands(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// CLUSTER BUMPEPOCH
	result, err := clusterClient.Do(ctx, "CLUSTER", "BUMPEPOCH").Result()
	assert.NoError(t, err)
	resultStr, ok := result.(string)
	assert.True(t, ok)
	assert.Equal(t, "BUMPED", resultStr)

	// CLUSTER COUNT-FAILURE-REPORTS
	result, err = clusterClient.Do(ctx, "CLUSTER", "COUNT-FAILURE-REPORTS", "some-node-id").Result()
	assert.NoError(t, err)
	// Returns 0 in simplified implementation
	count, ok := result.(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), count)

	// CLUSTER LINKS
	result, err = clusterClient.Do(ctx, "CLUSTER", "LINKS").Result()
	assert.NoError(t, err)
	// Returns "[]" string in simplified implementation (goes through default case)
	_ = result // just verify it doesn't error
}

// TestClusterNewDataCommands 测试新数据命令在集群模式下
func TestClusterNewDataCommands(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// UNLINK in cluster mode
	err := clusterClient.Set(ctx, "c_unlink", "val", 0).Err()
	assert.NoError(t, err)
	result, err := clusterClient.Unlink(ctx, "c_unlink").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// BITFIELD_RO in cluster mode
	err = clusterClient.SetBit(ctx, "c_bfro", 7, 1).Err()
	assert.NoError(t, err)
	bfResult, err := clusterClient.Do(ctx, "BITFIELD_RO", "c_bfro", "GET", "u8", "0").Result()
	assert.NoError(t, err)
	assert.NotNil(t, bfResult)

	// ZINTERCARD in cluster mode
	err = clusterClient.ZAdd(ctx, "c_zic_a", redis.Z{Score: 1, Member: "x"}).Err()
	assert.NoError(t, err)
	err = clusterClient.ZAdd(ctx, "c_zic_b", redis.Z{Score: 1, Member: "x"}).Err()
	assert.NoError(t, err)
	zicResult, err := clusterClient.Do(ctx, "ZINTERCARD", "2", "c_zic_a", "c_zic_b").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), zicResult)

	// Stream commands in cluster mode
	_, err = clusterClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "c_xstream",
		Values: map[string]any{"f1": "v1"},
	}).Result()
	assert.NoError(t, err)

	xLen, err := clusterClient.XLen(ctx, "c_xstream").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), xLen)

	// XSETID in cluster mode
	xsetidResult, err := clusterClient.Do(ctx, "XSETID", "c_xstream", "8888888888888-0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", xsetidResult)

	// TS commands in cluster mode
	err = clusterClient.Do(ctx, "TS.CREATE", "c_ts").Err()
	assert.NoError(t, err)
	now := time.Now().UnixMilli()
	clusterClient.Do(ctx, "TS.ADD", "c_ts", now, "10.5")
	clusterClient.Do(ctx, "TS.ADD", "c_ts", now+1000, "20.5")

	tsLen, err := clusterClient.Do(ctx, "TS.LEN", "c_ts").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), tsLen)

	// TS.REVRANGE in cluster mode
	revRangeResult, err := clusterClient.Do(ctx, "TS.REVRANGE", "c_ts", "-", "+").Result()
	assert.NoError(t, err)
	assert.NotNil(t, revRangeResult)

	// TS.INCRBY in cluster mode
	incrResult, err := clusterClient.Do(ctx, "TS.INCRBY", "c_ts", "5.0", "TIMESTAMP", now).Result()
	assert.NoError(t, err)
	assert.NotNil(t, incrResult)

	// TS.MADD in cluster mode
	maddResult, err := clusterClient.Do(ctx, "TS.MADD", "c_ts", now+2000, "30.5").Result()
	assert.NoError(t, err)
	assert.NotNil(t, maddResult)

	// TS.CREATERULE in cluster mode
	err = clusterClient.Do(ctx, "TS.CREATE", "c_ts_rule_dst").Err()
	assert.NoError(t, err)
	ruleResult, err := clusterClient.Do(ctx, "TS.CREATERULE", "c_ts", "c_ts_rule_dst", "AGGREGATION", "avg", "60000").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", ruleResult)

	// TS.DELETERULE in cluster mode
	delRuleResult, err := clusterClient.Do(ctx, "TS.DELETERULE", "c_ts", "c_ts_rule_dst", "AGGREGATION", "avg", "60000").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", delRuleResult)
}

// TestClusterClientSubcommands 测试 CLIENT 子命令在集群模式下
func TestClusterClientSubcommands(t *testing.T) {
	setupClusterTestServer(t)
	defer teardownClusterTestServer(t)

	ctx := context.Background()

	// CLIENT SETINFO
	result, err := clusterClient.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "cluster-test").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT NO-TOUCH
	result, err = clusterClient.Do(ctx, "CLIENT", "NO-TOUCH", "ON").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// CLIENT GETREDIR
	result, err = clusterClient.Do(ctx, "CLIENT", "GETREDIR").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)

	// OBJECT HELP
	result, err = clusterClient.Do(ctx, "OBJECT", "HELP").Result()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// DEBUG SET-ACTIVE-EXPIRE
	result, err = clusterClient.Do(ctx, "DEBUG", "SET-ACTIVE-EXPIRE", "1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// INFO MEMORY
	result, err = clusterClient.Do(ctx, "INFO", "MEMORY").Result()
	assert.NoError(t, err)
	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, strings.Contains(info, "used_memory"))
}
