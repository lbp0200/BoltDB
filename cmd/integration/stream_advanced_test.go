package integration

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// TestXLen 测试 XLEN 命令
func TestXLen(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]any{"field1": "value1"},
	}).Result()
	assert.NoError(t, err)
	_, err = sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]any{"field2": "value2"},
	}).Result()
	assert.NoError(t, err)
	_, err = sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]any{"field3": "value3"},
	}).Result()
	assert.NoError(t, err)

	// XLEN - 获取流长度
	length, err := sharedClient.XLen(ctx, "mystream").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// XLEN - 不存在的流
	length, err = sharedClient.XLen(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)
}

// TestXDel 测试 XDEL 命令
func TestXDel(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	id1, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mydelstream",
		Values: map[string]any{"field": "value1"},
	}).Result()
	assert.NoError(t, err)
	_, err = sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "mydelstream",
		Values: map[string]any{"field": "value2"},
	}).Result()
	assert.NoError(t, err)

	// XDEL - 删除消息
	deleted, err := sharedClient.Do(ctx, "XDEL", "mydelstream", id1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// 验证流长度
	length, _ := sharedClient.XLen(ctx, "mydelstream").Result()
	assert.Equal(t, int64(1), length)

	// XDEL - 删除不存在的ID
	deleted, err = sharedClient.Do(ctx, "XDEL", "mydelstream", "9999999999-0").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
}

// TestXAck 测试 XACK 命令
func TestXAck(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	id1, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "ackstream",
		Values: map[string]any{"field": "value1"},
	}).Result()
	assert.NoError(t, err)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "ackstream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// 读取消息
	_, err = sharedClient.Do(ctx, "XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "ackstream", "0").Result()
	assert.NoError(t, err)

	// XACK - 确认消息
	acked, err := sharedClient.Do(ctx, "XACK", "ackstream", "mygroup", id1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), acked)

	// XACK - 确认不存在的消息
	acked, err = sharedClient.Do(ctx, "XACK", "ackstream", "mygroup", "9999999999-0").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), acked)
}

// TestXGroupCreate 测试 XGROUP CREATE 命令
func TestXGroupCreate(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "groupstream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	// XGROUP CREATE - 创建消费组
	result, err := sharedClient.Do(ctx, "XGROUP", "CREATE", "groupstream", "mygroup", "0").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// XGROUP CREATE - 创建带MKSTREAM选项的组
	result, err = sharedClient.Do(ctx, "XGROUP", "CREATE", "newstream", "newgroup", "$", "MKSTREAM").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 验证流被创建
	length, _ := sharedClient.XLen(ctx, "newstream").Result()
	assert.Equal(t, int64(0), length)
}

// TestXGroupDestroy 测试 XGROUP DESTROY 命令
func TestXGroupDestroy(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流并创建消费组
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "dstream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	err = sharedClient.XGroupCreate(ctx, "dstream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// XGROUP DESTROY - 销毁消费组
	result, err := sharedClient.Do(ctx, "XGROUP", "DESTROY", "dstream", "mygroup").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result)

	// 验证消费组已销毁
	result, _ = sharedClient.Do(ctx, "XINFO", "GROUPS", "dstream").Result()
	arr, _ := result.([]interface{})
	assert.Equal(t, 0, len(arr))
}

// TestXGroupSetId 测试 XGROUP SETID 命令
func TestXGroupSetId(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "sidstream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "sidstream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// XGROUP SETID - 设置组ID
	result, err := sharedClient.Do(ctx, "XGROUP", "SETID", "sidstream", "mygroup", "100").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestXGroupDelConsumer 测试 XGROUP DELCONSUMER 命令
func TestXGroupDelConsumer(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流并创建消费组
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "cstream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	err = sharedClient.XGroupCreate(ctx, "cstream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// XGROUP DELCONSUMER - 删除消费者
	result, err := sharedClient.Do(ctx, "XGROUP", "DELCONSUMER", "cstream", "mygroup", "consumer1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result) // 消费者不存在
}

// TestXReadGroup 测试 XREADGROUP 命令
func TestXReadGroup(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "readstream",
		Values: map[string]any{"field": "value1"},
	}).Result()
	assert.NoError(t, err)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "readstream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// XREADGROUP - 从组读取
	result, err := sharedClient.Do(ctx, "XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "readstream", ">").Result()
	assert.NoError(t, err)

	// Redis returns: [[streamKey, [[entryID, [field, value]]]]]
	// Outer array length equals number of streams (1 in this case)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr)) // 1 stream

	// Each stream element is [streamKey, [entries]]
	streamArr, ok := arr[0].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(streamArr)) // [streamKey, entries]
	assert.Equal(t, "readstream", streamArr[0])
}

// TestXClaim 测试 XCLAIM 命令
func TestXClaim(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	id1, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "claimstream",
		Values: map[string]any{"field": "value1"},
	}).Result()
	assert.NoError(t, err)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "claimstream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// 读取消息
	_, err = sharedClient.Do(ctx, "XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "claimstream", ">").Result()
	assert.NoError(t, err)

	// XCLAIM - 认领消息；Redis 默认形状 [[id, [field, value, ...]], ...]
	result, err := sharedClient.Do(ctx, "XCLAIM", "claimstream", "mygroup", "consumer2", "0", id1).Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr))
	entry, ok := arr[0].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(entry))
	assert.Equal(t, id1, entry[0].(string))
	fields, ok := entry[1].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(fields)) // field + value1
	// go-redis typed API must also parse full entry shape
	claimed, err := sharedClient.XClaim(ctx, &redis.XClaimArgs{
		Stream:   "claimstream",
		Group:    "mygroup",
		Consumer: "consumer3",
		MinIdle:  0,
		Messages: []string{id1},
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(claimed))
	assert.Equal(t, id1, claimed[0].ID)
}

// TestXPending 测试 XPENDING 命令
func TestXPending(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "pendingstream",
		Values: map[string]any{"field": "value1"},
	}).Result()
	assert.NoError(t, err)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "pendingstream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// 读取消息
	_, err = sharedClient.Do(ctx, "XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "pendingstream", ">").Result()
	assert.NoError(t, err)

	// XPENDING summary: [count, minID, maxID, [[consumer, n], ...]]
	result, err := sharedClient.Do(ctx, "XPENDING", "pendingstream", "mygroup").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(arr))
	assert.Equal(t, int64(1), arr[0].(int64))

	// Extended form used by go-redis XPendingExt
	ext, err := sharedClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "pendingstream",
		Group:  "mygroup",
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ext))
	assert.Equal(t, "consumer1", ext[0].Consumer)
}

// TestXInfoGroups 测试 XINFO GROUPS 命令
func TestXInfoGroups(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "infostream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "infostream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// XINFO GROUPS - 获取组信息
	result, err := sharedClient.Do(ctx, "XINFO", "GROUPS", "infostream").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 至少有一个组
	assert.Equal(t, 1, len(arr))

	group, ok := arr[0].([]interface{})
	assert.True(t, ok)
	// 组信息包含 name, consumers, pending 等
	assert.True(t, len(group) >= 6)
}

// TestXInfoConsumers 测试 XINFO CONSUMERS 命令
func TestXInfoConsumers(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "constream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	// 创建消费组
	err = sharedClient.XGroupCreate(ctx, "constream", "mygroup", "0").Err()
	assert.NoError(t, err)

	// XINFO CONSUMERS - 获取消费者信息
	result, err := sharedClient.Do(ctx, "XINFO", "CONSUMERS", "constream", "mygroup").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 没有消费者时返回空数组
	assert.Equal(t, 0, len(arr))
}

// TestXTrim 测试 XTRIM 命令
func TestXTrim(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加多条消息
	for i := 0; i < 10; i++ {
		_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
			Stream: "trimstream",
			Values: map[string]any{"field": i},
		}).Result()
		assert.NoError(t, err)
	}

	// XTRIM - 修剪流，只保留最后5条
	result, err := sharedClient.Do(ctx, "XTRIM", "trimstream", "~", "5").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result) // 删除了5条

	// 验证长度
	length, _ := sharedClient.XLen(ctx, "trimstream").Result()
	assert.Equal(t, int64(5), length)
}

// TestXRange 测试 XRANGE 命令
func TestXRange(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "rangestream",
		Values: map[string]any{"field": "value1"},
	}).Result()
	assert.NoError(t, err)
	_, err = sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "rangestream",
		Values: map[string]any{"field": "value2"},
	}).Result()
	assert.NoError(t, err)

	// XRANGE - 范围读取
	result, err := sharedClient.Do(ctx, "XRANGE", "rangestream", "-", "+").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr))

	// XRANGE - 限制数量
	result, err = sharedClient.Do(ctx, "XRANGE", "rangestream", "-", "+", "COUNT", "1").Result()
	assert.NoError(t, err)

	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr))
}

// TestXRevRange 测试 XREVRANGE 命令
func TestXRevRange(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "revstream",
		Values: map[string]any{"field": "value1"},
	}).Result()
	assert.NoError(t, err)
	_, err = sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "revstream",
		Values: map[string]any{"field": "value2"},
	}).Result()
	assert.NoError(t, err)

	// XREVRANGE - 逆序范围读取
	result, err := sharedClient.Do(ctx, "XREVRANGE", "revstream", "+", "-").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 逆序应该先返回最新的消息
	assert.Equal(t, 2, len(arr))
}

// TestXInfoStream 测试 XINFO STREAM 命令
func TestXInfoStream(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 添加测试流
	_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "infostream",
		Values: map[string]any{"field": "value"},
	}).Result()
	assert.NoError(t, err)

	// XINFO STREAM - 获取流信息
	result, err := sharedClient.Do(ctx, "XINFO", "STREAM", "infostream").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 流信息包含 length, groups, first-entry, last-entry 等
	assert.True(t, len(arr) >= 8)
}
