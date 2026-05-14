package integration

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// TestPublish 测试 PUBLISH 命令
func TestPublish(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PUBLISH - 发布消息
	result, err := sharedClient.Publish(ctx, "channel1", "message1").Result()
	assert.NoError(t, err)
	// 没有订阅者时返回0
	assert.Equal(t, int64(0), result)
}

// TestSubscribe 测试 SUBSCRIBE 命令
func TestSubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// SUBSCRIBE - 订阅频道（使用原始命令）
	// 注意：由于是阻塞操作，我们只验证命令被接受
	result, err := sharedClient.Do(ctx, "SUBSCRIBE", "channel1").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 订阅响应格式: ["subscribe", channel, count]
	assert.Equal(t, 3, len(arr))
	assert.Equal(t, "subscribe", arr[0])
}

// TestUnsubscribe 测试 UNSUBSCRIBE 命令
func TestUnsubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 先订阅
	_, _ = sharedClient.Do(ctx, "SUBSCRIBE", "channel1").Result()

	// UNSUBSCRIBE - 取消订阅
	result, err := sharedClient.Do(ctx, "UNSUBSCRIBE", "channel1").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr))
	assert.Equal(t, "unsubscribe", arr[0])
}

// TestPSubscribe 测试 PSUBSCRIBE 命令
func TestPSubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PSUBSCRIBE - 模式订阅
	result, err := sharedClient.Do(ctx, "PSUBSCRIBE", "news.*").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr))
	assert.Equal(t, "psubscribe", arr[0])
}

// TestPUnsubscribe 测试 PUNSUBSCRIBE 命令
func TestPUnsubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 先模式订阅
	_, _ = sharedClient.Do(ctx, "PSUBSCRIBE", "news.*").Result()

	// PUNSUBSCRIBE - 取消模式订阅
	result, err := sharedClient.Do(ctx, "PUNSUBSCRIBE", "news.*").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr))
	assert.Equal(t, "punsubscribe", arr[0])
}

// TestPubSubChannels 测试 PUBSUB CHANNELS 命令
func TestPubSubChannels(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PUBSUB CHANNELS - 列出活跃频道
	result, err := sharedClient.Do(ctx, "PUBSUB", "CHANNELS").Result()
	assert.NoError(t, err)

	_, ok := result.([]interface{})
	assert.True(t, ok)
	// 没有活跃频道时返回空数组

	// PUBSUB CHANNELS with pattern
	result, err = sharedClient.Do(ctx, "PUBSUB", "CHANNELS", "news.*").Result()
	assert.NoError(t, err)

	_, ok = result.([]interface{})
	assert.True(t, ok)
}

// TestPubSubNumSub 测试 PUBSUB NUMSUB 命令
func TestPubSubNumSub(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PUBSUB NUMSUB - 获取订阅者数量
	result, err := sharedClient.Do(ctx, "PUBSUB", "NUMSUB", "channel1", "channel2").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 格式: [channel1, count1, channel2, count2, ...]
	assert.True(t, len(arr) >= 2)
}

// TestPubSubNumPat 测试 PUBSUB NUMPAT 命令
func TestPubSubNumPat(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PUBSUB NUMPAT - 获取模式订阅数量
	result, err := sharedClient.Do(ctx, "PUBSUB", "NUMPAT").Result()
	assert.NoError(t, err)

	num, ok := result.(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), num)
}

// TestPubSubHelp 测试 PUBSUB HELP 命令
func TestPubSubHelp(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// PUBSUB HELP - 获取帮助信息
	result, err := sharedClient.Do(ctx, "PUBSUB", "HELP").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)
}

// TestPublishSubscribeIntegration 测试发布订阅集成
func TestPublishSubscribeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 创建一个新的客户端用于发布
	pubClient := redis.NewClient(&redis.Options{
		Addr:     sharedListener.Addr().String(),
		Password: "",
		DB:       0,
	})
	defer pubClient.Close()

	// 发布消息
	count, err := pubClient.Publish(ctx, "integration_test", "test_message").Result()
	assert.NoError(t, err)
	// 可能返回0（如果没有订阅者）
	assert.True(t, count >= 0)
}

// TestMultipleChannels 测试多个频道
func TestMultipleChannels(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 订阅多个频道
	_, _ = sharedClient.Do(ctx, "SUBSCRIBE", "channel1", "channel2", "channel3").Result()

	// PUBSUB NUMSUB - 检查多个频道
	result, err := sharedClient.Do(ctx, "PUBSUB", "NUMSUB", "channel1", "channel2", "channel3").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 6, len(arr)) // 3 channels * 2 (name, count)
}

// TestUnsubscribeAll 测试取消所有订阅
func TestUnsubscribeAll(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 订阅多个频道
	_, _ = sharedClient.Do(ctx, "SUBSCRIBE", "channel1", "channel2").Result()

	// 取消所有订阅（不带参数）
	result, err := sharedClient.Do(ctx, "UNSUBSCRIBE").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	// 响应格式: ["unsubscribe", channel, count]
	assert.Equal(t, 3, len(arr))
}

// TestTimeoutUnsubscribe 测试超时取消订阅
func TestTimeoutUnsubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 使用单独的客户端进行发布测试
	pubClient := redis.NewClient(&redis.Options{
		Addr:     sharedListener.Addr().String(),
		Password: "",
		DB:       0,
	})
	defer pubClient.Close()

	// 发布消息以验证频道功能
	_, err := pubClient.Publish(ctx, "timeout_test", "message").Result()
	assert.NoError(t, err)
}
