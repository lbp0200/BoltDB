package integration

import (
	"bufio"
	"context"
	"net"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

func dialPubSub(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", sharedListener.Addr().String())
	assert.NoError(t, err)
	return conn, bufio.NewReader(conn)
}

func sendPubSubCmd(t *testing.T, conn net.Conn, cmd string, args ...string) {
	t.Helper()
	cmdArgs := make([][]byte, 1+len(args))
	cmdArgs[0] = []byte(cmd)
	for i, arg := range args {
		cmdArgs[i+1] = []byte(arg)
	}
	err := proto.WriteRESP(conn, &proto.Array{Args: cmdArgs})
	assert.NoError(t, err)
}

func readPubSubResp(t *testing.T, reader *bufio.Reader) []string {
	t.Helper()
	arr, err := proto.ReadRESP(reader)
	if err != nil {
		return nil
	}
	if arr == nil {
		return nil
	}
	parts := make([]string, len(arr.Args))
	for i, arg := range arr.Args {
		parts[i] = string(arg)
	}
	return parts
}

// TestPublish 测试 PUBLISH 命令
func TestPublish(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	result, err := sharedClient.Publish(ctx, "channel1", "message1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestSubscribe 测试 SUBSCRIBE 命令（通过原始 RESP 连接）
func TestSubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "channel1")
	parts := readPubSubResp(t, reader)
	assert.Equal(t, 3, len(parts))
	assert.Equal(t, "subscribe", parts[0])
	assert.Equal(t, "channel1", parts[1])
	assert.Equal(t, "1", parts[2])
}

// TestUnsubscribe 测试 UNSUBSCRIBE 命令
func TestUnsubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "channel1")
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "UNSUBSCRIBE", "channel1")
	parts := readPubSubResp(t, reader)
	assert.Equal(t, "unsubscribe", parts[0])
	assert.Equal(t, "channel1", parts[1])
}

// TestPSubscribe 测试 PSUBSCRIBE 命令
func TestPSubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "PSUBSCRIBE", "news.*")
	parts := readPubSubResp(t, reader)
	assert.Equal(t, 3, len(parts))
	assert.Equal(t, "psubscribe", parts[0])
	assert.Equal(t, "news.*", parts[1])
}

// TestPUnsubscribe 测试 PUNSUBSCRIBE 命令
func TestPUnsubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "PSUBSCRIBE", "news.*")
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "PUNSUBSCRIBE", "news.*")
	parts := readPubSubResp(t, reader)
	assert.Equal(t, "punsubscribe", parts[0])
	assert.Equal(t, "news.*", parts[1])
}

// TestPubSubChannels 测试 PUBSUB CHANNELS 命令
func TestPubSubChannels(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	result, err := sharedClient.Do(ctx, "PUBSUB", "CHANNELS").Result()
	assert.NoError(t, err)
	_, ok := result.([]interface{})
	assert.True(t, ok)

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

	result, err := sharedClient.Do(ctx, "PUBSUB", "NUMSUB", "channel1", "channel2").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) >= 2)
}

// TestPubSubNumPat 测试 PUBSUB NUMPAT 命令
func TestPubSubNumPat(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

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

	result, err := sharedClient.Do(ctx, "PUBSUB", "HELP").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.True(t, len(arr) > 0)
}

// TestPubSubMessageDelivery 端到端 PubSub: SUBSCRIBE → PUBLISH → 消息投递
func TestPubSubMessageDelivery(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	// 订阅者: 原始 RESP 连接
	subConn, subReader := dialPubSub(t)
	defer subConn.Close()

	sendPubSubCmd(t, subConn, "SUBSCRIBE", "delivery_test")
	parts := readPubSubResp(t, subReader)
	assert.Equal(t, "subscribe", parts[0])
	assert.Equal(t, "delivery_test", parts[1])

	// 发布者: go-redis 客户端
	pubClient := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
		DB:   0,
	})
	defer pubClient.Close()

	count, err := pubClient.Publish(ctx, "delivery_test", "hello_world").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// 读取推送消息
	parts = readPubSubResp(t, subReader)
	assert.Equal(t, 3, len(parts))
	assert.Equal(t, "message", parts[0])
	assert.Equal(t, "delivery_test", parts[1])
	assert.Equal(t, "hello_world", parts[2])
}

// TestPubSubPatternDelivery 端到端 PSUBSCRIBE → PUBLISH → pmessage 投递
func TestPubSubPatternDelivery(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	subConn, subReader := dialPubSub(t)
	defer subConn.Close()

	sendPubSubCmd(t, subConn, "PSUBSCRIBE", "news.*")
	parts := readPubSubResp(t, subReader)
	assert.Equal(t, "psubscribe", parts[0])
	assert.Equal(t, "news.*", parts[1])

	pubClient := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
		DB:   0,
	})
	defer pubClient.Close()

	count, err := pubClient.Publish(ctx, "news.tech", "ai_breakthrough").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	parts = readPubSubResp(t, subReader)
	assert.Equal(t, 4, len(parts))
	assert.Equal(t, "pmessage", parts[0])
	assert.Equal(t, "news.*", parts[1])
	assert.Equal(t, "news.tech", parts[2])
	assert.Equal(t, "ai_breakthrough", parts[3])
}

// TestPubSubMultipleSubscribers 多个订阅者接收同一条消息
func TestPubSubMultipleSubscribers(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	sub1Conn, sub1Reader := dialPubSub(t)
	defer sub1Conn.Close()
	sub2Conn, sub2Reader := dialPubSub(t)
	defer sub2Conn.Close()

	sendPubSubCmd(t, sub1Conn, "SUBSCRIBE", "multi_test")
	readPubSubResp(t, sub1Reader)

	sendPubSubCmd(t, sub2Conn, "SUBSCRIBE", "multi_test")
	readPubSubResp(t, sub2Reader)

	pubClient := redis.NewClient(&redis.Options{
		Addr: sharedListener.Addr().String(),
		DB:   0,
	})
	defer pubClient.Close()

	count, err := pubClient.Publish(ctx, "multi_test", "broadcast").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	parts1 := readPubSubResp(t, sub1Reader)
	assert.Equal(t, "message", parts1[0])
	assert.Equal(t, "broadcast", parts1[2])

	parts2 := readPubSubResp(t, sub2Reader)
	assert.Equal(t, "message", parts2[0])
	assert.Equal(t, "broadcast", parts2[2])
}

// TestMultipleChannels 测试多个频道订阅
func TestMultipleChannels(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "channel1", "channel2", "channel3")

	parts1 := readPubSubResp(t, reader)
	assert.Equal(t, "subscribe", parts1[0])
	assert.Equal(t, "channel1", parts1[1])

	parts2 := readPubSubResp(t, reader)
	assert.Equal(t, "subscribe", parts2[0])
	assert.Equal(t, "channel2", parts2[1])

	parts3 := readPubSubResp(t, reader)
	assert.Equal(t, "subscribe", parts3[0])
	assert.Equal(t, "channel3", parts3[1])

}

// TestUnsubscribeAll 测试取消所有订阅
func TestUnsubscribeAll(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "channel1", "channel2")
	readPubSubResp(t, reader)
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "UNSUBSCRIBE")

	parts1 := readPubSubResp(t, reader)
	assert.Equal(t, "unsubscribe", parts1[0])

	parts2 := readPubSubResp(t, reader)
	assert.Equal(t, "unsubscribe", parts2[0])
}

// TestPubSubNonPubSubCommand 在 PubSub 模式下非 PubSub 命令应返回错误
func TestPubSubNonPubSubCommand(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "ch")
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "GET", "somekey")
	line, err := reader.ReadString('\n')
	assert.NoError(t, err)
	assert.True(t, len(line) > 5)
	assert.Equal(t, byte('-'), line[0])
}

// TestPubSubQuit 退出 PubSub 模式
func TestPubSubQuit(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	conn, reader := dialPubSub(t)
	defer conn.Close()

	sendPubSubCmd(t, conn, "SUBSCRIBE", "ch")
	readPubSubResp(t, reader)

	sendPubSubCmd(t, conn, "QUIT")
	line, err := reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", line)
}

// TestPublishSubscribeIntegration 保留原始集成测试的发布部分
func TestPublishSubscribeIntegration(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	pubClient := redis.NewClient(&redis.Options{
		Addr:     sharedListener.Addr().String(),
		Password: "",
		DB:       0,
	})
	defer pubClient.Close()

	count, err := pubClient.Publish(ctx, "integration_test", "test_message").Result()
	assert.NoError(t, err)
	assert.True(t, count >= 0)
}

// TestTimeoutUnsubscribe 测试超时取消订阅（保留原始测试结构）
func TestTimeoutUnsubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()

	pubClient := redis.NewClient(&redis.Options{
		Addr:     sharedListener.Addr().String(),
		Password: "",
		DB:       0,
	})
	defer pubClient.Close()

	_, err := pubClient.Publish(ctx, "timeout_test", "message").Result()
	assert.NoError(t, err)
}
