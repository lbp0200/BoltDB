package integration

import (
	"bufio"
	"context"
	"math"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

const goroutineSettleTime = 500 * time.Millisecond
const goroutineTolerance = 10

func baselineGoroutines(t *testing.T) int {
	t.Helper()
	time.Sleep(goroutineSettleTime)
	return runtime.NumGoroutine()
}

func assertNoLeak(t *testing.T, baseline int, final int) {
	t.Helper()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("goroutine leak: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

func TestGoroutineLeak_PubSubSubscribeUnsubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn, reader := dialPubSub(t)
		sendPubSubCmd(t, conn, "SUBSCRIBE", "leak:ch")
		readPubSubResp(t, reader)
		sendPubSubCmd(t, conn, "UNSUBSCRIBE", "leak:ch")
		readPubSubResp(t, reader)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_PubSubSubscribeDisconnect(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn, reader := dialPubSub(t)
		sendPubSubCmd(t, conn, "SUBSCRIBE", "leak:disc")
		readPubSubResp(t, reader)
		conn.Close()
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_PubSubPattern(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn, reader := dialPubSub(t)
		sendPubSubCmd(t, conn, "PSUBSCRIBE", "leak:*")
		readPubSubResp(t, reader)
		sendPubSubCmd(t, conn, "PUNSUBSCRIBE", "leak:*")
		readPubSubResp(t, reader)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_PubSubMultipleChannels(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn, reader := dialPubSub(t)
		sendPubSubCmd(t, conn, "SUBSCRIBE", "leak:a", "leak:b", "leak:c")
		readPubSubResp(t, reader)
		readPubSubResp(t, reader)
		readPubSubResp(t, reader)
		sendPubSubCmd(t, conn, "UNSUBSCRIBE")
		readPubSubResp(t, reader)
		readPubSubResp(t, reader)
		readPubSubResp(t, reader)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_PubSubWithPublish(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn, reader := dialPubSub(t)
		sendPubSubCmd(t, conn, "SUBSCRIBE", "leak:pub")
		readPubSubResp(t, reader)

		sharedClient.Publish(ctx, "leak:pub", "data")
		readPubSubResp(t, reader)

		sendPubSubCmd(t, conn, "UNSUBSCRIBE", "leak:pub")
		readPubSubResp(t, reader)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_BlockingPopTimeout(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		ctx := context.Background()
		_, err := sharedClient.BLPop(ctx, 1*time.Second, "leak:empty").Result()
		assert.Equal(t, redis.Nil, err)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_BlockingPopWithPush(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", sharedListener.Addr().String())
		assert.NoError(t, err)

		proto.WriteRESP(conn, &proto.Array{
			Args: [][]byte{[]byte("BLPOP"), []byte("leak:push"), []byte("3")},
		})

		time.Sleep(100 * time.Millisecond)
		sharedClient.LPush(context.Background(), "leak:push", "val")

		reader := bufio.NewReader(conn)
		_, err = proto.ReadRESP(reader)
		assert.NoError(t, err)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_TransactionFullCycle(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 10; i++ {
		conn := dialConn(t)
		rawCmd(t, conn, "MULTI")
		rawOK(t, conn)
		rawCmd(t, conn, "SET", "leak:tx", "val")
		rawQueued(t, conn)
		rawCmd(t, conn, "EXEC")
		rawRead(t, conn)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_TransactionDiscard(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 10; i++ {
		conn := dialConn(t)
		rawCmd(t, conn, "MULTI")
		rawOK(t, conn)
		rawCmd(t, conn, "SET", "leak:disc", "val")
		rawQueued(t, conn)
		rawCmd(t, conn, "DISCARD")
		rawOK(t, conn)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_WatchUnwatch(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 10; i++ {
		conn := dialConn(t)
		rawCmd(t, conn, "WATCH", "leak:watch")
		rawWatchOK(t, conn)
		rawCmd(t, conn, "UNWATCH")
		rawOK(t, conn)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_WatchDisconnect(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 10; i++ {
		conn := dialConn(t)
		rawCmd(t, conn, "WATCH", "leak:wdisc")
		rawWatchOK(t, conn)
		conn.Close()
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_ConnectDisconnectCycle(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 20; i++ {
		conn, err := net.Dial("tcp", sharedListener.Addr().String())
		assert.NoError(t, err)
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_ClientKill(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	ctx := context.Background()
	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn := dialConn(t)
		rawCmd(t, conn, "PING")
		rawRead(t, conn)

		sharedClient.Do(ctx, "CLIENT", "KILL", "ADDR", conn.RemoteAddr().String())
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_XReadBlocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", sharedListener.Addr().String())
		assert.NoError(t, err)

		proto.WriteRESP(conn, &proto.Array{
			Args: [][]byte{[]byte("XREAD"), []byte("BLOCK"), []byte("1000"), []byte("STREAMS"), []byte("leak:stream"), []byte("$")},
		})

		reader := bufio.NewReader(conn)
		resp, err := proto.ReadRESP(reader)
		if err == nil && resp != nil {
			assert.Equal(t, 0, len(resp.Args))
		}
		conn.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_MixedPubSubBlockingTx(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, reader := dialPubSub(t)
			defer conn.Close()

			sendPubSubCmd(t, conn, "SUBSCRIBE", "leak:mixed")
			readPubSubResp(t, reader)

			sharedClient.Publish(ctx, "leak:mixed", "hello")
			readPubSubResp(t, reader)

			time.Sleep(50 * time.Millisecond)
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := dialConn(t)
			defer conn.Close()

			rawCmd(t, conn, "MULTI")
			rawOK(t, conn)
			rawCmd(t, conn, "SET", "leak:mix", "v")
			rawQueued(t, conn)
			rawCmd(t, conn, "EXEC")
			rawRead(t, conn)
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sharedClient.BLPop(ctx, 1*time.Second, "leak:nope").Result()
			assert.Equal(t, redis.Nil, err)
		}()
	}

	wg.Wait()
	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_BRPopLPUSHBlocking(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		ctx := context.Background()
		sharedClient.RPush(ctx, "leak:brsrc", "val")
		_, err := sharedClient.BRPopLPush(ctx, "leak:brsrc", "leak:brdst", 0).Result()
		assert.NoError(t, err)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_BlockingWithClientKill(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", sharedListener.Addr().String())
		assert.NoError(t, err)

		proto.WriteRESP(conn, &proto.Array{
			Args: [][]byte{[]byte("BLPOP"), []byte("leak:killme"), []byte("0")},
		})

		time.Sleep(50 * time.Millisecond)
		sharedClient.Do(ctx, "CLIENT", "KILL", "ADDR", conn.RemoteAddr().String())
		conn.Close()
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_ReplconfRace(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		sharedClient.Do(ctx, "REPLCONF", "LISTENING-PORT", "6379")
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_ServerGoroutineStability(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	measurements := make([]int, 5)
	for i := 0; i < 5; i++ {
		ctx := context.Background()
		sharedClient.Ping(ctx)
		time.Sleep(200 * time.Millisecond)
		measurements[i] = runtime.NumGoroutine()
	}

	for i, n := range measurements {
		leak := n - baseline
		if leak > goroutineTolerance {
			t.Errorf("measurement %d: goroutine count drifted: baseline=%d, now=%d, leak=%d",
				i, baseline, n, leak)
		}
	}
}

func TestGoroutineLeak_RepeatSubscribeUnsubscribePattern(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		sub := sharedClient.Subscribe(ctx, "leak:repeat")
		sub.Unsubscribe(ctx, "leak:repeat")
		sub.Close()
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_PubSubGoRedisClient(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		pubSub := sharedClient.Subscribe(ctx, "leak:goredis")
		time.Sleep(50 * time.Millisecond)

		sharedClient.Publish(ctx, "leak:goredis", "msg")

		pubSub.Close()
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	final := runtime.NumGoroutine()
	leak := final - baseline
	if leak > goroutineTolerance {
		t.Errorf("go-redis PubSub goroutine leak: %d (baseline=%d, final=%d)", leak, baseline, final)
	}
}

func TestGoroutineLeak_ConcurrentConnectDisconnect(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", sharedListener.Addr().String())
			if err != nil {
				return
			}
			proto.WriteRESP(conn, &proto.Array{
				Args: [][]byte{[]byte("PING")},
			})
			reader := bufio.NewReader(conn)
			proto.ReadRESP(reader)
			conn.Close()
		}()
	}
	wg.Wait()

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_TransactionDisconnectMidway(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 10; i++ {
		conn := dialConn(t)
		rawCmd(t, conn, "MULTI")
		rawOK(t, conn)
		rawCmd(t, conn, "SET", "leak:mid", "val")
		rawQueued(t, conn)
		conn.Close()
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_BlockingPopDisconnectMidway(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", sharedListener.Addr().String())
		assert.NoError(t, err)

		proto.WriteRESP(conn, &proto.Array{
			Args: [][]byte{[]byte("BLPOP"), []byte("leak:midblock"), []byte("0")},
		})

		time.Sleep(50 * time.Millisecond)
		conn.Close()
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_BRPopLPUSHBlockingTimeout(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	for i := 0; i < 5; i++ {
		ctx := context.Background()
		_, err := sharedClient.BRPopLPush(ctx, "leak:brlempty", "leak:brldst", 1*time.Second).Result()
		assert.Equal(t, redis.Nil, err)
	}

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_ConcurrentPublishSubscribe(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, reader := dialPubSub(t)
			defer conn.Close()

			ch := "leak:conc"
			sendPubSubCmd(t, conn, "SUBSCRIBE", ch)
			readPubSubResp(t, reader)

			for j := 0; j < 3; j++ {
				sharedClient.Publish(ctx, ch, "data")
				readPubSubResp(t, reader)
			}

			sendPubSubCmd(t, conn, "UNSUBSCRIBE", ch)
			readPubSubResp(t, reader)
		}()
	}
	wg.Wait()

	time.Sleep(goroutineSettleTime)
	assertNoLeak(t, baseline, runtime.NumGoroutine())
}

func TestGoroutineLeak_MultiTypeChaos(t *testing.T) {
	setupTest(t)
	defer teardownTest(t)

	baseline := baselineGoroutines(t)

	var wg sync.WaitGroup
	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				conn, reader := dialPubSub(t)
				sendPubSubCmd(t, conn, "SUBSCRIBE", "leak:chaos")
				readPubSubResp(t, reader)
				time.Sleep(20 * time.Millisecond)
				conn.Close()
			}
		}()
	}

	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				conn := dialConn(t)
				rawCmd(t, conn, "MULTI")
				rawOK(t, conn)
				rawCmd(t, conn, "SET", "leak:chaos", "v")
				rawQueued(t, conn)
				rawCmd(t, conn, "EXEC")
				rawRead(t, conn)
				conn.Close()
			}
		}()
	}

	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				conn := dialConn(t)
				rawCmd(t, conn, "WATCH", "leak:chaos")
				rawWatchOK(t, conn)
				conn.Close()
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	time.Sleep(goroutineSettleTime)

	final := runtime.NumGoroutine()
	leak := final - baseline
	maxAllowed := goroutineTolerance + int(math.Round(float64(goroutineTolerance)*0.5))
	if leak > maxAllowed {
		t.Errorf("multi-type chaos goroutine leak: %d (baseline=%d, final=%d, max_allowed=%d)",
			leak, baseline, final, maxAllowed)
	}
}
