package integration

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/redis/go-redis/v9"
)

func setupBenchmark(b *testing.B) {
	b.Helper()
	sharedServerOnce.Do(func() {
		if sharedDB == nil {
			b.Fatal("shared server not initialized - TestMain not run")
		}
	})
	if err := sharedDB.ClearAllData(); err != nil {
		b.Fatalf("failed to clear data: %v", err)
	}
	sharedDB.ClearCaches()
	sharedServer.PubSub.Clear()
}

func BenchmarkSET(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := sharedClient.Set(ctx, fmt.Sprintf("bench:set:%d", i), "value", 0).Err()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGET(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	err := sharedClient.Set(ctx, "bench:get", "value", 0).Err()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := sharedClient.Get(ctx, "bench:get").Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipeline_10(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pipe := sharedClient.Pipeline()
		for j := 0; j < 10; j++ {
			pipe.Set(ctx, fmt.Sprintf("bench:pipe:%d:%d", i, j), "value", 0)
		}
		_, err := pipe.Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipeline_100(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pipe := sharedClient.Pipeline()
		for j := 0; j < 100; j++ {
			pipe.Set(ctx, fmt.Sprintf("bench:pipe100:%d:%d", i, j), "value", 0)
		}
		_, err := pipe.Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMGET_100(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("bench:mget:%d", i)
		err := sharedClient.Set(ctx, key, "value", 0).Err()
		if err != nil {
			b.Fatal(err)
		}
		keys[i] = key
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := sharedClient.MGet(ctx, keys...).Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLRANGE_1000(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		err := sharedClient.RPush(ctx, "bench:lrange", fmt.Sprintf("item:%d", i)).Err()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := sharedClient.LRange(ctx, "bench:lrange", 0, -1).Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPubSubFanout_10(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	const numSubs = 10
	subs := make([]*redis.PubSub, numSubs)
	for i := 0; i < numSubs; i++ {
		sub := sharedClient.Subscribe(ctx, "bench:pubsub")
		subs[i] = sub
		// wait for subscription confirmation via Receive (which returns *Subscription)
		_, err := sub.Receive(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
	defer func() {
		for _, sub := range subs {
			sub.Close()
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := sharedClient.Publish(ctx, "bench:pubsub", "message").Result()
		if err != nil {
			b.Fatal(err)
		}
		for _, sub := range subs {
			_, err := sub.ReceiveMessage(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTransaction_5(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pipe := sharedClient.TxPipeline()
		pipe.Set(ctx, fmt.Sprintf("bench:tx:%d:a", i), "value1", 0)
		pipe.Set(ctx, fmt.Sprintf("bench:tx:%d:b", i), "value2", 0)
		pipe.Get(ctx, fmt.Sprintf("bench:tx:%d:a", i))
		pipe.Get(ctx, fmt.Sprintf("bench:tx:%d:b", i))
		pipe.Del(ctx, fmt.Sprintf("bench:tx:%d:a", i), fmt.Sprintf("bench:tx:%d:b", i))
		_, err := pipe.Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXAdd(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
			Stream: "bench:xadd",
			Values: map[string]any{"field": "value"},
		}).Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXReadRange(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	// pre-populate 100 entries
	for i := 0; i < 100; i++ {
		_, err := sharedClient.XAdd(ctx, &redis.XAddArgs{
			Stream: "bench:read",
			Values: map[string]any{"field": fmt.Sprintf("value%d", i)},
		}).Result()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msgs, err := sharedClient.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"bench:read", "0"},
			Count:   100,
		}).Result()
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) == 0 {
			b.Fatal("no messages returned")
		}
	}
}

func BenchmarkConcurrentSET(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		id := rand.Int63()
		var i int64
		for pb.Next() {
			err := sharedClient.Set(ctx, fmt.Sprintf("bench:con:set:%d:%d", id, i), "value", 0).Err()
			if err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

func BenchmarkConcurrentGET(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	err := sharedClient.Set(ctx, "bench:con:get", "value", 0).Err()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := sharedClient.Get(ctx, "bench:con:get").Result()
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkINCR(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := sharedClient.Incr(ctx, "bench:incr").Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMSET_10(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pairs := make([]any, 0, 20)
		for j := 0; j < 10; j++ {
			pairs = append(pairs, fmt.Sprintf("bench:mset:%d:%d", i, j), "value")
		}
		err := sharedClient.MSet(ctx, pairs...).Err()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDEL(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		err := sharedClient.Set(ctx, fmt.Sprintf("bench:del:%d", i), "value", 0).Err()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := sharedClient.Del(ctx, fmt.Sprintf("bench:del:%d", i%1000)).Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHSET_HGET(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench:hash:%d", i)
		err := sharedClient.HSet(ctx, key, "field", "value").Err()
		if err != nil {
			b.Fatal(err)
		}
		_, err = sharedClient.HGet(ctx, key, "field").Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSADD_SMEMBERS(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()

	err := sharedClient.SAdd(ctx, "bench:set", "member").Err()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := sharedClient.SMembers(ctx, "bench:set").Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZADD_ZRANGE(b *testing.B) {
	setupBenchmark(b)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		member := fmt.Sprintf("member:%d", i)
		err := sharedClient.ZAdd(ctx, "bench:zset", redis.Z{Score: float64(i), Member: member}).Err()
		if err != nil {
			b.Fatal(err)
		}
		_, err = sharedClient.ZScore(ctx, "bench:zset", member).Result()
		if err != nil {
			b.Fatal(err)
		}
	}
}
