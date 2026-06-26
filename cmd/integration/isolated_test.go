package integration

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestIsolatedServer_SetGet 验证 StartIsolatedServer 创建的隔离服务器正常工作。
// 这是 "集成测试独立化"（P2c）的示例：每个测试获得独立的 DB + 服务器 + 客户端，
// 无共享状态污染。可作为后续重构其他集成测试的模板。
func TestIsolatedServer_SetGet(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()

	// SET
	err := srv.Client.Set(ctx, "mykey", "myvalue", 0).Err()
	if err != nil {
		t.Fatalf("SET: %v", err)
	}

	// GET
	val, err := srv.Client.Get(ctx, "mykey").Result()
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if val != "myvalue" {
		t.Fatalf("GET = %q, want %q", val, "myvalue")
	}
}

// TestIsolatedServer_TTL 验证隔离服务器上 TTL 操作正确。
func TestIsolatedServer_TTL(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()

	// 使用 SET EX 设置带过期时间的键
	err := srv.Client.Set(ctx, "ttlkey", "ttlval", 10*time.Second).Err()
	if err != nil {
		t.Fatalf("SET EX: %v", err)
	}

	ttl, err := srv.Client.TTL(ctx, "ttlkey").Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > 10*time.Second {
		t.Fatalf("TTL = %v, want between 0 and 10s", ttl)
	}

	// DEL
	deleted, err := srv.Client.Del(ctx, "ttlkey").Result()
	if err != nil {
		t.Fatalf("DEL: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DEL returned %d, want 1", deleted)
	}
}

// TestIsolatedServer_MultiDataType 验证隔离服务器支持多种数据类型。
func TestIsolatedServer_MultiDataType(t *testing.T) {
	srv := StartIsolatedServer(t)
	ctx := context.Background()

	// String
	err := srv.Client.Set(ctx, "str", "hello", 0).Err()
	if err != nil {
		t.Fatalf("SET: %v", err)
	}

	// Hash
	err = srv.Client.HSet(ctx, "h", "f1", "v1", "f2", "v2").Err()
	if err != nil {
		t.Fatalf("HSET: %v", err)
	}

	// List
	err = srv.Client.LPush(ctx, "lst", "a", "b", "c").Err()
	if err != nil {
		t.Fatalf("LPUSH: %v", err)
	}

	// Set
	err = srv.Client.SAdd(ctx, "s", "m1", "m2").Err()
	if err != nil {
		t.Fatalf("SADD: %v", err)
	}

	// Sorted Set
	err = srv.Client.ZAdd(ctx, "z", redis.Z{Score: 1.0, Member: "a"}, redis.Z{Score: 2.0, Member: "b"}).Err()
	if err != nil {
		t.Fatalf("ZADD: %v", err)
	}

	// Verify all types exist
	str, err := srv.Client.Get(ctx, "str").Result()
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if str != "hello" {
		t.Fatalf("GET = %q, want %q", str, "hello")
	}

	hlen, err := srv.Client.HLen(ctx, "h").Result()
	if err != nil {
		t.Fatalf("HLEN: %v", err)
	}
	if hlen != 2 {
		t.Fatalf("HLEN = %d, want 2", hlen)
	}

	llen, err := srv.Client.LLen(ctx, "lst").Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if llen != 3 {
		t.Fatalf("LLEN = %d, want 3", llen)
	}

	scard, err := srv.Client.SCard(ctx, "s").Result()
	if err != nil {
		t.Fatalf("SCARD: %v", err)
	}
	if scard != 2 {
		t.Fatalf("SCARD = %d, want 2", scard)
	}

	zcard, err := srv.Client.ZCard(ctx, "z").Result()
	if err != nil {
		t.Fatalf("ZCARD: %v", err)
	}
	if zcard != 2 {
		t.Fatalf("ZCARD = %d, want 2", zcard)
	}
}
