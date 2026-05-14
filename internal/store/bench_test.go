package store

import (
	"testing"
)

// setupTestStoreBenchmark creates a store for benchmark testing
func setupTestStoreBenchmark(b *testing.B) *BotreonStore {
	dbPath := b.TempDir()
	store, err := NewBadgerStore(dbPath)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	return store
}

// BenchmarkStringSet benchmarks string Set operations
func BenchmarkStringSet(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set("key", "value")
	}
}

// BenchmarkStringGet benchmarks string Get operations
func BenchmarkStringGet(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		_ = store.Set("key", "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get("key")
	}
}

// BenchmarkZAdd benchmarks sorted set ZAdd operations
func BenchmarkZAdd(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	members := []ZSetMember{{Member: "member", Score: 1.0}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.ZAdd("zset", members)
	}
}

// BenchmarkZRange benchmarks sorted set ZRange operations
func BenchmarkZRange(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	// Pre-populate
	members := make([]ZSetMember, 100)
	for i := 0; i < 100; i++ {
		members[i] = ZSetMember{Member: string(rune('a' + i)), Score: float64(i)}
	}
	_ = store.ZAdd("zset", members)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.ZRange("zset", 0, -1)
	}
}

// BenchmarkLPush benchmarks list LPush operations
func BenchmarkLPush(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.LPush("list", "value")
	}
}

// BenchmarkRPush benchmarks list RPush operations
func BenchmarkRPush(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.RPush("list", "value")
	}
}

// BenchmarkLRange benchmarks list LRange operations
func BenchmarkLRange(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	// Pre-populate
	for i := 0; i < 100; i++ {
		_, _ = store.RPush("list", "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.LRange("list", 0, -1)
	}
}

// BenchmarkHSet benchmarks hash HSet operations
func BenchmarkHSet(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.HSet("hash", "field", "value")
	}
}

// BenchmarkHGet benchmarks hash HGet operations
func BenchmarkHGet(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		_ = store.HSet("hash", "field", "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.HGet("hash", "field")
	}
}

// BenchmarkSAdd benchmarks set SAdd operations
func BenchmarkSAdd(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.SAdd("set", "member")
	}
}

// BenchmarkSMembers benchmarks set SMembers operations
func BenchmarkSMembers(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	// Pre-populate
	for i := 0; i < 100; i++ {
		_, _ = store.SAdd("set", string(rune('a'+i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.SMembers("set")
	}
}

// BenchmarkDel benchmarks Delete operations
func BenchmarkDel(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	// Pre-populate
	for i := 0; i < 100; i++ {
		_ = store.Set("key", "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Re-populate for each iteration
		for j := 0; j < 100; j++ {
			_ = store.Set("key", "value")
		}
		_, _ = store.Del("key")
	}
}

// BenchmarkExists benchmarks Exists operations
func BenchmarkExists(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		_ = store.Set("key", "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Exists("key")
	}
}
