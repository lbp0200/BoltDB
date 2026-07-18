package store

import (
	"fmt"
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

// preloadZSet populates a zset with n members for benchmark setup.
func preloadZSet(b *testing.B, store *BotreonStore, key string, n int) {
	b.Helper()
	members := make([]ZSetMember, n)
	for i := 0; i < n; i++ {
		members[i] = ZSetMember{
			Member: fmt.Sprintf("member-%d", i),
			Score:  float64(i),
		}
	}
	if err := store.ZAdd(key, members); err != nil {
		b.Fatalf("failed to preload zset: %v", err)
	}
}

// BenchmarkZAdd_100 benchmarks ZAdd on a 100-entry sorted set.
func BenchmarkZAdd_100(b *testing.B) {
	benchmarkZAddSized(b, 100)
}

// BenchmarkZAdd_1K benchmarks ZAdd on a 1K-entry sorted set.
func BenchmarkZAdd_1K(b *testing.B) {
	benchmarkZAddSized(b, 1000)
}

// BenchmarkZAdd_10K benchmarks ZAdd on a 10K-entry sorted set.
func BenchmarkZAdd_10K(b *testing.B) {
	benchmarkZAddSized(b, 10000)
}

func benchmarkZAddSized(b *testing.B, size int) {
	store := setupTestStoreBenchmark(b)
	preloadZSet(b, store, "zset", size)
	member := ZSetMember{Member: "new-member", Score: float64(size) + 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.ZAdd("zset", []ZSetMember{member})
	}
}

// BenchmarkZRange_100 benchmarks ZRange on a 100-entry sorted set.
func BenchmarkZRange_100(b *testing.B) {
	benchmarkZRangeSized(b, 100)
}

// BenchmarkZRange_1K benchmarks ZRange on a 1K-entry sorted set.
func BenchmarkZRange_1K(b *testing.B) {
	benchmarkZRangeSized(b, 1000)
}

// BenchmarkZRange_10K benchmarks ZRange on a 10K-entry sorted set.
func BenchmarkZRange_10K(b *testing.B) {
	benchmarkZRangeSized(b, 10000)
}

func benchmarkZRangeSized(b *testing.B, size int) {
	store := setupTestStoreBenchmark(b)
	preloadZSet(b, store, "zset", size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.ZRange("zset", 0, -1)
	}
}

// BenchmarkZRank_100 benchmarks ZRank on a 100-entry sorted set.
func BenchmarkZRank_100(b *testing.B) {
	benchmarkZRankSized(b, 100)
}

// BenchmarkZRank_1K benchmarks ZRank on a 1K-entry sorted set.
func BenchmarkZRank_1K(b *testing.B) {
	benchmarkZRankSized(b, 1000)
}

// BenchmarkZRank_10K benchmarks ZRank on a 10K-entry sorted set.
func BenchmarkZRank_10K(b *testing.B) {
	benchmarkZRankSized(b, 10000)
}

func benchmarkZRankSized(b *testing.B, size int) {
	store := setupTestStoreBenchmark(b)
	preloadZSet(b, store, "zset", size)
	// Rank the last member (worst case — full scan)
	target := fmt.Sprintf("member-%d", size-1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.ZRank("zset", target)
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
