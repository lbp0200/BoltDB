package store

import (
	"fmt"
	"testing"
)

func BenchmarkStringMGet_5(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	for i := 0; i < 5; i++ {
		_ = store.Set(fmt.Sprintf("key%d", i), "value")
	}

	keys := []string{"key0", "key1", "key2", "key3", "key4"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.MGet(keys...)
	}
}

func BenchmarkStringMGet_10(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	for i := 0; i < 10; i++ {
		_ = store.Set(fmt.Sprintf("key%d", i), "value")
	}

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.MGet(keys...)
	}
}

func BenchmarkStringMGet_100(b *testing.B) {
	store := setupTestStoreBenchmark(b)

	for i := 0; i < 100; i++ {
		_ = store.Set(fmt.Sprintf("key%d", i), "value")
	}

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.MGet(keys...)
	}
}
