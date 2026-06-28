package store

import (
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestNewLRUCache(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(100, time.Minute)
	assert.Equal(t, 0, cache.Size())
	assert.NotNil(t, cache)
}

func TestLRUCache_SetAndGet(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	cache.Set("key1", []byte("value1"))
	cache.Set("key2", []byte("value2"))

	val, ok := cache.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", string(val))

	val, ok = cache.Get("key2")
	assert.True(t, ok)
	assert.Equal(t, "value2", string(val))

	assert.Equal(t, 2, cache.Size())
}

func TestLRUCache_GetMissing(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	_, ok := cache.Get("nonexistent")
	assert.False(t, ok)
}

func TestLRUCache_GetAfterDelete(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	cache.Set("key1", []byte("value1"))
	cache.Delete("key1")

	_, ok := cache.Get("key1")
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Size())
}

func TestLRUCache_DeleteNonexistent(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	cache.Set("key1", []byte("value1"))
	cache.Delete("nonexistent")
	assert.Equal(t, 1, cache.Size())
	val, ok := cache.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", string(val))
}

func TestLRUCache_Eviction(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(3, 0)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	cache.Set("c", []byte("3"))
	assert.Equal(t, 3, cache.Size())

	cache.Set("d", []byte("4"))
	assert.Equal(t, 3, cache.Size())

	_, ok := cache.Get("a")
	assert.False(t, ok) // oldest evicted

	for _, k := range []string{"b", "c", "d"} {
		_, ok := cache.Get(k)
		assert.True(t, ok)
	}
}

func TestLRUCache_LRUOrder(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(3, 0)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	cache.Set("c", []byte("3"))

	cache.Get("a") // makes "a" most recently used

	cache.Set("d", []byte("4")) // "b" should be evicted

	_, ok := cache.Get("a")
	assert.True(t, ok) // recently accessed, not evicted
	_, ok = cache.Get("b")
	assert.False(t, ok) // oldest, evicted
	_, ok = cache.Get("c")
	assert.True(t, ok)
	_, ok = cache.Get("d")
	assert.True(t, ok)
}

func TestLRUCache_UpdateExisting(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	cache.Set("key1", []byte("old_value"))
	cache.Set("key1", []byte("new_value"))

	assert.Equal(t, 1, cache.Size())
	val, ok := cache.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "new_value", string(val))
}

func TestLRUCache_UpdatePreservesLRUOrder(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(3, 0)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	cache.Set("c", []byte("3"))

	cache.Set("a", []byte("updated")) // updates and moves to MRU

	cache.Set("d", []byte("4")) // "b" should be evicted

	_, ok := cache.Get("a")
	assert.True(t, ok)
	_, ok = cache.Get("b")
	assert.False(t, ok) // oldest, evicted
}

func TestLRUCache_TTL(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 50*time.Millisecond)
	cache.Set("key1", []byte("value1"))

	val, ok := cache.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", string(val))

	time.Sleep(100 * time.Millisecond)

	_, ok = cache.Get("key1")
	assert.False(t, ok) // expired

	assert.Equal(t, 0, cache.Size()) // expired entry removed
}

func TestLRUCache_TTLZero(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	cache.Set("key1", []byte("value1"))

	for i := 0; i < 5; i++ {
		val, ok := cache.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", string(val))
	}
}

func TestLRUCache_Clear(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	assert.Equal(t, 2, cache.Size())

	cache.Clear()
	assert.Equal(t, 0, cache.Size())

	_, ok := cache.Get("a")
	assert.False(t, ok)
	_, ok = cache.Get("b")
	assert.False(t, ok)
}

func TestLRUCache_ClearReusable(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(5, 0)
	cache.Set("a", []byte("1"))
	cache.Clear()

	cache.Set("b", []byte("2"))
	assert.Equal(t, 1, cache.Size())
	val, ok := cache.Get("b")
	assert.True(t, ok)
	assert.Equal(t, "2", string(val))
}

func TestLRUCache_EmptyCache(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(5, 0)
	assert.Equal(t, 0, cache.Size())
	_, ok := cache.Get("anything")
	assert.False(t, ok)
}

func TestLRUCache_SingleSlotEviction(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(1, 0)
	cache.Set("a", []byte("1"))
	assert.Equal(t, 1, cache.Size())

	cache.Set("b", []byte("2"))
	assert.Equal(t, 1, cache.Size())

	_, ok := cache.Get("a")
	assert.False(t, ok) // evicted
	val, ok := cache.Get("b")
	assert.True(t, ok)
	assert.Equal(t, "2", string(val))
}

func TestLRUCache_EvictionPreservesMostRecent(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(3, 0)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	cache.Set("c", []byte("3"))

	cache.Get("a")
	cache.Get("b")

	cache.Set("d", []byte("4")) // "c" should be evicted

	for _, k := range []string{"a", "b", "d"} {
		_, ok := cache.Get(k)
		assert.True(t, ok)
	}
	_, ok := cache.Get("c")
	assert.False(t, ok) // evicted
}

func TestLRUCache_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(100, 0)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id%26))
			cache.Set(key, []byte{byte(id)})
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id%26))
			cache.Get(key)
		}(i)
	}

	wg.Wait()
	assert.True(t, cache.Size() <= 26) // 26 unique keys (a-z), cache can't have more
}

func TestLRUCache_ConcurrentSetAndDelete(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(50, 0)
	var wg sync.WaitGroup

	for i := 0; i < 30; i++ {
		key := string(rune('a' + i%26))
		cache.Set(key, []byte{byte(i)})
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id%26))
			cache.Set(key, []byte{byte(id)})
			cache.Delete(key)
			cache.Set(key, []byte{byte(id + 100)})
		}(i)
	}

	wg.Wait()
}

func TestLRUCache_EvictionMaintainsOrderAfterGets(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(4, 0)
	cache.Set("w", []byte("1"))
	cache.Set("x", []byte("2"))
	cache.Set("y", []byte("3"))
	cache.Set("z", []byte("4"))

	cache.Get("y")
	cache.Get("z")

	cache.Set("new", []byte("5")) // "w" should be evicted

	_, ok := cache.Get("w")
	assert.False(t, ok) // oldest, evicted
	for _, k := range []string{"x", "y", "z", "new"} {
		_, ok := cache.Get(k)
		assert.True(t, ok)
	}
}

func TestLRUCache_GetOnExpiredRemovesEntry(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 30*time.Millisecond)
	cache.Set("key1", []byte("value1"))

	time.Sleep(50 * time.Millisecond)

	_, ok := cache.Get("key1")
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Size())
}

func TestLRUCache_ZeroMaxSize(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(0, 0)
	assert.Equal(t, 0, cache.Size())

	cache.Set("key1", []byte("value1"))
	// With maxSize=0, eviction occurs on every Set but evictLRU is a no-op
	// since accessOrder is empty. Entry is still stored.
	// This is by design — maxSize=0 is not a valid configuration.
	assert.Equal(t, 1, cache.Size())
	val, ok := cache.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", string(val))
}

func TestLRUCache_DeletePreservesOtherEntries(t *testing.T) {
	t.Parallel()
	cache := NewLRUCache(10, 0)
	cache.Set("a", []byte("1"))
	cache.Set("b", []byte("2"))
	cache.Set("c", []byte("3"))

	cache.Delete("b")

	assert.Equal(t, 2, cache.Size())
	for _, k := range []string{"a", "c"} {
		_, ok := cache.Get(k)
		assert.True(t, ok)
	}
}
