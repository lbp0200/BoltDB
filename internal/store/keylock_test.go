package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestKeyLockManager_GetShard_Deterministic(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(256)

	// Same key always maps to same shard
	shard1 := klm.getShard("testkey")
	shard2 := klm.getShard("testkey")
	assert.Equal(t, shard1, shard2)

	// Different keys produce different shards (statistically likely)
	shardA := klm.getShard("aaa")
	shardB := klm.getShard("bbb")
	assert.NotEqual(t, shardA, shardB) // different keys should hash to different shards
}

func TestKeyLockManager_GetShard_CaseSensitive(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(256)
	// FNV is case-sensitive — "foo" and "FOO" should differ
	shardLower := klm.getShard("foo")
	shardUpper := klm.getShard("FOO")
	assert.NotEqual(t, shardLower, shardUpper) // FNV is case-sensitive
}

func TestKeyLockManager_LockUnlock_SameKey(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)
	klm.Lock("key1")
	// Second Lock on same shard would block — just test basic acquire/release
	klm.Unlock("key1")
}

func TestKeyLockManager_RLockRUnlock_SameKey(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)
	klm.RLock("key1")
	klm.RUnlock("key1")
}

func TestKeyLockManager_Lock_ExcludesOtherGoroutine(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	locked := make(chan struct{})
	proceed := make(chan struct{})

	// Goroutine 1: acquires lock, signals, waits
	wg.Add(1)
	go func() {
		defer wg.Done()
		klm.Lock("contended")
		close(locked)
		<-proceed
		klm.Unlock("contended")
	}()

	// Wait for goroutine 1 to acquire lock
	<-locked

	// Goroutine 2: tries to acquire same lock — should block
	blocked := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		klm.Lock("contended")
		close(blocked)
		klm.Unlock("contended")
	}()

	// Verify goroutine 2 is blocked (lock not acquired within 10ms)
	select {
	case <-blocked:
		errCh <- fmt.Errorf("goroutine 2 should be blocked by goroutine 1's lock")
	case <-time.After(10 * time.Millisecond):
		// Expected — goroutine 2 is waiting
	}

	// Release goroutine 1's lock, unblock goroutine 2
	close(proceed)

	// Goroutine 2 should now acquire the lock quickly
	select {
	case <-blocked:
		// OK
	case <-time.After(time.Second):
		errCh <- fmt.Errorf("goroutine 2 should have acquired lock after release")
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestKeyLockManager_DifferentKeysNoContention(t *testing.T) {
	// KeyLockManager uses shard-based locking. Two keys that hash to
	// DIFFERENT shards should NOT contend.
	t.Parallel()
	klm := NewKeyLockManager(256)

	// Find two keys that map to different shards
	var keyA, keyB string
	for i := 0; i < 1000; i++ {
		ka := string(rune('a' + i%26))
		kb := string(rune('A' + i%26))
		if klm.getShard(ka) != klm.getShard(kb) {
			keyA, keyB = ka, kb
			break
		}
	}
	if keyA == "" {
		t.Fatalf("could not find non-colliding keys in sample")
	}

	var wg sync.WaitGroup
	lockedA := make(chan struct{})
	lockedB := make(chan struct{})

	// Goroutine A: lock keyA
	wg.Add(1)
	go func() {
		defer wg.Done()
		klm.Lock(keyA)
		close(lockedA)
		time.Sleep(50 * time.Millisecond) // hold lock
		klm.Unlock(keyA)
	}()

	// Wait for A to acquire
	<-lockedA

	// Goroutine B: lock keyB — should NOT block (different shard)
	wg.Add(1)
	go func() {
		defer wg.Done()
		klm.Lock(keyB)
		close(lockedB)
		klm.Unlock(keyB)
	}()

	// B should acquire immediately (no contention)
	select {
	case <-lockedB:
		// OK — no contention as expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("different-shard keys should not contend")
	}

	wg.Wait()
}

func TestKeyLockManager_SameShardContention(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(8)

	// Find two keys that collide on the same shard
	var keyA, keyB string
	for i := 0; i < 500; i++ {
		for j := i + 1; j < 500; j++ {
			ka := string(rune('a' + i%26))
			kb := string(rune('A' + j%26))
			if klm.getShard(ka) == klm.getShard(kb) && ka != kb {
				keyA, keyB = ka, kb
				break
			}
		}
		if keyA != "" {
			break
		}
	}
	if keyA == "" {
		t.Fatalf("could not find colliding keys for shard contention test")
	}

	// Acquire lock on keyA and hold it
	klm.Lock(keyA)

	// Try to lock keyB (same shard) in a goroutine — should block
	lockedB := make(chan struct{})
	go func() {
		klm.Lock(keyB)
		close(lockedB)
	}()

	select {
	case <-lockedB:
		t.Fatal("same-shard keys should contend")
	case <-time.After(10 * time.Millisecond):
		// Expected — B is blocked by A on same shard
	}

	// Release A, then B should acquire
	klm.Unlock(keyA)
	select {
	case <-lockedB:
		klm.Unlock(keyB)
	case <-time.After(time.Second):
		t.Fatal("B should have acquired lock after A released")
	}
}

func TestKeyLockManager_RLockAllowsMultipleReaders(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)

	var wg sync.WaitGroup
	reader1Ready := make(chan struct{})
	reader1Done := make(chan struct{})

	// Reader 1: acquire RLock and hold
	wg.Add(1)
	go func() {
		defer wg.Done()
		klm.RLock("shared")
		close(reader1Ready)
		time.Sleep(50 * time.Millisecond)
		klm.RUnlock("shared")
		close(reader1Done)
	}()

	<-reader1Ready

	// Reader 2: acquire RLock on same key — should NOT block (RLock allows multiple readers)
	reader2Done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		klm.RLock("shared")
		klm.RUnlock("shared")
		close(reader2Done)
	}()

	select {
	case <-reader2Done:
		// OK — RLock allows concurrent readers
	case <-time.After(100 * time.Millisecond):
		t.Fatal("RLock should allow multiple concurrent readers")
	}

	<-reader1Done
	wg.Wait()
}

func TestKeyLockManager_RLockBlocksExclusiveLock(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)

	klm.RLock("reader-key")

	// Try to acquire exclusive lock — should block
	locked := make(chan struct{})
	go func() {
		klm.Lock("reader-key")
		close(locked)
		klm.Unlock("reader-key")
	}()

	select {
	case <-locked:
		t.Fatal("Lock should be blocked while RLock is held")
	case <-time.After(10 * time.Millisecond):
		// Expected — Lock blocked by RLock
	}

	klm.RUnlock("reader-key")

	// Now the Lock should acquire quickly
	select {
	case <-locked:
		// OK
	case <-time.After(time.Second):
		t.Fatal("Lock should acquire after RLock is released")
	}
}

func TestKeyLockManager_ConcurrentStress(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 20 goroutines hammering Lock+write+Unlock on shared keys
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := string(rune('a' + (id+j)%10))
				klm.Lock(key)
				mu.Lock()
				mu.Unlock()
				klm.Unlock(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestKeyLockManager_ConcurrentReadWriteStress(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(128)
	var wg sync.WaitGroup

	// Mix of readers and writers on overlapping keys
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				key := string(rune('a' + (id+j)%5))
				if j%3 == 0 {
					klm.Lock(key)
					klm.Unlock(key)
				} else {
					klm.RLock(key)
					klm.RUnlock(key)
				}
			}
		}(i)
	}

	wg.Wait()
	// Must not deadlock or panic
}

func TestKeyLockManager_SingleShard(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(1) // collapses to global mutex
	assert.Equal(t, 1, klm.shards)

	klm.Lock("anything")
	klm.Unlock("anything")
	klm.RLock("anything")
	klm.RUnlock("anything")
}

func TestKeyLockManager_DefaultShardsCount(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(0)
	assert.Equal(t, 256, klm.shards)
}

func TestKeyLockManager_LockUnlockDifferentKeysSameShard(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(4) // 4 shards — high collision probability

	// Lock two different keys — they might share a shard but that's by design
	klm.Lock("a")
	klm.Lock("b")
	klm.Unlock("a")
	klm.Unlock("b")
	// Should not deadlock if keys share a shard (Lock is not reentrant)
}
