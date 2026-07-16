package store

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestGetTotalSubscriberCount_Coverage(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	assert.Equal(t, 0, psm.GetTotalSubscriberCount())
	sub := NewSubscriber("test-sub")
	psm.mu.Lock()
	psm.subscribers[sub] = true
	psm.mu.Unlock()
	assert.Equal(t, 1, psm.GetTotalSubscriberCount())
}

func TestGetBlockedClientCount_Coverage(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	assert.Equal(t, 0, store.GetBlockedClientCount())
	ch := make(chan BlockingResult)
	store.blockingMu.Lock()
	store.blockingPopChans["key1"] = append(store.blockingPopChans["key1"], ch)
	store.blockingPopChans["key1"] = append(store.blockingPopChans["key1"], ch)
	store.blockingMu.Unlock()
	assert.Equal(t, 2, store.GetBlockedClientCount())
}

func TestNewBadgerStoreWithCompression_Coverage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewBadgerStoreWithCompression(dir, CompressionNone)
	assert.NoError(t, err)
	assert.True(t, s != nil)
	s.Close()
}

func TestKeyLockManager_RLock_RUnlock_Coverage(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)
	// Concurrent readers must not block each other indefinitely
	done := make(chan struct{}, 2)
	klm.RLock("testkey")
	go func() {
		klm.RLock("testkey")
		done <- struct{}{}
		klm.RUnlock("testkey")
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second RLock blocked under shared read lock")
	}
	klm.RUnlock("testkey")
}

func TestKeyLockManager_Lock_Unlock_Coverage(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)
	klm.Lock("testkey")
	// Exclusive lock held: another Lock must wait until Unlock
	acquired := make(chan struct{})
	go func() {
		klm.Lock("testkey")
		close(acquired)
		klm.Unlock("testkey")
	}()
	select {
	case <-acquired:
		t.Fatal("second Lock acquired while exclusive lock held")
	case <-time.After(50 * time.Millisecond):
		// expected: blocked
	}
	klm.Unlock("testkey")
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("second Lock did not acquire after Unlock")
	}
}

func TestKeyLockManager_NewKeyLockManager_DefaultShards_Coverage(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(0)
	assert.Equal(t, 256, klm.shards)
}
