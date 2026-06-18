package store

import (
	"testing"

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
	klm.RLock("testkey")
	klm.RUnlock("testkey")
}

func TestKeyLockManager_Lock_Unlock_Coverage(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(64)
	klm.Lock("testkey")
	klm.Unlock("testkey")
}

func TestKeyLockManager_NewKeyLockManager_DefaultShards_Coverage(t *testing.T) {
	t.Parallel()
	klm := NewKeyLockManager(0)
	assert.Equal(t, 256, klm.shards)
}
