package store

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/zeebo/assert"
)

// TestClearAllData_PreservesSystemKeys verifies that FLUSHDB (ClearAllData)
// clears user data but keeps replication metadata and cluster config —
// deleting those breaks replId identity (restart generates a new ID, slaves
// forced into FULLRESYNC) and cluster topology (restart claims all slots,
// multi-node restart brain-splits). See 2026-08 incident.
func TestClearAllData_PreservesSystemKeys(t *testing.T) {
	s := setupTestStore(t)

	// Save replication metadata and a fake cluster config.
	assert.NoError(t, s.SaveReplID("deadbeef1234567890abcdef1234567890abcdef12"))
	assert.NoError(t, s.SaveMasterReplOffset(12345))
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("cluster:config"), []byte(`{"node_id":"n1"}`))
	}); err != nil {
		t.Fatalf("save cluster config: %v", err)
	}

	// User data.
	assert.NoError(t, s.Set("user:key:1", "value1"))
	assert.NoError(t, s.Set("user:key:2", "value2"))

	// Clear everything.
	if err := s.ClearAllData(); err != nil {
		t.Fatalf("ClearAllData: %v", err)
	}

	// User data must be gone.
	assert.Equal(t, int64(0), userKeyCount(t, s))

	// System keys must survive.
	id, err := s.LoadReplID()
	assert.NoError(t, err)
	assert.Equal(t, "deadbeef1234567890abcdef1234567890abcdef12", id)

	off, err := s.LoadMasterReplOffset()
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), off)

	var cfg []byte
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("cluster:config"))
		if err != nil {
			return err
		}
		cfg, err = item.ValueCopy(nil)
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, `{"node_id":"n1"}`, string(cfg))
}

// userKeyCount counts non-system keys in the store.
func userKeyCount(t *testing.T, s *BotreonStore) int64 {
	t.Helper()
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if !isSystemKey(it.Item().KeyCopy(nil)) {
				count++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("count keys: %v", err)
	}
	return count
}
