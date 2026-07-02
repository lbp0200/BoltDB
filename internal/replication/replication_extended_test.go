package replication

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestReplicationManagerExtended_Role tests role management
func TestReplicationManagerExtended_Role(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initial role should be "master"
	assert.Equal(t, RoleMaster, rm.GetRole())

	// Set role to slave
	rm.SetRole(RoleSlave)
	assert.Equal(t, RoleSlave, rm.GetRole())

	// Check IsMaster and IsSlave
	assert.False(t, rm.IsMaster())
	assert.True(t, rm.IsSlave())

	// Set back to master
	rm.SetRole(RoleMaster)
	assert.True(t, rm.IsMaster())
	assert.False(t, rm.IsSlave())
}

// TestReplicationManagerExtended_ReplicationID tests replication ID
func TestReplicationManagerExtended_ReplicationID(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	replID := rm.GetReplicationID()
	assert.True(t, len(replID) > 0)
}

// TestReplicationManagerExtended_MasterAddr tests master address management
func TestReplicationManagerExtended_MasterAddr(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initial master address should be empty
	assert.Equal(t, "", rm.GetMasterAddr())

	// Set master address
	rm.SetMasterAddr("127.0.0.1:6379")
	assert.Equal(t, "127.0.0.1:6379", rm.GetMasterAddr())
}

// TestReplicationManagerExtended_SlaveByID tests getting slave by ID
func TestReplicationManagerExtended_SlaveByID(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Non-existent slave should return nil
	slave := rm.GetSlaveByID("non-existent")
	assert.True(t, slave == nil)
}

// TestReplicationManagerExtended_SlaveByAddr tests getting slave by address
func TestReplicationManagerExtended_SlaveByAddr(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Non-existent slave should return nil
	slave := rm.GetSlaveByAddr("127.0.0.1:6380")
	assert.True(t, slave == nil)
}

// TestReplicationManagerExtended_SlaveCount tests slave count
func TestReplicationManagerExtended_SlaveCount(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initial slave count should be 0
	assert.Equal(t, 0, rm.GetSlaveCount())
}

// TestReplicationManagerExtended_GetSlaves tests getting all slaves
func TestReplicationManagerExtended_GetSlaves(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initial slaves should be empty
	slaves := rm.GetSlaves()
	assert.Equal(t, 0, len(slaves))
}

// TestReplicationManagerExtended_Offset tests replication offset
func TestReplicationManagerExtended_Offset(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initial offset should be 0
	assert.Equal(t, int64(0), rm.GetMasterReplOffset())

	// Set offset
	rm.SetMasterReplOffset(100)
	assert.Equal(t, int64(100), rm.GetMasterReplOffset())

	// Increment offset
	rm.IncrementReplOffset(50)
	assert.Equal(t, int64(150), rm.GetMasterReplOffset())
}

// TestReplicationManagerExtended_Backlog tests backlog
func TestReplicationManagerExtended_Backlog(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Get backlog
	backlog := rm.GetBacklog()
	assert.True(t, backlog != nil)
}

// TestReplicationManagerExtended_MasterConnection tests master connection
func TestReplicationManagerExtended_MasterConnection(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	// Initially no master connection
	assert.True(t, rm.GetMasterConnection() == nil)
}

// TestReplicationManagerExtended_UpdateSlaveAckOffset tests UpdateSlaveAckOffset
func TestReplicationManagerExtended_UpdateSlaveAckOffset(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	assert.Equal(t, 0, rm.GetSlaveCount())
	rm.UpdateSlaveAckOffset("slave1", 100)
	assert.Equal(t, 0, rm.GetSlaveCount())
}

// TestReplicationManager_LoadRDB tests LoadRDB
func TestReplicationManager_LoadRDB(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	err := rm.LoadRDB([]byte{})
	assert.Error(t, err)
}

// TestReplicationManager_PersistedReplId verifies that NewReplicationManager
// loads the persisted replId and masterReplOffset from the store (F1a/F1b).
func TestReplicationManager_PersistedReplId(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create store with metadata, close
	s1, err := store.NewBadgerStore(dir)
	assert.NoError(t, err)
	err = s1.SaveReplID("persisted-repl-id")
	assert.NoError(t, err)
	err = s1.SaveMasterReplOffset(12345)
	assert.NoError(t, err)
	assert.NoError(t, s1.Close())

	// Phase 2: create new rm from same store, verify it loads persisted values
	s2, err := store.NewBadgerStore(dir)
	assert.NoError(t, err)
	defer s2.Close()
	rm := NewReplicationManager(s2)
	assert.Equal(t, "persisted-repl-id", rm.GetReplicationID())
	assert.Equal(t, int64(12345), rm.GetMasterReplOffset())
	rm.Stop()
}
