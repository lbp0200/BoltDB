package sentinel

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestMasterInstance_New tests MasterInstance creation
func TestMasterInstance_New(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	assert.Equal(t, "test-master", master.GetName())
	assert.Equal(t, "127.0.0.1:6379", master.GetAddr())
	assert.Equal(t, 2, master.GetQuorum())
}

// TestMasterInstance_State tests MasterInstance state management
func TestMasterInstance_State(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial state should be "ok"
	assert.Equal(t, "ok", master.GetState())

	// Set new state
	master.SetState("odown")
	assert.Equal(t, "odown", master.GetState())
}

// TestMasterInstance_Sdown tests sdown count
func TestMasterInstance_Sdown(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial sdown count should be 0
	assert.Equal(t, 0, master.GetSdownCount())

	// Increment sdown count
	master.IncrSdownCount()
	assert.Equal(t, 1, master.GetSdownCount())

	master.IncrSdownCount()
	assert.Equal(t, 2, master.GetSdownCount())
}

// TestMasterInstance_Slaves tests slave management
func TestMasterInstance_Slaves(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial slaves should be empty
	slaves := master.GetSlaves()
	assert.Equal(t, 0, len(slaves))

	// Add a slave
	slave := NewSlaveInstance("slave1", "127.0.0.1:6380")
	master.AddSlave(slave)

	// Should have one slave now
	slaves = master.GetSlaves()
	assert.Equal(t, 1, len(slaves))
}

// TestMasterInstance_Sentinel tests sentinel management
func TestMasterInstance_Sentinel(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial known sentinel count is 1 (the sentinel itself)
	sentinels := master.GetSentinelCount()
	assert.Equal(t, 1, sentinels)
}

// TestMasterInstance_Addr tests address management
func TestMasterInstance_Addr(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial address
	assert.Equal(t, "127.0.0.1:6379", master.GetAddr())

	// Update address
	master.SetAddr("127.0.0.1:6380")
	assert.Equal(t, "127.0.0.1:6380", master.GetAddr())
}

// TestMasterInstance_IsDown tests IsDown
func TestMasterInstance_IsDown(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initially not down
	assert.False(t, master.IsDown())
}

// TestMasterInstance_SetODown tests SetODown
func TestMasterInstance_SetODown(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Set ODown
	master.SetODown()

	// Should be ODown now
	assert.True(t, master.IsODown())
}

// TestMasterInstance_UpdateSlaveOffset tests UpdateSlaveOffset
func TestMasterInstance_UpdateSlaveOffset(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Update slave offset - should not panic
	master.UpdateSlaveOffset("127.0.0.1:6380", 1000)
	assert.True(t, true)
}

// TestMasterInstance_GetBestSlave tests GetBestSlave
func TestMasterInstance_GetBestSlave(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// No slaves - should return nil
	slave := master.GetBestSlave()
	assert.True(t, slave == nil)
}

// TestMasterInstance_Stop tests Stop
func TestMasterInstance_Stop(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Stop should not panic
	master.Stop()
	assert.True(t, true)
}

// TestSlaveInstance_New tests SlaveInstance creation
func TestSlaveInstance_New(t *testing.T) {
	slave := NewSlaveInstance("slave1", "127.0.0.1:6380")

	assert.Equal(t, "slave1", slave.ID)
	assert.Equal(t, "127.0.0.1:6380", slave.Addr)
}

// TestSentinel_GetRunID tests GetRunID
func TestSentinel_GetRunID(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	runID := sentinel.GetRunID()
	assert.True(t, len(runID) > 0)
}

// TestSentinel_GetConfigEpoch tests GetConfigEpoch
func TestSentinel_GetConfigEpoch(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	epoch := sentinel.GetConfigEpoch()
	assert.True(t, epoch >= 0)
}

// TestSentinel_IncrementConfigEpoch tests IncrementConfigEpoch
func TestSentinel_IncrementConfigEpoch(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	initial := sentinel.GetConfigEpoch()
	next := sentinel.IncrementConfigEpoch()

	assert.Equal(t, initial+1, next)
	assert.True(t, next > initial)
}

// TestSentinel_GetMaster tests GetMaster
func TestSentinel_GetMaster(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Add a master
	err := sentinel.AddMaster("test-master", "127.0.0.1:6379", 2)
	assert.NoError(t, err)

	// Get the master
	master := sentinel.GetMaster("test-master")
	assert.True(t, master != nil)
	assert.Equal(t, "test-master", master.GetName())
}

// TestSentinel_GetMaster_NotFound tests GetMaster when not found
func TestSentinel_GetMaster_NotFound(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Get non-existent master
	master := sentinel.GetMaster("non-existent")
	assert.True(t, master == nil)
}
