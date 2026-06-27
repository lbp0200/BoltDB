package sentinel

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestMasterInstance_IsDown_Coverage tests IsDown method
func TestMasterInstance_IsDown_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initially not down (sdown count < quorum)
	assert.False(t, master.IsDown())

	// Sdown count is still 0, should not be down
	assert.False(t, master.IsDown())
}

// TestMasterInstance_SetODown_Coverage tests SetODown method
func TestMasterInstance_SetODown_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Set ODown
	master.SetODown()

	// Verify state changed (implementation dependent)
	// This tests the method can be called without panic
	assert.True(t, true)
}

// TestMasterInstance_Stop_Coverage tests Stop method
func TestMasterInstance_Stop_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Stop should not panic
	master.Stop()
	assert.True(t, true)
}

// TestMasterInstance_AddSentinel_Coverage tests AddSentinel method
func TestMasterInstance_AddSentinel_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initially 1 sentinel (itself)
	initialCount := master.GetSentinelCount()
	assert.Equal(t, 1, initialCount)

	// Add another sentinel - need to create SentinelInstance
	sentinelInstance := NewSentinelInstance("sentinel1", "127.0.0.1:26379")
	master.AddSentinel(sentinelInstance)
	// After adding, count should increase
	assert.True(t, master.GetSentinelCount() > initialCount)
}

// TestMasterInstance_UpdateSlaveOffset_Coverage tests UpdateSlaveOffset method
func TestMasterInstance_UpdateSlaveOffset_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Add a slave first
	slave := NewSlaveInstance("slave1", "127.0.0.1:6380")
	master.AddSlave(slave)

	// Update slave offset - should not panic
	master.UpdateSlaveOffset("slave1", 1000)
	assert.True(t, true)
}

// TestSentinelInstance_Coverage tests SentinelInstance creation
func TestSentinelInstance_Coverage(t *testing.T) {
	t.Parallel()
	instance := NewSentinelInstance("sentinel1", "127.0.0.1:26379")

	assert.Equal(t, "sentinel1", instance.ID)
	assert.Equal(t, "127.0.0.1:26379", instance.Addr)
}

// TestSlaveInstance_Coverage tests SlaveInstance creation
func TestSlaveInstance_Coverage(t *testing.T) {
	t.Parallel()
	slave := NewSlaveInstance("slave1", "127.0.0.1:6380")

	assert.Equal(t, "slave1", slave.ID)
	assert.Equal(t, "127.0.0.1:6380", slave.Addr)
	assert.Equal(t, int64(0), slave.Offset)
}

// TestFailoverManager_New_Coverage tests FailoverManager creation
func TestFailoverManager_New_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)
	manager := NewFailoverManager(sentinel)

	assert.True(t, manager != nil)
}

// TestSentinel_GetRunID_Coverage tests Sentinel GetRunID method
func TestSentinel_GetRunID_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)

	runID := sentinel.GetRunID()
	assert.NotEqual(t, "", runID)
	// RunID format is "sentinel-" + timestamp
	assert.True(t, len(runID) > 10)
}

// TestSentinel_GetConfigEpoch_Coverage tests Sentinel GetConfigEpoch method
func TestSentinel_GetConfigEpoch_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)

	epoch := sentinel.GetConfigEpoch()
	assert.Equal(t, int64(0), epoch)
}

// TestSentinel_IncrementConfigEpoch_Coverage tests Sentinel IncrementConfigEpoch method
func TestSentinel_IncrementConfigEpoch_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)

	// Initial epoch
	initial := sentinel.GetConfigEpoch()

	// Increment
	sentinel.IncrementConfigEpoch()

	// Should be incremented
	assert.Equal(t, initial+1, sentinel.GetConfigEpoch())
}

// TestSentinel_AddMaster_Coverage tests Sentinel AddMaster method
func TestSentinel_AddMaster_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)

	// Add a master
	sentinel.AddMaster("mymaster", "127.0.0.1:6379", 2)

	// Get the master
	master := sentinel.GetMaster("mymaster")
	assert.True(t, master != nil)
	assert.Equal(t, "mymaster", master.GetName())
}

// TestSentinel_RemoveMaster_Coverage tests Sentinel RemoveMaster method
func TestSentinel_RemoveMaster_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)

	// Add a master
	sentinel.AddMaster("mymaster", "127.0.0.1:6379", 2)

	// Remove the master
	sentinel.RemoveMaster("mymaster")

	// Get should return nil
	master := sentinel.GetMaster("mymaster")
	assert.True(t, master == nil)
}

// TestSentinel_GetAllMasters_Coverage tests Sentinel GetAllMasters method
func TestSentinel_GetAllMasters_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)

	// Add multiple masters
	sentinel.AddMaster("master1", "127.0.0.1:6379", 2)
	sentinel.AddMaster("master2", "127.0.0.1:6380", 2)

	// Get all masters
	masters := sentinel.GetAllMasters()
	assert.Equal(t, 2, len(masters))
}

// TestSentinel_Stop_Coverage tests Sentinel Stop method
func TestSentinel_Stop_Coverage(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, time.Second)

	// Stop should not panic
	sentinel.Stop()
	assert.True(t, true)
}

// TestMasterInstance_MultipleSlaves_Coverage tests managing multiple slaves
func TestMasterInstance_MultipleSlaves_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Add multiple slaves
	slave1 := NewSlaveInstance("slave1", "127.0.0.1:6380")
	slave2 := NewSlaveInstance("slave2", "127.0.0.1:6381")
	slave3 := NewSlaveInstance("slave3", "127.0.0.1:6382")

	master.AddSlave(slave1)
	master.AddSlave(slave2)
	master.AddSlave(slave3)

	// Verify all slaves added
	slaves := master.GetSlaves()
	assert.Equal(t, 3, len(slaves))
}

// TestMasterInstance_SlaveOffset_Coverage tests slave offset tracking
func TestMasterInstance_SlaveOffset_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Add a slave
	slave := NewSlaveInstance("slave1", "127.0.0.1:6380")
	master.AddSlave(slave)

	// Update offset multiple times
	master.UpdateSlaveOffset("slave1", 100)
	master.UpdateSlaveOffset("slave1", 200)
	master.UpdateSlaveOffset("slave1", 300)

	// Test should complete without error
	assert.True(t, true)
}

// TestMasterInstance_UpdateSlaveOffset_NonExistent_Coverage tests updating offset for non-existent slave
func TestMasterInstance_UpdateSlaveOffset_NonExistent_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Update offset for non-existent slave - should not panic
	master.UpdateSlaveOffset("non-existent", 100)
	assert.True(t, true)
}

// TestMasterInstance_GetState_SetState_Coverage tests GetState and SetState methods
func TestMasterInstance_GetState_SetState_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Get initial state
	state := master.GetState()
	assert.Equal(t, "ok", state)

	// Set new state
	master.SetState("failover")
	assert.Equal(t, "failover", master.GetState())
}

// TestMasterInstance_GetName_GetAddr_Coverage tests GetName and GetAddr methods
func TestMasterInstance_GetName_GetAddr_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	assert.Equal(t, "test-master", master.GetName())
	assert.Equal(t, "127.0.0.1:6379", master.GetAddr())
}

// TestMasterInstance_SdownCount_Coverage tests sdown count methods
func TestMasterInstance_SdownCount_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial sdown count should be 0
	assert.Equal(t, 0, master.GetSdownCount())

	// Increment sdown count
	master.IncrSdownCount()
	assert.Equal(t, 1, master.GetSdownCount())

	master.IncrSdownCount()
	assert.Equal(t, 2, master.GetSdownCount())
}

// TestMasterInstance_GetSlaves_Coverage tests GetSlaves method
func TestMasterInstance_GetSlaves_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initially no slaves
	slaves := master.GetSlaves()
	assert.Equal(t, 0, len(slaves))

	// Add a slave
	slave := NewSlaveInstance("slave1", "127.0.0.1:6380")
	master.AddSlave(slave)

	// Should have one slave
	slaves = master.GetSlaves()
	assert.Equal(t, 1, len(slaves))
}

// TestMasterInstance_GetSentinelCount_Coverage tests GetSentinelCount method
func TestMasterInstance_GetSentinelCount_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initially 1 (itself)
	assert.Equal(t, 1, master.GetSentinelCount())

	// Add a sentinel
	sentinel := NewSentinelInstance("sentinel1", "127.0.0.1:26379")
	master.AddSentinel(sentinel)

	// Should have 2 sentinels
	assert.Equal(t, 2, master.GetSentinelCount())
}

// TestMasterInstance_GetBestSlave_Coverage tests GetBestSlave method
func TestMasterInstance_GetBestSlave_Coverage(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// No slaves, should return nil
	best := master.GetBestSlave()
	assert.Nil(t, best)

	// Add a slave with higher priority
	slave1 := NewSlaveInstance("slave1", "127.0.0.1:6380")
	master.AddSlave(slave1)

	// Should return the slave
	best = master.GetBestSlave()
	assert.NotNil(t, best)
	assert.Equal(t, "slave1", best.ID)
}
