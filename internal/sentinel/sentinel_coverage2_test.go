package sentinel

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestConfigProvider_GetMasterAddrByName tests GetMasterAddrByName method
func TestConfigProvider_GetMasterAddrByName_Coverage(t *testing.T) {
	sentinelInstance := NewSentinel(1, 0)
	sentinelInstance.AddMaster("mymaster", "127.0.0.1:6379", 2)

	provider := NewConfigProvider(sentinelInstance)
	addr, err := provider.GetMasterAddrByName("mymaster")
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:6379", addr)
}

// TestConfigProvider_GetMasterAddrByName_NotFound tests GetMasterAddrByName with non-existent master
func TestConfigProvider_GetMasterAddrByName_NotFound_Coverage(t *testing.T) {
	sentinelInstance := NewSentinel(1, 0)

	provider := NewConfigProvider(sentinelInstance)
	_, err := provider.GetMasterAddrByName("nonexistent")
	assert.True(t, err != nil)
}

// TestConfigProvider_GetMasters tests GetMasters method
func TestConfigProvider_GetMasters_Coverage(t *testing.T) {
	sentinelInstance := NewSentinel(1, 0)
	sentinelInstance.AddMaster("master1", "127.0.0.1:6379", 2)
	sentinelInstance.AddMaster("master2", "127.0.0.1:6380", 2)

	provider := NewConfigProvider(sentinelInstance)
	masters := provider.GetMasters()
	assert.Equal(t, 2, len(masters))
}

// TestConfigProvider_GetSlaves tests GetSlaves method
func TestConfigProvider_GetSlaves_Coverage(t *testing.T) {
	sentinelInstance := NewSentinel(1, 0)
	sentinelInstance.AddMaster("mymaster", "127.0.0.1:6379", 2)

	provider := NewConfigProvider(sentinelInstance)
	slaves := provider.GetSlaves("mymaster")
	// Initially no slaves
	assert.Equal(t, 0, len(slaves))
}

// TestMasterInstance_GetQuorum tests GetQuorum method
func TestMasterInstance_GetQuorum_Coverage(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 3)
	assert.Equal(t, 3, master.GetQuorum())
}

// TestMasterInstance_GetName tests GetName method
func TestMasterInstance_GetName_Coverage(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)
	assert.Equal(t, "test-master", master.GetName())
}

// TestMasterInstance_GetAddr tests GetAddr method
func TestMasterInstance_GetAddr_Coverage(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)
	assert.Equal(t, "127.0.0.1:6379", master.GetAddr())
}

// TestMasterInstance_SetAddr tests SetAddr method
func TestMasterInstance_SetAddr_Coverage(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)
	master.SetAddr("127.0.0.1:6380")
	assert.Equal(t, "127.0.0.1:6380", master.GetAddr())
}

// TestSentinel_AddMaster_Duplicate tests adding duplicate master
func TestSentinel_AddMaster_Duplicate_Coverage(t *testing.T) {
	sentinelInstance := NewSentinel(1, 0)

	// Add first master
	err := sentinelInstance.AddMaster("mymaster", "127.0.0.1:6379", 2)
	assert.NoError(t, err)

	// Add duplicate should return error
	err = sentinelInstance.AddMaster("mymaster", "127.0.0.1:6379", 2)
	assert.True(t, err != nil)
}

// TestMasterInstance_IncrSdownCount tests IncrSdownCount method
func TestMasterInstance_IncrSdownCount_Coverage(t *testing.T) {
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	initial := master.GetSdownCount()
	master.IncrSdownCount()
	assert.Equal(t, initial+1, master.GetSdownCount())
}
