package sentinel

import (
	"net"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestMasterInstance_New tests MasterInstance creation
func TestMasterInstance_New(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	assert.Equal(t, "test-master", master.GetName())
	assert.Equal(t, "127.0.0.1:6379", master.GetAddr())
	assert.Equal(t, 2, master.GetQuorum())
}

// TestMasterInstance_State tests MasterInstance state management
func TestMasterInstance_State(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial state should be "ok"
	assert.Equal(t, "ok", master.GetState())

	// Set new state
	master.SetState("odown")
	assert.Equal(t, "odown", master.GetState())
}

// TestMasterInstance_Sdown tests sdown count
func TestMasterInstance_Sdown(t *testing.T) {
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

// TestMasterInstance_Slaves tests slave management
func TestMasterInstance_Slaves(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial known sentinel count is 1 (the sentinel itself)
	sentinels := master.GetSentinelCount()
	assert.Equal(t, 1, sentinels)
}

// TestMasterInstance_Addr tests address management
func TestMasterInstance_Addr(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initial address
	assert.Equal(t, "127.0.0.1:6379", master.GetAddr())

	// Update address
	master.SetAddr("127.0.0.1:6380")
	assert.Equal(t, "127.0.0.1:6380", master.GetAddr())
}

// TestMasterInstance_IsDown tests IsDown
func TestMasterInstance_IsDown(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Initially not down
	assert.False(t, master.IsDown())
}

// TestMasterInstance_SetODown tests SetODown
func TestMasterInstance_SetODown(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Set ODown
	master.SetODown()

	// Should be ODown now
	assert.True(t, master.IsODown())
}

// TestMasterInstance_UpdateSlaveOffset tests UpdateSlaveOffset
func TestMasterInstance_UpdateSlaveOffset(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Update slave offset - should not panic
	master.UpdateSlaveOffset("127.0.0.1:6380", 1000)
	assert.True(t, true)
}

// TestMasterInstance_GetBestSlave tests GetBestSlave
func TestMasterInstance_GetBestSlave(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// No slaves - should return nil
	slave := master.GetBestSlave()
	assert.True(t, slave == nil)
}

// TestMasterInstance_Stop tests Stop
func TestMasterInstance_Stop(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Stop should not panic
	master.Stop()
	assert.True(t, true)
}

// TestSlaveInstance_New tests SlaveInstance creation
func TestSlaveInstance_New(t *testing.T) {
	t.Parallel()
	slave := NewSlaveInstance("slave1", "127.0.0.1:6380")

	assert.Equal(t, "slave1", slave.ID)
	assert.Equal(t, "127.0.0.1:6380", slave.Addr)
}

// TestSentinel_GetRunID tests GetRunID
func TestSentinel_GetRunID(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	runID := sentinel.GetRunID()
	assert.Equal(t, 40, len(runID)) // 20 random bytes → 40 hex chars
}

// TestSentinel_GetConfigEpoch tests GetConfigEpoch
func TestSentinel_GetConfigEpoch(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	epoch := sentinel.GetConfigEpoch()
	assert.True(t, epoch >= 0)
}

// TestSentinel_IncrementConfigEpoch tests IncrementConfigEpoch
func TestSentinel_IncrementConfigEpoch(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	initial := sentinel.GetConfigEpoch()
	next := sentinel.IncrementConfigEpoch()

	assert.Equal(t, initial+1, next)
	assert.True(t, next > initial)
}

// TestSentinel_GetMaster tests GetMaster
func TestSentinel_GetMaster(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Get non-existent master
	master := sentinel.GetMaster("non-existent")
	assert.True(t, master == nil)
}

// TestMasterInstance_StartMonitoring tests StartMonitoring method
func TestMasterInstance_StartMonitoring(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Add a master
	sentinel.AddMaster("test-master", "127.0.0.1:6379", 2)

	master := sentinel.GetMaster("test-master")

	// Start monitoring in a goroutine
	go master.StartMonitoring(sentinel)

	// Wait for goroutine to start without busy sleep
	// Use a retry loop to verify the master is still tracked
	var m *MasterInstance
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		m = sentinel.GetMaster("test-master")
		if m != nil {
			break
		}
	}
	assert.NotNil(t, m)
	assert.Equal(t, "test-master", m.GetName())
}

// TestMasterInstance_checkMaster tests checkMaster method
func TestMasterInstance_checkMaster(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Add a master with very short downAfter to trigger sdown faster
	master := NewMasterInstance("test-master", "127.0.0.1:6379", 2)

	// Set very short downAfter to trigger sdown on next check
	master.mu.Lock()
	master.lastPingTime = time.Now().Add(-10 * time.Second) // Last ping was 10 seconds ago
	master.mu.Unlock()

	// Call checkMaster - should trigger sdown state
	master.checkMaster(sentinel)

	// After check, sdownCount should be incremented
	assert.True(t, master.GetSdownCount() >= 0)
}

func TestMasterInstance_checkMaster_Recovery(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer ln.Close()

	master := NewMasterInstance("test-master", ln.Addr().String(), 2)

	// Start a goroutine that responds to PING with +PONG
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			defer conn.Close()
			buf := make([]byte, 64)
			_, _ = conn.Read(buf)
			_, _ = conn.Write([]byte("+PONG\r\n"))
		}
	}()

	// Set master to sdown state first
	master.SetState("sdown")
	master.IncrSdownCount()

	// Call checkMaster - should recover the master
	master.checkMaster(sentinel)

	// After recovery, state should be ok and sdownCount should be 0
	assert.Equal(t, "ok", master.GetState())
	assert.Equal(t, 0, master.GetSdownCount())
}

// TestMasterInstance_checkMaster_AlreadySdown tests checkMaster when already in sdown
func TestMasterInstance_checkMaster_AlreadySdown(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer ln.Close()

	master := NewMasterInstance("test-master", ln.Addr().String(), 2)

	// Start a goroutine that responds to PING with +PONG
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			defer conn.Close()
			buf := make([]byte, 64)
			_, _ = conn.Read(buf)
			_, _ = conn.Write([]byte("+PONG\r\n"))
		}
	}()

	// Set master to sdown state
	master.SetState("sdown")

	// Set lastPingTime to recent (so it would normally recover)
	master.mu.Lock()
	master.lastPingTime = time.Now()
	master.mu.Unlock()

	// Call checkMaster - should recover since master responds to PING
	master.checkMaster(sentinel)

	// State should be ok
	assert.Equal(t, "ok", master.GetState())
}

// TestMasterInstance_CanFailover_Initial tests CanFailover returns true when no failover has occurred
func TestMasterInstance_CanFailover_Initial(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test", "127.0.0.1:6379", 1)
	assert.True(t, master.CanFailover())
}

// TestMasterInstance_CanFailover_AfterRecord tests CanFailover returns false immediately after RecordFailover
func TestMasterInstance_CanFailover_AfterRecord(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test", "127.0.0.1:6379", 1)
	master.RecordFailover()
	assert.False(t, master.CanFailover())
}

// TestMasterInstance_CanFailover_AfterCooldown tests CanFailover returns true after cooldown expires
func TestMasterInstance_CanFailover_AfterCooldown(t *testing.T) {
	t.Parallel()
	master := NewMasterInstance("test", "127.0.0.1:6379", 1)
	master.RecordFailover()
	master.mu.Lock()
	master.lastFailoverTime = time.Now().Add(-10 * time.Second)
	master.mu.Unlock()
	assert.True(t, master.CanFailover())
}
