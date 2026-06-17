package replication

import (
	"runtime"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

func TestNewSlaveReconnector(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	assert.Equal(t, SlaveDisconnected, sr.GetState())
	assert.Equal(t, "127.0.0.1:6379", sr.GetMasterAddr())
}

func TestSlaveReconnector_StartStop(t *testing.T) {
	t.Parallel()
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:59999")
	sr.Start()
	time.Sleep(50 * time.Millisecond)

	// Should be in connecting or disconnected state
	state := sr.GetState()
	assert.True(t, state == SlaveConnecting || state == SlaveDisconnected)

	sr.Stop()
	assert.Equal(t, SlaveDisconnected, sr.GetState())
}

func TestReconnectConfig_Defaults(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, DefaultReconnectConfig.MaxRetries)
	assert.Equal(t, 1*time.Second, DefaultReconnectConfig.BaseBackoff)
	assert.Equal(t, 60*time.Second, DefaultReconnectConfig.MaxBackoff)
	assert.Equal(t, 30*time.Second, DefaultReconnectConfig.ResetAfter)
}

func TestSlaveReconnector_GoroutineLeak(t *testing.T) {
	t.Parallel()
	before := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		testStore := setupTestStore(t)
		rm := NewReplicationManager(testStore)
		sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:59999")
		sr.Start()
		time.Sleep(10 * time.Millisecond)
		sr.Stop()
		rm.Stop()
		testStore.CloseWithTimeout(store.CloseTimeout)
		// Let BadgerDB background goroutines settle before next iteration.
		runtime.Gosched()
		time.Sleep(20 * time.Millisecond)
	}

	// Give goroutines time to fully exit (BadgerDB compaction/GC goroutines
	// may not terminate immediately after Close returns).
	for i := 0; i < 20; i++ {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
		after := runtime.NumGoroutine()
		if after-before <= 15 {
			return
		}
	}
	after := runtime.NumGoroutine()
	t.Errorf("goroutine leak suspected: before=%d after=%d leaked=%d", before, after, after-before)
}
