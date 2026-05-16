package replication

import (
	"runtime"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestNewSlaveReconnector(t *testing.T) {
	testStore := setupTestStore(t)
	rm := NewReplicationManager(testStore)
	defer rm.Stop()

	sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:6379")
	assert.Equal(t, SlaveDisconnected, sr.GetState())
	assert.Equal(t, "127.0.0.1:6379", sr.GetMasterAddr())
}

func TestSlaveReconnector_StartStop(t *testing.T) {
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
	assert.Equal(t, 0, DefaultReconnectConfig.MaxRetries)
	assert.Equal(t, 1*time.Second, DefaultReconnectConfig.BaseBackoff)
	assert.Equal(t, 60*time.Second, DefaultReconnectConfig.MaxBackoff)
	assert.Equal(t, 30*time.Second, DefaultReconnectConfig.ResetAfter)
}

func TestSlaveReconnector_GoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		testStore := setupTestStore(t)
		rm := NewReplicationManager(testStore)
		sr := NewSlaveReconnector(rm, testStore, "127.0.0.1:59999")
		sr.Start()
		time.Sleep(10 * time.Millisecond)
		sr.Stop()
		rm.Stop()
		testStore.Close()
	}

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > 5 {
		t.Errorf("goroutine leak detected: before=%d after=%d leaked=%d", before, after, leaked)
	}
}
