package sentinel

import (
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// Full API coverage for slave.go — state transitions, offsets, online window.
// Prefer getters/setters over raw field writes so mutex paths are exercised.

func TestSlaveInstance_AccessorsAndOffset(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("s1", "10.0.0.2:6380")

	assert.Equal(t, "online", si.GetState())
	assert.Equal(t, "10.0.0.2:6380", si.GetAddr())
	assert.Equal(t, int64(0), si.GetOffset())
	assert.True(t, !si.GetLastSeen().IsZero())
	assert.True(t, si.IsOnline())

	si.SetOffset(12345)
	assert.Equal(t, int64(12345), si.GetOffset())
}

func TestSlaveInstance_HeartbeatUpdatesLastSeenAndOffset(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("s1", "127.0.0.1:6380")
	before := si.GetLastSeen()
	time.Sleep(5 * time.Millisecond)

	si.RecordHeartbeat(999)
	assert.Equal(t, int64(999), si.GetOffset())
	assert.Equal(t, "online", si.GetState())
	assert.True(t, si.GetLastSeen().After(before) || si.GetLastSeen().Equal(before.Add(time.Millisecond)))
	assert.True(t, si.IsOnline())
}

func TestSlaveInstance_OfflineThenHeartbeatReconnects(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("s1", "127.0.0.1:6380")
	si.MarkOffline()
	assert.False(t, si.IsOnline())
	assert.Equal(t, "offline", si.GetState())
	assert.Equal(t, int64(0), si.Reconnects)

	si.RecordHeartbeat(50)
	assert.Equal(t, int64(1), si.Reconnects)
	assert.Equal(t, int64(50), si.GetOffset())
	assert.True(t, si.IsOnline())

	// Already online: reconnect counter must not bump again
	si.RecordHeartbeat(51)
	assert.Equal(t, int64(1), si.Reconnects)
	assert.Equal(t, int64(51), si.GetOffset())
}

func TestSlaveInstance_InfoErrorThresholdMarksOffline(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("s1", "127.0.0.1:6380")

	// Threshold is > 3 (4th error)
	for i := 0; i < 3; i++ {
		si.RecordInfoError()
		assert.Equal(t, "online", si.GetState())
		assert.True(t, si.IsOnline())
	}
	assert.Equal(t, int64(3), si.InfoErrors)

	si.RecordInfoError()
	assert.Equal(t, int64(4), si.InfoErrors)
	assert.Equal(t, "offline", si.GetState())
	assert.False(t, si.IsOnline())
}

func TestSlaveInstance_StaleLastSeenNotOnline(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("s1", "127.0.0.1:6380")
	// 30s window is part of IsOnline contract
	si.mu.Lock()
	si.LastSeen = time.Now().Add(-31 * time.Second)
	si.mu.Unlock()
	assert.False(t, si.IsOnline())

	si.RecordHeartbeat(1)
	assert.True(t, si.IsOnline())
}

func TestSlaveInstance_ConcurrentHeartbeat(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("s1", "127.0.0.1:6380")
	var wg sync.WaitGroup
	const n = 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(off int64) {
			defer wg.Done()
			si.RecordHeartbeat(off)
			_ = si.IsOnline()
			_ = si.GetOffset()
		}(int64(i))
	}
	wg.Wait()
	assert.True(t, si.IsOnline())
	assert.True(t, si.GetOffset() >= 0)
	assert.Equal(t, "online", si.GetState())
}
