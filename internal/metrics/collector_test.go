package metrics

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestNewCollector(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	assert.NotEqual(t, nil, c)
	assert.Equal(t, Snapshot{}, c.lastSnapshot)
	assert.True(t, c.snapshotAt.IsZero())
}

func TestCollector_Snapshot_CacheHit(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	c.mu.Lock()
	c.lastSnapshot = Snapshot{ActiveRetries: 42}
	c.snapshotAt = time.Now()
	c.mu.Unlock()

	s := c.Snapshot()
	assert.Equal(t, int64(42), s.ActiveRetries)
}

func TestCollector_Snapshot_CacheExpired(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	c.mu.Lock()
	c.lastSnapshot = Snapshot{ActiveRetries: 42}
	c.snapshotAt = time.Now().Add(-2 * time.Second)
	c.mu.Unlock()

	s := c.Snapshot()
	assert.Equal(t, int64(0), s.ActiveRetries)
}

func TestCollector_Snapshot_WithAllFunctions(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	c.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		return 1, 2, 3, 4, 5, 6.5
	}
	c.MasterReplOffsetFn = func() int64 { return 1000 }
	c.SlaveReplOffsetFn = func() int64 { return 900 }
	c.ReconnectCountFn = func() int64 { return 7 }
	c.SlaveCountFn = func() int { return 3 }
	c.BacklogSizeFn = func() int64 { return 50000 }
	c.BacklogAvailFn = func() int64 { return 30000 }
	c.RoleFn = func() string { return "slave" }
	c.ActiveClientsFn = func() int { return 10 }
	c.BlockedClientsFn = func() int { return 2 }
	c.MonitorClientsFn = func() int { return 1 }
	c.PubSubClientsFn = func() int { return 5 }
	c.PubSubSubsFn = func() int { return 20 }
	c.TotalOutputBytesFn = func() int64 { return 99999 }

	s := c.refresh()

	assert.Equal(t, int64(1), s.ActiveRetries)
	assert.Equal(t, int64(2), s.TotalRetries)
	assert.Equal(t, int64(3), s.WritesBlocked)
	assert.Equal(t, int64(4), s.L0Rejected)
	assert.Equal(t, int64(5), s.L0Delayed)
	assert.Equal(t, 6.5, s.L0Score)

	assert.Equal(t, int64(1000), s.MasterReplOffset)
	assert.Equal(t, int64(900), s.SlaveReplOffset)
	assert.Equal(t, int64(100), s.ReplicationLag)
	assert.Equal(t, int64(7), s.ReconnectCount)
	assert.Equal(t, 3, s.SlaveCount)

	assert.Equal(t, int64(50000), s.BacklogSize)
	assert.Equal(t, int64(30000), s.BacklogAvailable)
	assert.Equal(t, "slave", s.Role)

	assert.Equal(t, 10, s.ActiveClients)
	assert.Equal(t, 2, s.BlockedClients)
	assert.Equal(t, 1, s.MonitorClients)
	assert.Equal(t, 5, s.PubSubClients)
	assert.Equal(t, 20, s.PubSubSubs)
	assert.Equal(t, int64(99999), s.TotalOutputBytes)

	assert.True(t, s.Goroutines > 0)
	assert.True(t, s.AllocBytes > 0)
}

func TestCollector_Snapshot_RoleMasterNoLag(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	c.MasterReplOffsetFn = func() int64 { return 1000 }
	c.SlaveReplOffsetFn = func() int64 { return 900 }
	c.RoleFn = func() string { return "master" }

	s := c.refresh()
	assert.Equal(t, "master", s.Role)
	assert.Equal(t, int64(0), s.ReplicationLag)
}

func TestCollector_Snapshot_NilFunctions(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	s := c.refresh()
	assert.Equal(t, int64(0), s.ActiveRetries)
	assert.Equal(t, int64(0), s.MasterReplOffset)
	assert.Equal(t, int64(0), s.SlaveReplOffset)
	assert.Equal(t, int64(0), s.ReplicationLag)
	assert.Equal(t, "master", s.Role)
	assert.Equal(t, 0, s.ActiveClients)
}

func TestCollector_Snapshot_L0ScoreFallback(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	c.L0ScoreFn = func() float64 { return 3.14 }

	s := c.refresh()
	assert.Equal(t, 3.14, s.L0Score)
}

func TestCollector_Snapshot_L0ScoreFallbackIgnoredWhenRetryMetricsProvides(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	c.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		return 0, 0, 0, 0, 0, 8.5
	}
	c.L0ScoreFn = func() float64 { return 3.14 }

	s := c.refresh()
	assert.Equal(t, 8.5, s.L0Score)
}
