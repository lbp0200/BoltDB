package sentinel

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestSlaveInstance_IsOnline_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	assert.True(t, si.IsOnline())
}

func TestSlaveInstance_IsOnline_Offline_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	si.State = "offline"
	assert.False(t, si.IsOnline())
}

func TestSlaveInstance_IsOnline_Stale_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	si.LastSeen = time.Now().Add(-31 * time.Second)
	assert.False(t, si.IsOnline())
}

func TestSlaveInstance_RecordHeartbeat_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	si.RecordHeartbeat(42)
	assert.Equal(t, int64(42), si.Offset)
	assert.Equal(t, "online", si.State)
}

func TestSlaveInstance_RecordHeartbeat_Reconnect_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	si.MarkOffline()
	assert.Equal(t, int64(0), si.Reconnects)
	si.RecordHeartbeat(99)
	assert.Equal(t, int64(1), si.Reconnects)
	assert.Equal(t, "online", si.State)
}

func TestSlaveInstance_MarkOffline_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	si.MarkOffline()
	assert.Equal(t, "offline", si.State)
}

func TestSlaveInstance_RecordInfoError_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	si.RecordInfoError()
	assert.Equal(t, int64(1), si.InfoErrors)
	assert.Equal(t, "online", si.State)
}

func TestSlaveInstance_RecordInfoError_Offline_Coverage(t *testing.T) {
	t.Parallel()
	si := NewSlaveInstance("slave1", "127.0.0.1:6379")
	for i := 0; i < 3; i++ {
		si.RecordInfoError()
	}
	assert.Equal(t, int64(3), si.InfoErrors)
	assert.Equal(t, "online", si.State)
	si.RecordInfoError()
	assert.Equal(t, int64(4), si.InfoErrors)
	assert.Equal(t, "offline", si.State)
}

func TestMetrics_RecordSdown_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdown("mymaster")
	assert.Equal(t, int64(1), m.DetectionCount)
	m.RecordSdown("mymaster")
	assert.Equal(t, int64(2), m.DetectionCount)
}

func TestMetrics_RecordNewMaster_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordNewMaster("mymaster")
	assert.Equal(t, int64(1), m.SuccessfulFailovers)
}

func TestMetrics_RecordLeaderChange_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordLeaderChange()
	assert.Equal(t, int64(1), m.LeaderChanges)
}

func TestMetrics_RecordSdownBroadcast_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdownBroadcast()
	assert.Equal(t, int64(1), m.SDownBroadcasts)
}

func TestMetrics_RecordSdownReceived_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdownReceived()
	assert.Equal(t, int64(1), m.SDownReceived)
}

func TestMetrics_RecordGossipSend_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordGossipSend("msg1")
	_, exists := m.GossipSendTimes["msg1"]
	assert.True(t, exists)
}

func TestMetrics_RecordGossipRecv_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordGossipRecv("msg1")
	_, exists := m.GossipRecvTimes["msg1"]
	assert.True(t, exists)
}

func TestNewConfigManager_Coverage(t *testing.T) {
	t.Parallel()
	s := NewSentinel(1, 0)
	cm := newConfigManager(s, "/tmp/sentinel-test")
	assert.True(t, cm != nil)
	assert.Equal(t, s, cm.sentinel)
	assert.Equal(t, "/tmp/sentinel-test", cm.dataDir)
}

func TestConfigManager_Save_Load_Coverage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := NewSentinelWithDataDir(2, 0, dir)
	assert.True(t, s.ConfigManager != nil)
	err := s.AddMaster("mymaster", "127.0.0.1:6379", 2)
	assert.NoError(t, err)
	path := filepath.Join(dir, configFileName)
	_, err = os.Stat(path)
	assert.NoError(t, err)
}

func TestConfigManager_Load_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := NewSentinelWithDataDir(2, 0, dir)
	cm := newConfigManager(s, dir)
	found, err := cm.Load()
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestConfigManager_Load_InvalidData_Coverage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, configFileName)
	err := os.WriteFile(path, []byte("invalid json"), 0644)
	assert.NoError(t, err)
	s := NewSentinelWithDataDir(2, 0, dir)
	cm := newConfigManager(s, dir)
	found, err := cm.Load()
	assert.Error(t, err)
	assert.False(t, found)
}

func TestSentinelHandler_Stop_Coverage(t *testing.T) {
	sh := NewSentinelHandler(NewSentinel(1, 0))
	sh.Stop()
}

func TestSentinelHandler_Stop_WithConnections_Coverage(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	sh := NewSentinelHandler(NewSentinel(1, 0))
	sh.mu.Lock()
	sh.conns[server] = struct{}{}
	sh.wg.Add(1)
	sh.mu.Unlock()
	done := make(chan struct{})
	go func() {
		sh.wg.Done()
		close(done)
	}()
	sh.Stop()
	<-done
}

func TestMetrics_GetDetectionCount_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdown("mymaster")
	assert.Equal(t, int64(1), m.GetDetectionCount())
}

func TestMetrics_GetLeaderChanges_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordLeaderChange()
	assert.Equal(t, int64(1), m.GetLeaderChanges())
}

func TestMetrics_GetSuccessfulFailovers_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordNewMaster("mymaster")
	assert.Equal(t, int64(1), m.GetSuccessfulFailovers())
}

func TestMetrics_GetFailedFailovers_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFailoverFailed("mymaster")
	assert.Equal(t, int64(1), m.GetFailedFailovers())
}

func TestMetrics_GetFailoverStarted_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFailoverStart("mymaster")
	assert.Equal(t, int64(1), m.GetFailoverStarted())
}

func TestMetrics_GetODownReached_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordODown("mymaster")
	assert.Equal(t, int64(1), m.GetODownReached())
}

func TestMetrics_GetSDownBroadcasts_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdownBroadcast()
	assert.Equal(t, int64(1), m.GetSDownBroadcasts())
}

func TestMetrics_GetSDownReceived_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdownReceived()
	assert.Equal(t, int64(1), m.GetSDownReceived())
}

func TestMetrics_DetectionLatency_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	assert.Equal(t, time.Duration(0), m.DetectionLatency("nonexistent"))
}

func TestMetrics_DetectionLatency_Found_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdown("mymaster")
	m.RecordODown("mymaster")
	assert.True(t, m.DetectionLatency("mymaster") >= 0)
}

func TestMetrics_ElectionDuration_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	assert.Equal(t, time.Duration(0), m.ElectionDuration("nonexistent"))
}

func TestMetrics_ElectionDuration_Found_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordODown("mymaster")
	m.RecordFailoverStart("mymaster")
	assert.True(t, m.ElectionDuration("mymaster") >= 0)
}

func TestMetrics_RecoveryDuration_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	assert.Equal(t, time.Duration(0), m.RecoveryDuration("nonexistent"))
}

func TestMetrics_RecoveryDuration_Found_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFailoverStart("mymaster")
	m.RecordNewMaster("mymaster")
	assert.True(t, m.RecoveryDuration("mymaster") >= 0)
}

func TestMetrics_LeaderStabilization_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	assert.Equal(t, time.Duration(0), m.LeaderStabilization("nonexistent"))
}

func TestMetrics_LeaderStabilization_Found_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordNewMaster("mymaster")
	m.mu.Lock()
	m.stableTime["mymaster"] = time.Now()
	m.mu.Unlock()
	assert.True(t, m.LeaderStabilization("mymaster") >= 0)
}

func TestMetrics_GossipPropagationTime_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	assert.Equal(t, time.Duration(0), m.GossipPropagationTime("nonexistent"))
}

func TestMetrics_GossipPropagationTime_Found_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordGossipSend("msg1")
	m.RecordGossipRecv("msg1")
	assert.True(t, m.GossipPropagationTime("msg1") >= 0)
}

func TestMetrics_SdownTimestamp_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordSdown("mymaster")
	assert.False(t, m.SdownTimestamp("mymaster").IsZero())
}

func TestMetrics_SdownTimestamp_NotFound_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	assert.True(t, m.SdownTimestamp("nonexistent").IsZero())
}

func TestMetrics_ODownTimestamp_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordODown("mymaster")
	assert.False(t, m.ODownTimestamp("mymaster").IsZero())
}

func TestMetrics_FailoverStartTime_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFailoverStart("mymaster")
	assert.False(t, m.FailoverStartTime("mymaster").IsZero())
}

func TestMetrics_NewMasterTime_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordNewMaster("mymaster")
	assert.False(t, m.NewMasterTime("mymaster").IsZero())
}

func TestMetrics_GossipPropagationTime_ZeroDuration_Coverage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	assert.Equal(t, time.Duration(0), m.GossipPropagationTime("nosend"))
}

func TestGossipProtocol_AddPeer_Coverage(t *testing.T) {
	t.Parallel()
	s := NewSentinel(1, 0)
	gp := NewGossipProtocol(s, DefaultGossipConfig())
	err := gp.AddPeer("127.0.0.1:26379", "runid123")
	assert.NoError(t, err)
	assert.Equal(t, 1, gp.GetPeersCount())
}
