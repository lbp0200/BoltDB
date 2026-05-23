package sentinel

import (
	"sync"
	"time"
)

type Metrics struct {
	mu sync.Mutex

	sdownTimes    map[string]time.Time
	odownTimes    map[string]time.Time
	failoverStart map[string]time.Time
	newMasterTime map[string]time.Time
	stableTime    map[string]time.Time

	GossipSendTimes map[string]time.Time
	GossipRecvTimes map[string]time.Time

	DetectionCount      int64
	ODownReached        int64
	FailoverStarted     int64
	SuccessfulFailovers int64
	FailedFailovers     int64
	LeaderChanges       int64
	SDownBroadcasts     int64
	SDownReceived       int64
}

func NewMetrics() *Metrics {
	return &Metrics{
		sdownTimes:      make(map[string]time.Time),
		odownTimes:      make(map[string]time.Time),
		failoverStart:   make(map[string]time.Time),
		newMasterTime:   make(map[string]time.Time),
		stableTime:      make(map[string]time.Time),
		GossipSendTimes: make(map[string]time.Time),
		GossipRecvTimes: make(map[string]time.Time),
	}
}

func (m *Metrics) RecordSdown(masterName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.sdownTimes[masterName]; !exists {
		m.sdownTimes[masterName] = time.Now()
	}
	m.DetectionCount++
}

func (m *Metrics) RecordODown(masterName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.odownTimes[masterName]; !exists {
		m.odownTimes[masterName] = time.Now()
	}
	m.ODownReached++
}

func (m *Metrics) RecordFailoverStart(masterName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.failoverStart[masterName]; !exists {
		m.failoverStart[masterName] = time.Now()
	}
	m.FailoverStarted++
}

func (m *Metrics) RecordNewMaster(masterName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.newMasterTime[masterName]; !exists {
		m.newMasterTime[masterName] = time.Now()
	}
	m.SuccessfulFailovers++
}

func (m *Metrics) RecordFailoverFailed(masterName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FailedFailovers++
}

func (m *Metrics) RecordLeaderChange() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LeaderChanges++
}

func (m *Metrics) RecordSdownBroadcast() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SDownBroadcasts++
}

func (m *Metrics) RecordSdownReceived() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SDownReceived++
}

func (m *Metrics) RecordGossipSend(msgID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.GossipSendTimes[msgID] = time.Now()
}

func (m *Metrics) RecordGossipRecv(msgID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.GossipRecvTimes[msgID] = time.Now()
}

func (m *Metrics) GetDetectionCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.DetectionCount
}

func (m *Metrics) GetLeaderChanges() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LeaderChanges
}

func (m *Metrics) GetSuccessfulFailovers() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SuccessfulFailovers
}

func (m *Metrics) GetFailedFailovers() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.FailedFailovers
}

func (m *Metrics) GetFailoverStarted() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.FailoverStarted
}

func (m *Metrics) GetODownReached() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ODownReached
}

func (m *Metrics) GetSDownBroadcasts() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SDownBroadcasts
}

func (m *Metrics) GetSDownReceived() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SDownReceived
}

func (m *Metrics) DetectionLatency(masterName string) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	sdown, ok1 := m.sdownTimes[masterName]
	odown, ok2 := m.odownTimes[masterName]
	if !ok1 || !ok2 {
		return 0
	}
	return odown.Sub(sdown)
}

func (m *Metrics) ElectionDuration(masterName string) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	odown, ok1 := m.odownTimes[masterName]
	start, ok2 := m.failoverStart[masterName]
	if !ok1 || !ok2 {
		return 0
	}
	return start.Sub(odown)
}

func (m *Metrics) RecoveryDuration(masterName string) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	start, ok1 := m.failoverStart[masterName]
	newMaster, ok2 := m.newMasterTime[masterName]
	if !ok1 || !ok2 {
		return 0
	}
	return newMaster.Sub(start)
}

func (m *Metrics) LeaderStabilization(masterName string) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	newMaster, ok1 := m.newMasterTime[masterName]
	stable, ok2 := m.stableTime[masterName]
	if !ok1 || !ok2 {
		return 0
	}
	return stable.Sub(newMaster)
}

func (m *Metrics) GossipPropagationTime(msgID string) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	send, ok1 := m.GossipSendTimes[msgID]
	recv, ok2 := m.GossipRecvTimes[msgID]
	if !ok1 || !ok2 {
		return 0
	}
	return recv.Sub(send)
}

func (m *Metrics) SdownTimestamp(masterName string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sdownTimes[masterName]
}

func (m *Metrics) ODownTimestamp(masterName string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.odownTimes[masterName]
}

func (m *Metrics) FailoverStartTime(masterName string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failoverStart[masterName]
}

func (m *Metrics) NewMasterTime(masterName string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.newMasterTime[masterName]
}
