package sentinel

import (
	"sync"
	"time"
)

// SlaveInstance 从节点实例
type SlaveInstance struct {
	mu            sync.RWMutex
	ID            string
	Addr          string
	Master        *MasterInstance
	Flags         []string
	State         string // "online" | "offline" | "disconnected"
	Offset        int64
	Lag           int64
	LastSeen      time.Time
	LastConnected time.Time
	Reconnects    int64
	InfoErrors    int64
}

// NewSlaveInstance 创建新的从节点实例
func NewSlaveInstance(id, addr string) *SlaveInstance {
	now := time.Now()
	return &SlaveInstance{
		ID:            id,
		Addr:          addr,
		Flags:         []string{"slave"},
		State:         "online",
		LastSeen:      now,
		LastConnected: now,
	}
}

// GetState returns the slave state
func (si *SlaveInstance) GetState() string {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.State
}

// GetOffset returns the slave replication offset
func (si *SlaveInstance) GetOffset() int64 {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.Offset
}

// GetAddr returns the slave address
func (si *SlaveInstance) GetAddr() string {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.Addr
}

// SetOffset sets the slave replication offset
func (si *SlaveInstance) SetOffset(offset int64) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.Offset = offset
}

// IsOnline returns true if the slave was seen within the last 30 seconds.
func (si *SlaveInstance) IsOnline() bool {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.State == "online" && time.Since(si.LastSeen) < 30*time.Second
}

// RecordHeartbeat updates the last seen timestamp and marks the slave online.
func (si *SlaveInstance) RecordHeartbeat(offset int64) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.LastSeen = time.Now()
	si.Offset = offset
	if si.State == "offline" {
		si.Reconnects++
	}
	si.State = "online"
}

// MarkOffline marks the slave as offline.
func (si *SlaveInstance) MarkOffline() {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.State = "offline"
}

// RecordInfoError increments the info error counter.
func (si *SlaveInstance) RecordInfoError() {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.InfoErrors++
	if si.InfoErrors > 3 {
		si.State = "offline"
	}
}

// GetLastSeen returns the last seen timestamp
func (si *SlaveInstance) GetLastSeen() time.Time {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.LastSeen
}
