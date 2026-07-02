package sentinel

import (
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// MasterInstance 主节点实例
type MasterInstance struct {
	mu           sync.RWMutex
	closeOnce    sync.Once
	name         string
	addr         string
	quorum       int
	authPass     string // optional AUTH password for this master
	slaves       []*SlaveInstance
	sentinels    []*SentinelInstance
	state        string // "ok" | "odown" | "sdown" | "failover"
	lastPingTime time.Time
	lastPongTime time.Time
	downAfter    time.Duration
	stopCh       chan struct{}
	// 每个哨兵的 SDOWN 报告追踪（key=sourceRunID，value=true 表示该哨兵认为主节点 SDOWN）
	sdownReporters map[string]bool
	// 已知哨兵数量
	knownSentinelCount int
	// 故障转移冷却
	lastFailoverTime time.Time
	failoverCooldown time.Duration
}

// NewMasterInstance 创建新的主节点实例
func NewMasterInstance(name, addr string, quorum int) *MasterInstance {
	return NewMasterInstanceWithDownAfter(name, addr, quorum, 30*time.Second)
}

// NewMasterInstanceWithDownAfter 创建新的主节点实例，可指定 downAfter
func NewMasterInstanceWithDownAfter(name, addr string, quorum int, downAfter time.Duration) *MasterInstance {
	now := time.Now()
	return &MasterInstance{
		name:               name,
		addr:               addr,
		quorum:             quorum,
		slaves:             make([]*SlaveInstance, 0),
		sentinels:          make([]*SentinelInstance, 0),
		state:              "ok",
		downAfter:          downAfter,
		lastPingTime:       now,
		lastPongTime:       now,
		stopCh:             make(chan struct{}),
		sdownReporters:     make(map[string]bool),
		knownSentinelCount: 1,
		failoverCooldown:   5 * time.Second,
	}
}

// StartMonitoring 开始监控主节点
func (mi *MasterInstance) StartMonitoring(sentinel *Sentinel) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-mi.stopCh:
			return
		case <-ticker.C:
			mi.checkMaster(sentinel)
		}
	}
}

// checkMaster 检查主节点状态（通过 TCP 连接探测）
func (mi *MasterInstance) checkMaster(sentinel *Sentinel) {
	mi.mu.RLock()
	addr := mi.addr
	lastFailoverTime := mi.lastFailoverTime
	failoverCooldown := mi.failoverCooldown
	mi.mu.RUnlock()

	// Use PING-based health check with AUTH support (connects, sends PING, closes).
	err := pingCheck(addr, mi.GetAuthPass())

	var shouldBroadcast bool
	var shouldTriggerFailover bool
	var sdownReporterCount int

	mi.mu.Lock()

	if err != nil {
		// 连接失败，检查是否超过 downAfter 未收到 pong
		if mi.state == "ok" && time.Since(mi.lastPongTime) > mi.downAfter {
			mi.state = "sdown"
			// 使用本哨兵的 runID 作为报告者（per-sentinel 去重）
			mi.sdownReporters[sentinel.runID] = true
			mi.lastPingTime = time.Now()
			sdownReporterCount = len(mi.sdownReporters)
			sentinel.Metrics.RecordSdown(mi.name)
			logger.Logger.Warn().
				Str("master_name", mi.name).
				Str("master_addr", mi.addr).
				Int("sdown_reporters", sdownReporterCount).
				Int("quorum", mi.quorum).
				Msg("主节点主观下线")

			shouldBroadcast = true

			if sdownReporterCount >= mi.quorum {
				sentinel.Metrics.RecordODown(mi.name)
				shouldTriggerFailover = true
				logger.Logger.Info().
					Str("master_name", mi.name).
					Int("sdown_reporters", sdownReporterCount).
					Int("quorum", mi.quorum).
					Msg("主节点本地达到客观下线条件（多哨兵共识）")
			}
		}
	} else {
		mi.lastPongTime = time.Now()

		if mi.state == "sdown" {
			mi.state = "ok"
			mi.sdownReporters = make(map[string]bool) // 清除所有报告
			logger.Logger.Info().
				Str("master_name", mi.name).
				Str("master_addr", mi.addr).
				Msg("主节点恢复")
		}
	}

	mi.mu.Unlock()

	if shouldBroadcast {
		sentinel.BroadcastSdown(mi.name, sdownReporterCount)
	}

	if shouldTriggerFailover {
		canFailover := lastFailoverTime.IsZero() || time.Since(lastFailoverTime) >= failoverCooldown
		if !canFailover {
			logger.Logger.Warn().
				Str("master_name", mi.name).
				Msg("故障转移冷却中，跳过触发")
			return
		}
		mi.mu.Lock()
		mi.lastFailoverTime = time.Now()
		mi.mu.Unlock()
		sentinel.Metrics.RecordFailoverStart(mi.name)
		fm := NewFailoverManager(sentinel)
		sentinel.wg.Add(1)
		go func() {
			defer sentinel.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					logger.Logger.Error().
						Str("master_name", mi.name).
						Interface("panic", r).
						Msg("自动故障转移 PANIC")
					sentinel.Metrics.RecordFailoverFailed(mi.name)
				}
			}()
			if err := fm.AutoFailover(mi.name); err != nil {
				sentinel.Metrics.RecordFailoverFailed(mi.name)
				logger.Logger.Error().
					Str("master_name", mi.name).
					Err(err).
					Msg("自动故障转移失败")
			}
		}()
	}
}

// AddSlave 添加从节点
func (mi *MasterInstance) AddSlave(slave *SlaveInstance) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.slaves = append(mi.slaves, slave)
}

// GetSlaves 获取所有从节点
func (mi *MasterInstance) GetSlaves() []*SlaveInstance {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	result := make([]*SlaveInstance, len(mi.slaves))
	copy(result, mi.slaves)
	return result
}

// AddSentinel 添加哨兵实例
func (mi *MasterInstance) AddSentinel(sentinel *SentinelInstance) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.sentinels = append(mi.sentinels, sentinel)
	mi.knownSentinelCount++
}

// GetState 获取状态
func (mi *MasterInstance) GetState() string {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.state
}

// SetState 设置状态
func (mi *MasterInstance) SetState(state string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.state = state
}

// GetName 获取名称
func (mi *MasterInstance) GetName() string {
	return mi.name
}

// SetAuthPass 设置此主节点的 AUTH 密码（对应 sentinel auth-pass 指令）
func (mi *MasterInstance) SetAuthPass(password string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.authPass = password
}

// GetAuthPass 获取此主节点的 AUTH 密码
func (mi *MasterInstance) GetAuthPass() string {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.authPass
}

// GetAddr 获取地址
func (mi *MasterInstance) GetAddr() string {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.addr
}

// SetAddr 设置地址
func (mi *MasterInstance) SetAddr(addr string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.addr = addr
}

// GetQuorum 获取quorum数量
func (mi *MasterInstance) GetQuorum() int {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.quorum
}

// GetSdownCount 获取当前报告主观下线的哨兵数量
func (mi *MasterInstance) GetSdownCount() int {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return len(mi.sdownReporters)
}

// ReportSdown 记录某个哨兵报告了主观下线（per-sentinel 去重）
func (mi *MasterInstance) ReportSdown(sourceRunID string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.sdownReporters[sourceRunID] = true
}

// IncrSdownCount 已废弃：保留接口兼容，内部调用 ReportSdown
func (mi *MasterInstance) IncrSdownCount() {
	// 此方法已废弃，仅保留接口兼容。新代码应使用 ReportSdown。
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.sdownReporters["unknown"] = true
}

// IsDown 检查是否下线（主观或客观）
func (mi *MasterInstance) IsDown() bool {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.state == "sdown" || mi.state == "odown" || mi.state == "failover"
}

// IsODown 检查是否客观下线
// 客观下线需要满足：
// 1. 超过 quorum 数量的不同哨兵报告主观下线（per-sentinel 去重）
// 2. 多个哨兵之间需要达成共识
func (mi *MasterInstance) IsODown() bool {
	mi.mu.RLock()
	state := mi.state
	sdownReporters := len(mi.sdownReporters)
	quorum := mi.quorum
	mi.mu.RUnlock()

	// 如果已经是客观下线状态，直接返回
	if state == "odown" || state == "failover" {
		return true
	}

	// 检查是否达到 quorum（不同哨兵的数量 >= quorum）
	if sdownReporters >= quorum {
		logger.Logger.Info().
			Str("master_name", mi.name).
			Int("sdown_reporters", sdownReporters).
			Int("quorum", quorum).
			Msg("主节点已达到客观下线条件（多哨兵共识）")
		return true
	}

	return false
}

// SetODown 设置为客观下线状态
func (mi *MasterInstance) SetODown() {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	if mi.state != "failover" {
		mi.state = "odown"
	}
}

// Stop 停止监控
func (mi *MasterInstance) Stop() {
	mi.closeOnce.Do(func() { close(mi.stopCh) })
}

// CanFailover 检查是否允许触发故障转移（冷却期内不允许）
func (mi *MasterInstance) CanFailover() bool {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	if mi.lastFailoverTime.IsZero() {
		return true
	}
	return time.Since(mi.lastFailoverTime) >= mi.failoverCooldown
}

// RecordFailover 记录故障转移时间
func (mi *MasterInstance) RecordFailover() {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.lastFailoverTime = time.Now()
}

// GetSentinelCount 获取已知哨兵数量
func (mi *MasterInstance) GetSentinelCount() int {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.knownSentinelCount
}

// UpdateSlaveOffset 更新从节点偏移量
func (mi *MasterInstance) UpdateSlaveOffset(slaveAddr string, offset int64) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	for _, slave := range mi.slaves {
		if slave.GetAddr() == slaveAddr {
			slave.SetOffset(offset)
			break
		}
	}
}

// GetBestSlave 获取最佳从节点（偏移量最大的）
func (mi *MasterInstance) GetBestSlave() *SlaveInstance {
	mi.mu.RLock()
	defer mi.mu.RUnlock()

	var best *SlaveInstance
	var bestOffset int64 = -1

	for _, slave := range mi.slaves {
		if slave.GetState() == "online" && slave.GetOffset() > bestOffset {
			bestOffset = slave.GetOffset()
			best = slave
		}
	}

	return best
}
