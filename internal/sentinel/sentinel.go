package sentinel

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// Sentinel 哨兵实例
type Sentinel struct {
	mu           sync.RWMutex
	masters      map[string]*MasterInstance
	quorum       int
	downAfter    time.Duration
	parallelSync int
	runID        string
	configEpoch  int64
	stopCh       chan struct{}
	// 其他哨兵地址列表
	otherSentinels []string
	// gossip消息通道
	gossipCh chan *GossipMessage

	// TCP-based gossip protocol for inter-sentinel communication
	Gossip        *GossipProtocol
	Metrics       *Metrics
	ConfigManager *configManager

	wg sync.WaitGroup
}

// GossipMessage 哨兵间通信消息
type GossipMessage struct {
	Type         string // "sdown" | "hello" | "masters"
	MasterName   string
	SourceRunID  string
	SentinelAddr string
	State        string
	SdownCount   int
	Timestamp    time.Time
}

// NewSentinel 创建新的哨兵实例
func NewSentinel(quorum int, downAfter time.Duration) *Sentinel {
	return NewSentinelWithDataDir(quorum, downAfter, "")
}

// NewSentinelWithDataDir 创建哨兵实例并指定配置持久化目录
func NewSentinelWithDataDir(quorum int, downAfter time.Duration, dataDir string) *Sentinel {
	s := &Sentinel{
		masters:      make(map[string]*MasterInstance),
		quorum:       quorum,
		downAfter:    downAfter,
		parallelSync: 1,
		runID:        generateRunID(),
		configEpoch:  0,
		stopCh:       make(chan struct{}),
		gossipCh:     make(chan *GossipMessage, 100),
		Metrics:      NewMetrics(),
	}
	if dataDir != "" {
		s.ConfigManager = newConfigManager(s, dataDir)
		_, _ = s.ConfigManager.Load()
	}
	return s
}

// generateRunID 生成40字符十六进制运行ID（与 Redis 兼容）
func generateRunID() string {
	bytes := make([]byte, 20)
	if _, err := rand.Read(bytes); err != nil {
		// fallback: 不可能失败除非系统熵耗尽
		return fmt.Sprintf("%040x", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)
}

// AddMaster 添加主节点监控
func (s *Sentinel) AddMaster(name, addr string, quorum int) error {
	s.mu.Lock()

	if _, exists := s.masters[name]; exists {
		s.mu.Unlock()
		return fmt.Errorf("master %s already exists", name)
	}

	master := NewMasterInstanceWithDownAfter(name, addr, quorum, s.downAfter)
	s.masters[name] = master
	s.mu.Unlock()

	logger.Logger.Info().
		Str("master_name", name).
		Str("master_addr", addr).
		Int("quorum", quorum).
		Msg("添加主节点监控")

	if s.ConfigManager != nil {
		_ = s.ConfigManager.Save()
	}
	return nil
}

// RemoveMaster 移除主节点监控
func (s *Sentinel) RemoveMaster(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if master, exists := s.masters[name]; exists {
		master.Stop()
		delete(s.masters, name)
		logger.Logger.Info().
			Str("master_name", name).
			Msg("移除主节点监控")
		return nil
	}

	return fmt.Errorf("master %s not found", name)
}

// GetMaster 获取主节点实例
func (s *Sentinel) GetMaster(name string) *MasterInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.masters[name]
}

// GetAllMasters 获取所有主节点
func (s *Sentinel) GetAllMasters() map[string]*MasterInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*MasterInstance)
	for k, v := range s.masters {
		result[k] = v
	}
	return result
}

// AddSentinel 添加其他哨兵地址
func (s *Sentinel) AddSentinel(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, existing := range s.otherSentinels {
		if existing == addr {
			return
		}
	}
	s.otherSentinels = append(s.otherSentinels, addr)
	logger.Logger.Info().Str("sentinel", addr).Msg("添加哨兵")
}

// Start 启动哨兵
func (s *Sentinel) Start() {
	logger.Logger.Info().Msg("启动哨兵")

	// 启动所有主节点的监控
	s.mu.RLock()
	masters := make([]*MasterInstance, 0, len(s.masters))
	for _, master := range s.masters {
		masters = append(masters, master)
	}
	s.mu.RUnlock()

	for _, master := range masters {
		s.wg.Add(1)
		m := master
		go func() {
			defer s.wg.Done()
			m.StartMonitoring(s)
		}()
	}

	// 启动gossip协程
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.startGossipProcessor()
	}()
}

// Stop 停止哨兵
func (s *Sentinel) Stop() {
	s.mu.Lock()
	close(s.stopCh)

	for _, master := range s.masters {
		master.Stop()
	}
	// 释放锁后再等待 goroutine 收束，避免 AutoFailover 请求 s.mu.RLock()
	// 时与 s.wg.Wait() 产生死锁。
	s.mu.Unlock()

	s.wg.Wait()
}

// GetRunID 获取运行ID
func (s *Sentinel) GetRunID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.runID
}

// GetConfigEpoch 获取配置纪元
func (s *Sentinel) GetConfigEpoch() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configEpoch
}

// IncrementConfigEpoch 增加配置纪元
func (s *Sentinel) IncrementConfigEpoch() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configEpoch++
	return s.configEpoch
}

// BroadcastSdown 广播主观下线消息到其他哨兵
func (s *Sentinel) BroadcastSdown(masterName string) {
	master := s.GetMaster(masterName)
	if master == nil {
		return
	}

	msg := &GossipMessage{
		Type:         "sdown",
		MasterName:   masterName,
		SourceRunID:  s.runID,
		SentinelAddr: "", // 稍后填充
		State:        "sdown",
		SdownCount:   master.GetSdownCount(),
		Timestamp:    time.Now(),
	}

	// 发送到gossip处理器
	select {
	case s.gossipCh <- msg:
	default:
		// 通道满，忽略
	}
}

// startGossipProcessor 启动gossip消息处理器
func (s *Sentinel) startGossipProcessor() {
	for {
		select {
		case <-s.stopCh:
			return
		case msg := <-s.gossipCh:
			s.processGossipMessage(msg)
		}
	}
}

// processGossipMessage 处理gossip消息
func (s *Sentinel) processGossipMessage(msg *GossipMessage) {
	switch msg.Type {
	case "sdown":
		s.handleSdownMessage(msg)
	case "hello":
		s.handleHelloMessage(msg)
	}
}

// handleSdownMessage 处理主观下线消息
func (s *Sentinel) handleSdownMessage(msg *GossipMessage) {
	master := s.GetMaster(msg.MasterName)
	if master == nil {
		return
	}

	// 更新sdown计数（简化实现，实际应该记录每个哨兵的报告）
	master.IncrSdownCount()

	logger.Logger.Info().
		Str("master_name", msg.MasterName).
		Str("from_sentinel", msg.SourceRunID).
		Int("sdown_count", master.GetSdownCount()).
		Int("quorum", master.GetQuorum()).
		Msg("收到其他哨兵的sdown报告")

	// 检查是否达到客观下线
	if master.IsODown() {
		s.Metrics.RecordODown(msg.MasterName)
		logger.Logger.Info().
			Str("master_name", msg.MasterName).
			Msg("主节点已客观下线，触发故障转移")

		if !master.CanFailover() {
			logger.Logger.Warn().
				Str("master_name", msg.MasterName).
				Msg("故障转移冷却中，跳过触发")
			return
		}
		master.RecordFailover()

		// 触发故障转移
		fm := NewFailoverManager(s)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := fm.AutoFailover(msg.MasterName); err != nil {
				s.Metrics.RecordFailoverFailed(msg.MasterName)
				logger.Logger.Error().
					Str("master_name", msg.MasterName).
					Err(err).
					Msg("自动故障转移失败")
			}
		}()
	}
}

// handleHelloMessage 处理hello消息
func (s *Sentinel) handleHelloMessage(msg *GossipMessage) {
	// 添加到已知哨兵列表
	s.AddSentinel(msg.SentinelAddr)

	logger.Logger.Debug().
		Str("from_sentinel", msg.SourceRunID).
		Str("addr", msg.SentinelAddr).
		Msg("收到哨兵hello消息")
}

// SendHello 发送hello消息到指定哨兵
func (s *Sentinel) SendHello(sentinelAddr string) error {
	if s.Gossip != nil {
		// 注册到 gossip 协议层，后续由 gossip 管理周期性通信
		_ = s.Gossip.AddPeer(sentinelAddr, "")
		// 立即尝试发送 HELLO 消息建立连接
		if err := s.Gossip.sendHello(sentinelAddr); err != nil {
			logger.Logger.Debug().Str("sentinel", sentinelAddr).Err(err).Msg("发送hello消息失败")
			return err
		}
		return nil
	}
	logger.Logger.Debug().Str("sentinel", sentinelAddr).Msg("发送hello消息（Gossip未初始化）")
	return nil
}

// StartGossip 启动与指定哨兵的gossip通信
func (s *Sentinel) StartGossip(sentinelAddr string) {
	s.AddSentinel(sentinelAddr)

	// 定期发送hello消息
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				if err := s.SendHello(sentinelAddr); err != nil {
					logger.Logger.Warn().
						Str("sentinel", sentinelAddr).
						Err(err).
						Msg("发送hello消息失败")
				}
			}
		}
	}()
}
