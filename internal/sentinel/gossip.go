package sentinel

import (
	"bufio"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// GossipConfig gossip协议配置
type GossipConfig struct {
	Port          int
	RunID         string
	HelloInterval time.Duration
	PingInterval  time.Duration
	PeerTimeout   time.Duration
	MaxPeers      int
}

// DefaultGossipConfig 默认配置
func DefaultGossipConfig() *GossipConfig {
	return &GossipConfig{
		Port:          0, // 随机端口
		HelloInterval: 2 * time.Second,
		PingInterval:  5 * time.Second,
		PeerTimeout:   30 * time.Second,
		MaxPeers:      10,
	}
}

// GossipPeer 远程哨兵对等体
type GossipPeer struct {
	Addr      string
	RunID     string
	LastSeen  time.Time
	HelloSent bool
}

// GossipProtocol gossip协议管理器
type GossipProtocol struct {
	mu       sync.RWMutex
	sentinel *Sentinel
	config   *GossipConfig
	listener net.Listener
	peers    map[string]*GossipPeer
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewGossipProtocol 创建gossip协议管理器
func NewGossipProtocol(sentinel *Sentinel, config *GossipConfig) *GossipProtocol {
	if config == nil {
		config = DefaultGossipConfig()
	}

	return &GossipProtocol{
		sentinel: sentinel,
		config:   config,
		peers:    make(map[string]*GossipPeer),
		stopCh:   make(chan struct{}),
	}
}

// Start 启动gossip协议
func (gp *GossipProtocol) Start() error {
	// 监听端口
	var err error
	addr := ":" + strconv.Itoa(gp.config.Port)
	gp.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	logger.Logger.Info().Int("port", gp.listener.Addr().(*net.TCPAddr).Port).Msg("Gossip协议监听端口已启动")

	// 启动接受连接协程
	gp.wg.Add(1)
	go func() {
		defer gp.wg.Done()
		gp.acceptConnections()
	}()

	// 启动连接管理协程
	gp.wg.Add(1)
	go func() {
		defer gp.wg.Done()
		gp.managePeers()
	}()

	return nil
}

func (gp *GossipProtocol) Stop() {
	close(gp.stopCh)

	if gp.listener != nil {
		if err := gp.listener.Close(); err != nil {
			logger.Logger.Warn().Err(err).Msg("Failed to close gossip listener")
		}
	}

	gp.wg.Wait()

	gp.mu.Lock()
	for addr := range gp.peers {
		delete(gp.peers, addr)
	}
	gp.mu.Unlock()
}

// GetPort 获取监听端口
func (gp *GossipProtocol) GetPort() int {
	if gp.listener == nil {
		return 0
	}
	return gp.listener.Addr().(*net.TCPAddr).Port
}

// acceptConnections 接受传入的连接
func (gp *GossipProtocol) acceptConnections() {
	for {
		select {
		case <-gp.stopCh:
			return
		default:
		}

		if tcpListener, ok := gp.listener.(*net.TCPListener); ok {
			if err := tcpListener.SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
				logger.Logger.Debug().Err(err).Msg("Failed to set deadline on listener")
			}
		}

		conn, err := gp.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Logger.Error().Err(err).Msg("接受gossip连接失败")
			continue
		}

		gp.wg.Add(1)
		go func(c net.Conn) {
			defer gp.wg.Done()
			gp.handleConnection(c)
		}(conn)
	}
}

// handleConnection 处理传入连接
func (gp *GossipProtocol) handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close gossip connection")
		}
	}()

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-gp.stopCh:
			return
		default:
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
				logger.Logger.Debug().Err(err).Msg("Failed to set deadline on connection")
			}
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		gp.handleMessage(conn, line)
	}
}

// handleMessage 处理gossip消息
func (gp *GossipProtocol) handleMessage(conn net.Conn, line string) {
	parts := strings.Split(line, " ")
	if len(parts) == 0 {
		return
	}

	switch parts[0] {
	case "HELLO":
		gp.handleHello(conn, parts[1:])
	case "PING":
		gp.handlePing(conn)
	case "PONG":
		gp.handlePong(conn, parts[1:])
	case "SDOWN":
		gp.handleSdown(conn, parts[1:])
	case "MASTERS":
		gp.handleMasters(conn)
	}
}

// handleHello 处理HELLO消息
func (gp *GossipProtocol) handleHello(conn net.Conn, parts []string) {
	if len(parts) < 3 {
		return
	}

	runID := parts[0]
	epoch, _ := strconv.ParseInt(parts[2], 10, 64)

	peerAddr := conn.RemoteAddr().String()

	// 添加或更新对等体
	gp.addOrUpdatePeer(peerAddr, runID)

	logger.Logger.Debug().
		Str("peer", peerAddr).
		Str("run_id", runID).
		Int64("epoch", epoch).
		Msg("收到HELLO消息")

	// 发送PONG响应
	response := gp.formatPong()
	if err := gp.sendMessage(conn, response); err != nil {
		logger.Logger.Warn().Err(err).Msg("Failed to send PONG response")
	}
}

// handlePing 处理PING消息
func (gp *GossipProtocol) handlePing(conn net.Conn) {
	peerAddr := conn.RemoteAddr().String()
	gp.touchPeer(peerAddr)

	response := gp.formatPong()
	if err := gp.sendMessage(conn, response); err != nil {
		logger.Logger.Warn().Err(err).Msg("Failed to send PONG response")
	}
}

// handlePong 处理PONG消息
func (gp *GossipProtocol) handlePong(conn net.Conn, parts []string) {
	if len(parts) < 1 {
		return
	}

	runID := parts[0]
	peerAddr := conn.RemoteAddr().String()

	gp.addOrUpdatePeer(peerAddr, runID)

	logger.Logger.Debug().
		Str("peer", peerAddr).
		Str("run_id", runID).
		Msg("收到PONG消息")
}

// handleSDOWN 处理SDOWN消息
func (gp *GossipProtocol) handleSdown(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		return
	}

	masterName := parts[0]
	reportedSdownCount, _ := strconv.Atoi(parts[1])

	// 更新主节点的sdown计数
	master := gp.sentinel.GetMaster(masterName)
	if master != nil {
		// 使用报告的sdown计数和当前计数中的较大值
		currentCount := master.GetSdownCount()
		if reportedSdownCount > currentCount {
			master.mu.Lock()
			master.sdownCount = reportedSdownCount
			master.mu.Unlock()
		}

		logger.Logger.Info().
			Str("master_name", masterName).
			Int("reported_sdown_count", reportedSdownCount).
			Int("current_sdown_count", master.GetSdownCount()).
			Int("quorum", master.GetQuorum()).
			Msg("收到SDOWN消息")

		// 检查是否达到客观下线
		if master.IsODown() {
			if !master.CanFailover() {
				logger.Logger.Warn().
					Str("master_name", masterName).
					Msg("故障转移冷却中，跳过触发")
				return
			}
			master.RecordFailover()
			fm := NewFailoverManager(gp.sentinel)
			gp.wg.Add(1)
			go func() {
				defer gp.wg.Done()
				if err := fm.AutoFailover(masterName); err != nil {
					logger.Logger.Error().
						Str("master_name", masterName).
						Err(err).
						Msg("自动故障转移失败")
				}
			}()
		}
	}
}

// handleMasters 处理MASTERS消息
func (gp *GossipProtocol) handleMasters(conn net.Conn) {
	masters := gp.sentinel.GetAllMasters()
	data, _ := json.Marshal(masters)

	response := "MASTERS " + string(data) + "\n"
	if err := gp.sendMessage(conn, response); err != nil {
		logger.Logger.Warn().Err(err).Msg("Failed to send MASTERS response")
	}
}

func (gp *GossipProtocol) addOrUpdatePeer(addr, runID string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	if _, exists := gp.peers[addr]; !exists && len(gp.peers) >= gp.config.MaxPeers {
		return
	}

	gp.peers[addr] = &GossipPeer{
		Addr:     addr,
		RunID:    runID,
		LastSeen: time.Now(),
	}
}

func (gp *GossipProtocol) touchPeer(addr string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	if peer, exists := gp.peers[addr]; exists {
		peer.LastSeen = time.Now()
	}
}

// managePeers 管理对等体连接
func (gp *GossipProtocol) managePeers() {
	ticker := time.NewTicker(gp.config.HelloInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gp.stopCh:
			return
		case <-ticker.C:
			gp.sendHellos()
		}
	}
}

func (gp *GossipProtocol) sendHellos() {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	for addr, peer := range gp.peers {
		if time.Since(peer.LastSeen) > gp.config.PeerTimeout {
			delete(gp.peers, addr)
			continue
		}

		if peer.HelloSent {
			continue
		}

		if err := gp.sendHello(addr); err != nil {
			logger.Logger.Warn().Str("peer", addr).Err(err).Msg("发送HELLO消息失败")
		} else {
			peer.HelloSent = true
		}
	}
}

// sendHello 发送HELLO消息到指定地址
func (gp *GossipProtocol) sendHello(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close gossip connection")
		}
	}()

	response := gp.formatHello()
	return gp.sendMessage(conn, response)
}

// formatHello 格式化HELLO消息
func (gp *GossipProtocol) formatHello() string {
	port := gp.GetPort()
	runID := gp.config.RunID
	if runID == "" {
		runID = gp.sentinel.GetRunID()
	}
	epoch := gp.sentinel.GetConfigEpoch()
	return "HELLO " + runID + " " + strconv.Itoa(port) + " " + strconv.FormatInt(epoch, 10) + "\n"
}

// formatPong 格式化PONG消息
func (gp *GossipProtocol) formatPong() string {
	runID := gp.config.RunID
	if runID == "" {
		runID = gp.sentinel.GetRunID()
	}
	return "PONG " + runID + "\n"
}

// sendMessage 发送消息
func (gp *GossipProtocol) sendMessage(conn net.Conn, message string) error {
	_, err := conn.Write([]byte(message))
	return err
}

func (gp *GossipProtocol) BroadcastSdown(masterName string, sdownCount int) {
	gp.mu.RLock()
	addrs := make([]string, 0, len(gp.peers))
	for addr := range gp.peers {
		addrs = append(addrs, addr)
	}
	gp.mu.RUnlock()

	message := "SDOWN " + masterName + " " + strconv.Itoa(sdownCount) + "\n"
	gp.wg.Add(len(addrs))
	for _, addr := range addrs {
		go func(peerAddr string) {
			defer gp.wg.Done()
			conn, err := net.DialTimeout("tcp", peerAddr, 5*time.Second)
			if err != nil {
				return
			}
			defer func() {
				if err := conn.Close(); err != nil {
					logger.Logger.Debug().Err(err).Msg("Failed to close gossip connection")
				}
			}()

			if err := gp.sendMessage(conn, message); err != nil {
				logger.Logger.Warn().Err(err).Msg("Failed to broadcast SDOWN message")
			}
		}(addr)
	}
}

func (gp *GossipProtocol) GetPeersCount() int {
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	return len(gp.peers)
}

func (gp *GossipProtocol) AddPeer(addr, runID string) error {
	gp.addOrUpdatePeer(addr, runID)
	return nil
}
