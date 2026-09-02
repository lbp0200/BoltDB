package replication

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

type SlaveState int32

const (
	SlaveDisconnected SlaveState = iota
	SlaveConnecting
	SlaveSyncing
	SlaveConnected
)

var DefaultReconnectConfig = ReconnectConfig{
	MaxRetries:  0,
	BaseBackoff: 1 * time.Second,
	MaxBackoff:  60 * time.Second,
	ResetAfter:  30 * time.Second,
}

// replStallTimeout: 主节点通告的 offset 高于已应用 offset、且数据流空闲超过
// 该时长时，判定复制流停滞（命令已入 backlog 但投递静默中断——无读错误、
// 无 send_drop/apply_skip，见 docs/plans/TODO.md §1c），readCommandLoop
// 返回错误强制重连，由 PSYNC CONTINUE 重放缺失区间自愈。
const replStallTimeout = 2 * time.Second

// replStallArmWindow: 停滞检测的武装窗口。仅在"近期（该窗口内）曾与主节点
// 收敛（ACK 显示 masterOffset <= lastOffset）"后才判定停滞——追赶排水期的
// 空闲间隙是突发性发送停顿，不是投递丢失；收敛后主节点水位前进但数据流
// 空闲超过 replStallTimeout 才是真实的尾巴投递缺口（TODO §1c）。
const replStallArmWindow = 30 * time.Second

// replDrainStallTimeout: 追赶排水期的冻结阈值（未收敛时）。排水期的瞬态
// 发送停顿（数百毫秒~数秒）是常态，不判停滞；超过该时长仍无数据到达即
// 排水冻结（尾巴投递中断——TODO §1c 捕获轮 2026-09-01：lag=162 冻结 40s、
// 收敛超时），强制重连由 PSYNC 重放缺失尾巴自愈。
// 30s = 2026-09-02 §1c 根因定案后的阈值：store 读争用（复制期间外部读——
// A/B 对照确认）引发的排水数据间隙可达 ~10-21s（10s 阈值误判触发降级丢失），
// 30s 放行读争用间隙 + 真冻结（40s+ 收敛超时）仍在收敛窗内触发自愈。
// 2026-09-03 实证：阈值调参路径定性失败（30s→40s 验证 3/15 FAIL vs 30s 基线
// 2/15——间隙随阈值增长无上界）——30s 为已知最佳调参；完整修复需存储级
// 读写隔离或恢复路径重设计（TODO §1c 记录）。
const replDrainStallTimeout = 30 * time.Second

type ReconnectConfig struct {
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	ResetAfter  time.Duration
}

type SlaveReconnector struct {
	rm         *ReplicationManager
	store      *store.BotreonStore
	config     ReconnectConfig
	masterAddr string

	state          atomic.Int32
	stopCh         chan struct{}
	closeOnce      sync.Once
	wg             sync.WaitGroup
	conn           atomic.Pointer[MasterConnection]
	lastReplId     string
	lastOffset     atomic.Int64
	connectedSince time.Time

	reconnectCount atomic.Int64

	// masterWater: 主节点对周期 REPLCONF GETACK * 的回复（REPLCONF ACK）
	// 中通告的 offset。与 lastDataTime 一起用于检测投递停滞的尾巴缺口。
	masterWater atomic.Int64
	// lastDataTime: 最近一次收到数据命令（非 PING/REPLCONF/SELECT）的时间。
	lastDataTime atomic.Int64
	// stallTimeout: 停滞判定阈值，测试可缩短；默认 replStallTimeout。
	stallTimeout time.Duration
	// drainStallTimeout: 追赶排水期的冻结阈值（未收敛时），测试可缩短；
	// 默认 replDrainStallTimeout。
	drainStallTimeout time.Duration
	// lastConvergedTime: 最近一次与主节点收敛（ACK 显示 masterOffset <=
	// lastOffset）的时刻。停滞检测仅在该时刻落在 replStallArmWindow 内时
	// 才武装（避免追赶排水期的瞬时发送停顿被误判为停滞）。
	lastConvergedTime atomic.Int64
}

func NewSlaveReconnector(rm *ReplicationManager, store *store.BotreonStore, masterAddr string) *SlaveReconnector {
	return &SlaveReconnector{
		rm:         rm,
		store:      store,
		config:     DefaultReconnectConfig,
		masterAddr: masterAddr,
		stopCh:     make(chan struct{}),
	}
}

func (sr *SlaveReconnector) GetState() SlaveState {
	return SlaveState(sr.state.Load())
}

func (sr *SlaveReconnector) GetLastOffset() int64 {
	return sr.lastOffset.Load()
}

func (sr *SlaveReconnector) GetMasterAddr() string {
	return sr.masterAddr
}

func (sr *SlaveReconnector) GetReconnectCount() int64 {
	return sr.reconnectCount.Load()
}

func (sr *SlaveReconnector) Start() {
	sr.wg.Add(1)
	go sr.reconnectLoop()
}

func (sr *SlaveReconnector) Stop() {
	sr.closeOnce.Do(func() {
		close(sr.stopCh)
		if mc := sr.conn.Load(); mc != nil {
			if err := mc.Close(); err != nil {
				logger.Logger.Debug().Err(err).Msg("close master conn on stop")
			}
		}
	})
	sr.wg.Wait()
}

func (sr *SlaveReconnector) reconnectLoop() {
	defer sr.wg.Done()

	retries := 0

	for {
		select {
		case <-sr.stopCh:
			sr.state.Store(int32(SlaveDisconnected))
			return
		default:
		}

		sr.reconnectCount.Add(1)
		sr.state.Store(int32(SlaveConnecting))

		err := sr.tryReplicate()

		sr.rm.mu.Lock()
		sr.rm.masterConn = nil
		sr.rm.mu.Unlock()

		if err == nil {
			sr.state.Store(int32(SlaveDisconnected))
			return
		}

		sr.state.Store(int32(SlaveDisconnected))

		if !sr.connectedSince.IsZero() {
			retries = 0
			sr.connectedSince = time.Time{}
		}

		select {
		case <-sr.stopCh:
			return
		default:
		}

		if sr.config.MaxRetries > 0 && retries >= sr.config.MaxRetries {
			logger.Logger.Error().
				Int("retries", retries).
				Str("master_addr", sr.masterAddr).
				Msg("达到最大重连次数，停止复制")
			return
		}

		retries++

		backoff := sr.config.BaseBackoff * time.Duration(math.Pow(2, float64(retries-1)))
		if backoff > sr.config.MaxBackoff {
			backoff = sr.config.MaxBackoff
		}

		logger.Logger.Warn().
			Str("master_addr", sr.masterAddr).
			Int("retry", retries).
			Dur("backoff", backoff).
			Msg("复制连接断开，准备重连")

		timer := time.NewTimer(backoff)
		select {
		case <-sr.stopCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (sr *SlaveReconnector) tryReplicate() error {
	sr.state.Store(int32(SlaveConnecting))

	masterConn, err := dialMaster(sr.masterAddr, sr.rm.GetTLSConfig())
	if err != nil {
		return fmt.Errorf("connect to master failed: %w", err)
	}

	sr.conn.Store(masterConn)
	sr.rm.mu.Lock()
	sr.rm.masterConn = masterConn
	sr.rm.mu.Unlock()

	cleanup := func() {
		sr.conn.Store(nil)
		if err := masterConn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("close master conn after tryReplicate")
		}
	}

	if err := sr.sendHandshake(masterConn); err != nil {
		cleanup()
		return err
	}

	fullResync, err := sr.sendPSYNC(masterConn)
	if err != nil {
		cleanup()
		return err
	}

	if fullResync {
		sr.state.Store(int32(SlaveSyncing))

		rdbData, err := masterConn.ReadBulkString()
		if err != nil {
			cleanup()
			return fmt.Errorf("read RDB failed: %w", err)
		}

		if err := sr.rm.LoadRDB(rdbData); err != nil {
			cleanup()
			return fmt.Errorf("load RDB failed: %w", err)
		}
	}

	sr.state.Store(int32(SlaveConnected))
	sr.connectedSince = time.Now()

	err = sr.readCommandLoop(masterConn)
	cleanup()

	return err
}

func (sr *SlaveReconnector) sendHandshake(mc *MasterConnection) error {
	handshake := [][][]byte{
		{[]byte("PING")},
		{[]byte("REPLCONF"), []byte("listening-port"), []byte("6380")},
		{[]byte("REPLCONF"), []byte("capa"), []byte("eof")},
		{[]byte("REPLCONF"), []byte("capa"), []byte("psync2")},
	}

	for i, cmd := range handshake {
		if err := mc.SendCommand(cmd); err != nil {
			return fmt.Errorf("handshake step %d failed: %w", i, err)
		}
		if _, err := mc.ReadResponse(); err != nil {
			return fmt.Errorf("handshake step %d response failed: %w", i, err)
		}
	}

	return nil
}

func (sr *SlaveReconnector) sendPSYNC(mc *MasterConnection) (fullResync bool, err error) {
	psyncReplId := sr.lastReplId
	if psyncReplId == "" {
		psyncReplId = "?"
	}
	psyncOffset := sr.lastOffset.Load()
	if psyncReplId == "?" {
		psyncOffset = -1
	}

	if err := mc.SendCommand([][]byte{
		[]byte("PSYNC"),
		[]byte(psyncReplId),
		[]byte(strconv.FormatInt(psyncOffset, 10)),
	}); err != nil {
		return false, fmt.Errorf("send PSYNC failed: %w", err)
	}

	resp, err := mc.ReadResponse()
	if err != nil {
		return false, fmt.Errorf("read PSYNC response failed: %w", err)
	}

	respStr := resp.String()
	if strings.HasPrefix(respStr, "+FULLRESYNC") {
		parts := strings.Fields(respStr)
		if len(parts) >= 3 {
			sr.lastReplId = parts[1]
			offset, _ := strconv.ParseInt(parts[2], 10, 64)
			sr.lastOffset.Store(offset)
		}
		return true, nil
	}

	if strings.HasPrefix(respStr, "+CONTINUE") {
		parts := strings.Fields(respStr)
		if len(parts) >= 2 {
			sr.lastReplId = parts[1]
		}
		return false, nil
	}

	return false, fmt.Errorf("unexpected PSYNC response: %s", respStr)
}

func (sr *SlaveReconnector) readCommandLoop(mc *MasterConnection) error {
	// 从 stopCh 派生 context，使阻塞操作（如 XReadGroup）可被 shutdown 取消
	replCtx, replCancel := context.WithCancel(context.Background())
	defer replCancel()

	// 停滞检测的空闲时钟从连接建立起算，避免 0 值（1970）被误判为"早已空闲"。
	sr.lastDataTime.Store(time.Now().UnixNano())
	// 武装时钟也从连接建立起算：上一连接（或上一测试迭代）的收敛不得为
	// 本连接排水期的瞬时发送停顿武装停滞检测——否则排水期误判会强制重连、
	// PSYNC 偏移撞非命令边界而降级 FULLRESYNC，引发从节点数据丢失
	// （§1c 捕获轮 2026-09-01：5,731 缺失值 + send_drop=1）。
	sr.lastConvergedTime.Store(0)

	go func() {
		select {
		case <-sr.stopCh:
			replCancel()
		case <-replCtx.Done():
		}
	}()

	// 周期向主节点发 REPLCONF GETACK *，主节点以 REPLCONF ACK <offset>
	// 回复（见 replication_handler.go handleSlaveReplicationConnection 的
	// GETACK 分支）。readCommandLoop 用该回复检测投递停滞的尾巴缺口。
	// replCtx 取消（readCommandLoop 返回 / shutdown）时本 goroutine 退出。
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := sr.writeRespToMaster(mc, []byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")); err != nil {
					return
				}
			case <-replCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-sr.stopCh:
			logger.Logger.Info().Msg("从节点复制已停止")
			return nil
		default:
		}

		mc.mu.RLock()
		reader := mc.Reader
		mc.mu.RUnlock()

		req, err := proto.ReadRESP(reader)
		if err != nil {
			if errorsIsStop(err, sr.stopCh) {
				return nil
			}
			return fmt.Errorf("read command from master failed: %w", err)
		}

		cmd := strings.ToUpper(string(req.Args[0]))

		// 处理 PING — 响应 PONG，保持连接活跃。
		// 注意：masterReplOffset 不包含 PING/REPLCONF/SELECT 字节，
		// 因此 slave 的 lastOffset 也不能计入它们，否则 slave offset
		// 会 drift 超过 master offset，导致 PSYNC 偏移量校验失败、
		// 反复 FULLRESYNC 和数据丢失。
		if cmd == "PING" {
			if err := sr.writeRespToMaster(mc, []byte("+PONG\r\n")); err != nil {
				return fmt.Errorf("write PONG to master failed: %w", err)
			}
			continue
		}

		// 处理 REPLCONF GETACK — 响应 REPLCONF ACK <offset>，防止主节点超时断连
		if cmd == "REPLCONF" && len(req.Args) >= 3 &&
			strings.ToUpper(string(req.Args[1])) == "GETACK" {
			offset := sr.lastOffset.Load()
			ackResp := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
				len(strconv.FormatInt(offset, 10)), offset)
			if err := sr.writeRespToMaster(mc, []byte(ackResp)); err != nil {
				return fmt.Errorf("write REPLCONF ACK to master failed: %w", err)
			}
			continue
		}

		// 处理 REPLCONF ACK <offset> — 主节点对我们 GETACK 的回复，通告其当前
		// offset（= backlog 水位）。若主节点水位高于已应用 offset 且数据流
		// 空闲超过 stallTimeout，说明有条目在投递中静默丢失（无读错误、无
		// send_drop/apply_skip 计数），返回错误强制重连，PSYNC CONTINUE
		// 会从 lastOffset 重放缺失区间。
		if cmd == "REPLCONF" && len(req.Args) >= 3 &&
			strings.ToUpper(string(req.Args[1])) == "ACK" {
			if masterOffset, err := strconv.ParseInt(string(req.Args[2]), 10, 64); err == nil {
				sr.masterWater.Store(masterOffset)
				slaveOffset := sr.lastOffset.Load()
				lastData := time.Unix(0, sr.lastDataTime.Load())
				stall := sr.stallTimeout
				if stall <= 0 {
					stall = replStallTimeout
				}
				if masterOffset <= slaveOffset {
					// 已收敛：记录收敛时刻（停滞检测的武装前提）。
					sr.lastConvergedTime.Store(time.Now().UnixNano())
				} else {
					idle := time.Since(lastData)
					armed := time.Since(time.Unix(0, sr.lastConvergedTime.Load())) < replStallArmWindow
					drainStall := sr.drainStallTimeout
					if drainStall <= 0 {
						drainStall = replDrainStallTimeout
					}
					// 已武装（本连接近期曾收敛）：短暂空闲即判定尾巴缺口；
					// 未武装（追赶排水期）：仅超长空闲才判定排水冻结——瞬态
					// 发送停顿（捕获轮 idle=2.55s）不误判，真冻结（lag=162
					// 停 40s）触发强制重连自愈（TODO §1c）。
					if (armed && idle > stall) || (!armed && idle > drainStall) {
						logger.Logger.Warn().
							Int64("master_offset", masterOffset).
							Int64("slave_offset", slaveOffset).
							Dur("idle", idle).
							Bool("armed", armed).
							Msg("复制流停滞：主节点水位高于已应用 offset 且数据流空闲，强制重连")
						return fmt.Errorf("replication stalled: master at %d, slave at %d", masterOffset, slaveOffset)
					}
				}
			}
			continue
		}

		// 处理 SELECT — 忽略数据库选择，不推进 offset（master 不追踪 SELECT）
		if cmd == "SELECT" {
			continue
		}

		// 到达此处即数据命令：记录最近数据到达时间（停滞检测用）。
		sr.lastDataTime.Store(time.Now().UnixNano())

		cmdBytes := serializeCommand(req.Args)

		if err := executeReplicatedCommand(sr.store, req.Args, replCtx); err != nil {
			currentOffset := sr.lastOffset.Load()
			if isTransientReplicationError(err, cmd, currentOffset) {
				// 命令字节已从主节点流中消费，必须推进 offset 与主节点保持锁步：
				// 否则从节点 lastOffset 落后，重连时 PSYNC CONTINUE 取到错位字节流，
				// ReadRESP 误把 key 名当命令名（如 K:HASH:47）→ 无限重同步循环。
				sr.rm.applySkipCount.Add(1)
				logger.Logger.Warn().Err(err).
					Str("cmd", cmd).
					Int64("offset", currentOffset).
					Int64("apply_skip_count", sr.rm.applySkipCount.Load()).
					Msg("复制命令暂时失败，跳过（已推进 offset 保持字节级对齐）")
				sr.lastOffset.Add(int64(len(cmdBytes)))
				continue
			}
			logger.Logger.Error().Err(err).
				Str("cmd", cmd).
				Msg("执行复制命令失败，重新同步")
			return fmt.Errorf("execute replicated command failed: %w", err)
		}

		sr.lastOffset.Add(int64(len(cmdBytes)))
	}
}

// writeRespToMaster 向主节点写入响应数据
func (sr *SlaveReconnector) writeRespToMaster(mc *MasterConnection, data []byte) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if _, err := mc.Writer.Write(data); err != nil {
		return err
	}
	return mc.Writer.Flush()
}

func dialMaster(addr string, tlsCfg *tls.Config) (*MasterConnection, error) {
	var conn net.Conn
	var err error
	if tlsCfg != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(10 * time.Second)
	}

	return &MasterConnection{
		Addr:   addr,
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		Writer: bufio.NewWriter(conn),
		stopCh: make(chan struct{}),
	}, nil
}

func errorsIsStop(err error, stopCh <-chan struct{}) bool {
	if err == nil {
		return false
	}
	select {
	case <-stopCh:
		return true
	default:
		return false
	}
}

// isTransientReplicationError 判断复制命令的错误是否可安全跳过。
//
// 重要：L0 背压（write rejected）和 retry 耗尽（max retries exhausted）
// 绝不能跳过——跳过会永久丢失该条 mutation，导致主从静默发散。
// 这些错误应触发重连 / FULLRESYNC（readCommandLoop 返回 error）。
//
// 仅保留真正幂等/无害的情况：key not found（主已删而从本无此键）。
//
// cmd 和 offset 用于结构化日志，帮助收集 soak 下触发发散的实际错误类型。
func isTransientReplicationError(err error, cmd string, offset int64) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// 键不存在（主从短暂不一致时的正常现象，对 DEL/SREM 等幂等）
	if strings.Contains(errStr, "key not found") {
		logger.Logger.Warn().
			Err(err).
			Str("cmd", cmd).
			Int64("offset", offset).
			Str("err_str", errStr).
			Msg("复制命令跳过（key not found）— 需收集 soak 实证确认此路径是否真的触发")
		return true
	}
	return false
}
