package replication

import (
	"bufio"
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
	lastOffset     int64
	connectedSince time.Time
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

func (sr *SlaveReconnector) GetMasterAddr() string {
	return sr.masterAddr
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

		if !sr.connectedSince.IsZero() && time.Since(sr.connectedSince) >= sr.config.ResetAfter {
			retries = 0
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

		select {
		case <-sr.stopCh:
			return
		case <-time.After(backoff):
		}
	}
}

func (sr *SlaveReconnector) tryReplicate() error {
	sr.state.Store(int32(SlaveConnecting))

	masterConn, err := dialMaster(sr.masterAddr)
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
	psyncOffset := sr.lastOffset
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
			sr.lastOffset = offset
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

		if err := executeReplicatedCommand(sr.store, req.Args); err != nil {
			logger.Logger.Error().Err(err).
				Str("cmd", string(req.Args[0])).
				Msg("执行复制命令失败，重新同步")
			return fmt.Errorf("execute replicated command failed: %w", err)
		}

		cmdBytes := serializeCommand(req.Args)
		sr.lastOffset += int64(len(cmdBytes))
	}
}

func dialMaster(addr string) (*MasterConnection, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
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
