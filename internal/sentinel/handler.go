package sentinel

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// SentinelHandler 哨兵命令处理器
type SentinelHandler struct {
	sentinel       *Sentinel
	configProvider *ConfigProvider
	failoverMgr    *FailoverManager

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// NewSentinelHandler 创建新的哨兵处理器
func NewSentinelHandler(sentinel *Sentinel) *SentinelHandler {
	return &SentinelHandler{
		sentinel:       sentinel,
		configProvider: NewConfigProvider(sentinel),
		failoverMgr:    NewFailoverManager(sentinel),
		conns:          make(map[net.Conn]struct{}),
	}
}

// Stop 关闭所有活跃连接并等待所有处理 goroutine 退出
func (sh *SentinelHandler) Stop() {
	sh.mu.Lock()
	for conn := range sh.conns {
		if err := conn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("关闭哨兵客户端连接")
		}
	}
	sh.mu.Unlock()
	sh.wg.Wait()
}

// HandleConnection 处理连接
func (sh *SentinelHandler) HandleConnection(conn net.Conn) {
	sh.mu.Lock()
	sh.conns[conn] = struct{}{}
	sh.mu.Unlock()
	sh.wg.Add(1)

	defer func() {
		sh.mu.Lock()
		delete(sh.conns, conn)
		sh.mu.Unlock()
		sh.wg.Done()
	}()

	defer func() {
		if err := conn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close connection")
		}
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer func() {
		if err := writer.Flush(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to flush writer")
		}
	}()

	for {
		req, err := proto.ReadRESP(reader)
		if err != nil {
			logger.Logger.Debug().Err(err).Msg("Sentinel: read request failed")
			return
		}

		if len(req.Args) == 0 {
			_ = proto.WriteRESP(writer, proto.NewError("ERR no command"))
			if err := writer.Flush(); err != nil {
				logger.Logger.Debug().Err(err).Msg("failed to flush writer")
			}
			continue
		}

		cmd := strings.ToUpper(string(req.Args[0]))
		args := req.Args[1:]

		resp := sh.executeCommand(cmd, args)
		if resp == nil {
			resp = proto.NewError("ERR internal error")
		}

		if err := proto.WriteRESP(writer, resp); err != nil {
			logger.Logger.Debug().Err(err).Msg("Sentinel: write response failed")
			return
		}
		if err := writer.Flush(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Sentinel: flush failed")
			return
		}
	}
}

// executeCommand 执行命令
func (sh *SentinelHandler) executeCommand(cmd string, args [][]byte) proto.RESP {
	switch cmd {
	case "PING":
		return proto.NewSimpleString("PONG")

	case "SENTINEL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SENTINEL' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		return sh.handleSentinelCommand(subcommand, args[1:])

	default:
		return proto.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

// handleSentinelCommand 处理SENTINEL子命令
func (sh *SentinelHandler) handleSentinelCommand(subcommand string, args [][]byte) proto.RESP {
	switch subcommand {
	case "MONITOR":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'SENTINEL MONITOR' command")
		}
		name := string(args[0])
		ip := string(args[1])
		port := string(args[2])
		quorum, err := strconv.Atoi(string(args[3]))
		if err != nil {
			return proto.NewError("ERR invalid quorum")
		}
		addr := fmt.Sprintf("%s:%s", ip, port)
		if err := sh.sentinel.AddMaster(name, addr, quorum); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "MASTERS":
		masters := sh.configProvider.GetMasters()
		results := make([][]byte, 0)
		for _, master := range masters {
			// 格式化为数组
			for k, v := range master {
				results = append(results, []byte(k), []byte(v))
			}
		}
		return &proto.Array{Args: results}

	case "SLAVES":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SENTINEL SLAVES' command")
		}
		masterName := string(args[0])
		slaves := sh.configProvider.GetSlaves(masterName)
		results := make([][]byte, 0)
		for _, slave := range slaves {
			for k, v := range slave {
				results = append(results, []byte(k), []byte(v))
			}
		}
		return &proto.Array{Args: results}

	case "GET-MASTER-ADDR-BY-NAME":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SENTINEL GET-MASTER-ADDR-BY-NAME' command")
		}
		masterName := string(args[0])
		addr, err := sh.configProvider.GetMasterAddrByName(masterName)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		parts := strings.Split(addr, ":")
		if len(parts) != 2 {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(parts[0]), []byte(parts[1])}}

	case "FAILOVER":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SENTINEL FAILOVER' command")
		}
		masterName := string(args[0])
		if err := sh.failoverMgr.StartFailover(masterName); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	default:
		return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
	}
}
