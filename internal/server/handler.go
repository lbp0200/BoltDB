package server

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

type Handler struct {
	Db          *store.BotreonStore
	Cluster     *cluster.Cluster
	Replication *replication.ReplicationManager
	Backup      *backup.BackupManager
	PubSub      *store.PubSubManager
	Port        int // 服务器监听端口
	// 事务状态（每个连接独立）
	transaction *TransactionState
	// 客户端信息（连接级别）
	clientInfo *ClientInfo
	// 集群ASKING状态
	clusterAsking bool
}

// ClientInfo 客户端连接信息
type ClientInfo struct {
	ID       int64               // 客户端 ID
	Name     string              // 客户端名称
	Addr     string              // 客户端地址
	FD       int                 // 文件描述符
	Age      int64               // 连接时长（秒）
	Idle     int64               // 空闲时间（秒）
	Flags    string              // 客户端标志
	DB       int                 // 当前数据库 ID
	Sub      int                 // 订阅频道数
	PSub     int                 // 模式订阅数
	Multi    int                 //事务中的命令数
	Cmd      string              // 最后执行的命令
	OFlags   string              // 客户端输出缓冲区限制标志
	Events   string              // 事件处理标志
	Keys     map[string]struct{} // 客户端监控的键
	ReadOnly bool                // 只读模式
}

// TransactionState 事务状态
type TransactionState struct {
	Commands      []TransactionCommand // 排队的命令
	WatchKeys     map[string]struct{} // 监控的键
	IsWatching    bool                // 是否处于监视状态
	InTransaction bool                // 是否在事务中（MULTI 已执行）
	DirtyKeys     map[string]struct{} // 被修改的键
}

// TransactionCommand 事务中的命令
type TransactionCommand struct {
	Command string
	Args    [][]byte
}

// resetTransaction 重置事务状态
func (h *Handler) resetTransaction() {
	h.transaction = nil
}

// markDirtyKeys 标记键为脏（被修改）
func (h *Handler) markDirtyKeys(keys ...string) {
	if h.transaction != nil && h.transaction.IsWatching {
		for _, key := range keys {
			h.transaction.DirtyKeys[key] = struct{}{}
		}
	}
}

// checkAndHandleRedirect 检查键是否需要重定向到其他节点
// 返回 nil 表示不需要重定向，可以继续执行命令
// 返回非 nil 表示需要重定向，包含重定向信息
func (h *Handler) checkAndHandleRedirect(key string) proto.RESP {
	if h.Cluster == nil {
		return nil // 不在集群模式，直接执行
	}

	// 如果处于 ASKING 状态，检查是否是导入中的槽
	if h.clusterAsking {
		slot := cluster.Slot(key)
		if h.Cluster.IsImportingSlot(slot) {
			// 导入中的槽，允许执行命令
			return nil
		}
		// ASKING 状态已过期，重置
		h.clusterAsking = false
	}

	redirect := h.Cluster.CheckSlotRedirect(key)
	if redirect != nil {
		// 如果是 MOVED 重定向，返回错误
		if redirect.Type == "MOVED" {
			return proto.NewError(redirect.Error())
		}
		// 如果是 ASK 重定向，返回错误
		if redirect.Type == "ASK" {
			return proto.NewError(redirect.Error())
		}
	}
	return nil
}

// checkAndHandleMultiKeyRedirect 检查多个键是否需要重定向
// 如果所有键都在当前节点，返回 nil
// 如果有键需要重定向，返回 MOVED 错误
func (h *Handler) checkAndHandleMultiKeyRedirect(keys []string) proto.RESP {
	if h.Cluster == nil {
		return nil // 不在集群模式，直接执行
	}
	var movedError *cluster.RedirectError
	for _, key := range keys {
		redirect := h.Cluster.CheckSlotRedirect(key)
		if redirect != nil {
			if redirect.Type == "MOVED" {
				movedError = redirect
			}
		}
	}
	if movedError != nil {
		return proto.NewError(movedError.Error())
	}
	return nil
}

// ServeTCP 监听并处理连接
func (h *Handler) ServeTCP(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go h.handleConnection(conn)
	}
}

func (h *Handler) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("新连接建立")

	// 标记连接是否已由复制处理接管
	// 如果为true，主handler不关闭连接，由复制处理的goroutine负责关闭
	replicationOwned := false

	defer func() {
		if !replicationOwned {
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("连接关闭")
			if err := conn.Close(); err != nil {
				logger.Logger.Debug().Err(err).Msg("failed to close connection")
			}
		} else {
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("连接已由复制处理接管")
		}
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// 在复制接管时，需要关闭reader/writer以防止defer尝试Flush
	// 但连接本身保持打开，由handleSlaveReplicationConnection负责关闭
	defer func() {
		if replicationOwned {
			// 只关闭writer的Flush错误，不关闭连接
			// 连接由handleSlaveReplicationConnection goroutine关闭
			return
		}
		if err := writer.Flush(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to flush writer")
		}
	}()

	// 设置 TCP_NODELAY 以减少延迟
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to set TCP_NODELAY")
		}
	}

	for {
		// 尝试读取所有可用的命令（支持 Pipeline）
		// 先尝试读取第一个命令
		req, err := proto.ReadRESP(reader)
		if err != nil {
			// 连接关闭或读取错误，直接返回
			// 不发送错误响应，因为连接可能已关闭
			// 这可能是正常的连接关闭（如 redis-benchmark 完成测试后关闭连接）
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("读取请求失败")
			return
		}

		// 收集所有响应
		var responses []proto.RESP
		commandsProcessed := 0

		// 处理第一个命令
		if resp := h.processRequest(req, reader, remoteAddr, writer, conn); resp != nil {
			// 检查是否是复制接管信号
			if _, isTakeover := resp.(ReplicationTakeoverSignal); isTakeover {
				replicationOwned = true
				logger.Logger.Debug().
					Str("remote_addr", remoteAddr).
					Msg("复制接管连接")
				return
			}
			responses = append(responses, resp)
			commandsProcessed++
		} else {
			// 处理失败或连接已由复制接管，直接返回
			return
		}

		// 尝试读取更多已缓冲的命令（Pipeline）
		for reader.Buffered() > 0 {
			req, err := proto.ReadRESP(reader)
			if err != nil {
				// 如果读取失败，可能是连接关闭
				logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("Pipeline 中读取请求失败")
				break
			}

			if resp := h.processRequest(req, reader, remoteAddr, writer, conn); resp != nil {
				// 检查是否是复制接管信号
				if _, isTakeover := resp.(ReplicationTakeoverSignal); isTakeover {
					replicationOwned = true
					logger.Logger.Debug().
						Str("remote_addr", remoteAddr).
						Msg("复制接管连接")
					return
				}
				responses = append(responses, resp)
				commandsProcessed++
			} else {
				// 处理失败或连接已由复制接管，直接返回
				return
			}
		}

		// 批量发送所有响应
		for _, resp := range responses {
			if err := proto.WriteRESP(writer, resp); err != nil {
				logger.Logger.Warn().
					Str("remote_addr", remoteAddr).
					Err(err).
					Msg("写入响应失败")
				return
			}
		}

		// 一次性刷新所有响应
		if err := writer.Flush(); err != nil {
			logger.Logger.Warn().
				Str("remote_addr", remoteAddr).
				Err(err).
				Msg("刷新缓冲区失败")
			return
		}

		logger.Logger.Debug().
			Str("remote_addr", remoteAddr).
			Int("commands_processed", commandsProcessed).
			Msg("Pipeline 命令处理完成")
	}
}

// processRequest 处理单个请求，返回响应
// PSYNC特殊处理：如果需要全量同步，会在返回响应后发送RDB数据
// 返回 nil 表示连接已由复制接管，需要关闭处理循环
func (h *Handler) processRequest(req *proto.Array, reader *bufio.Reader, remoteAddr string, writer *bufio.Writer, conn net.Conn) proto.RESP {
	args := req.Args
	if len(args) == 0 {
		logger.Logger.Warn().Str("remote_addr", remoteAddr).Msg("收到空命令")
		return proto.NewError("ERR no command")
	}
	cmd := strings.ToUpper(string(args[0]))
	logger.Logger.Debug().
		Str("remote_addr", remoteAddr).
		Str("command", cmd).
		Int("arg_count", len(args)-1).
		Msg("执行命令")

	// PSYNC特殊处理
	if cmd == "PSYNC" && h.Replication != nil && h.Replication.IsMaster() {
		resp := h.handlePSyncWithRDB(args[1:], remoteAddr, conn, reader, writer)
		// 如果返回nil，表示连接已由复制接管，需要关闭处理循环
		if resp == nil {
			return nil // 信号: 关闭连接
		}
		return resp
	}

	resp := h.executeCommand(cmd, args[1:], remoteAddr)
	if resp == nil {
		logger.Logger.Error().
			Str("remote_addr", remoteAddr).
			Str("command", cmd).
			Msg("命令执行返回 nil")
		return proto.NewError("ERR internal error")
	}

	// 如果是主节点且是写命令，传播到从节点
	if h.Replication != nil && h.Replication.IsMaster() && isWriteCommand(cmd) {
		if cmd != "REPLICAOF" && cmd != "PSYNC" && cmd != "REPLCONF" {
			h.Replication.PropagateCommand(req.Args)
		}
	}

	logger.Logger.Debug().
		Str("remote_addr", remoteAddr).
		Str("command", cmd).
		Str("response_type", getResponseType(resp)).
		Msg("命令执行完成")

	return resp
}

// getResponseType 获取响应类型（用于日志）
func getResponseType(resp proto.RESP) string {
	switch resp.(type) {
	case *proto.SimpleString:
		return "SimpleString"
	case *proto.BulkString:
		return "BulkString"
	case proto.Error:
		return "Error"
	case proto.Integer:
		return "Integer"
	case *proto.Array:
		return "Array"
	default:
		return "Unknown"
	}
}

// handlePSyncWithRDB 处理PSYNC命令并发送RDB数据（全量同步）
// 这是executeCommand的特例，用于在全量同步时直接发送RDB数据
// 返回 nil 表示正常处理
// 返回 ReplicationTakeoverSignal{} 表示连接已由复制接管，需要关闭处理循环但不关闭连接
type ReplicationTakeoverSignal struct{}

func (ReplicationTakeoverSignal) String() string           { return "replication-takeover" }
func (ReplicationTakeoverSignal) Error() string            { return "replication takeover" }
func (ReplicationTakeoverSignal) IsError() bool            { return false }

func (h *Handler) handlePSyncWithRDB(args [][]byte, remoteAddr string, conn net.Conn, reader *bufio.Reader, writer *bufio.Writer) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'PSYNC' command")
	}

	replId := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR invalid offset")
	}

	// 处理PSYNC
	result, err := replication.HandlePSync(h.Replication, replId, offset)
	if err != nil {
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}

	if result.FullResync {
		// 发送FULLRESYNC响应
		response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", result.ReplId, result.Offset)

		// 先发送FULLRESYNC响应
		if _, err := writer.WriteString(response); err != nil {
			logger.Logger.Error().Err(err).Msg("发送FULLRESYNC失败")
			return proto.NewError("ERR failed to send FULLRESYNC")
		}

		// 生成并发送RDB数据
		rdbData, err := replication.GenerateRDB(h.Db)
		if err != nil {
			logger.Logger.Error().Err(err).Msg("生成RDB数据失败")
			return proto.NewError("ERR failed to generate RDB")
		}

		// 发送RDB数据（Bulk String格式）
		rdbHeader := fmt.Sprintf("$%d\r\n", len(rdbData))
		if _, err := writer.WriteString(rdbHeader); err != nil {
			logger.Logger.Error().Err(err).Msg("发送RDB header失败")
			return proto.NewError("ERR failed to send RDB header")
		}

		if _, err := writer.Write(rdbData); err != nil {
			logger.Logger.Error().Err(err).Msg("发送RDB数据失败")
			return proto.NewError("ERR failed to send RDB data")
		}

		if _, err := writer.WriteString("\r\n"); err != nil {
			logger.Logger.Error().Err(err).Msg("发送RDB尾部失败")
			return proto.NewError("ERR failed to send RDB trailer")
		}

		if err := writer.Flush(); err != nil {
			logger.Logger.Error().Err(err).Msg("刷新writer失败")
			return proto.NewError("ERR failed to flush writer")
		}

		logger.Logger.Info().
			Str("slave_addr", remoteAddr).
			Str("repl_id", result.ReplId).
			Int64("offset", result.Offset).
			Int("rdb_size", len(rdbData)).
			Msg("发送FULLRESYNC和RDB到从节点")

		// 创建从节点连接并注册
		slaveConn := replication.NewSlaveConnection(conn)
		// 设置初始复制偏移量
		slaveConn.SetReplOffset(result.Offset)
		// 标记为已就绪（可以接收命令）
		slaveConn.SetReady(true)

		// 添加到复制管理器
		h.Replication.AddSlave(slaveConn)

		// 启动goroutine处理从节点的复制连接（接收REPLCONF ACK等）
		go h.handleSlaveReplicationConnection(slaveConn)

		// 返回复制接管信号，主handler不会关闭连接
		return ReplicationTakeoverSignal{}
	} else {
		// 发送CONTINUE响应
		response := fmt.Sprintf("+CONTINUE %s\r\n", result.ReplId)
		return proto.NewSimpleString(strings.TrimSpace(response))
	}
}

// handleSlaveReplicationConnection 处理从节点的复制连接
// 这个goroutine负责从节点连接的生命周期：
// 1. 接收 REPLCONF ACK 命令（从节点确认已接收的命令偏移量）
// 2. 保持连接打开，直到从节点断开
// 3. 负责关闭连接
func (h *Handler) handleSlaveReplicationConnection(slave *replication.SlaveConnection) {
	defer func() {
		// 关闭连接
		if err := slave.Close(); err != nil {
			logger.Logger.Debug().
				Str("slave_id", slave.ID).
				Err(err).
				Msg("关闭从节点连接失败")
		}
		// 连接关闭时移除从节点
		h.Replication.RemoveSlave(slave.ID)
		logger.Logger.Info().
			Str("slave_id", slave.ID).
			Str("slave_addr", slave.Addr).
			Msg("从节点连接已关闭")
	}()

	logger.Logger.Info().
		Str("slave_id", slave.ID).
		Str("slave_addr", slave.Addr).
		Msg("开始处理从节点复制连接")

	// 持续接收从节点的命令（主要是REPLCONF ACK）
	for {
		req, err := proto.ReadRESP(slave.Reader)
		if err != nil {
			logger.Logger.Debug().
				Str("slave_id", slave.ID).
				Err(err).
				Msg("读取从节点命令失败")
			return
		}

		// 解析命令
		if len(req.Args) == 0 {
			continue
		}

		cmd := strings.ToUpper(string(req.Args[0]))
		logger.Logger.Debug().
			Str("slave_id", slave.ID).
			Str("cmd", cmd).
			Msg("收到从节点命令")

		// 处理 REPLCONF ACK 命令
		if cmd == "REPLCONF" && len(req.Args) >= 3 {
			if strings.ToUpper(string(req.Args[1])) == "ACK" {
				// 解析偏移量
				offset, _ := strconv.ParseInt(string(req.Args[2]), 10, 64)
				slave.UpdateReplAck(offset)
				h.Replication.UpdateSlaveAckOffset(slave.ID, offset)
				continue
			}
		}

		// 其他命令（理论上不应该有）
		logger.Logger.Warn().
			Str("slave_id", slave.ID).
			Str("cmd", cmd).
			Msg("从节点发送了未知命令")
	}
}

func (h *Handler) executeCommand(cmd string, args [][]byte, remoteAddr string) proto.RESP {
	switch cmd {
	// 连接命令
	case "PING":
		return proto.NewSimpleString("PONG")

	case "ROLE":
		// 返回角色信息，兼容 redis-sentinel
		// 格式: [master|slave|sentinel, master地址, 复制偏移量]
		// 对于主节点: ["master", "repl_offset"]
		// 对于从节点: ["slave", "master地址", master端口, 状态, 已同步偏移量]
		if h.Replication != nil {
			role := h.Replication.GetRole()
			if role == "master" {
				offset := h.Replication.GetMasterReplOffset()
				return &proto.Array{Args: [][]byte{
					[]byte("master"),
					[]byte(strconv.FormatInt(offset, 10)),
				}}
			} else {
				// 从节点
				masterAddr := h.Replication.GetMasterAddr()
				masterHost := ""
				masterPort := "6379"
				if masterAddr != "" {
					parts := strings.Split(masterAddr, ":")
					if len(parts) >= 2 {
						masterHost = parts[0]
						masterPort = parts[1]
					} else if len(parts) == 1 {
						masterHost = parts[0]
					}
				}
				offset := h.Replication.GetMasterReplOffset()
				return &proto.Array{Args: [][]byte{
					[]byte("slave"),
					[]byte(masterHost),
					[]byte(masterPort),
					[]byte("connected"),
					[]byte(strconv.FormatInt(offset, 10)),
				}}
			}
		}
		// 默认主节点
		return &proto.Array{Args: [][]byte{
			[]byte("master"),
			[]byte("0"),
		}}

	case "ECHO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ECHO' command")
		}
		return proto.NewBulkString(args[0])

	case "CLIENT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "LIST":
			// 返回当前客户端列表（简化实现）
			return proto.NewBulkString([]byte("id=1 addr=127.0.0.1:12345 fd=6 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 cmd=client events=r oFlags= keys=0"))
		case "GETNAME":
			if h.clientInfo != nil && h.clientInfo.Name != "" {
				return proto.NewBulkString([]byte(h.clientInfo.Name))
			}
			// 返回 nil BulkString（go-redis 会将其作为 nil 返回）
			nilResp := proto.NewBulkString(nil)
			return nilResp
		case "SETNAME":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT SETNAME' command")
			}
			// 设置客户端名称（仅内存存储）
			name := string(args[1])
			if h.clientInfo == nil {
				h.clientInfo = &ClientInfo{}
			}
			h.clientInfo.Name = name
			return proto.OK
		case "ID":
			if h.clientInfo != nil {
				return proto.NewInteger(h.clientInfo.ID)
			}
			return proto.NewInteger(1)
		case "KILL":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT KILL' command")
			}
			addr := string(args[1])
			// 简化实现：检查地址格式，但不真正关闭连接
			// 由于无法真正关闭连接，总是返回0（连接不存在）
			if addr == "" {
				return proto.NewError("ERR Invalid address")
			}
			// 返回0表示没有杀死任何连接（简化实现）
			return proto.NewInteger(0)
		case "PAUSE":
			// 暂停客户端（简化实现：空操作）
			return proto.OK
		case "UNPAUSE":
			// 取消暂停（简化实现：空操作）
			return proto.OK
		case "INFO":
			// 返回客户端连接信息
			addr := "127.0.0.1:6379"
			clientID := int64(0)
			fd := 0
			clientName := ""
			if h.clientInfo != nil {
				if h.clientInfo.Addr != "" {
					addr = h.clientInfo.Addr
				}
				clientID = h.clientInfo.ID
				fd = h.clientInfo.FD
				clientName = h.clientInfo.Name
			}
			clientAge := "0"
			idleTime := "0"
			flags := "N"
			db := "0"
			sub := "0"
			psub := "0"
			multi := "-1"
			keys := "0"
			info := fmt.Sprintf("id=%d addr=%s fd=%d name=%s age=%s idle=%s flags=%s db=%s sub=%s psub=%s multi=%s cmd=client events=r oFlags= keys=%s",
				clientID, addr, fd, clientName, clientAge, idleTime, flags, db, sub, psub, multi, keys)
			return proto.NewBulkString([]byte(info))
		case "NOEVICT":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT NOEVICT' command")
			}
			mode := strings.ToUpper(string(args[1]))
			if mode != "ON" && mode != "OFF" {
				return proto.NewError("ERR syntax error")
			}
			// noevict 模式（简化实现）
			return proto.OK
		case "TRACKING":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT TRACKING' command")
			}
			mode := strings.ToUpper(string(args[1]))
			if mode != "ON" && mode != "OFF" {
				return proto.NewError("ERR syntax error")
			}
			// tracking 模式（简化实现）
			return proto.OK
		default:
			return proto.NewError("ERR syntax error")
		}

	// String命令
	case "SET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SET' command")
		}
		key, value := string(args[0]), string(args[1])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		h.markDirtyKeys(key)
		if err := h.Db.Set(key, value); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "GET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'GET' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		value, err := h.Db.Get(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(value))

	case "SETEX":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SETEX' command")
		}
		key, value := string(args[0]), string(args[2])
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR invalid integer")
		}
		h.markDirtyKeys(key)
		if err := h.Db.SetEX(key, value, seconds); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "PSETEX":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'PSETEX' command")
		}
		key, value := string(args[0]), string(args[2])
		milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR invalid integer")
		}
		h.markDirtyKeys(key)
		if err := h.Db.PSETEX(key, value, milliseconds); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "SETNX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SETNX' command")
		}
		key, value := string(args[0]), string(args[1])
		h.markDirtyKeys(key)
		success, err := h.Db.SetNX(key, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "GETSET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GETSET' command")
		}
		key, value := string(args[0]), string(args[1])
		h.markDirtyKeys(key)
		oldValue, err := h.Db.GetSet(key, value)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(oldValue))

	case "MGET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'MGET' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		// 检查集群重定向
		if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
			return resp
		}
		values, err := h.Db.MGet(keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(values))
		for i, v := range values {
			if v == "" {
				results[i] = nil
			} else {
				results[i] = []byte(v)
			}
		}
		return &proto.Array{Args: results}

	case "MSET":
		if len(args) < 2 || len(args)%2 != 0 {
			return proto.NewError("ERR wrong number of arguments for 'MSET' command")
		}
		pairs := make([]string, len(args))
		for i, arg := range args {
			pairs[i] = string(arg)
		}
		// 检查集群重定向（取所有键中的第一个作为检查点）
		keys := make([]string, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			keys = append(keys, string(args[i]))
		}
		if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
			return resp
		}
		if err := h.Db.MSet(pairs...); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "MSETNX":
		if len(args) < 2 || len(args)%2 != 0 {
			return proto.NewError("ERR wrong number of arguments for 'MSETNX' command")
		}
		pairs := make([]string, len(args))
		for i, arg := range args {
			pairs[i] = string(arg)
		}
		// 检查集群重定向（取所有键中的第一个作为检查点）
		keys := make([]string, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			keys = append(keys, string(args[i]))
		}
		if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
			return resp
		}
		success, err := h.Db.MSetNX(pairs...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "INCR":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'INCR' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		h.markDirtyKeys(key)
		value, err := h.Db.INCR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "INCRBY":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'INCRBY' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		increment, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		value, err := h.Db.INCRBY(key, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "DECR":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DECR' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		h.markDirtyKeys(key)
		value, err := h.Db.DECR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "DECRBY":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'DECRBY' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		decrement, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		value, err := h.Db.DECRBY(key, decrement)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "INCRBYFLOAT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'INCRBYFLOAT' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		increment, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		h.markDirtyKeys(key)
		value, err := h.Db.INCRBYFLOAT(key, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))

	case "APPEND":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'APPEND' command")
		}
		key, value := string(args[0]), string(args[1])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		h.markDirtyKeys(key)
		length, err := h.Db.APPEND(key, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	case "STRLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'STRLEN' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		length, err := h.Db.StrLen(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	// Bitmap commands
	case "SETBIT":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SETBIT' command")
		}
		key := string(args[0])
		offset, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		bit, err := strconv.ParseUint(string(args[2]), 10, 8)
		if err != nil || (bit != 0 && bit != 1) {
			return proto.NewError("ERR bit is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		newBit, err := h.Db.SetBit(key, int(offset), int(bit))
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(newBit))

	case "GETBIT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GETBIT' command")
		}
		key := string(args[0])
		offset, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		bit, err := h.Db.GetBit(key, int(offset))
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(bit))

	case "BITCOUNT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'BITCOUNT' command")
		}
		key := string(args[0])
		// BITCOUNT key [start end]
		start := 0
		end := -1
		if len(args) >= 3 {
			s, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			start = s
			e, err := strconv.Atoi(string(args[2]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			end = e
		}
		count, err := h.Db.BitCount(key, start, end)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "BITOP":
		// BITOP operation destkey key [key ...]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'BITOP' command")
		}
		operation := strings.ToUpper(string(args[0]))
		destKey := string(args[1])
		sourceKeys := make([]string, len(args)-2)
		for i := 2; i < len(args); i++ {
			sourceKeys[i-2] = string(args[i])
		}
		// 验证操作类型
		if operation != "AND" && operation != "OR" && operation != "XOR" && operation != "NOT" {
			return proto.NewError("ERR syntax error")
		}
		// NOT 只能有一个源键
		if operation == "NOT" && len(sourceKeys) != 1 {
			return proto.NewError("ERR BITOP NOT must be called with exactly one source key")
		}
		h.markDirtyKeys(destKey)
		length, err := h.Db.BitOp(operation, destKey, sourceKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(length))

	case "BITFIELD":
		// BITFIELD key [GET type offset | SET type offset value | INCRBY type offset increment] ...
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'BITFIELD' command")
		}
		key := string(args[0])
		operations := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			operations = append(operations, string(args[i]))
		}
		results, err := h.Db.BitField(key, operations)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// Single operation returns integer, multiple operations return array
		if len(results) == 1 {
			switch v := results[0].(type) {
			case int64:
				return proto.NewInteger(v)
			case []interface{}:
				// Overflow case
				return proto.NewError(fmt.Sprintf("ERR overflow: %v", v))
			}
		}
		// Convert results to RESP array for multiple operations
		respArgs := make([][]byte, len(results))
		for i, r := range results {
			switch v := r.(type) {
			case int64:
				respArgs[i] = []byte(strconv.FormatInt(v, 10))
			case []interface{}:
				respArgs[i] = []byte(fmt.Sprintf("%v:%v", v[0], v[1]))
			}
		}
		return &proto.Array{Args: respArgs}

	case "BITPOS":
		// BITPOS key bit [start [end [BYTE | BIT]]]
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BITPOS' command")
		}
		key := string(args[0])
		bit, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		start, end := 0, -1
		if len(args) >= 3 {
			start, err = strconv.Atoi(string(args[2]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
		}
		if len(args) >= 4 {
			end, err = strconv.Atoi(string(args[3]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
		}
		pos, err := h.Db.BitPos(key, bit, start, end)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(pos))

	case "BITLEN":
		// BITLEN key
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'BITLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.BitLen(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(length))

	case "GETRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'GETRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.Atoi(string(args[1]))
		end, err2 := strconv.Atoi(string(args[2]))
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		value, err := h.Db.GetRange(key, start, end)
		if err != nil {
			return proto.NewBulkString([]byte(""))
		}
		return proto.NewBulkString([]byte(value))

	case "SETRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SETRANGE' command")
		}
		key, value := string(args[0]), string(args[2])
		offset, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		length, err := h.Db.SetRange(key, offset, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	// 通用键管理命令
	case "DEL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DEL' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		// 检查集群重定向
		if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
			return resp
		}
		h.markDirtyKeys(keys...)
		count := int64(0)
		for _, arg := range args {
			key := string(arg)
			deleted, err := h.Db.Del(key)
			if err == nil {
				count += deleted
			}
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(count)

	case "EXISTS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'EXISTS' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		// 检查集群重定向
		if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
			return resp
		}
		count := 0
		for _, arg := range args {
			key := string(arg)
			exists, err := h.Db.Exists(key)
			if err == nil && exists {
				count++
			}
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "PFADD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PFADD' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		elements := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			elements[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		changed, err := h.Db.PFAdd(key, elements...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(changed)

	case "PFCOUNT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PFCOUNT' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		// 检查集群重定向
		if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
			return resp
		}
		count, err := h.Db.PFCount(keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "PFMERGE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PFMERGE' command")
		}
		destKey := string(args[0])
		sourceKeys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			sourceKeys[i-1] = string(args[i])
		}
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(destKey); resp != nil {
			return resp
		}
		h.markDirtyKeys(destKey)
		err := h.Db.PFMerge(destKey, sourceKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "PFINFO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PFINFO' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		info, err := h.Db.PFInfo(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回数组格式: [key1, value1, key2, value2, ...]
		result := make([][]byte, 0, len(info)*2)
		for k, v := range info {
			result = append(result, []byte(k), []byte(strconv.FormatInt(v, 10)))
		}
		return &proto.Array{Args: result}

	case "TYPE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TYPE' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(key); resp != nil {
			return resp
		}
		keyType, err := h.Db.Type(key)
		if err != nil {
			return proto.NewSimpleString("none")
		}
		return proto.NewSimpleString(keyType)

	case "DUMP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DUMP' command")
		}
		key := string(args[0])
		data, err := h.Db.Dump(key)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString(data)

	case "RESTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RESTORE' command")
		}
		key := string(args[0])
		// 解析 TTL（毫秒）
		var ttl time.Duration = 0
		replace := false
		if len(args) > 2 {
			ttlArg := string(args[1])
			// 检查是否是 TTL（数字）而不是序列化数据
			if ttlMS, err := strconv.ParseInt(ttlArg, 10, 64); err == nil {
				// 参数位置偏移：key, ttl, serializedData, [REPLACE|ABSTTL]
				if len(args) < 4 {
					return proto.NewError("ERR wrong number of arguments for 'RESTORE' command")
				}
				// 序列化数据现在在 args[2]
				absttl := false
				for i := 3; i < len(args); i++ {
					upper := strings.ToUpper(string(args[i]))
					if upper == "REPLACE" {
						replace = true
					} else if upper == "ABSTTL" {
						absttl = true
					}
				}
				if absttl {
					// ABSTTL: TTL 是绝对时间戳（毫秒）
					now := time.Now().UnixMilli()
					if ttlMS > now {
						ttl = time.Duration(ttlMS-now) * time.Millisecond
					}
				} else {
					// TTL 是相对时间（毫秒）
					ttl = time.Duration(ttlMS) * time.Millisecond
				}
				serializedData := string(args[2])
				err := h.Db.Restore(key, []byte(serializedData), ttl, replace)
				if err != nil {
					return proto.NewError(fmt.Sprintf("ERR %v", err))
				}
				return proto.OK
			}
		}
		// 旧格式：key, serializedData, [REPLACE]
		serializedData := string(args[1])
		for i := 2; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "REPLACE" {
				replace = true
				break
			}
		}
		err := h.Db.Restore(key, []byte(serializedData), ttl, replace)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "OBJECT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'OBJECT' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		key := string(args[1])

		switch subcommand {
		case "REFCOUNT":
			refcount, err := h.Db.ObjectRefCount(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			if refcount == 0 {
				return proto.NewBulkString(nil)
			}
			return proto.NewInteger(refcount)
		case "ENCODING":
			encoding, err := h.Db.ObjectEncoding(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			if encoding == "" {
				return proto.NewBulkString(nil)
			}
			return proto.NewBulkString([]byte(encoding))
		case "IDLETIME":
			idletime, err := h.Db.ObjectIdleTime(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(idletime)
		case "FREQ":
			// BoltDB doesn't support LFU, return 0
			return proto.NewInteger(0)
		default:
			return proto.NewError("ERR syntax error")
		}

	case "EXPIRE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'EXPIRE' command")
		}
		key := string(args[0])
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		success, err := h.Db.Expire(key, seconds)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "EXPIREAT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'EXPIREAT' command")
		}
		key := string(args[0])
		timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		success, err := h.Db.ExpireAt(key, timestamp)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "PEXPIRE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PEXPIRE' command")
		}
		key := string(args[0])
		milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		success, err := h.Db.PExpire(key, milliseconds)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "PEXPIREAT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PEXPIREAT' command")
		}
		key := string(args[0])
		timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		success, err := h.Db.PExpireAt(key, timestamp)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "TTL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TTL' command")
		}
		key := string(args[0])
		ttl, err := h.Db.TTL(key)
		if err != nil {
			return proto.NewInteger(-2)
		}
		return proto.NewInteger(ttl)

	case "PTTL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PTTL' command")
		}
		key := string(args[0])
		pttl, err := h.Db.PTTL(key)
		if err != nil {
			return proto.NewInteger(-2)
		}
		return proto.NewInteger(pttl)

	case "PERSIST":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PERSIST' command")
		}
		key := string(args[0])
		h.markDirtyKeys(key)
		success, err := h.Db.Persist(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "RENAME":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RENAME' command")
		}
		key, newKey := string(args[0]), string(args[1])
		h.markDirtyKeys(key, newKey)
		if err := h.Db.Rename(key, newKey); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "RENAMENX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RENAMENX' command")
		}
		key, newKey := string(args[0]), string(args[1])
		h.markDirtyKeys(key, newKey)
		success, err := h.Db.RenameNX(key, newKey)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "COPY":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'COPY' command")
		}
		srcKey := string(args[0])
		dstKey := string(args[1])
		replace := false
		db := int(0)
		i := 2
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "REPLACE":
				replace = true
				i++
			case "DB":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				dbNum, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				db = dbNum
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}
		// 不支持跨数据库COPY
		if db != 0 {
			return proto.NewError("ERR DB option not supported")
		}
		// 获取源键类型
		srcType, err := h.Db.Type(srcKey)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if srcType == "none" {
			return proto.NewInteger(0) // 源键不存在
		}
		// 检查目标键是否存在
		dstExists, _ := h.Db.Exists(dstKey)
		if dstExists && !replace {
			return proto.NewInteger(0) // 目标存在且不替换
		}
		h.markDirtyKeys(srcKey, dstKey)
		// 根据类型复制
		var copied bool
		switch srcType {
		case "string":
			val, err := h.Db.Get(srcKey)
			if err == nil {
				err = h.Db.Set(dstKey, val)
			}
			copied = err == nil
		case "list":
			copied = h.copyList(srcKey, dstKey)
		case "hash":
			copied = h.copyHash(srcKey, dstKey)
		case "set":
			copied = h.copySet(srcKey, dstKey)
		case "zset":
			copied = h.copySortedSet(srcKey, dstKey)
		default:
			return proto.NewError("ERR unknown type")
		}
		if !copied {
			return proto.NewError("ERR copy failed")
		}
		return proto.NewInteger(1)

	case "SWAPDB":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SWAPDB' command")
		}
		// BoltDB 是单数据库实现，SWAPDB 是空操作
		return proto.NewSimpleString("OK")

	case "TOUCH":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TOUCH' command")
		}
		// TOUCH 返回键的数量（BadgerDB 不维护访问时间，所以只是 EXIST 的变体）
		count := int64(0)
		for _, arg := range args {
			key := string(arg)
			exists, _ := h.Db.Exists(key)
			if exists {
				count++
			}
		}
		return proto.NewInteger(count)

	case "SHUTDOWN":
		// SHUTDOWN 命令（简化实现：返回错误，因为没有优雅关闭机制）
		return proto.NewError("ERR Redis is running in read-only mode. To shutdown use SHUTDOWN NOSAVE or SHUTDOWN SAVE")

	case "KEYS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'KEYS' command")
		}
		pattern := string(args[0])
		keys, err := h.Db.Keys(pattern)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(keys))
		for i, k := range keys {
			results[i] = []byte(k)
		}
		return &proto.Array{Args: results}

	case "SCAN":
		cursor := uint64(0)
		pattern := "*"
		count := 10
		if len(args) >= 1 {
			var err error
			cursor, err = strconv.ParseUint(string(args[0]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid cursor")
			}
		}
		if len(args) >= 3 && strings.ToUpper(string(args[1])) == "MATCH" {
			pattern = string(args[2])
		}
		if len(args) >= 5 && strings.ToUpper(string(args[3])) == "COUNT" {
			var err error
			count, err = strconv.Atoi(string(args[4]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
		}
		result, err := h.Db.Scan(cursor, pattern, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回嵌套数组格式: [cursor, [key1, key2, ...]]
		return proto.NewScanResponse(result.Cursor, result.Keys)

	case "RANDOMKEY":
		key, err := h.Db.RandomKey()
		if err != nil || key == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(key))

	// List命令
	case "LPUSH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LPUSH' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.LPush(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "RPUSH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RPUSH' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.RPush(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "LPOP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'LPOP' command")
		}
		key := string(args[0])
		value, err := h.Db.LPop(key)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "RPOP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'RPOP' command")
		}
		key := string(args[0])
		value, err := h.Db.RPop(key)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "LLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'LLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.LLen(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	case "LINDEX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LINDEX' command")
		}
		key := string(args[0])
		index, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		value, err := h.Db.LIndex(key, index)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "LRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		values, err := h.Db.LRange(key, start, stop)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(values))
		for i, v := range values {
			results[i] = []byte(v)
		}
		return &proto.Array{Args: results}

	case "LSET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LSET' command")
		}
		key, value := string(args[0]), string(args[2])
		index, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		if err := h.Db.LSet(key, index, value); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "LTRIM":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LTRIM' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		if err := h.Db.LTrim(key, start, stop); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "LINSERT":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'LINSERT' command")
		}
		key, pivot, value := string(args[0]), string(args[2]), string(args[3])
		where := strings.ToUpper(string(args[1]))
		if where != "BEFORE" && where != "AFTER" {
			return proto.NewError("ERR syntax error")
		}
		h.markDirtyKeys(key)
		count, err := h.Db.LInsert(key, where, pivot, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "LPOS":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LPOS' command")
		}
		key := string(args[0])
		element := string(args[1])
		rank := int64(0)
		count := int64(0)
		maxlen := int64(0)

		// 解析可选参数
		i := 2
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			if opt == "RANK" && i+1 < len(args) {
				r, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
				rank = r
				i += 2
			} else if opt == "COUNT" && i+1 < len(args) {
				c, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
				count = c
				i += 2
			} else if opt == "MAXLEN" && i+1 < len(args) {
				m, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
				maxlen = m
				i += 2
			} else {
				return proto.NewError("ERR syntax error")
			}
		}

		positions, err := h.Db.LPos(key, element, rank, count, maxlen)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		if len(positions) == 0 {
			return proto.NewBulkString(nil)
		}

		if count == 0 && rank == 0 {
			// 返回单个位置
			return proto.NewInteger(positions[0])
		}

		// 返回多个位置
		result := make([][]byte, len(positions))
		for j, pos := range positions {
			result[j] = []byte(fmt.Sprintf("%d", pos))
		}
		return &proto.Array{Args: result}

	case "LREM":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LREM' command")
		}
		key, value := string(args[0]), string(args[2])
		count, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		removed, err := h.Db.LRem(key, count, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(removed))

	case "RPOPLPUSH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RPOPLPUSH' command")
		}
		source, destination := string(args[0]), string(args[1])
		h.markDirtyKeys(source, destination)
		value, err := h.Db.RPopLPush(source, destination)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "LMOVE":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'LMOVE' command")
		}
		source := string(args[0])
		destination := string(args[1])
		sourceDirection := strings.ToUpper(string(args[2]))
		destinationDirection := strings.ToUpper(string(args[3]))
		h.markDirtyKeys(source, destination)
		value, err := h.Db.LMove(source, destination, sourceDirection, destinationDirection)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "BLMOVE":
		if len(args) < 5 {
			return proto.NewError("ERR wrong number of arguments for 'BLMOVE' command")
		}
		source := string(args[0])
		destination := string(args[1])
		sourceDirection := strings.ToUpper(string(args[2]))
		destinationDirection := strings.ToUpper(string(args[3]))
		timeout, err := strconv.ParseFloat(string(args[4]), 64)
		if err != nil {
			return proto.NewError("ERR timeout is not a float")
		}
		h.markDirtyKeys(source, destination)
		value, err := h.Db.BLMoveBlocking(source, destination, sourceDirection, destinationDirection, timeout)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "LPUSHX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'LPUSHX' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.LPUSHX(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "RPUSHX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RPUSHX' command")
		}
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.RPUSHX(key, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "BLPOP":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BLPOP' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, value, err := h.Db.BLPOPBlocking(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}

	case "BRPOP":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BRPOP' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, value, err := h.Db.BRPOPBlocking(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}

	case "BRPOPLPUSH":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'BRPOPLPUSH' command")
		}
		source, destination := string(args[0]), string(args[1])
		timeout, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		value, err := h.Db.BRPOPLPUSHBlocking(source, destination, timeout)
		if err != nil || value == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	// Hash命令
	case "HSET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HSET' command")
		}
		key := string(args[0])
		h.markDirtyKeys(key)
		count := 0
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			field, value := string(args[i]), args[i+1]
			if err := h.Db.HSet(key, field, string(value)); err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("ERR WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			count++
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "HGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HGET' command")
		}
		key, field := string(args[0]), string(args[1])
		value, err := h.Db.HGet(key, field)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil || value == nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString(value)

	case "HDEL":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HDEL' command")
		}
		key := string(args[0])
		fields := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			fields[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.HDel(key, fields...)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "HLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.HLen(key)
		if err != nil {
			return proto.NewInteger(0)
		}
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	case "HGETALL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HGETALL' command")
		}
		key := string(args[0])
		data, err := h.Db.HGetAll(key)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, 0, len(data)*2)
		for field, value := range data {
			results = append(results, []byte(field), value)
		}
		return &proto.Array{Args: results}

	case "HEXISTS":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HEXISTS' command")
		}
		key, field := string(args[0]), string(args[1])
		exists, err := h.Db.HExists(key, field)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(boolToInt(exists)))

	case "HKEYS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HKEYS' command")
		}
		key := string(args[0])
		keys, err := h.Db.HKeys(key)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(keys))
		for i, k := range keys {
			results[i] = []byte(k)
		}
		return &proto.Array{Args: results}

	case "HVALS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HVALS' command")
		}
		key := string(args[0])
		values, err := h.Db.HVals(key)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(values))
		copy(results, values)
		return &proto.Array{Args: results}

	case "HMSET":
		if len(args) < 3 || len(args)%2 == 0 {
			return proto.NewError("ERR wrong number of arguments for 'HMSET' command")
		}
		key := string(args[0])
		h.markDirtyKeys(key)
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			field, value := string(args[i]), string(args[i+1])
			if err := h.Db.HSet(key, field, value); err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
		}
		return proto.OK

	case "HMGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HMGET' command")
		}
		key := string(args[0])
		fields := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			fields[i-1] = string(args[i])
		}
		values, err := h.Db.HMGet(key, fields...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(values))
		for i, v := range values {
			if v == nil {
				results[i] = nil
			} else {
				results[i] = v
			}
		}
		return &proto.Array{Args: results}

	case "HSETNX":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HSETNX' command")
		}
		key, field, value := string(args[0]), string(args[1]), string(args[2])
		h.markDirtyKeys(key)
		success, err := h.Db.HSetNX(key, field, value)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "HINCRBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HINCRBY' command")
		}
		key, field := string(args[0]), string(args[1])
		increment, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		value, err := h.Db.HIncrBy(key, field, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(value)

	case "HINCRBYFLOAT":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'HINCRBYFLOAT' command")
		}
		key, field := string(args[0]), string(args[1])
		increment, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		h.markDirtyKeys(key)
		value, err := h.Db.HIncrByFloat(key, field, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))

	case "HSTRLEN":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HSTRLEN' command")
		}
		key, field := string(args[0]), string(args[1])
		length, err := h.Db.HStrLen(key, field)
		if err != nil {
			return proto.NewInteger(0)
		}
		// #nosec G115 - length is bounded by practical data size limits
		return proto.NewInteger(int64(length))

	case "HRANDFIELD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HRANDFIELD' command")
		}
		key := string(args[0])
		count := 1
		withValues := false
		// 解析可选参数: HRANDFIELD key [count [WITHVALUES]]
		if len(args) >= 2 {
			// 第二个参数可能是 count 或 WITHVALUES
			secondArg := strings.ToUpper(string(args[1]))
			if secondArg != "WITHVALUES" {
				// 是 count
				c, err := strconv.Atoi(string(args[1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer or out of range")
				}
				count = c
			}
		}
		// 检查是否有 WITHVALUES 选项
		for i := 1; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "WITHVALUES" {
				withValues = true
			}
		}
		fields, values, err := h.Db.HRandField(key, count, withValues)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 构建响应
		if withValues {
			// 返回字段和值的交替数组
			result := make([][]byte, 0, len(fields)*2)
			for i, field := range fields {
				result = append(result, []byte(field))
				result = append(result, []byte(values[i]))
			}
			return &proto.Array{Args: result}
		}
		// 只返回字段
		result := make([][]byte, len(fields))
		for i, field := range fields {
			result[i] = []byte(field)
		}
		return &proto.Array{Args: result}

	// Set命令
	case "SADD":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SADD' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.SAdd(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SREM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SREM' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.SRem(key, members...)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SCARD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SCARD' command")
		}
		key := string(args[0])
		count, err := h.Db.SCard(key)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil {
			return proto.NewInteger(0)
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SISMEMBER":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SISMEMBER' command")
		}
		key, member := string(args[0]), string(args[1])
		exists, err := h.Db.SIsMember(key, member)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(int64(boolToInt(exists)))

	case "SMEMBERS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SMEMBERS' command")
		}
		key := string(args[0])
		members, err := h.Db.SMembers(key)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SPOP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SPOP' command")
		}
		key := string(args[0])
		member, err := h.Db.SPop(key)
		if err != nil || member == "" {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(member))

	case "SRANDMEMBER":
		key := string(args[0])
		if len(args) == 1 {
			// SRANDMEMBER key - return single member
			member, err := h.Db.SRandMember(key)
			if err != nil || member == "" {
				return proto.NewBulkString(nil)
			}
			return proto.NewBulkString([]byte(member))
		}
		// SRANDMEMBER key count - return array of members
		count, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		members, err := h.Db.SRandMemberN(key, count)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SMOVE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SMOVE' command")
		}
		source, destination, member := string(args[0]), string(args[1]), string(args[2])
		success, err := h.Db.SMove(source, destination, member)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "SINTER":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SINTER' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		members, err := h.Db.SInter(keys...)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SUNION":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SUNION' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		members, err := h.Db.SUnion(keys...)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SDIFF":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SDIFF' command")
		}
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = string(arg)
		}
		members, err := h.Db.SDiff(keys...)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}

	case "SINTERSTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SINTERSTORE' command")
		}
		destination := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		h.markDirtyKeys(destination)
		count, err := h.Db.SInterStore(destination, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SMISMEMBER":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SMISMEMBER' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		results, err := h.Db.SMIsMember(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 转换为响应数组
		resp := make([][]byte, len(results))
		for i, v := range results {
			resp[i] = []byte(strconv.FormatInt(v, 10))
		}
		return &proto.Array{Args: resp}

	case "SINTERCARD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SINTERCARD' command")
		}
		sinterKeys := make([]string, len(args))
		for i, arg := range args {
			sinterKeys[i] = string(arg)
		}
		count, err := h.Db.SInterCard(sinterKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "SUNIONSTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SUNIONSTORE' command")
		}
		destination := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		h.markDirtyKeys(destination)
		count, err := h.Db.SUnionStore(destination, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SDIFFSTORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SDIFFSTORE' command")
		}
		destination := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		h.markDirtyKeys(destination)
		count, err := h.Db.SDiffStore(destination, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SSCAN":
		// SSCAN key cursor [MATCH pattern] [COUNT count]
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SSCAN' command")
		}
		key := string(args[0])
		cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		pattern := ""
		count := 10
		// Parse optional MATCH and COUNT
		if len(args) > 2 {
			for i := 2; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "MATCH" && i+1 < len(args) {
					pattern = string(args[i+1])
					i++
				} else if opt == "COUNT" && i+1 < len(args) {
					count, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i++
				}
			}
		}
		result, err := h.Db.SScan(key, cursor, pattern, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回格式: [cursor, [member1, member2, ...]]
		response := make([][]byte, 0, 2+len(result.Members))
		response = append(response, []byte(strconv.FormatUint(result.Cursor, 10)))
		for _, m := range result.Members {
			response = append(response, []byte(m))
		}
		return &proto.Array{Args: response}

	// SortedSet命令 - 由于代码太长，这里只实现主要命令
	case "ZADD":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZADD' command")
		}
		key := string(args[0])
		members := make([]store.ZSetMember, 0)
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			score, err := strconv.ParseFloat(string(args[i]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			member := string(args[i+1])
			members = append(members, store.ZSetMember{Member: member, Score: score})
		}
		h.markDirtyKeys(key)
		if err := h.Db.ZAdd(key, members); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(len(members)))

	case "ZREM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZREM' command")
		}
		key := string(args[0])
		h.markDirtyKeys(key)
		count := 0
		for i := 1; i < len(args); i++ {
			member := string(args[i])
			err := h.Db.ZRem(key, member)
			if err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				if errors.Is(err, store.ErrMemberNotFound) {
					continue
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			count++
		}
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "ZCARD":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ZCARD' command")
		}
		key := string(args[0])
		count, err := h.Db.ZCard(key)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewInteger(0)
		}
		return proto.NewInteger(count)

	case "ZSCORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZSCORE' command")
		}
		key, member := string(args[0]), string(args[1])
		score, exists, err := h.Db.ZScore(key, member)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil || !exists {
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", score)))

	case "ZMSCORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZMSCORE' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		scores, err := h.Db.ZMScore(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回数组，每个元素是分数或 nil
		result := make([][]byte, len(scores))
		for i, score := range scores {
			result[i] = []byte(fmt.Sprintf("%.10g", score))
		}
		return &proto.Array{Args: result}

	case "ZRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		// 检查是否有 WITHSCORES 选项
		withScores := false
		if len(args) >= 4 && strings.ToUpper(string(args[3])) == "WITHSCORES" {
			withScores = true
		}
		members, err := h.Db.ZRange(key, start, stop)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, 0)
		if withScores {
			// 有 WITHSCORES：返回 member 和 score 的交替数组
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
			// 没有 WITHSCORES：只返回 member 列表
			for _, m := range members {
				results = append(results, []byte(m.Member))
			}
		}
		return &proto.Array{Args: results}

	case "ZREVRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGE' command")
		}
		key := string(args[0])
		start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		// 检查是否有 WITHSCORES 选项
		withScores := false
		if len(args) >= 4 && strings.ToUpper(string(args[3])) == "WITHSCORES" {
			withScores = true
		}
		members, err := h.Db.ZRevRange(key, start, stop)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}
		results := make([][]byte, 0)
		if withScores {
			// 有 WITHSCORES：返回 member 和 score 的交替数组
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
			// 没有 WITHSCORES：只返回 member 列表
			for _, m := range members {
				results = append(results, []byte(m.Member))
			}
		}
		return &proto.Array{Args: results}

	case "ZRANGEBYSCORE":
		// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGEBYSCORE' command")
		}
		key := string(args[0])
		minStr := string(args[1])
		maxStr := string(args[2])

		// 解析分数范围（包含排除标志）
		minScore, minExclusive, err := parseScoreExclusive(minStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}
		maxScore, maxExclusive, err := parseScoreExclusive(maxStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}

		// 解析可选参数
		offset := 0
		count := -1 // -1 表示返回所有
		withScores := false

		i := 3
		for i < len(args) {
			arg := strings.ToUpper(string(args[i]))
			i++
			if arg == "WITHSCORES" {
				withScores = true
			} else if arg == "LIMIT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				offset, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT offset is not an integer")
				}
				count, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT count is not an integer")
				}
			}
		}

		members, err := h.Db.ZRangeByScore(key, minScore, maxScore, offset, count, minExclusive, maxExclusive)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}

		results := make([][]byte, 0)
		if withScores {
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
			for _, m := range members {
				results = append(results, []byte(m.Member))
			}
		}
		return &proto.Array{Args: results}

	case "ZREVRANGEBYSCORE":
		// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGEBYSCORE' command")
		}
		key := string(args[0])
		maxStr := string(args[1])
		minStr := string(args[2])

		// 解析分数范围（包含排除标志）
		maxScore, maxExclusive, err := parseScoreExclusive(maxStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}
		minScore, minExclusive, err := parseScoreExclusive(minStr)
		if err != nil {
			return proto.NewError("ERR min or max is not a float")
		}

		// 解析可选参数
		offset := 0
		count := -1 // -1 表示返回所有
		withScores := false

		i := 3
		for i < len(args) {
			arg := strings.ToUpper(string(args[i]))
			i++
			if arg == "WITHSCORES" {
				withScores = true
			} else if arg == "LIMIT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				offset, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT offset is not an integer")
				}
				count, err = strconv.Atoi(string(args[i]))
				i++
				if err != nil {
					return proto.NewError("ERR LIMIT count is not an integer")
				}
			}
		}

		members, err := h.Db.ZRevRangeByScore(key, maxScore, minScore, offset, count, minExclusive, maxExclusive)
		if err != nil {
			return &proto.Array{Args: [][]byte{}}
		}

		results := make([][]byte, 0)
		if withScores {
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
			}
		} else {
			for _, m := range members {
				results = append(results, []byte(m.Member))
			}
		}
		return &proto.Array{Args: results}

	case "ZRANK":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANK' command")
		}
		key, member := string(args[0]), string(args[1])
		rank, err := h.Db.ZRank(key, member)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewInteger(rank)

	case "ZREVRANK":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANK' command")
		}
		key, member := string(args[0]), string(args[1])
		rank, err := h.Db.ZRevRank(key, member)
		if err != nil {
			return proto.NewBulkString(nil)
		}
		return proto.NewInteger(rank)

	case "ZCOUNT":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZCOUNT' command")
		}
		key := string(args[0])
		min, err1 := strconv.ParseFloat(string(args[1]), 64)
		max, err2 := strconv.ParseFloat(string(args[2]), 64)
		if err1 != nil || err2 != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		count, err := h.Db.ZCount(key, min, max)
		if err != nil {
			return proto.NewInteger(0)
		}
		return proto.NewInteger(count)

	case "ZINCRBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZINCRBY' command")
		}
		key, member := string(args[0]), string(args[2])
		increment, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		h.markDirtyKeys(key)
		score, err := h.Db.ZIncrBy(key, member, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", score)))

	case "ZREMRANGEBYRANK":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYRANK' command")
		}
		key := string(args[0])
		start, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		stop, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(key)
		count, err := h.Db.ZRemRangeByRank(key, start, stop)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZREMRANGEBYSCORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYSCORE' command")
		}
		key := string(args[0])
		min, minExclusive, err := parseScoreExclusive(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		max, maxExclusive, err := parseScoreExclusive(string(args[2]))
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		h.markDirtyKeys(key)
		count, err := h.Db.ZRemRangeByScore(key, min, max, minExclusive, maxExclusive)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZPOPMAX":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ZPOPMAX' command")
		}
		key := string(args[0])
		count := 1
		if len(args) >= 2 {
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
		}
		h.markDirtyKeys(key)
		members, err := h.Db.ZPopMax(key, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回 member 和 score 的交替数组
		result := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			result = append(result, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
		}
		return &proto.Array{Args: result}

	case "ZPOPMIN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ZPOPMIN' command")
		}
		key := string(args[0])
		count := 1
		if len(args) >= 2 {
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
		}
		h.markDirtyKeys(key)
		members, err := h.Db.ZPopMin(key, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回 member 和 score 的交替数组
		result := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			result = append(result, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
		}
		return &proto.Array{Args: result}

	case "BZPOPMAX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BZPOPMAX' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, member, err := h.Db.BZPopMax(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(member.Member), []byte(fmt.Sprintf("%.10g", member.Score))}}

	case "BZPOPMIN":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'BZPOPMIN' command")
		}
		keys := make([]string, len(args)-1)
		for i := 0; i < len(args)-1; i++ {
			keys[i] = string(args[i])
		}
		timeout, err := strconv.Atoi(string(args[len(args)-1]))
		if err != nil {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		key, member, err := h.Db.BZPopMin(keys, timeout)
		if err != nil || key == "" {
			return &proto.Array{Args: [][]byte{}}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(member.Member), []byte(fmt.Sprintf("%.10g", member.Score))}}

	case "ZUNIONSTORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZUNIONSTORE' command")
		}
		destination := string(args[0])
		// 解析参数: ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
		numKeys, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[2+i])
		}
		weights := []float64{}
		aggregate := "SUM"
		// 解析可选参数
		i := 2 + numKeys
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "WEIGHTS":
				if i+numKeys >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				weights = make([]float64, numKeys)
				for j := 0; j < numKeys; j++ {
					w, err := strconv.ParseFloat(string(args[i+1+j]), 64)
					if err != nil {
						return proto.NewError("ERR weight is not a float")
					}
					weights[j] = w
				}
				i += 1 + numKeys
			case "AGGREGATE":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				aggregate = strings.ToUpper(string(args[i+1]))
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					return proto.NewError("ERR syntax error")
				}
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}
		h.markDirtyKeys(destination)
		count, err := h.Db.ZUnionStore(destination, keys, weights, aggregate)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZINTERSTORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZINTERSTORE' command")
		}
		destination := string(args[0])
		// 解析参数
		numKeys, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[2+i])
		}
		weights := []float64{}
		aggregate := "SUM"
		// 解析可选参数
		i := 2 + numKeys
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "WEIGHTS":
				if i+numKeys >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				weights = make([]float64, numKeys)
				for j := 0; j < numKeys; j++ {
					w, err := strconv.ParseFloat(string(args[i+1+j]), 64)
					if err != nil {
						return proto.NewError("ERR weight is not a float")
					}
					weights[j] = w
				}
				i += 1 + numKeys
			case "AGGREGATE":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				aggregate = strings.ToUpper(string(args[i+1]))
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					return proto.NewError("ERR syntax error")
				}
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}
		h.markDirtyKeys(destination)
		count, err := h.Db.ZInterStore(destination, keys, weights, aggregate)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZDIFFSTORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZDIFFSTORE' command")
		}
		destination := string(args[0])
		numKeys, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		if numKeys < 1 {
			return proto.NewError("ERR syntax error")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[2+i])
		}
		h.markDirtyKeys(destination)
		count, err := h.Db.ZDiffStore(destination, keys)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZLEXCOUNT":
		// ZLEXCOUNT key min max
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZLEXCOUNT' command")
		}
		zSetName := string(args[0])
		min := string(args[1])
		max := string(args[2])
		count, err := h.Db.ZLexCount(zSetName, min, max)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "ZRANGEBYLEX":
		// ZRANGEBYLEX key min max [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGEBYLEX' command")
		}
		zSetName := string(args[0])
		min := string(args[1])
		max := string(args[2])
		offset := 0
		count := -1
		var err error
		// Parse optional LIMIT
		if len(args) > 3 {
			for i := 3; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "LIMIT" && i+2 < len(args) {
					offset, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					count, err = strconv.Atoi(string(args[i+2]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i += 2
				}
			}
		}
		members, err := h.Db.ZRangeByLex(zSetName, min, max, offset, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		result := make([][]byte, len(members))
		for i, m := range members {
			result[i] = []byte(m)
		}
		return &proto.Array{Args: result}

	case "ZREVRANGEBYLEX":
		// ZREVRANGEBYLEX key max min [LIMIT offset count]
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGEBYLEX' command")
		}
		zSetName := string(args[0])
		max := string(args[1])
		min := string(args[2])
		offset := 0
		count := -1
		var err error
		// Parse optional LIMIT
		if len(args) > 3 {
			for i := 3; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "LIMIT" && i+2 < len(args) {
					offset, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					count, err = strconv.Atoi(string(args[i+2]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i += 2
				}
			}
		}
		members, err := h.Db.ZRevRangeByLex(zSetName, max, min, offset, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		result := make([][]byte, len(members))
		for i, m := range members {
			result[i] = []byte(m)
		}
		return &proto.Array{Args: result}

	case "ZREMRANGEBYLEX":
		// ZREMRANGEBYLEX key min max
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYLEX' command")
		}
		zSetName := string(args[0])
		min := string(args[1])
		max := string(args[2])
		h.markDirtyKeys(zSetName)
		removed, err := h.Db.ZRemRangeByLex(zSetName, min, max)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(removed)

	case "ZSCAN":
		// ZSCAN key cursor [MATCH pattern] [COUNT count]
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZSCAN' command")
		}
		zSetName := string(args[0])
		cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		pattern := ""
		count := 10
		// Parse optional MATCH and COUNT
		if len(args) > 2 {
			for i := 2; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "MATCH" && i+1 < len(args) {
					pattern = string(args[i+1])
					i++
				} else if opt == "COUNT" && i+1 < len(args) {
					count, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i++
				}
			}
		}
		result, err := h.Db.ZScan(zSetName, cursor, pattern, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 返回格式: [cursor, [member1, score1, member2, score2, ...]]
		membersArray := make([][]byte, len(result.Members)*2)
		for i, m := range result.Members {
			membersArray[i*2] = []byte(m.Member)
			membersArray[i*2+1] = []byte(fmt.Sprintf("%.10g", m.Score))
		}
		return &proto.NestedArray{
			Elems: []proto.RESP{
				proto.Integer(result.Cursor),
				&proto.Array{Args: membersArray},
			},
		}

	// ASKING 命令（用于集群槽迁移）
	case "ASKING":
		// ASKING 命令标记当前连接已发送 ASKING，允许执行针对导入中槽的命令
		h.clusterAsking = true
		logger.Logger.Debug().Msg("收到 ASKING 命令")
		return proto.OK

	// Cluster命令
	case "CLUSTER":
		if h.Cluster == nil {
			return proto.NewError("ERR This instance has cluster support disabled")
		}
		if len(args) == 0 {
			return proto.NewError("ERR wrong number of arguments for 'CLUSTER' command")
		}
		clusterCmd := cluster.NewClusterCommands(h.Cluster)
		subcommandArgs := make([]string, len(args))
		for i, arg := range args {
			subcommandArgs[i] = string(arg)
		}
		result, err := clusterCmd.HandleCommand(subcommandArgs)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// 根据返回类型转换
		switch v := result.(type) {
		case string:
			// 使用 BulkString 以正确处理多行响应（如 CLUSTER INFO）
			return proto.NewBulkString([]byte(v))
		case int64:
			return proto.NewInteger(v)
		case []string:
			// 对于CLUSTER NODES，返回多行字符串
			return proto.NewBulkString([]byte(strings.Join(v, "\n")))
		case []interface{}:
			// 对于CLUSTER SLOTS，返回数组
			// 简化处理：转换为字符串数组
			strs := make([][]byte, len(v))
			for i, item := range v {
				strs[i] = []byte(fmt.Sprintf("%v", item))
			}
			return &proto.Array{Args: strs}
		case [][]interface{}:
			// 对于CLUSTER SLOTS，槽位信息
			// 格式：[[startSlot, endSlot, ip, port, nodeId], ...]
			slotsResp := make([]proto.RESP, len(v))
			for i, slotEntry := range v {
				entry := make([][]byte, len(slotEntry))
				for j, item := range slotEntry {
					entry[j] = []byte(fmt.Sprintf("%v", item))
				}
				slotsResp[i] = &proto.Array{Args: entry}
			}
			return &proto.NestedArray{Elems: slotsResp}
		default:
			return proto.NewSimpleString(fmt.Sprintf("%v", v))
		}

	// CONFIG 命令（用于 redis-benchmark 兼容性）
	case "CONFIG":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'CONFIG' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "GET":
			// CONFIG GET 返回键值对数组
			// 格式: [key1, value1, key2, value2, ...]
			// 返回一些基本配置以兼容 redis-benchmark
			if len(args) == 1 || (len(args) >= 2 && string(args[1]) == "*") {
				// CONFIG GET 或 CONFIG GET * - 返回所有配置
				configs := []string{
					"save", "",
					"appendonly", "no",
					"maxmemory", "0",
					"maxmemory-policy", "noeviction",
				}
				results := make([][]byte, len(configs))
				for i, cfg := range configs {
					results[i] = []byte(cfg)
				}
				return &proto.Array{Args: results}
			} else if len(args) >= 2 {
				// CONFIG GET key - 返回特定配置
				key := string(args[1])
				var value string
				switch strings.ToLower(key) {
				case "save":
					value = ""
				case "appendonly":
					value = "no"
				case "maxmemory":
					value = "0"
				case "maxmemory-policy":
					value = "noeviction"
				default:
					value = ""
				}
				return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}
			} else {
				return proto.NewError("ERR wrong number of arguments for 'CONFIG GET' command")
			}
		case "SET":
			// CONFIG SET <key> <value>
			if len(args) < 3 {
				return proto.NewError("ERR wrong number of arguments for 'CONFIG SET' command")
			}
			// 简化实现：仅验证参数存在，返回 OK
			return proto.OK
		case "REWRITE":
			// CONFIG REWRITE - 简化实现，将配置重写到配置文件
			// 由于 BoltDB 使用动态配置，不写入文件
			return proto.OK
		default:
			return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
		}

	// 复制命令
	case "REPLICAOF", "SLAVEOF":
		if h.Replication == nil {
			return proto.NewError("ERR replication not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'REPLICAOF' command")
		}
		host := string(args[0])
		port := string(args[1])
		if host == "NO" && port == "ONE" {
			// 停止复制
			replication.StopSlaveReplication(h.Replication)
			return proto.OK
		}
		// 启动复制
		masterAddr := fmt.Sprintf("%s:%s", host, port)
		if err := replication.StartSlaveReplication(h.Replication, h.Db, masterAddr); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	// 注意：PSYNC 命令由 processRequest 中的 handlePSyncWithRDB 特殊处理
	// 这里不需要处理，master 节点会在收到 PSYNC 时直接发送 RDB 数据

	case "REPLCONF":
		if h.Replication == nil {
			return proto.NewError("ERR replication not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'REPLCONF' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "LISTENING-PORT":
			// REPLCONF listening-port <port>
			// 记录从节点的监听端口，兼容 redis-sentinel
			if len(args) >= 2 {
				port := string(args[1])
				logger.Logger.Debug().Str("remote_addr", remoteAddr).Str("port", port).Msg("从节点监听端口")
			}
			return proto.OK
		case "CAPA":
			// REPLCONF capa <capability>
			// 记录从节点的能力，兼容 redis-sentinel
			if len(args) >= 2 {
				capa := string(args[1])
				logger.Logger.Debug().Str("remote_addr", remoteAddr).Str("capability", capa).Msg("从节点能力")
			}
			return proto.OK
		case "ACK":
			// REPLCONF ACK <offset>
			// 从节点确认已复制的偏移量，redis-sentinel 依赖此功能
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'REPLCONF ACK' command")
			}
			offset, err := strconv.ParseInt(string(args[1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid offset")
			}
			// 使用 remoteAddr 找到对应的从节点并更新 ACK 偏移量
			if h.Replication.IsMaster() {
				slave := h.Replication.GetSlaveByAddr(remoteAddr)
				if slave != nil {
					slave.UpdateReplAck(offset)
					logger.Logger.Debug().
						Str("slave_id", slave.ID).
						Str("remote_addr", remoteAddr).
						Int64("ack_offset", offset).
						Msg("更新从节点ACK偏移量")
				}
			}
			return proto.OK
		case "GETACK":
			// REPLCONF GETACK *
			// 返回当前复制偏移量，兼容 redis-sentinel
			offset := h.Replication.GetMasterReplOffset()
			return &proto.Array{Args: [][]byte{
				[]byte("REPLCONF"),
				[]byte("ACK"),
				[]byte(strconv.FormatInt(offset, 10)),
			}}
		case "SYNC":
			// REPLCONF SYNC (用于 PSYNC2)
			return proto.OK
		case "NOREPLY":
			// REPLCONF NOREPLY <yes|no>
			return proto.OK
		default:
			return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
		}

	// INFO命令
	case "INFO":
		section := ""
		if len(args) >= 1 {
			section = strings.ToUpper(string(args[0]))
		}
		info := h.buildInfoResponse(section)
		return proto.NewBulkString([]byte(info))

	// 备份命令
	case "SAVE":
		if h.Backup == nil {
			return proto.NewError("ERR backup not enabled")
		}
		if err := h.Backup.Save(); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "BGSAVE":
		if h.Backup == nil {
			return proto.NewError("ERR backup not enabled")
		}
		if err := h.Backup.BGSave(); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewSimpleString("Background saving started")

	case "LASTSAVE":
		if h.Backup == nil {
			return proto.NewError("ERR backup not enabled")
		}
		lastSave := h.Backup.LastSave()
		return proto.NewInteger(lastSave)

	case "DBSIZE":
		keys, err := h.Db.Keys("*")
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(len(keys)))

	case "TIME":
		sec, usec, err := h.Db.Time()
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return &proto.Array{Args: [][]byte{
			[]byte(fmt.Sprintf("%d", sec)),
			[]byte(fmt.Sprintf("%d", usec)),
		}}

	case "FLUSHDB":
		err := h.Db.FlushDB()
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "FLUSHALL":
		err := h.Db.FlushDB()
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "SELECT":
		// BoltDB is a single-database implementation
		// Always return OK regardless of the database number
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SELECT' command")
		}
		return proto.OK

	case "MOVE":
		// BoltDB is a single-database implementation
		// MOVE always returns 0 (key was not moved)
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'MOVE' command")
		}
		// #nosec G115 - result is always 0 for single-db implementation
		return proto.NewInteger(0)

	case "WAIT":
		// BoltDB does not support replication yet
		// Return 0 (number of replicas acknowledged)
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'WAIT' command")
		}
		// #nosec G115 - result is always 0 for non-replicated implementation
		return proto.NewInteger(0)

	case "SLOWLOG":
		// BoltDB does not implement slow query logging yet
		// Return empty list for all subcommands
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SLOWLOG' command")
		}
		subCommand := strings.ToUpper(string(args[0]))
		switch subCommand {
		case "GET":
			// Return empty array for GET
			return &proto.Array{Args: [][]byte{}}
		case "LEN":
			// Return 0 (no slowlog entries)
			return proto.NewInteger(0)
		case "RESET":
			// Return OK for RESET
			return proto.OK
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("SLOWLOG GET <count> - returns top <count> entries from the slowlog"),
				[]byte("SLOWLOG LEN - returns the length of the slowlog"),
				[]byte("SLOWLOG RESET - clears the slowlog"),
				[]byte("SLOWLOG HELP - shows this help message"),
			}}
		default:
			return proto.NewError("ERR unknown subcommand for 'SLOWLOG'")
		}

	case "MEMORY":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'MEMORY' command")
		}
		subCommand := strings.ToUpper(string(args[0]))
		switch subCommand {
		case "USAGE":
			// MEMORY USAGE key [SAMPLES count]
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'MEMORY USAGE' command")
			}
			key := string(args[1])
			// Estimate memory usage - use key type size approximation
			size, err := h.Db.MemoryUsage(key)
			if err != nil {
				if errors.Is(err, store.ErrKeyNotFound) {
					return proto.NewBulkString(nil)
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(size)
		case "DOCTOR":
			// Return basic memory info
			return &proto.Array{Args: [][]byte{
				[]byte("BoltDB uses BadgerDB for storage"),
				[]byte("Memory usage is managed by the underlying BadgerDB engine"),
			}}
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("MEMORY USAGE key [SAMPLES count] - estimate memory usage of key"),
				[]byte("MEMORY DOCTOR - reports memory usage details"),
				[]byte("MEMORY HELP - shows this help message"),
			}}
		default:
			return proto.NewError("ERR unknown subcommand for 'MEMORY'")
		}

	// ==================== MODULE ====================
	case "MODULE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'MODULE' command")
		}
		subCommand := strings.ToUpper(string(args[0]))
		switch subCommand {
		case "LIST":
			// Return empty array - no modules loaded
			return &proto.Array{Args: [][]byte{}}
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("MODULE LIST - list loaded modules"),
				[]byte("MODULE HELP - shows this help message"),
			}}
		default:
			return proto.NewError("ERR unknown subcommand for 'MODULE'")
		}

	// ==================== LOLWUT ====================
	case "LOLWUT":
		// LOLWUT [VERSION version] - Redis version sanity check
		version := "redis.bolt.8.0"
		if len(args) > 0 && strings.ToUpper(string(args[0])) == "VERSION" && len(args) > 1 {
			version = string(args[1])
		}
		// Return a simple artistic pattern
		result := fmt.Sprintf("BoltDB %s - A disk-persistent Redis-compatible database", version)
		return proto.NewBulkString([]byte(result))

	// ==================== LATENCY ====================
	case "LATENCY":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'LATENCY' command")
		}
		subCmd := strings.ToUpper(string(args[0]))
		switch subCmd {
		case "LATEST":
			// LATENCY LATEST - return latest latency samples
			return &proto.Array{Args: [][]byte{}}
		case "RESET":
			// LATENCY RESET [EVENT ...] - reset latency data
			return proto.NewInteger(0)
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("LATENCY LATEST - returns the latest latency samples"),
				[]byte("LATENCY RESET [EVENT ...] - reset latency data for events"),
				[]byte("LATENCY DOCTOR - analyzes latency issues"),
				[]byte("LATENCY HELP - shows this help message"),
			}}
		case "DOCTOR":
			// Return a diagnostic message
			return &proto.Array{Args: [][]byte{
				[]byte("Latency doctor report:"),
				[]byte("- No latency issues detected"),
				[]byte("- BoltDB uses BadgerDB for disk-based storage"),
				[]byte("- Expected latency < 5ms for SSD, < 50ms for HDD"),
			}}
		default:
			return proto.NewError("ERR unknown subcommand for 'LATENCY'")
		}

	// ==================== READONLY ====================
	case "READONLY":
		// READONLY - enter read-only mode (for replicas in read-write splitting scenarios)
		// This is primarily used in Redis Cluster for replica nodes
		return proto.NewSimpleString("OK")

	// ==================== READWRITE ====================
	case "READWRITE":
		// READWRITE - exit read-only mode
		return proto.NewSimpleString("OK")

	// ==================== ZRANGESTORE ====================
	case "ZRANGESTORE":
		// ZRANGESTORE dstkey srckey min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGESTORE' command")
		}
		dstKey := string(args[0])
		srcKey := string(args[1])
		min := string(args[2])
		max := string(args[3])

		// Parse options
		byScore := false
		byLex := false
		rev := false
		var limitOffset, limitCount int64 = 0, -1

		i := 4
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "BYSCORE":
				byScore = true
				i++
			case "BYLEX":
				byLex = true
				i++
			case "REV":
				rev = true
				i++
			case "LIMIT":
				if i+2 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				offset, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR invalid LIMIT offset")
				}
				count, err := strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return proto.NewError("ERR invalid LIMIT count")
				}
				limitOffset = offset
				limitCount = count
				i += 3
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unexpected option: %s", opt))
			}
		}

		// Parse min/max as float64 only if BYSCORE
		var minScore, maxScore float64
		var err error
		if byScore {
			minScore, err = strconv.ParseFloat(min, 64)
			if err != nil {
				return proto.NewError("ERR min value is not a float")
			}
			maxScore, err = strconv.ParseFloat(max, 64)
			if err != nil {
				return proto.NewError("ERR max value is not a float")
			}
		}

		// Determine the range operation to use
		var members []store.ZSetMember

		if byLex {
			lexMembers, lexErr := h.Db.ZRangeByLex(srcKey, min, max, int(limitOffset), int(limitCount))
			if lexErr != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", lexErr))
			}
			// Convert []string to []ZSetMember (score=0 for all)
			members = make([]store.ZSetMember, len(lexMembers))
			for i, m := range lexMembers {
				members[i] = store.ZSetMember{Member: m, Score: 0}
			}
		} else if byScore {
			members, err = h.Db.ZRangeByScore(srcKey, minScore, maxScore, int(limitOffset), int(limitCount), false, false)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
		} else {
			// Default: treat min/max as ranks (integers)
			start, err := strconv.ParseInt(min, 10, 64)
			if err != nil {
				return proto.NewError("ERR min value is not an integer")
			}
			stop, err := strconv.ParseInt(max, 10, 64)
			if err != nil {
				return proto.NewError("ERR max value is not an integer")
			}
			// With REV, swap start and stop (range becomes [stop, start])
			if rev {
				start, stop = stop, start
			}
			ptrMembers, rangeErr := h.Db.ZRange(srcKey, start, stop)
			if rangeErr != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", rangeErr))
			}
			// Apply LIMIT for rank-based range
			if limitCount >= 0 && int64(len(ptrMembers)) > limitOffset {
				if limitCount == 0 || limitOffset+int64(limitCount) > int64(len(ptrMembers)) {
					ptrMembers = ptrMembers[limitOffset:]
				} else {
					ptrMembers = ptrMembers[limitOffset : limitOffset+int64(limitCount)]
				}
			}
			// Convert []*ZSetMember to []ZSetMember
			members = make([]store.ZSetMember, len(ptrMembers))
			for i, m := range ptrMembers {
				members[i] = store.ZSetMember{Member: m.Member, Score: m.Score}
			}
		}

		// Apply REV for BYSCORE and BYLEX (reverse the result)
		// Note: For rank-based ranges, REV is handled by swapping start/stop above
		if rev && (byScore || byLex) {
			for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
				members[i], members[j] = members[j], members[i]
			}
		}

		// Delete destination if it exists
		_, _ = h.Db.Del(dstKey)

		// Add members to destination
		if len(members) > 0 {
			err = h.Db.ZAdd(dstKey, members)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
		}

		// ZRANGESTORE always returns the count of elements stored
		return proto.NewInteger(int64(len(members)))

	// Pub/Sub命令
	case "PUBLISH":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PUBLISH' command")
		}
		channel := string(args[0])
		message := args[1]
		count := h.PubSub.Publish(channel, message)
		// #nosec G115 - count is bounded by practical data size limits
		return proto.NewInteger(int64(count))

	case "SUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SUBSCRIBE' command")
		}
		// 简化实现：返回订阅确认
		channels := make([]string, len(args))
		for i, arg := range args {
			channels[i] = string(arg)
		}
		// 实际应该创建订阅者并持续发送消息
		// 这里简化处理
		return &proto.Array{Args: [][]byte{
			[]byte("subscribe"),
			[]byte(channels[0]),
			[]byte("1"),
		}}

	case "PSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PSUBSCRIBE' command")
		}
		// 简化实现
		patterns := make([]string, len(args))
		for i, arg := range args {
			patterns[i] = string(arg)
		}
		return &proto.Array{Args: [][]byte{
			[]byte("psubscribe"),
			[]byte(patterns[0]),
			[]byte("1"),
		}}

	case "UNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		// 返回取消订阅确认
		channel := ""
		if len(args) >= 1 {
			channel = string(args[0])
		}
		return &proto.Array{Args: [][]byte{
			[]byte("unsubscribe"),
			[]byte(channel),
			[]byte("0"),
		}}

	case "PUNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		// 返回取消模式订阅确认
		pattern := ""
		if len(args) >= 1 {
			pattern = string(args[0])
		}
		return &proto.Array{Args: [][]byte{
			[]byte("punsubscribe"),
			[]byte(pattern),
			[]byte("0"),
		}}

	case "PUBSUB":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PUBSUB' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "CHANNELS":
			pattern := "*"
			if len(args) >= 2 {
				pattern = string(args[1])
			}
			channels := h.PubSub.GetChannels(pattern)
			results := make([][]byte, len(channels))
			for i, ch := range channels {
				results[i] = []byte(ch)
			}
			return &proto.Array{Args: results}
		case "NUMSUB":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'PUBSUB NUMSUB' command")
			}
			results := make([][]byte, 0)
			for i := 1; i < len(args); i++ {
				channel := string(args[i])
				count := h.PubSub.GetSubscriberCount(channel)
				results = append(results, []byte(channel), []byte(strconv.FormatInt(int64(count), 10)))
			}
			return &proto.Array{Args: results}
		case "NUMPAT":
			count := h.PubSub.GetPatternCount()
			return proto.NewInteger(int64(count))
		case "HELP":
			return &proto.Array{Args: [][]byte{
				[]byte("PUBSUB CHANNELS [pattern]  -- Return the list of active channels matching a pattern."),
				[]byte("PUBSUB NUMSUB [channel ...] -- Return the number of subscribers for the specified channels."),
				[]byte("PUBSUB NUMPAT              -- Return the number of subscriptions to patterns."),
				[]byte("PUBSUB HELP                -- Show helpful text about this subcommand."),
			}}
		default:
			return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
		}

	// Transaction commands - 事务命令
	case "MULTI":
		// 开始事务
		if h.transaction != nil && h.transaction.InTransaction {
			return proto.NewError("ERR MULTI calls can not be nested")
		}
		if h.transaction == nil {
			h.transaction = &TransactionState{
				Commands:   make([]TransactionCommand, 0),
				WatchKeys:  make(map[string]struct{}),
				DirtyKeys:  make(map[string]struct{}),
			}
		}
		h.transaction.InTransaction = true
		h.transaction.Commands = make([]TransactionCommand, 0) // 清除排队的命令
		// 注意：不重置 WatchKeys 和 IsWatching，允许 WATCH 在 MULTI 之前
		return proto.NewSimpleString("OK")

	case "EXEC":
		if h.transaction == nil || !h.transaction.InTransaction {
			return proto.NewError("ERR EXEC without MULTI")
		}
		// 检查 dirty keys（如果监视的键被修改，事务失败）
		if h.transaction.IsWatching && len(h.transaction.WatchKeys) > 0 {
			for watchKey := range h.transaction.WatchKeys {
				if _, dirty := h.transaction.DirtyKeys[watchKey]; dirty {
					// 键被修改，事务失败
					h.resetTransaction()
					return nil // 返回 nil 表示 WATCH 失败
				}
			}
		}
		// 执行所有排队的命令
		results := make([]proto.RESP, len(h.transaction.Commands))
		for i, tc := range h.transaction.Commands {
			results[i] = h.executeQueuedCommand(tc.Command, tc.Args)
		}
		h.resetTransaction()
		flatArgs := make([][]byte, 0)
		for _, r := range results {
			flatArgs = append(flatArgs, []byte(r.String()))
		}
		return &proto.Array{Args: flatArgs}

	case "DISCARD":
		if h.transaction == nil || !h.transaction.InTransaction {
			return proto.NewError("ERR DISCARD without MULTI")
		}
		h.resetTransaction()
		return proto.NewSimpleString("OK")

	case "WATCH":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'WATCH' command")
		}
		if h.transaction != nil && h.transaction.InTransaction && len(h.transaction.Commands) > 0 {
			return proto.NewError("ERR WATCH inside MULTI is not allowed")
		}
		// 复用现有事务状态或创建新的
		if h.transaction == nil {
			h.transaction = &TransactionState{
				Commands:   make([]TransactionCommand, 0),
				WatchKeys:  make(map[string]struct{}),
				IsWatching: true,
				DirtyKeys:  make(map[string]struct{}),
			}
		} else {
			h.transaction.IsWatching = true
			h.transaction.WatchKeys = make(map[string]struct{})
		}
		for _, arg := range args {
			key := string(arg)
			h.transaction.WatchKeys[key] = struct{}{}
		}
		return proto.NewInteger(int64(len(args)))

	case "UNWATCH":
		if h.transaction != nil {
			h.transaction.IsWatching = false
			h.transaction.WatchKeys = make(map[string]struct{})
		}
		h.resetTransaction()
		return proto.NewSimpleString("OK")

	// ==================== GEOADD ====================
	case "GEOADD":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'GEOADD' command")
		}
		key := string(args[0])
		members := make([]store.GeoMember, 0)
		for i := 1; i+2 < len(args); i += 3 {
			lon, err1 := strconv.ParseFloat(string(args[i]), 64)
			lat, err2 := strconv.ParseFloat(string(args[i+1]), 64)
			if err1 != nil || err2 != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			members = append(members, store.GeoMember{
				Lat:    lat,
				Lon:    lon,
				Member: string(args[i+2]),
			})
		}
		h.markDirtyKeys(key)
		added, err := h.Db.GeoAdd(key, members)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(added)

	// ==================== GEOPOS ====================
	case "GEOPOS":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GEOPOS' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		positions, err := h.Db.GeoPos(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([][]byte, len(positions))
		for i, pos := range positions {
			if pos[0] == 0 && pos[1] == 0 {
				results[i] = nil
			} else {
				results[i] = []byte(fmt.Sprintf("%.6f,%.6f", pos[1], pos[0]))
			}
		}
		return &proto.Array{Args: results}

	// ==================== GEOHASH ====================
	case "GEOHASH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GEOHASH' command")
		}
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		hashes, err := h.Db.GeoHash(key, members...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		hashResults := make([][]byte, len(hashes))
		for i, h := range hashes {
			hashResults[i] = []byte(h)
		}
		return &proto.Array{Args: hashResults}

	// ==================== GEODIST ====================
	case "GEODIST":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'GEODIST' command")
		}
		key := string(args[0])
		member1 := string(args[1])
		member2 := string(args[2])
		unit := "m"
		if len(args) >= 4 {
			unit = string(args[3])
		}
		dist, err := h.Db.GeoDist(key, member1, member2, unit)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.4f", dist)))

	// ==================== GEOSEARCH ====================
	case "GEOSEARCH":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'GEOSEARCH' command")
		}
		key := string(args[0])
		// Parse: FROMMEMBER member [FROMLONLAT lon lat] [BYRADIUS radius unit | BYBOX width height unit] [ASC | DESC] [COUNT count] [WITHCOORD] [WITHDIST] [WITHHASH]
		var centerLon, centerLat float64
		var radius float64
		var unit string
		var count int = 0
		var withDist, withHash, withCoord bool

		i := 1
		// Check for FROMMEMBER or FROMLONLAT
		if strings.ToUpper(string(args[i])) == "FROMMEMBER" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			member := string(args[i+1])
			positions, err := h.Db.GeoPos(key, member)
			if err != nil || len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
				return proto.NewError("ERR could not decode query zset member")
			}
			centerLon = positions[0][1]
			centerLat = positions[0][0]
			i += 2
		} else if strings.ToUpper(string(args[i])) == "FROMLONLAT" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err1, err2 error
			centerLon, err1 = strconv.ParseFloat(string(args[i+1]), 64)
			centerLat, err2 = strconv.ParseFloat(string(args[i+2]), 64)
			if err1 != nil || err2 != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			i += 3
		} else {
			return proto.NewError("ERR syntax error")
		}

		// BYRADIUS or BYBOX
		if i >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		if strings.ToUpper(string(args[i])) == "BYRADIUS" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err error
			radius, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			unit = string(args[i+2])
			i += 3
		} else if strings.ToUpper(string(args[i])) == "BYBOX" {
			// Simplified: treat as radius with width
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			width, err := strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			unit = string(args[i+3])
			radius = width / 2
			i += 4
		} else {
			return proto.NewError("ERR syntax error")
		}

		// Optional modifiers
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "ASC", "DESC":
				i++
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "WITHCOORD":
				withCoord = true
				i++
			case "WITHDIST":
				withDist = true
				i++
			case "WITHHASH":
				withHash = true
				i++
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
			}
		}

		results, err := h.Db.GeoSearch(key, centerLon, centerLat, radius, unit, count, withDist, withHash, withCoord)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Format results
		var response [][]byte
		for _, r := range results {
			if withCoord {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(fmt.Sprintf("%.6f", r.Lon)))
				response = append(response, []byte(fmt.Sprintf("%.6f", r.Lat)))
			} else if withDist && withHash {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(fmt.Sprintf("%.4f", r.Dist)))
				response = append(response, []byte(r.Hash))
			} else if withDist {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(fmt.Sprintf("%.4f", r.Dist)))
			} else if withHash {
				response = append(response, []byte(r.Member))
				response = append(response, []byte(r.Hash))
			} else {
				response = append(response, []byte(r.Member))
			}
		}
		return &proto.Array{Args: response}

	// ==================== GEOSEARCHSTORE ====================
	case "GEOSEARCHSTORE":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'GEOSEARCHSTORE' command")
		}
		dstKey := string(args[0])
		srcKey := string(args[1])

		var centerLon, centerLat float64
		var radius float64
		var unit string
		var count int = 0
		var storeDist bool

		i := 2
		// Check for FROMMEMBER or FROMLONLAT
		if i < len(args) && strings.ToUpper(string(args[i])) == "FROMMEMBER" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			member := string(args[i+1])
			positions, err := h.Db.GeoPos(srcKey, member)
			if err != nil || len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
				return proto.NewError("ERR could not decode query zset member")
			}
			centerLon = positions[0][1]
			centerLat = positions[0][0]
			i += 2
		} else if i < len(args) && strings.ToUpper(string(args[i])) == "FROMLONLAT" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err1, err2 error
			centerLon, err1 = strconv.ParseFloat(string(args[i+1]), 64)
			centerLat, err2 = strconv.ParseFloat(string(args[i+2]), 64)
			if err1 != nil || err2 != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			i += 3
		}

		// BYRADIUS or BYBOX
		if i >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		if strings.ToUpper(string(args[i])) == "BYRADIUS" {
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err error
			radius, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			unit = string(args[i+2])
			i += 3
		} else {
			return proto.NewError("ERR syntax error")
		}

		// Optional modifiers
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "ASC", "DESC":
				i++
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "STOREDIST":
				storeDist = true
				i++
			default:
				i++
			}
		}

		stored, err := h.Db.GeoSearchStore(dstKey, srcKey, centerLon, centerLat, radius, unit, count, storeDist)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(stored)

	// ==================== XADD ====================
	case "XADD":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XADD' command")
		}
		key := string(args[0])
		var opts store.StreamXAddOptions
		var id string
		var fields = make(map[string]string)

		// Parse options
		i := 1
		for i < len(args)-2 && string(args[i])[0] == '-' {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "MAXLEN":
				if i+1 >= len(args)-2 {
					return proto.NewError("ERR syntax error")
				}
				maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				opts.MaxLen = maxlen
				i += 2
			case "MINID":
				if i+1 >= len(args)-2 {
					return proto.NewError("ERR syntax error")
				}
				opts.MinID = string(args[i+1])
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
			}
		}

		// ID or field name
		id = string(args[i])
		if id == "*" || (len(id) > 0 && id[0] == '-') {
			// It's the ID (* or an option), skip it
			i++
		} else {
			// It's the ID
			i++
		}

		// Remaining args are field-value pairs
		for i < len(args) {
			field := string(args[i])
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			value := string(args[i+1])
			fields[field] = value
			i += 2
		}

		resultID, err := h.Db.XAdd(key, opts, id, fields)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(resultID))

	// ==================== XLEN ====================
	case "XLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'XLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.XLen(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(length)

	// ==================== XREAD ====================
	case "XREAD":
		var count int64 = 0
		var block int64 = 0

		// Parse options
		i := 0
		if i < len(args) && strings.ToUpper(string(args[i])) == "COUNT" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			i += 2
		}
		if i < len(args) && strings.ToUpper(string(args[i])) == "BLOCK" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			block = b
			i += 2
		}

		// Check for STREAMS
		if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
			return proto.NewError("ERR syntax error, missing STREAMS keyword")
		}
		i++

		// Parse stream IDs
		// Format: key1 id1 key2 id2 ...
		remaining := len(args) - i
		if remaining < 2 || remaining%2 != 0 {
			return proto.NewError(fmt.Sprintf("ERR syntax error: remaining=%d, i=%d, len(args)=%d", remaining, i, len(args)))
		}
		numStreams := remaining / 2
		streamKeys := make([]string, numStreams)
		streamIDs := make([]string, numStreams)
		for j := 0; j < numStreams; j++ {
			streamKeys[j] = string(args[i+j*2])
			streamIDs[j] = string(args[i+j*2+1])
		}

		// Combine keys and IDs
		allArgs := make([]string, 0)
		for j := 0; j < numStreams; j++ {
			allArgs = append(allArgs, streamKeys[j])
			allArgs = append(allArgs, streamIDs[j])
		}

		results, err := h.Db.XRead(count, block, allArgs...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Format response
		var response [][]byte
		for _, streamMap := range results {
			for streamKey, entries := range streamMap {
				response = append(response, []byte(streamKey))
				entryArray := make([][]byte, 0)
				for _, entry := range entries {
					entryArray = append(entryArray, []byte(entry.ID))
					fieldArray := make([][]byte, 0)
					for k, v := range entry.Fields {
						fieldArray = append(fieldArray, []byte(k))
						fieldArray = append(fieldArray, []byte(v))
					}
					entryArray = append(entryArray, fieldArray...)
				}
				response = append(response, entryArray...)
			}
		}
		if len(response) == 0 {
			return proto.NewBulkString(nil)
		}
		return &proto.Array{Args: response}

	// ==================== XRANGE ====================
	case "XRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XRANGE' command")
		}
		key := string(args[0])
		start := string(args[1])
		stop := string(args[2])
		count := int64(0)

		// Parse COUNT option
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "COUNT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				break
			}
		}

		entries, err := h.Db.XRange(key, start, stop, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// XRANGE returns [[entryID, [field, value, ...]], ...]
		// go-redis parses nested arrays as flat when structure is [id, [fields...]]
		// Format expected by test: [[id1, field1, value1], [id2, field2, value2]]
		if len(entries) == 0 {
			return &proto.Array{Args: [][]byte{}}
		}
		var resultElems []proto.RESP
		for _, entry := range entries {
			var fieldElems []proto.RESP
			for k, v := range entry.Fields {
				bsK := proto.BulkString(k)
				bsV := proto.BulkString(v)
				fieldElems = append(fieldElems, &bsK, &bsV)
			}
			bsID := proto.BulkString(entry.ID)
			// Entry: [id, [field, value, ...]]
			resultElems = append(resultElems, &proto.NestedArray{
				Elems: []proto.RESP{
					&bsID,
					&proto.NestedArray{Elems: fieldElems},
				},
			})
		}
		return &proto.NestedArray{Elems: resultElems}

	// ==================== XREVRANGE ====================
	case "XREVRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XREVRANGE' command")
		}
		key := string(args[0])
		start := string(args[1])
		stop := string(args[2])
		count := int64(0)

		// Parse COUNT option
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "COUNT" {
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				break
			}
		}

		entries, err := h.Db.XRevRange(key, start, stop, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// XREVRANGE returns [[entryID, [field, value, ...]], ...] (reverse order)
		if len(entries) == 0 {
			return &proto.Array{Args: [][]byte{}}
		}
		var resultElems []proto.RESP
		for _, entry := range entries {
			var fieldElems []proto.RESP
			for k, v := range entry.Fields {
				bsK := proto.BulkString(k)
				bsV := proto.BulkString(v)
				fieldElems = append(fieldElems, &bsK, &bsV)
			}
			bsID := proto.BulkString(entry.ID)
			resultElems = append(resultElems, &proto.NestedArray{
				Elems: []proto.RESP{
					&bsID,
					&proto.NestedArray{Elems: fieldElems},
				},
			})
		}
		return &proto.NestedArray{Elems: resultElems}

	// ==================== XDEL ====================
	case "XDEL":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XDEL' command")
		}
		key := string(args[0])
		ids := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			ids[i-1] = string(args[i])
		}
		deleted, err := h.Db.XDel(key, ids...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(deleted)

	// ==================== XACK ====================
	case "XACK":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XACK' command")
		}
		key := string(args[0])
		group := string(args[1])
		ids := make([]string, len(args)-2)
		for i := 2; i < len(args); i++ {
			ids[i-2] = string(args[i])
		}
		acknowledged, err := h.Db.XAck(key, group, ids...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(acknowledged)

	// ==================== XGROUP ====================
	case "XGROUP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'XGROUP' command")
		}
		subcommand := strings.ToUpper(string(args[0]))

		switch subcommand {
		case "CREATE":
			if len(args) < 4 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP CREATE' command")
			}
			key := string(args[1])
			group := string(args[2])
			startID := string(args[3])
			// Skip MKSTREAM option for now
			err := h.Db.XGroupCreate(key, group, startID)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.OK
		case "DESTROY":
			if len(args) < 3 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP DESTROY' command")
			}
			key := string(args[1])
			group := string(args[2])
			err := h.Db.XGroupDestroy(key, group)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(1)
		case "SETID":
			if len(args) < 4 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP SETID' command")
			}
			key := string(args[1])
			group := string(args[2])
			id := string(args[3])
			err := h.Db.XGroupSetID(key, group, id)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.OK
		case "DELCONSUMER":
			if len(args) < 4 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP DELCONSUMER' command")
			}
			key := string(args[1])
			group := string(args[2])
			consumer := string(args[3])
			removed, err := h.Db.XGroupDelConsumer(key, group, consumer)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(removed)
		default:
			return proto.NewError("ERR syntax error")
		}

	// ==================== XREADGROUP ====================
	case "XREADGROUP":
		var count int64 = 0
		var block int64 = 0
		var group, consumer string

		// Find GROUP keyword first
		groupIdx := -1
		for i := 0; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "GROUP" {
				groupIdx = i
				break
			}
		}
		if groupIdx < 0 {
			return proto.NewError("ERR syntax error, missing GROUP keyword")
		}

		// Parse options before GROUP
		i := 0
		for i < groupIdx {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "COUNT":
				if i+1 >= groupIdx {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "BLOCK":
				if i+1 >= groupIdx {
					return proto.NewError("ERR syntax error")
				}
				b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				block = b
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
			}
		}

		// Parse group and consumer
		if groupIdx+2 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		group = string(args[groupIdx+1])
		consumer = string(args[groupIdx+2])
		i = groupIdx + 3

		// Parse options (COUNT, BLOCK) after group/consumer
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			if opt == "STREAMS" {
				break
			}
			switch opt {
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count = c
				i += 2
			case "BLOCK":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				block = b
				i += 2
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s at index %d", opt, i))
			}
		}

		// Check for STREAMS
		if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
			return proto.NewError("ERR syntax error, missing STREAMS keyword")
		}
		i++

		// Parse stream IDs
		remaining := len(args) - i
		if remaining < 2 || remaining%2 != 0 {
			return proto.NewError(fmt.Sprintf("ERR syntax error: remaining=%d, i=%d, len(args)=%d", remaining, i, len(args)))
		}
		numStreams := remaining / 2
		streamKeys := make([]string, numStreams)
		streamIDs := make([]string, numStreams)
		for j := 0; j < numStreams; j++ {
			streamKeys[j] = string(args[i+j*2])
			streamIDs[j] = string(args[i+j*2+1])
		}

		results, err := h.Db.XReadGroup(group, consumer, count, block, streamKeys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Format response - XREADGROUP returns [[stream, [[entry1], [entry2], ...]], ...]
		// Test expects format: [streamKey, [[entries]]]
		// This produces nested arrays: [key, [entries]]
		var response []proto.RESP
		for _, streamMap := range results {
			for streamKey, entries := range streamMap {
				// Build entries array for this stream
				var entryArrayElems []proto.RESP
				for _, entry := range entries {
					// Build field array for this entry: [id, [field1, value1, ...]]
					var fieldElems []proto.RESP
					for k, v := range entry.Fields {
						bsK := proto.BulkString(k)
						bsV := proto.BulkString(v)
						fieldElems = append(fieldElems, &bsK, &bsV)
					}
					// Entry is [id, [fields...]]
					bsID := proto.BulkString(entry.ID)
					entryArrayElems = append(entryArrayElems, &proto.NestedArray{
						Elems: []proto.RESP{&bsID, &proto.NestedArray{Elems: fieldElems}},
					})
				}
				// Stream result is [streamKey, entriesArray]
				bsKey := proto.BulkString(streamKey)
				response = append(response, &proto.NestedArray{
					Elems: []proto.RESP{
						&bsKey,
						&proto.NestedArray{Elems: entryArrayElems},
					},
				})
			}
		}
		if len(response) == 0 {
			return proto.NewBulkString(nil)
		}
		return &proto.NestedArray{Elems: response}

	// ==================== XCLAIM ====================
	case "XCLAIM":
		if len(args) < 5 {
			return proto.NewError("ERR wrong number of arguments for 'XCLAIM' command")
		}
		key := string(args[0])
		group := string(args[1])
		consumer := string(args[2])
		minIdleTime, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		ids := make([]string, len(args)-4)
		for i := 4; i < len(args); i++ {
			ids[i-4] = string(args[i])
		}
		claimed, err := h.Db.XClaim(key, group, consumer, minIdleTime, ids...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// XCLAIM returns array of message IDs
		result := make([][]byte, len(claimed))
		for i, id := range claimed {
			result[i] = []byte(id)
		}
		return &proto.Array{Args: result}

	// ==================== XAUTOCLAIM ====================
	case "XAUTOCLAIM":
		if len(args) < 5 {
			return proto.NewError("ERR wrong number of arguments for 'XAUTOCLAIM' command")
		}
		key := string(args[0])
		group := string(args[1])
		consumer := string(args[2])
		minIdleTime, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		start := string(args[4])

		// Parse options
		opts := store.XAutoClaimOptions{Count: 100, JustID: false}
		i := 5
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "COUNT":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				count, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				opts.Count = count
				i += 2
			case "JUSTID":
				opts.JustID = true
				i++
			default:
				return proto.NewError("ERR syntax error")
			}
		}

		result, err := h.Db.XAutoClaim(key, group, consumer, minIdleTime, start, opts)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Build response
		response := make([][]byte, 0)
		response = append(response, []byte(result.NextID))
		if opts.JustID {
			for _, id := range result.ClaimedIDs {
				response = append(response, []byte(id))
			}
		} else {
			for _, id := range result.ClaimedIDs {
				response = append(response, []byte(id))
			}
			for _, msg := range result.Messages {
				entry := [][]byte{[]byte("id"), []byte(msg.ID)}
				for k, v := range msg.Fields {
					entry = append(entry, []byte(k), []byte(v))
				}
				response = append(response, entry...)
			}
		}
		return &proto.Array{Args: response}

	// ==================== XPENDING ====================
	case "XPENDING":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XPENDING' command")
		}
		key := string(args[0])
		group := string(args[1])
		entries, err := h.Db.XPending(key, group)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// Redis XPENDING format: [pending_count, min_id, max_id, [[id, consumer, delivery_count, last_delivery_time], ...]]
		response := make([]proto.RESP, 0)

		// Count pending entries
		count := len(entries)
		response = append(response, proto.Integer(count))

		// Find min and max IDs
		var minID, maxID string
		for _, e := range entries {
			if minID == "" || e.ID < minID {
				minID = e.ID
			}
			if maxID == "" || e.ID > maxID {
				maxID = e.ID
			}
		}
		response = append(response, proto.NewBulkString([]byte(minID)))
		response = append(response, proto.NewBulkString([]byte(maxID)))

		// Build entries array
		var entriesArray []proto.RESP
		for _, e := range entries {
			entryArray := []proto.RESP{
				proto.NewBulkString([]byte(e.ID)),
				proto.NewBulkString([]byte(e.Consumer)),
				proto.Integer(e.DeliveryCount),
				proto.Integer(e.LastDelivery),
			}
			entriesArray = append(entriesArray, &proto.NestedArray{Elems: entryArray})
		}
		response = append(response, &proto.NestedArray{Elems: entriesArray})

		return &proto.NestedArray{Elems: response}

	// ==================== XINFO ====================
	case "XINFO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'XINFO' command")
		}
		subcommand := strings.ToUpper(string(args[0]))

		switch subcommand {
		case "HELP":
			response := [][]byte{
				[]byte("XINFO <subcommand> [<arg> ...]"),
				[]byte("Returns information about streams and consumer groups."),
				[]byte(""),
				[]byte("XINFO STREAM <key> [FULL]"),
				[]byte("  -- Returns information about a stream."),
				[]byte(""),
				[]byte("XINFO GROUPS <key>"),
				[]byte("  -- Returns the consumer groups of a stream."),
				[]byte(""),
				[]byte("XINFO CONSUMERS <key> <group>"),
				[]byte("  -- Returns the consumers of a consumer group."),
				[]byte(""),
				[]byte("XINFO STREAM <key> FULL [COUNT <count>]"),
				[]byte("  -- Returns full information about a stream including entries."),
			}
			return &proto.Array{Args: response}
		case "STREAM":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'XINFO STREAM' command")
			}
			key := string(args[1])
			info, err := h.Db.XInfo(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			response := [][]byte{
				[]byte("length"),
				[]byte(strconv.FormatInt(info.Length, 10)),
				[]byte("first-entry-id"),
				[]byte(info.FirstID),
				[]byte("last-entry-id"),
				[]byte(info.LastID),
				[]byte("max-deleted-entry-id"),
				[]byte(info.MaxDeletedID),
			}
			return &proto.Array{Args: response}
		case "GROUPS":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'XINFO GROUPS' command")
			}
			key := string(args[1])
			groups, err := h.Db.XInfoGroups(key)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			// Return array of groups, each group is a nested array of [key, value, ...]
			var response []proto.RESP
			for _, g := range groups {
				groupInfo := []proto.RESP{
					proto.NewBulkString([]byte("name")),
					proto.NewBulkString([]byte(g.Name)),
					proto.NewBulkString([]byte("consumers")),
					proto.NewBulkString([]byte(strconv.Itoa(len(g.Consumers)))),
					proto.NewBulkString([]byte("pending")),
					proto.NewBulkString([]byte(strconv.Itoa(len(g.Pending)))),
				}
				response = append(response, &proto.NestedArray{Elems: groupInfo})
			}
			return &proto.NestedArray{Elems: response}
		case "CONSUMERS":
			if len(args) < 3 {
				return proto.NewError("ERR wrong number of arguments for 'XINFO CONSUMERS' command")
			}
			key := string(args[1])
			group := string(args[2])
			consumers, err := h.Db.XInfoConsumers(key, group)
			if err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			response := make([][]byte, 0)
			for _, c := range consumers {
				response = append(response, []byte("name"))
				response = append(response, []byte(c.Name))
				response = append(response, []byte("seen"))
				response = append(response, []byte(strconv.FormatInt(c.LastSeen, 10)))
			}
			return &proto.Array{Args: response}
		default:
			return proto.NewError("ERR syntax error")
		}

	// ==================== XTRIM ====================
	case "XTRIM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XTRIM' command")
		}
		key := string(args[0])
		var maxLen int64 = 0
		var minID string
		approximate := false

		// Parse options
		i := 1
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "MAXLEN":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				// Handle ~ approximation option
				nextArg := strings.ToUpper(string(args[i+1]))
				if nextArg == "~" {
					approximate = true
					if i+2 >= len(args) {
						return proto.NewError("ERR syntax error")
					}
					maxlen, err := strconv.ParseInt(string(args[i+2]), 10, 64)
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					maxLen = maxlen
					i += 3
				} else {
					maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					maxLen = maxlen
					i += 2
				}
			case "MINID":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				minID = string(args[i+1])
				i += 2
			case "~":
				// ~ can come before MAXLEN with its value
				// e.g., XTRIM key ~ count (defaults to MAXLEN)
				_ = approximate
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				maxLen = maxlen
				i += 2
			default:
				// Try to parse as a number (shorthand for MAXLEN)
				if _, err := strconv.ParseInt(opt, 10, 64); err == nil {
					maxLen, _ = strconv.ParseInt(opt, 10, 64)
					i++
				} else {
					return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option %s", opt))
				}
			}
		}

		trimmed, err := h.Db.XTrim(key, maxLen, minID)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(trimmed)

	// ==================== SORT ====================
	case "SORT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'SORT' command")
		}
		key := string(args[0])

		// Parse options
		var offset, count int64 = 0, -1
		var getPatterns []string
		var asc bool = true
		var alpha bool
		var destKey string
		var byPattern string

		i := 1
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "BY":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				byPattern = string(args[i+1])
				i += 2
			case "LIMIT":
				if i+2 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				parseResult, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				offset = parseResult
				count, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				i += 3
			case "GET":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				getPatterns = append(getPatterns, string(args[i+1]))
				i += 2
			case "ASC":
				asc = true
				i++
			case "DESC":
				asc = false
				i++
			case "ALPHA":
				alpha = true
				i++
			case "STORE":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				destKey = string(args[i+1])
				i += 2
			default:
				i++
			}
		}

		// Get source type
		keyType, _ := h.Db.Type(key)
		var values []string
		var scores []float64

		switch keyType {
		case "list":
			listValues, err := h.Db.LRange(key, 0, -1)
			if err == nil {
				values = listValues
			} else {
				values = []string{}
			}
		case "set":
			setValues, err := h.Db.SMembers(key)
			if err == nil {
				values = setValues
			} else {
				values = []string{}
			}
		case "string":
			val, _ := h.Db.Get(key)
			values = []string{val}
		case "zset":
			members, _ := h.Db.ZRange(key, 0, -1)
			for _, m := range members {
				values = append(values, m.Member)
				scores = append(scores, m.Score)
			}
		default:
			return proto.NewError("ERR Operation against a key holding the wrong kind of value")
		}

		// Apply BY pattern - get weights from external keys
		if byPattern != "" && len(values) > 0 {
			weights := make([]float64, len(values))
			for idx, val := range values {
				targetKey := strings.Replace(byPattern, "*", val, 1)
				weightVal, _ := h.Db.Get(targetKey)
				if weightVal != "" {
					if f, err := strconv.ParseFloat(weightVal, 64); err == nil {
						weights[idx] = f
					} else {
						weights[idx] = float64(idx)
					}
				} else {
					weights[idx] = float64(idx)
				}
			}
			scores = weights
			// When using BY, sort by scores (numeric)
			alpha = false
		}

		// Sort values
		if len(scores) == 0 && !alpha && len(values) > 0 {
			// Numeric sort
			scores = make([]float64, len(values))
			for idx, v := range values {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					scores[idx] = f
				} else {
					scores[idx] = 0
				}
			}
		}

		// Simple bubble sort (for simplicity)
		n := len(values)
		for i := 0; i < n-1; i++ {
			for j := 0; j < n-i-1; j++ {
				swap := false
				if alpha {
					if asc {
						swap = values[j] > values[j+1]
					} else {
						swap = values[j] < values[j+1]
					}
				} else {
					if asc {
						swap = scores[j] > scores[j+1]
					} else {
						swap = scores[j] < scores[j+1]
					}
				}
				if swap {
					values[j], values[j+1] = values[j+1], values[j]
					if len(scores) > 0 {
						scores[j], scores[j+1] = scores[j+1], scores[j]
					}
				}
			}
		}

		// Apply LIMIT
		if offset > 0 {
			if offset >= int64(len(values)) {
				values = []string{}
			} else if offset < int64(len(values)) {
				values = values[offset:]
				if len(scores) > 0 {
					scores = scores[offset:]
				}
			}
		}
		if count >= 0 && int64(len(values)) > count {
			values = values[:count]
			if len(scores) > 0 {
				scores = scores[:count]
			}
		}

		// Apply GET patterns
		if len(getPatterns) > 0 {
			finalValues := make([]string, 0)
			for _, pattern := range getPatterns {
				for _, val := range values {
					targetKey := strings.Replace(pattern, "*", val, 1)
					targetVal, _ := h.Db.Get(targetKey)
					finalValues = append(finalValues, targetVal)
				}
			}
			values = finalValues
		}

		// STORE
		if destKey != "" {
			// Store as a list
			for idx, v := range values {
				if idx == 0 {
					_, _ = h.Db.Del(destKey)
				}
				_, _ = h.Db.RPush(destKey, v)
			}
			return proto.NewInteger(int64(len(values)))
		}

		// Return result
		results := make([][]byte, len(values))
		for idx, v := range values {
			results[idx] = []byte(v)
		}
		return &proto.Array{Args: results}

	// ==================== AUTH ====================
	case "AUTH":
		// 简化实现：检查密码
		// 支持环境变量 BOLTDB_PASSWORD
		password := os.Getenv("BOLTDB_PASSWORD")
		if password == "" {
			// 没有配置密码，任何密码都接受
			return proto.NewSimpleString("OK")
		}

		// 格式: AUTH password 或 AUTH username password
		var inputPassword string
		if len(args) >= 1 {
			inputPassword = string(args[0])
		}

		if inputPassword == password {
			return proto.NewSimpleString("OK")
		}
		return proto.NewError("ERR invalid password")

	// ==================== JSON ====================
	case "JSON.SET":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.SET' command")
		}
		key, path := string(args[0]), string(args[1])
		value := string(args[2])
		nx, xx := false, false
		// Parse optional NX/XX arguments
		for i := 3; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			if opt == "NX" {
				nx = true
			} else if opt == "XX" {
				xx = true
			}
		}
		h.markDirtyKeys(key)
		result, err := h.Db.JSONSet(key, path, value, nx, xx)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewSimpleString(result)

	case "JSON.GET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.GET' command")
		}
		key := string(args[0])
		paths := make([]string, 0)
		for i := 1; i < len(args); i++ {
			paths = append(paths, string(args[i]))
		}
		result, err := h.Db.JSONGet(key, paths...)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if len(result) == 1 {
			return proto.NewBulkString([]byte(result[0]))
		}
		// Multiple paths
		arr := make([][]byte, len(result))
		for i, v := range result {
			arr[i] = []byte(v)
		}
		return &proto.Array{Args: arr}

	case "JSON.DEL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.DEL' command")
		}
		key := string(args[0])
		paths := make([]string, 0)
		for i := 1; i < len(args); i++ {
			paths = append(paths, string(args[i]))
		}
		h.markDirtyKeys(key)
		count, err := h.Db.JSONDel(key, paths...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.TYPE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.TYPE' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		result, err := h.Db.JSONType(key, path)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(result))

	case "JSON.MGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.MGET' command")
		}
		path := string(args[len(args)-1])
		keys := make([]string, 0)
		for i := 0; i < len(args)-1; i++ {
			keys = append(keys, string(args[i]))
		}
		result, err := h.Db.JSONMGet(path, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, len(result))
		for i, v := range result {
			if v == "" {
				arr[i] = nil
			} else {
				arr[i] = []byte(v)
			}
		}
		return &proto.Array{Args: arr}

	case "JSON.ARRAPPEND":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.ARRAPPEND' command")
		}
		key, path := string(args[0]), string(args[1])
		values := make([]string, 0)
		for i := 2; i < len(args); i++ {
			values = append(values, string(args[i]))
		}
		h.markDirtyKeys(key)
		count, err := h.Db.JSONArrAppend(key, path, values...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.ARRLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.ARRLEN' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		count, err := h.Db.JSONArrLen(key, path)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.OBJKEYS":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.OBJKEYS' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		keys, err := h.Db.JSONObjKeys(key, path)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, len(keys))
		for i, k := range keys {
			arr[i] = []byte(k)
		}
		return &proto.Array{Args: arr}

	case "JSON.NUMINCRBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.NUMINCRBY' command")
		}
		key, path := string(args[0]), string(args[1])
		increment, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR increment must be a valid number")
		}
		h.markDirtyKeys(key)
		result, err := h.Db.JSONNumIncrBy(key, path, increment)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(result, 'f', -1, 64)))

	case "JSON.NUMMULTBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.NUMMULTBY' command")
		}
		key, path := string(args[0]), string(args[1])
		multiplier, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR multiplier must be a valid number")
		}
		h.markDirtyKeys(key)
		result, err := h.Db.JSONNumMultBy(key, path, multiplier)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(result, 'f', -1, 64)))

	case "JSON.CLEAR":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.CLEAR' command")
		}
		key := string(args[0])
		path := "$"
		if len(args) >= 2 {
			path = string(args[1])
		}
		h.markDirtyKeys(key)
		count, err := h.Db.JSONClear(key, path)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(count)

	case "JSON.DEBUG":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'JSON.DEBUG' command")
		}
		subCmd := strings.ToUpper(string(args[0]))
		if subCmd != "MEMORY" {
			return proto.NewError("ERR syntax error")
		}
		key := string(args[1])
		path := "$"
		if len(args) >= 3 {
			path = string(args[2])
		}
		memory, err := h.Db.JSONDebugMemory(key, path)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(memory)

	// ==================== Time Series ====================
	case "TS.CREATE":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.CREATE' command")
		}
		key := string(args[0])
		opts := store.TSCreateOptions{}
		i := 1
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "RETENTION":
				i++
				if i >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				retention, err := strconv.ParseInt(string(args[i]), 10, 64)
				if err != nil {
					return proto.NewError("ERR invalid RETENTION value")
				}
				opts.Retention = retention
			case "ENCODING":
				i++
				if i >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				opts.Encoding = string(args[i])
			case "DUPLICATE_POLICY":
				i++
				if i >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				opts.DuplicatePolicy = string(args[i])
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unexpected option: %s", opt))
			}
			i++
		}
		if err := h.Db.TSCreate(key, opts); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK

	case "TS.ADD":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'TS.ADD' command")
		}
		key := string(args[0])
		var timestamp int64
		if string(args[1]) == "*" {
			timestamp = time.Now().UnixNano() / int64(time.Millisecond)
		} else {
			var err error
			timestamp, err = strconv.ParseInt(string(args[1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid timestamp")
			}
		}
		value, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR invalid value")
		}
		opts := store.TSAddOptions{}
		if len(args) > 3 {
			opt := strings.ToUpper(string(args[3]))
			if opt == "ON_DUPLICATE" && len(args) > 4 {
				opts.OnDuplicate = string(args[4])
			}
		}
		h.markDirtyKeys(key)
		ts, err := h.Db.TSAdd(key, timestamp, value, opts)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(ts)

	case "TS.GET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.GET' command")
		}
		key := string(args[0])
		dp, err := h.Db.TSGet(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// Return as array: [timestamp, value]
		return &proto.Array{
			Args: [][]byte{
				[]byte(strconv.FormatInt(dp.Timestamp, 10)),
				[]byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)),
			},
		}

	case "TS.RANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'TS.RANGE' command")
		}
		key := string(args[0])
		start := string(args[1])
		stop := string(args[2])
		count := int64(-1)
		if len(args) > 3 {
			opt := strings.ToUpper(string(args[3]))
			if opt == "COUNT" && len(args) > 4 {
				c, err := strconv.ParseInt(string(args[4]), 10, 64)
				if err != nil {
					return proto.NewError("ERR invalid COUNT value")
				}
				count = c
			}
		}
		results, err := h.Db.TSRange(key, start, stop, count)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, 0, len(results)*2)
		for _, dp := range results {
			arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
			arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
		}
		return &proto.Array{Args: arr}

	case "TS.DEL":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'TS.DEL' command")
		}
		key := string(args[0])
		start := string(args[1])
		stop := string(args[2])
		h.markDirtyKeys(key)
		deleted, err := h.Db.TSDel(key, start, stop)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(deleted)

	case "TS.INFO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.INFO' command")
		}
		key := string(args[0])
		info, err := h.Db.TSInfo(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		// Return as array of key-value pairs
		return &proto.Array{
			Args: [][]byte{
				[]byte("totalSamples"), []byte(strconv.FormatInt(info.TotalSamples, 10)),
				[]byte("memoryUsage"), []byte(strconv.FormatInt(info.MemoryUsage, 10)),
				[]byte("firstTimestamp"), []byte(strconv.FormatInt(info.FirstTimestamp, 10)),
				[]byte("lastTimestamp"), []byte(strconv.FormatInt(info.LastTimestamp, 10)),
				[]byte("retentionTime"), []byte(strconv.FormatInt(info.RetentionTime, 10)),
				[]byte("encoding"), []byte(info.Encoding),
				[]byte("chunkCount"), []byte(strconv.FormatInt(info.ChunkCount, 10)),
			},
		}

	case "TS.LEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'TS.LEN' command")
		}
		key := string(args[0])
		length, err := h.Db.TSLen(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewBulkString(nil)
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(length)

	case "TS.MGET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'TS.MGET' command")
		}
		filter := string(args[0])
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		results, err := h.Db.TSMGet(filter, keys...)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		arr := make([][]byte, 0, len(results)*2)
		for _, dp := range results {
			if dp == nil {
				arr = append(arr, []byte{})
				arr = append(arr, []byte{})
			} else {
				arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
				arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
			}
		}
		return &proto.Array{Args: arr}

	default:
		// 如果在事务中，将命令加入队列
		if h.transaction != nil {
			h.transaction.Commands = append(h.transaction.Commands, TransactionCommand{
				Command: cmd,
				Args:    args,
			})
			return proto.NewSimpleString("QUEUED")
		}
		return proto.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// parseScore parses Redis-style score bounds including special values
// Supports: "-inf", "+inf", "(", "[", and numeric values
func parseScore(s string) (float64, error) {
	// 处理特殊值
	switch s {
	case "-inf":
		return float64(math.Inf(-1)), nil
	case "+inf", "inf":
		return float64(math.Inf(1)), nil
	case "-inf(", "-inf[":
		// Exclusive -inf is same as inclusive -inf for float comparison
		return float64(math.Inf(-1)), nil
	case "+inf(", "+inf[":
		return float64(math.Inf(1)), nil
	}

	// 处理带括号的排除边界 (value
	if len(s) > 0 && s[0] == '(' {
		// 对于排除边界，我们需要在比较时特殊处理
		// 这里简单处理，返回原值的前缀（不包括括号）
		s = s[1:]
	}

	return strconv.ParseFloat(s, 64)
}

// parseScoreExclusive checks if a score string represents an exclusive bound
func parseScoreExclusive(s string) (float64, bool, error) {
	exclusive := false
	if len(s) > 0 && s[0] == '(' {
		exclusive = true
		s = s[1:]
	} else if len(s) > 0 && s[0] == '[' {
		exclusive = false
		s = s[1:]
	}

	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// 检查特殊值
		switch s {
		case "-inf":
			return float64(math.Inf(-1)), exclusive, nil
		case "+inf", "inf":
			return float64(math.Inf(1)), exclusive, nil
		}
		return 0, false, err
	}
	return val, exclusive, nil
}

// executeQueuedCommand 执行事务队列中的命令
func (h *Handler) executeQueuedCommand(cmd string, args [][]byte) proto.RESP {
	switch cmd {
	case "SET":
		key, value := string(args[0]), string(args[1])
		if err := h.Db.Set(key, value); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK
	case "GET":
		key := string(args[0])
		value, err := h.Db.Get(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return nil
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(value))
	case "DEL":
		count := int64(0)
		for _, arg := range args {
			deleted, _ := h.Db.Del(string(arg))
			count += deleted
		}
		return proto.NewInteger(count)
	case "INCR":
		key := string(args[0])
		val, err := h.Db.INCR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "DECR":
		key := string(args[0])
		val, err := h.Db.DECR(key)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "INCRBY":
		key := string(args[0])
		delta, _ := strconv.ParseInt(string(args[1]), 10, 64)
		val, err := h.Db.INCRBY(key, delta)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "DECRBY":
		key := string(args[0])
		delta, _ := strconv.ParseInt(string(args[1]), 10, 64)
		val, err := h.Db.DECRBY(key, delta)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(val)
	case "APPEND":
		key, value := string(args[0]), string(args[1])
		length, _ := h.Db.APPEND(key, value)
		return proto.NewInteger(int64(length))
	case "STRLEN":
		key := string(args[0])
		length, _ := h.Db.StrLen(key)
		return proto.NewInteger(int64(length))
	case "EXISTS":
		key := string(args[0])
		exists, _ := h.Db.Exists(key)
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "EXPIRE":
		key := string(args[0])
		seconds, _ := strconv.Atoi(string(args[1]))
		success, _ := h.Db.Expire(key, seconds)
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TTL":
		key := string(args[0])
		ttl, _ := h.Db.TTL(key)
		return proto.NewInteger(ttl)
	case "PERSIST":
		key := string(args[0])
		success, _ := h.Db.Persist(key)
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TYPE":
		key := string(args[0])
		keyType, _ := h.Db.Type(key)
		return proto.NewSimpleString(keyType)
	case "LPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, _ := h.Db.LPush(key, values...)
		return proto.NewInteger(int64(count))
	case "RPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, _ := h.Db.RPush(key, values...)
		return proto.NewInteger(int64(count))
	case "LPOP":
		key := string(args[0])
		val, _ := h.Db.LPop(key)
		if val == "" {
			return nil
		}
		return proto.NewBulkString([]byte(val))
	case "RPOP":
		key := string(args[0])
		val, _ := h.Db.RPop(key)
		if val == "" {
			return nil
		}
		return proto.NewBulkString([]byte(val))
	case "LLEN":
		key := string(args[0])
		length, _ := h.Db.LLen(key)
		return proto.NewInteger(int64(length))
	case "LRANGE":
		key := string(args[0])
		start, _ := strconv.ParseInt(string(args[1]), 10, 64)
		stop, _ := strconv.ParseInt(string(args[2]), 10, 64)
		items, _ := h.Db.LRange(key, start, stop)
		results := make([][]byte, len(items))
		for i, item := range items {
			results[i] = []byte(item)
		}
		return &proto.Array{Args: results}
	case "HSET":
		key := string(args[0])
		field, value := string(args[1]), string(args[2])
		_ = h.Db.HSet(key, field, value)
		return proto.NewInteger(1)
	case "HGET":
		key, field := string(args[0]), string(args[1])
		val, _ := h.Db.HGet(key, field)
		if len(val) == 0 {
			return nil
		}
		return proto.NewBulkString(val)
	case "HGETALL":
		key := string(args[0])
		data, _ := h.Db.HGetAll(key)
		flatArgs := make([][]byte, 0)
		for k, v := range data {
			flatArgs = append(flatArgs, []byte(k), []byte(v))
		}
		return &proto.Array{Args: flatArgs}
	case "HDEL":
		key := string(args[0])
		fields := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			fields[i-1] = string(args[i])
		}
		count, _ := h.Db.HDel(key, fields...)
		return proto.NewInteger(int64(count))
	case "SADD":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, _ := h.Db.SAdd(key, members...)
		return proto.NewInteger(int64(count))
	case "SMEMBERS":
		key := string(args[0])
		members, _ := h.Db.SMembers(key)
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}
	case "SISMEMBER":
		key, member := string(args[0]), string(args[1])
		exists, _ := h.Db.SIsMember(key, member)
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "SCARD":
		key := string(args[0])
		count, _ := h.Db.SCard(key)
		return proto.NewInteger(int64(count))
	case "SREM":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, _ := h.Db.SRem(key, members...)
		return proto.NewInteger(int64(count))
	case "ZADD":
		key := string(args[0])
		members := make([]store.ZSetMember, 0)
		for i := 1; i < len(args); i += 2 {
			score, _ := strconv.ParseFloat(string(args[i]), 64)
			members = append(members, store.ZSetMember{Score: score, Member: string(args[i+1])})
		}
		_ = h.Db.ZAdd(key, members)
		return proto.NewInteger(int64(len(members)))
	case "ZREM":
		key := string(args[0])
		member := string(args[1])
		_ = h.Db.ZRem(key, member)
		return proto.NewInteger(1)
	case "ZCARD":
		key := string(args[0])
		count, _ := h.Db.ZCard(key)
		return proto.NewInteger(int64(count))
	case "ZSCORE":
		key, member := string(args[0]), string(args[1])
		score, _, _ := h.Db.ZScore(key, member)
		return proto.NewBulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
	case "ZINCRBY":
		key, member := string(args[0]), string(args[2])
		delta, _ := strconv.ParseFloat(string(args[1]), 64)
		newScore, _ := h.Db.ZIncrBy(key, member, delta)
		return proto.NewBulkString([]byte(strconv.FormatFloat(newScore, 'f', -1, 64)))
	default:
		return proto.NewError(fmt.Sprintf("ERR command '%s' not supported in transaction", cmd))
	}
}

// copyList 复制列表
func (h *Handler) copyList(srcKey, dstKey string) bool {
	length, err := h.Db.LLen(srcKey)
	if err != nil {
		return false
	}
	if length == 0 {
		return true
	}
	// 获取所有元素
	items, err := h.Db.LRange(srcKey, 0, int64(length-1))
	if err != nil {
		return false
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 添加到目标列表
	_, err = h.Db.RPush(dstKey, items...)
	return err == nil
}

// copyHash 复制Hash
func (h *Handler) copyHash(srcKey, dstKey string) bool {
	data, err := h.Db.HGetAll(srcKey)
	if err != nil {
		return false
	}
	if len(data) == 0 {
		return true
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 设置所有字段
	for k, v := range data {
		if err := h.Db.HSet(dstKey, k, v); err != nil {
			return false
		}
	}
	return true
}

// copySet 复制Set
func (h *Handler) copySet(srcKey, dstKey string) bool {
	members, err := h.Db.SMembers(srcKey)
	if err != nil {
		return false
	}
	if len(members) == 0 {
		return true
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 添加所有成员
	_, err = h.Db.SAdd(dstKey, members...)
	return err == nil
}

// copySortedSet 复制SortedSet
func (h *Handler) copySortedSet(srcKey, dstKey string) bool {
	members, err := h.Db.ZRange(srcKey, 0, -1)
	if err != nil {
		return false
	}
	if len(members) == 0 {
		return true
	}
	// 先删除目标
	_, _ = h.Db.Del(dstKey)
	// 添加所有成员
	zMembers := make([]store.ZSetMember, len(members))
	for i, m := range members {
		zMembers[i] = store.ZSetMember{Score: m.Score, Member: m.Member}
	}
	err = h.Db.ZAdd(dstKey, zMembers)
	return err == nil
}
