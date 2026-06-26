package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// connState holds per-connection state
type connState struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	inTransaction bool
	commands      []TransactionCommand
	transaction   *TransactionState
	clientInfo    *ClientInfo
	clusterAsking bool
	authenticated bool
	watchedKeys   map[string]struct{}
	dirtyKeys     map[string]struct{}
	subscriber    *store.Subscriber
	monitoring    bool
	monitorCh     chan []byte
	respVersion   int // 2 for RESP2 (default), 3 for RESP3; set by HELLO
	blocking      atomic.Bool
}

type Handler struct {
	Db          *store.BotreonStore
	Cluster     *cluster.Cluster
	Replication *replication.ReplicationManager
	Backup      *backup.BackupManager
	PubSub      *store.PubSubManager
	Port        int
	// 共享的 Watch 监视器：key → 正在监视该 key 的连接
	watchMonitors map[string]map[*connState]struct{}
	watchMu       sync.Mutex
	// Ctx 是服务器生命周期上下文，用于优雅关闭
	// 信号触发 cancel → 所有子 goroutine 的阻塞操作被取消
	Ctx context.Context

	// 连接注册表，支持 CLIENT LIST/KILL
	connsMu sync.RWMutex
	conns   map[*connState]*connMeta

	nextConnID atomic.Int64

	// 用于 MONITOR 命令的客户端注册表
	monitorClients map[*connState]chan []byte
	monitorMu      sync.Mutex

	// OutputBufferLimit 是每个连接输出缓冲区的硬限制（字节）
	// 当连接的待发送数据超过此值时，服务器断开该连接（slow client protection）
	// 0 表示不限制
	OutputBufferLimit int64

	// wg 跟踪所有后台 goroutine，确保关闭时完整收束
	wg sync.WaitGroup

	// shuttingDown is set to 1 when Shutdown begins, so handleConnection
	// goroutines that register after the conns iteration can exit promptly
	// instead of blocking on ReadRESP with nobody to close their connection.
	shuttingDown atomic.Int32
}

// connMeta 连接元数据
type connMeta struct {
	id          int64
	created     time.Time
	remoteAddr  string
	conn        net.Conn
	outputBytes int64
	lastRead    time.Time
	lastWrite   time.Time
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
	WatchKeys     map[string]struct{}  // 监控的键
	IsWatching    bool                 // 是否处于监视状态
	InTransaction bool                 // 是否在事务中（MULTI 已执行）
	DirtyKeys     map[string]struct{}  // 被修改的键
}

// TransactionCommand 事务中的命令
type TransactionCommand struct {
	Command string
	Args    [][]byte
}

// markDirtyKeys 标记键为脏（被修改）
// 通过共享 watchMonitors 通知所有正在监视该键的连接
func (h *Handler) markDirtyKeys(state *connState, keys ...string) {
	h.watchMu.Lock()
	for _, key := range keys {
		if watchers, exists := h.watchMonitors[key]; exists {
			for watcher := range watchers {
				watcher.mu.Lock()
				if watcher.dirtyKeys == nil {
					watcher.dirtyKeys = make(map[string]struct{})
				}
				watcher.dirtyKeys[key] = struct{}{}
				watcher.mu.Unlock()
			}
		}
	}
	h.watchMu.Unlock()
}

// checkAndHandleRedirect 检查键是否需要重定向到其他节点
// 返回 nil 表示不需要重定向，可以继续执行命令
// 返回非 nil 表示需要重定向，包含重定向信息
func (h *Handler) checkAndHandleRedirect(state *connState, key string) proto.RESP {
	if h.Cluster == nil {
		return nil
	}

	if state.clusterAsking {
		slot := cluster.Slot(key)
		if h.Cluster.IsImportingSlot(slot) {
			return nil
		}
		state.clusterAsking = false
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

// registerConnection 注册连接到连接表
func (h *Handler) registerConnection(state *connState, conn net.Conn, remoteAddr string) *connMeta {
	meta := &connMeta{
		id:         h.nextConnID.Add(1),
		created:    time.Now(),
		remoteAddr: remoteAddr,
		conn:       conn,
	}

	h.connsMu.Lock()
	if h.conns == nil {
		h.conns = make(map[*connState]*connMeta)
	}
	h.conns[state] = meta
	h.connsMu.Unlock()

	if state.clientInfo == nil {
		state.clientInfo = &ClientInfo{}
	}
	state.clientInfo.ID = meta.id
	state.clientInfo.Addr = remoteAddr
	state.clientInfo.FD = 0

	return meta
}

// unregisterConnection 从连接表移除连接
func (h *Handler) unregisterConnection(state *connState) {
	h.connsMu.Lock()
	delete(h.conns, state)
	h.connsMu.Unlock()
}

// ServeTCP 监听并处理连接
func (h *Handler) ServeTCP(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			if h.Ctx != nil {
				select {
				case <-h.Ctx.Done():
					return h.Ctx.Err()
				default:
				}
			}
			return err
		}
		h.wg.Add(1)
		go h.handleConnection(conn)
	}
}

// Shutdown 执行优雅关闭：关闭所有连接 → 等待所有 goroutine 退出
func (h *Handler) Shutdown() {
	logger.Logger.Info().Msg("开始关闭 handler 所有连接...")

	h.shuttingDown.Store(1)

	// 关闭所有活跃 TCP 连接（解除 ReadRESP 阻塞，让 goroutine 自然退出）
	h.connsMu.RLock()
	var targets []struct {
		state *connState
		conn  net.Conn
	}
	for state, meta := range h.conns {
		targets = append(targets, struct {
			state *connState
			conn  net.Conn
		}{state, meta.conn})
	}
	h.connsMu.RUnlock()

	for _, t := range targets {
		t.state.cancel()
		if t.conn != nil {
			if err := t.conn.Close(); err != nil {
				logger.Logger.Debug().Err(err).Msg("关闭客户端连接")
			}
		}
	}

	h.wg.Wait()
	logger.Logger.Info().Msg("handler 所有 goroutine 已退出")
}

func (h *Handler) handleConnection(conn net.Conn) {
	defer h.wg.Done()
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
		if h.Ctx != nil {
			if deadline, ok := h.Ctx.Deadline(); ok {
				_ = tcpConn.SetDeadline(deadline)
			}
		}
	}

	parentCtx := h.Ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	state := &connState{ctx: ctx, cancel: cancel}
	h.registerConnection(state, conn, remoteAddr)

	defer func() {
		state.mu.Lock()
		sub := state.subscriber
		state.subscriber = nil
		state.mu.Unlock()

		cancel()
		h.unregisterConnection(state)

		if sub != nil {
			h.PubSub.RemoveSubscriber(sub)
		}
		h.unregisterMonitorClient(state)
		if len(state.watchedKeys) > 0 {
			h.watchMu.Lock()
			for key := range state.watchedKeys {
				if set, exists := h.watchMonitors[key]; exists {
					delete(set, state)
					if len(set) == 0 {
						delete(h.watchMonitors, key)
					}
				}
			}
			h.watchMu.Unlock()
		}
	}()

	// 检查关闭信号：如果在 registerConnection 之后但在进入 ReadRESP
	// 之前 Shutdown 已经开始，避免 ReadRESP 永久阻塞（没有 conn.Close
	// 来解除阻塞）。
	if h.shuttingDown.Load() != 0 {
		return
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

		h.connsMu.Lock()
		if m, ok := h.conns[state]; ok {
			m.lastRead = time.Now()
		}
		h.connsMu.Unlock()

		// 收集所有响应
		var responses []proto.RESP
		commandsProcessed := 0

		// 处理第一个命令
		if resp := h.processRequest(req, reader, remoteAddr, writer, conn, state); resp != nil {
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

			if resp := h.processRequest(req, reader, remoteAddr, writer, conn, state); resp != nil {
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
			switch r := resp.(type) {
			case *MultiResponse:
				for _, subResp := range r.Responses {
					if err := proto.WriteRESP(writer, subResp); err != nil {
						logger.Logger.Warn().
							Str("remote_addr", remoteAddr).
							Err(err).
							Msg("写入响应失败")
						return
					}
				}
			default:
				if err := proto.WriteRESP(writer, resp); err != nil {
					logger.Logger.Warn().
						Str("remote_addr", remoteAddr).
						Err(err).
						Msg("写入响应失败")
					return
				}
			}
		}

		// 一次性刷新所有响应
		flushBytes := writer.Buffered()
		if err := writer.Flush(); err != nil {
			logger.Logger.Warn().
				Str("remote_addr", remoteAddr).
				Err(err).
				Msg("刷新缓冲区失败")
			return
		}
		h.connsMu.Lock()
		m, tracked := h.conns[state]
		if tracked {
			m.lastWrite = time.Now()
			m.outputBytes += int64(flushBytes)
		}
		h.connsMu.Unlock()
		if tracked && h.OutputBufferLimit > 0 && m.outputBytes > h.OutputBufferLimit {
			logger.Logger.Warn().
				Str("remote_addr", remoteAddr).
				Int64("output_bytes", m.outputBytes).
				Int64("limit", h.OutputBufferLimit).
				Msg("客户端输出缓冲区超限，断开连接")
			return
		}

		// 进入 PubSub 模式后切换到推送循环
		state.mu.Lock()
		inPubSub := state.subscriber != nil
		state.mu.Unlock()
		if inPubSub {
			h.runPubSubLoop(ctx, conn, reader, writer, state, remoteAddr)
			return
		}

		// 进入 MONITOR 模式后切换到推送循环
		state.mu.Lock()
		inMonitor := state.monitoring
		state.mu.Unlock()
		if inMonitor {
			h.runMonitorLoop(conn, writer, state, remoteAddr)
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
func (h *Handler) processRequest(req *proto.Array, reader *bufio.Reader, remoteAddr string, writer *bufio.Writer, conn net.Conn, state *connState) proto.RESP {
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

	resp := h.executeCommand(state, cmd, args[1:], remoteAddr)
	if resp == nil {
		logger.Logger.Error().
			Str("remote_addr", remoteAddr).
			Str("command", cmd).
			Msg("命令执行返回 nil")
		return proto.NewError("ERR internal error")
	}

	// 如果是主节点且是写命令，传播到从节点
	// 非确定性命令在传播前规范化
	propagateArgs := req.Args
	switch cmd {
	case "EXPIRE":
		if len(args) >= 2 {
			if seconds, err := strconv.Atoi(string(args[1])); err == nil {
				absoluteMS := time.Now().UnixNano()/int64(time.Millisecond) + int64(seconds)*1000
				propagateArgs = [][]byte{[]byte("PEXPIREAT"), args[0], []byte(strconv.FormatInt(absoluteMS, 10))}
			}
		}
	case "PEXPIRE":
		if len(args) >= 2 {
			if ms, err := strconv.ParseInt(string(args[1]), 10, 64); err == nil {
				absoluteMS := time.Now().UnixNano()/int64(time.Millisecond) + ms
				propagateArgs = [][]byte{[]byte("PEXPIREAT"), args[0], []byte(strconv.FormatInt(absoluteMS, 10))}
			}
		}
	}
	if h.Replication != nil && h.Replication.IsMaster() && isWriteCommand(cmd) {
		// MIGRATE has external side effects (RESTORE on target node) and
		// must NOT be propagated. Each replica's Del is independent.
		if cmd != "REPLICAOF" && cmd != "PSYNC" && cmd != "REPLCONF" && cmd != "MIGRATE" {
			h.Replication.PropagateCommand(propagateArgs)
		}
	}

	// 广播到 MONITOR 客户端（不广播 MONITOR 自身的请求）
	if cmd != "MONITOR" {
		h.broadcastToMonitors(cmd, args[1:], remoteAddr)
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
	case *proto.Map:
		return "Map"
	case *proto.Set:
		return "Set"
	case *proto.Push:
		return "Push"
	case *proto.Null:
		return "Null"
	case *proto.Double:
		return "Double"
	case *proto.Boolean:
		return "Boolean"
	case *proto.BigNumber:
		return "BigNumber"
	case *proto.VerbatimString:
		return "VerbatimString"
	default:
		return "Unknown"
	}
}

// handlePSyncWithRDB 处理PSYNC命令并发送RDB数据（全量同步）
// 这是executeCommand的特例，用于在全量同步时直接发送RDB数据
// 返回 nil 表示正常处理
// 返回 ReplicationTakeoverSignal{} 表示连接已由复制接管，需要关闭处理循环但不关闭连接
type ReplicationTakeoverSignal struct{}

func (ReplicationTakeoverSignal) String() string { return "replication-takeover" }
func (ReplicationTakeoverSignal) Error() string  { return "replication takeover" }
func (ReplicationTakeoverSignal) IsError() bool  { return false }

// PubSubQuitSignal signals that the pubsub loop should close after sending OK
type PubSubQuitSignal struct{}

func (PubSubQuitSignal) String() string { return "+OK\r\n" }

// MultiResponse carries multiple RESP responses for a single command (e.g. SUBSCRIBE ch1 ch2)
type MultiResponse struct {
	Responses []proto.RESP
}

func (m *MultiResponse) String() string {
	if len(m.Responses) == 0 {
		return ""
	}
	return m.Responses[0].String()
}

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
		// dual-timeline 边界修复：在 badger View 之前捕获 snapshotOffset。
		//
		// 序关系：store.Set()（badger commit）→ PropagateCommand()（offset 递增）
		// 因此所有 offset < snapshotOffset 的写入在 badger View 开始前已提交，
		// 一定在 MVCC 快照中可见 → 在 RDB 中。
		//
		// 所有 offset >= snapshotOffset 的写入可能/可能不在 RDB 中取决于
		// badger commit 时序，但它们在 backlog [snapshotOffset, currentOffset)
		// 中。副本应用 RDB + backlog 后达到 currentOffset 状态。
		//
		// 重复窗口（RDB 和 backlog 中都有的写入）仅限于 snapshotOffset 捕获
		// 到 View 开启之间（微秒级，通常 0 个并发写入）。
		//
		// 正确时序：
		//   [1] snapshotOffset = GetMasterReplOffset()（View 前捕获）
		//   [2] GenerateRDB（badger View，一致性快照）
		//   [3] FULLRESYNC 响应使用 snapshotOffset
		//   [4] 发送 RDB
		//   [5] 在 slaveConn.mu 保护下：AddSlave + 发送 backlog（snapshotOffset → currentOffset）
		//   [6] PropagateCommand 发送 currentOffset 之后的所有写入
		snapshotOffset := h.Replication.GetMasterReplOffset()

		rdbData, err := replication.GenerateRDB(h.Db)
		if err != nil {
			logger.Logger.Error().Err(err).Msg("生成RDB数据失败")
			return proto.NewError("ERR failed to generate RDB")
		}

		// 发送FULLRESYNC响应（使用 snapshotOffset）
		response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", result.ReplId, snapshotOffset)
		if _, err := writer.WriteString(response); err != nil {
			logger.Logger.Error().Err(err).Msg("发送FULLRESYNC失败")
			return proto.NewError("ERR failed to send FULLRESYNC")
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

		// 创建从节点连接并发送 backlog（snapshotOffset → currentOffset）
		slaveConn := replication.NewSlaveConnection(conn)
		backlog := h.Replication.GetBacklog()

		slaveConn.SetReady(true)

		// 在 slaveConn.mu 下原子化执行 AddSlave + 捕获 currentOffset。
		// 必须在 AddSlave 之后捕获 currentOffset，否则 PropagateCommand
		// 在捕获到 AddSlave 之间写入的数据会永久丢失。
		// I/O（SendBacklogData）在 Unlock 之后执行，避免锁住 sc.mu
		// 导致 CLIENT KILL → Close() 死锁。
		slaveConn.Lock()
		h.Replication.AddSlave(slaveConn)
		currentOffset := h.Replication.GetMasterReplOffset()
		slaveConn.SetReplOffset(currentOffset)
		slaveConn.Unlock()

		if currentOffset > snapshotOffset {
			if err := replication.SendBacklogData(slaveConn, backlog, snapshotOffset, currentOffset); err != nil {
				logger.Logger.Error().Err(err).
					Int64("snapshot_offset", snapshotOffset).
					Int64("current_offset", currentOffset).
					Msg("发送FULLRESYNC backlog数据失败")
			}
		}

		logger.Logger.Info().
			Str("slave_addr", remoteAddr).
			Str("repl_id", result.ReplId).
			Int64("snapshot_offset", snapshotOffset).
			Int64("current_offset", currentOffset).
			Int("rdb_size", len(rdbData)).
			Int64("backlog_range", currentOffset-snapshotOffset).
			Msg("发送FULLRESYNC, RDB和backlog到从节点")

		// 启动goroutine处理从节点的复制连接（接收REPLCONF ACK等）
		h.wg.Add(1)
		go h.handleSlaveReplicationConnection(h.Ctx, slaveConn)

		// 返回复制接管信号，主handler不会关闭连接
		return ReplicationTakeoverSignal{}
	} else {
		// 发送CONTINUE响应
		response := fmt.Sprintf("+CONTINUE %s\r\n", result.ReplId)
		if _, err := writer.WriteString(response); err != nil {
			logger.Logger.Error().Err(err).Msg("发送CONTINUE失败")
			return proto.NewError("ERR failed to send CONTINUE")
		}
		if err := writer.Flush(); err != nil {
			logger.Logger.Error().Err(err).Msg("刷新CONTINUE writer失败")
			return proto.NewError("ERR failed to flush CONTINUE")
		}

		// 创建从节点连接
		slaveConn := replication.NewSlaveConnection(conn)
		slaveConn.SetReplOffset(result.Offset)

		backlog := h.Replication.GetBacklog()

		// 先发送 backlog（[result.Offset, currentOffset)）再注册 slave。
		// 此时 slave 不在 ReplicationManager.slaves 中，PropagateCommand
		// 不会向其写入，因此无锁竞争。SendBacklogData 内部持有 sc.mu
		// 做 I/O，完成后立即释放。
		currentOffset := h.Replication.GetMasterReplOffset()
		if err := replication.SendBacklogData(slaveConn, backlog, result.Offset, currentOffset); err != nil {
			logger.Logger.Error().Err(err).
				Int64("start_offset", result.Offset).
				Int64("end_offset", currentOffset).
				Msg("发送CONTINUE backlog数据失败")
			return proto.NewError("ERR failed to send CONTINUE backlog")
		}

		slaveConn.SetReady(true)
		h.Replication.AddSlave(slaveConn)

		// 填充竞态缺口：currentOffset 捕获到 AddSlave 之间可能有多条
		// PropagateCommand 写入。这些写入的 offset >= currentOffset，
		// 不在已发送的 backlog 中。缺口通常为空或极窄（1-2 条命令）。
		afterOffset := h.Replication.GetMasterReplOffset()
		if afterOffset > currentOffset {
			if err := replication.SendBacklogData(slaveConn, backlog, currentOffset, afterOffset); err != nil {
				logger.Logger.Debug().Err(err).Msg("发送CONTINUE缺口backlog数据失败")
			}
		}

		logger.Logger.Info().
			Str("slave_addr", remoteAddr).
			Str("repl_id", result.ReplId).
			Int64("offset", result.Offset).
			Int64("current_offset", currentOffset).
			Msg("发送CONTINUE和backlog到从节点")

		// 启动goroutine处理从节点的复制连接
		h.wg.Add(1)
		go h.handleSlaveReplicationConnection(h.Ctx, slaveConn)

		// 返回复制接管信号
		return ReplicationTakeoverSignal{}
	}
}

// runPubSubLoop handles a connection that has entered PubSub mode via SUBSCRIBE/PSUBSCRIBE.
// It multiplexes between incoming subscription messages and PubSub commands.
func (h *Handler) runPubSubLoop(ctx context.Context, conn net.Conn, reader *bufio.Reader, writer *bufio.Writer, state *connState, remoteAddr string) {
	subscriber := state.subscriber
	if subscriber == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("进入 PubSub 模式")

	cmdCh := make(chan *proto.Array, 16)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			req, err := proto.ReadRESP(reader)
			if err != nil {
				select {
				case errCh <- err:
				case <-done:
				}
				return
			}
			select {
			case cmdCh <- req:
			case <-done:
				return
			}
		}
	}()

	flushTicker := time.NewTicker(100 * time.Millisecond)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("pubsub loop cancelled by context")
			return

		case msg, ok := <-subscriber.MessageCh:
			if !ok {
				return
			}
			resp := buildPubSubPush(msg, state.respVersion)
			if err := proto.WriteRESP(writer, resp); err != nil {
				return
			}

		case req := <-cmdCh:
			resp := h.processPubSubCommand(state, req, remoteAddr)
			switch r := resp.(type) {
			case *PubSubQuitSignal:
				_ = proto.WriteRESP(writer, proto.NewSimpleString("OK"))
				return
			case *MultiResponse:
				for _, subResp := range r.Responses {
					if err := proto.WriteRESP(writer, subResp); err != nil {
						return
					}
				}
			default:
				if err := proto.WriteRESP(writer, resp); err != nil {
					return
				}
			}

		case <-flushTicker.C:
			if n := writer.Buffered(); n > 0 {
				if err := writer.Flush(); err != nil {
					return
				}
				h.connsMu.Lock()
				m, tracked := h.conns[state]
				if tracked {
					m.outputBytes += int64(n)
				}
				h.connsMu.Unlock()
				if tracked && h.OutputBufferLimit > 0 && m.outputBytes > h.OutputBufferLimit {
					logger.Logger.Warn().
						Str("remote_addr", remoteAddr).
						Int64("output_bytes", m.outputBytes).
						Int64("limit", h.OutputBufferLimit).
						Msg("客户端输出缓冲区超限，断开连接")
					return
				}
			}

		case err := <-errCh:
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("pubsub read error")
			return
		}
	}
}

// buildPubSubPush constructs a RESP push message from a store.Message.
// For RESP3 (respVersion == 3), uses Push type; for RESP2, uses Array.
func buildPubSubPush(msg *store.Message, respVersion int) proto.RESP {
	var elems []proto.RESP
	if msg.Pattern != "" {
		elems = []proto.RESP{
			proto.NewBulkString([]byte("pmessage")),
			proto.NewBulkString([]byte(msg.Pattern)),
			proto.NewBulkString([]byte(msg.Channel)),
			proto.NewBulkString(msg.Data),
		}
	} else {
		elems = []proto.RESP{
			proto.NewBulkString([]byte("message")),
			proto.NewBulkString([]byte(msg.Channel)),
			proto.NewBulkString(msg.Data),
		}
	}
	if respVersion == 3 {
		return &proto.Push{Elems: elems}
	}
	return &proto.NestedArray{Elems: elems}
}

// makePushOrArray wraps elements in Push (RESP3) or NestedArray (RESP2)
func makePushOrArray(elems []proto.RESP, respVersion int) proto.RESP {
	if respVersion == 3 {
		return &proto.Push{Elems: elems}
	}
	return &proto.NestedArray{Elems: elems}
}

// processPubSubCommand handles commands received while in PubSub mode.
// Only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed.
func (h *Handler) processPubSubCommand(state *connState, req *proto.Array, remoteAddr string) proto.RESP {
	args := req.Args
	if len(args) == 0 {
		return proto.NewError("ERR no command")
	}
	cmd := strings.ToUpper(string(args[0]))

	switch cmd {
	case "SUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SUBSCRIBE' command")
		}
		channels := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			channels[i] = string(arg)
		}
		subscribed := h.PubSub.Subscribe(state.subscriber, channels...)
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(subscribed)),
		}
		for i, ch := range subscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("subscribe")),
				proto.NewBulkString([]byte(ch)),
				proto.NewInteger(int64(i + 1)),
			}, state.respVersion)
		}
		return resp

	case "PSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PSUBSCRIBE' command")
		}
		patterns := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			patterns[i] = string(arg)
		}
		subscribed := h.PubSub.PSubscribe(state.subscriber, patterns...)
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(subscribed)),
		}
		for i, p := range subscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("psubscribe")),
				proto.NewBulkString([]byte(p)),
				proto.NewInteger(int64(i + 1)),
			}, state.respVersion)
		}
		return resp

	case "UNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		var unsubscribed []string
		if len(args) > 1 {
			channels := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				channels[i] = string(arg)
			}
			unsubscribed = h.PubSub.Unsubscribe(state.subscriber, channels...)
		} else {
			unsubscribed = h.PubSub.Unsubscribe(state.subscriber)
		}
		if len(unsubscribed) == 0 {
			return makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("unsubscribe")),
				proto.NewBulkString([]byte("")),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(unsubscribed)),
		}
		for i, ch := range unsubscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("unsubscribe")),
				proto.NewBulkString([]byte(ch)),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		return resp

	case "PUNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		var unsubscribed []string
		if len(args) > 1 {
			patterns := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				patterns[i] = string(arg)
			}
			unsubscribed = h.PubSub.PUnsubscribe(state.subscriber, patterns...)
		} else {
			unsubscribed = h.PubSub.PUnsubscribe(state.subscriber)
		}
		if len(unsubscribed) == 0 {
			return makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("punsubscribe")),
				proto.NewBulkString([]byte("")),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(unsubscribed)),
		}
		for i, p := range unsubscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("punsubscribe")),
				proto.NewBulkString([]byte(p)),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		return resp

	case "PING":
		return proto.NewSimpleString("PONG")

	case "QUIT":
		return &PubSubQuitSignal{}

	default:
		return proto.NewError("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
	}
}

func (h *Handler) registerMonitorClient(state *connState) {
	h.monitorMu.Lock()
	defer h.monitorMu.Unlock()
	if h.monitorClients == nil {
		h.monitorClients = make(map[*connState]chan []byte)
	}
	ch := make(chan []byte, 1024)
	state.monitorCh = ch
	h.monitorClients[state] = ch
}

func (h *Handler) unregisterMonitorClient(state *connState) {
	h.monitorMu.Lock()
	defer h.monitorMu.Unlock()
	if ch, ok := h.monitorClients[state]; ok {
		close(ch)
		delete(h.monitorClients, state)
	}
}

func (h *Handler) broadcastToMonitors(cmd string, args [][]byte, remoteAddr string) {
	msg := formatMonitorMessage(cmd, args, remoteAddr)
	h.monitorMu.Lock()
	defer h.monitorMu.Unlock()
	for _, ch := range h.monitorClients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func formatMonitorMessage(cmd string, args [][]byte, remoteAddr string) []byte {
	now := time.Now()
	sec := now.Unix()
	usec := now.Nanosecond() / 1000
	timestamp := fmt.Sprintf("%d.%06d", sec, usec)

	var b strings.Builder
	b.WriteString("+")
	b.WriteString(timestamp)
	b.WriteString(" [0 ")
	b.WriteString(remoteAddr)
	b.WriteString("]")
	b.WriteString(" \"")
	b.WriteString(cmd)
	b.WriteString("\"")
	for _, arg := range args {
		b.WriteString(" \"")
		escaped := strings.ReplaceAll(string(arg), "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "\"", "\\\"")
		b.WriteString(escaped)
		b.WriteString("\"")
	}
	b.WriteString("\r\n")
	return []byte(b.String())
}

func (h *Handler) runMonitorLoop(conn net.Conn, writer *bufio.Writer, state *connState, remoteAddr string) {
	ch := state.monitorCh
	if ch == nil {
		return
	}

	logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("进入 MONITOR 模式")

	// 先发送 OK 响应
	if err := writer.Flush(); err != nil {
		return
	}

	reader := bufio.NewReader(conn)
	cmdCh := make(chan *proto.Array, 16)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			req, err := proto.ReadRESP(reader)
			if err != nil {
				select {
				case errCh <- err:
				case <-done:
				}
				return
			}
			select {
			case cmdCh <- req:
			case <-done:
				return
			}
		}
	}()

	flushTicker := time.NewTicker(100 * time.Millisecond)
	defer flushTicker.Stop()

	for {
		select {
		case <-state.ctx.Done():
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("monitor loop cancelled by context")
			return

		case msg, ok := <-ch:
			if !ok {
				return
			}
			if _, err := writer.Write(msg); err != nil {
				return
			}

		case req := <-cmdCh:
			resp := h.processMonitorCommand(req, remoteAddr)
			if _, isQuit := resp.(*PubSubQuitSignal); isQuit {
				_ = proto.WriteRESP(writer, proto.NewSimpleString("OK"))
				_ = writer.Flush()
				return
			}
			if err := proto.WriteRESP(writer, resp); err != nil {
				return
			}

		case <-flushTicker.C:
			if n := writer.Buffered(); n > 0 {
				if err := writer.Flush(); err != nil {
					return
				}
				h.connsMu.Lock()
				m, tracked := h.conns[state]
				if tracked {
					m.outputBytes += int64(n)
				}
				h.connsMu.Unlock()
				if tracked && h.OutputBufferLimit > 0 && m.outputBytes > h.OutputBufferLimit {
					logger.Logger.Warn().
						Str("remote_addr", remoteAddr).
						Int64("output_bytes", m.outputBytes).
						Int64("limit", h.OutputBufferLimit).
						Msg("客户端输出缓冲区超限，断开连接")
					return
				}
			}

		case err := <-errCh:
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("monitor read error")
			return
		}
	}
}

func (h *Handler) processMonitorCommand(req *proto.Array, remoteAddr string) proto.RESP {
	args := req.Args
	if len(args) == 0 {
		return proto.NewError("ERR no command")
	}
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "QUIT":
		return &PubSubQuitSignal{}
	case "PING":
		return proto.NewSimpleString("PONG")
	default:
		return proto.NewError("ERR only PING / QUIT allowed in this context")
	}
}

// handleSlaveReplicationConnection 处理从节点的复制连接
// 这个goroutine负责从节点连接的生命周期：
// 1. 接收 REPLCONF ACK 命令（从节点确认已接收的命令偏移量）
// 2. 保持连接打开，直到从节点断开
// 3. 负责关闭连接
func (h *Handler) handleSlaveReplicationConnection(ctx context.Context, slave *replication.SlaveConnection) {
	defer h.wg.Done()
	if ctx == nil {
		ctx = context.Background()
	}

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
		select {
		case <-ctx.Done():
			logger.Logger.Debug().
				Str("slave_id", slave.ID).
				Msg("取消从节点复制连接")
			return
		default:
		}

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
				offset, err := strconv.ParseInt(string(req.Args[2]), 10, 64)
				if err != nil {
					logger.Logger.Warn().
						Str("slave_id", slave.ID).
						Str("raw", string(req.Args[2])).
						Err(err).
						Msg("从节点 ACK 偏移量解析失败")
					continue
				}
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

// clientListRESP 返回 CLIENT LIST 的 RESP 响应
func (h *Handler) clientListRESP() proto.RESP {
	h.connsMu.RLock()
	defer h.connsMu.RUnlock()

	if len(h.conns) == 0 {
		return proto.NewBulkString([]byte("id=1 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 cmd=client events=r oFlags= keys=0"))
	}

	var lines []string
	now := time.Now()
	for state, meta := range h.conns {
		id := meta.id
		addr := meta.remoteAddr
		name := ""
		age := int(now.Sub(meta.created).Seconds())
		idle := 0

		state.mu.Lock()
		flags := "N"
		sub := 0
		psub := 0
		multi := -1
		if state.subscriber != nil {
			flags = "P"
			sub = len(state.subscriber.Channels)
			psub = len(state.subscriber.Patterns)
		} else if state.inTransaction {
			flags = "t"
		} else if state.blocking.Load() {
			flags = "b"
		}
		if state.inTransaction {
			multi = len(state.commands)
		}
		state.mu.Unlock()

		oFlags := ""
		if h.OutputBufferLimit > 0 && meta.outputBytes > h.OutputBufferLimit/2 {
			oFlags = ">"
		}
		line := fmt.Sprintf("id=%d addr=%s fd=0 name=%s age=%d idle=%d flags=%s db=0 sub=%d psub=%d multi=%d cmd=client events=r obl=0 oll=0 omem=%d oFlags=%s keys=0",
			id, addr, name, age, idle, flags, sub, psub, multi, meta.outputBytes, oFlags)
		lines = append(lines, line)
	}
	return proto.NewBulkString([]byte(strings.Join(lines, "\n")))
}

func wrapStoreError(err error) proto.RESP {
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return proto.NewError(fmt.Sprintf("ERR %v", err))
}

// targetRespError extracts a human-readable error string from a RESP response,
// or returns "" if the response indicates success (SimpleString "+OK").
func targetRespError(resp *proto.Array) string {
	if resp == nil || len(resp.Args) == 0 {
		return ""
	}
	msg := resp.Args[0]
	if msg == nil {
		return ""
	}
	firstWord := string(msg)
	if idx := strings.IndexByte(firstWord, ' '); idx > 0 {
		firstWord = firstWord[:idx]
	}
	if firstWord == "OK" {
		return ""
	}
	return string(msg)
}

func (h *Handler) executeCommand(state *connState, cmd string, args [][]byte, remoteAddr string) proto.RESP {
	if state == nil {
		return proto.NewError("ERR internal error: nil connState")
	}

	// 如果配置了密码，检查是否已认证
	if password := os.Getenv("BOLTDB_PASSWORD"); password != "" && !state.authenticated {
		switch cmd {
		case "AUTH", "PING", "QUIT", "COMMAND", "HELLO":
			// 这些命令可以绕过认证
		default:
			return proto.NewError("NOAUTH Authentication required.")
		}
	}

	// 如果在事务中（且不是事务控制命令），将命令加入队列
	if state.inTransaction {
		switch cmd {
		case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "PING", "QUIT":
			// 事务控制/连接命令不排队
		default:
			state.commands = append(state.commands, TransactionCommand{
				Command: cmd,
				Args:    args,
			})
			return proto.NewSimpleString("QUEUED")
		}
	}

	switch cmd {
	// 连接命令
	case "PING":
		return proto.NewSimpleString("PONG")

	case "COMMAND":
		return handleCommand(args)

	case "QUIT":
		state.cancel()
		return proto.NewSimpleString("OK")

	case "ROLE":
		return h.handleROLE(state, args, remoteAddr)

	case "ECHO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ECHO' command")
		}
		return proto.NewBulkString(args[0])

	case "ACL":
		return handleACL(args)

	case "CLIENT":
		return h.handleCLIENT(state, args, remoteAddr)
	case "SET":
		return h.handleSET(state, args, remoteAddr)

	case "GET":
		return h.handleGET(state, args, remoteAddr)

	case "GETDEL":
		return h.handleGETDEL(state, args, remoteAddr)

	case "GETEX":
		return h.handleGETEX(state, args, remoteAddr)

	case "SETEX":
		return h.handleSETEX(state, args, remoteAddr)

	case "PSETEX":
		return h.handlePSETEX(state, args, remoteAddr)

	case "SETNX":
		return h.handleSETNX(state, args, remoteAddr)

	case "GETSET":
		return h.handleGETSET(state, args, remoteAddr)

	case "MGET":
		return h.handleMGET(state, args, remoteAddr)

	case "MSET":
		return h.handleMSET(state, args, remoteAddr)

	case "MSETNX":
		return h.handleMSETNX(state, args, remoteAddr)

	case "INCR":
		return h.handleINCR(state, args, remoteAddr)

	case "INCRBY":
		return h.handleINCRBY(state, args, remoteAddr)

	case "DECR":
		return h.handleDECR(state, args, remoteAddr)

	case "DECRBY":
		return h.handleDECRBY(state, args, remoteAddr)

	case "INCRBYFLOAT":
		return h.handleINCRBYFLOAT(state, args, remoteAddr)

	case "APPEND":
		return h.handleAPPEND(state, args, remoteAddr)

	case "STRLEN":
		return h.handleSTRLEN(state, args, remoteAddr)

	case "SETBIT":
		return h.handleSETBIT(state, args, remoteAddr)

	case "GETBIT":
		return h.handleGETBIT(state, args, remoteAddr)

	case "BITCOUNT":
		return h.handleBITCOUNT(state, args, remoteAddr)

	case "BITOP":
		return h.handleBITOP(state, args, remoteAddr)

	case "BITFIELD":
		return h.handleBITFIELD(state, args, remoteAddr)

	case "BITPOS":
		return h.handleBITPOS(state, args, remoteAddr)

	case "BITLEN":
		return h.handleBITLEN(state, args, remoteAddr)

	case "GETRANGE":
		return h.handleGETRANGE(state, args, remoteAddr)

	case "SETRANGE":
		return h.handleSETRANGE(state, args, remoteAddr)

	// 通用键管理命令
	case "DEL":
		return h.handleDEL(state, args, remoteAddr)

	case "EXISTS":
		return h.handleEXISTS(state, args, remoteAddr)

	case "PFADD":
		return h.handlePFADD(state, args, remoteAddr)

	case "PFCOUNT":
		return h.handlePFCOUNT(state, args, remoteAddr)

	case "PFMERGE":
		return h.handlePFMERGE(state, args, remoteAddr)

	case "PFINFO":
		return h.handlePFINFO(state, args, remoteAddr)

	case "TYPE":
		return h.handleTYPE(state, args, remoteAddr)

	case "DUMP":
		return h.handleDUMP(state, args, remoteAddr)

	case "RESTORE":
		return h.handleRESTORE(state, args, remoteAddr)

	case "OBJECT":
		return h.handleOBJECT(state, args, remoteAddr)

	case "EXPIRE":
		return h.handleEXPIRE(state, args, remoteAddr)

	case "EXPIREAT":
		return h.handleEXPIREAT(state, args, remoteAddr)

	case "PEXPIRE":
		return h.handlePEXPIRE(state, args, remoteAddr)

	case "PEXPIREAT":
		return h.handlePEXPIREAT(state, args, remoteAddr)

	case "TTL":
		return h.handleTTL(state, args, remoteAddr)

	case "PTTL":
		return h.handlePTTL(state, args, remoteAddr)

	case "EXPIRETIME":
		return h.handleEXPIRETIME(state, args, remoteAddr)

	case "PEXPIRETIME":
		return h.handlePEXPIRETIME(state, args, remoteAddr)

	case "PERSIST":
		return h.handlePERSIST(state, args, remoteAddr)

	case "RENAME":
		return h.handleRENAME(state, args, remoteAddr)

	case "RENAMENX":
		return h.handleRENAMENX(state, args, remoteAddr)

	case "COPY":
		return h.handleCOPY(state, args, remoteAddr)

	case "SWAPDB":
		return h.handleSWAPDB(state, args, remoteAddr)

	case "TOUCH":
		return h.handleTOUCH(state, args, remoteAddr)

	case "SHUTDOWN":
		return h.handleSHUTDOWN(state, args, remoteAddr)

	case "KEYS":
		return h.handleKEYS(state, args, remoteAddr)

	case "SCAN":
		return h.handleSCAN(state, args, remoteAddr)

	case "RANDOMKEY":
		return h.handleRANDOMKEY(state, args, remoteAddr)
	case "LPUSH":
		return h.handleLPUSH(state, args, remoteAddr)

	case "RPUSH":
		return h.handleRPUSH(state, args, remoteAddr)

	case "LPOP":
		return h.handleLPOP(state, args, remoteAddr)

	case "RPOP":
		return h.handleRPOP(state, args, remoteAddr)

	case "LLEN":
		return h.handleLLEN(state, args, remoteAddr)

	case "LINDEX":
		return h.handleLINDEX(state, args, remoteAddr)

	case "LRANGE":
		return h.handleLRANGE(state, args, remoteAddr)

	case "LSET":
		return h.handleLSET(state, args, remoteAddr)

	case "LTRIM":
		return h.handleLTRIM(state, args, remoteAddr)

	case "LINSERT":
		return h.handleLINSERT(state, args, remoteAddr)

	case "LPOS":
		return h.handleLPOS(state, args, remoteAddr)

	case "LCS":
		return h.handleLCS(state, args, remoteAddr)

	case "LREM":
		return h.handleLREM(state, args, remoteAddr)

	case "RPOPLPUSH":
		return h.handleRPOPLPUSH(state, args, remoteAddr)

	case "LMOVE":
		return h.handleLMOVE(state, args, remoteAddr)

	case "BLMOVE":
		return h.handleBLMOVE(state, args, remoteAddr)

	case "LPUSHX":
		return h.handleLPUSHX(state, args, remoteAddr)

	case "RPUSHX":
		return h.handleRPUSHX(state, args, remoteAddr)

	case "BLPOP":
		return h.handleBLPOP(state, args, remoteAddr)

	case "BRPOP":
		return h.handleBRPOP(state, args, remoteAddr)

	case "BRPOPLPUSH":
		return h.handleBRPOPLPUSH(state, args, remoteAddr)
	case "HSET":
		return h.handleHSET(state, args, remoteAddr)

	case "HGET":
		return h.handleHGET(state, args, remoteAddr)

	case "HDEL":
		return h.handleHDEL(state, args, remoteAddr)

	case "HELLO":
		return h.handleHELLO(state, args, remoteAddr)

	case "HLEN":
		return h.handleHLEN(state, args, remoteAddr)

	case "HGETALL":
		return h.handleHGETALL(state, args, remoteAddr)

	case "HEXISTS":
		return h.handleHEXISTS(state, args, remoteAddr)

	case "HKEYS":
		return h.handleHKEYS(state, args, remoteAddr)

	case "HVALS":
		return h.handleHVALS(state, args, remoteAddr)

	case "HMSET":
		return h.handleHMSET(state, args, remoteAddr)

	case "HMGET":
		return h.handleHMGET(state, args, remoteAddr)

	case "HSETNX":
		return h.handleHSETNX(state, args, remoteAddr)

	case "HINCRBY":
		return h.handleHINCRBY(state, args, remoteAddr)

	case "HINCRBYFLOAT":
		return h.handleHINCRBYFLOAT(state, args, remoteAddr)

	case "HSTRLEN":
		return h.handleHSTRLEN(state, args, remoteAddr)

	case "HRANDFIELD":
		return h.handleHRANDFIELD(state, args, remoteAddr)
	case "SADD":
		return h.handleSADD(state, args, remoteAddr)

	case "SREM":
		return h.handleSREM(state, args, remoteAddr)

	case "SCARD":
		return h.handleSCARD(state, args, remoteAddr)

	case "SISMEMBER":
		return h.handleSISMEMBER(state, args, remoteAddr)

	case "SMEMBERS":
		return h.handleSMEMBERS(state, args, remoteAddr)

	case "SPOP":
		return h.handleSPOP(state, args, remoteAddr)

	case "SRANDMEMBER":
		return h.handleSRANDMEMBER(state, args, remoteAddr)

	case "SMOVE":
		return h.handleSMOVE(state, args, remoteAddr)

	case "SINTER":
		return h.handleSINTER(state, args, remoteAddr)

	case "SUNION":
		return h.handleSUNION(state, args, remoteAddr)

	case "SDIFF":
		return h.handleSDIFF(state, args, remoteAddr)

	case "SINTERSTORE":
		return h.handleSINTERSTORE(state, args, remoteAddr)

	case "SMISMEMBER":
		return h.handleSMISMEMBER(state, args, remoteAddr)

	case "SINTERCARD":
		return h.handleSINTERCARD(state, args, remoteAddr)

	case "SUNIONSTORE":
		return h.handleSUNIONSTORE(state, args, remoteAddr)

	case "SDIFFSTORE":
		return h.handleSDIFFSTORE(state, args, remoteAddr)

	case "SSCAN":
		return h.handleSSCAN(state, args, remoteAddr)
	case "HSCAN":
		return h.handleHSCAN(state, args, remoteAddr)
	case "ZADD":
		return h.handleZADD(state, args, remoteAddr)

	case "ZREM":
		return h.handleZREM(state, args, remoteAddr)

	case "ZREMRANGEBYRANK":
		return h.handleZREMRANGEBYRANK(state, args, remoteAddr)

	case "ZREMRANGEBYSCORE":
		return h.handleZREMRANGEBYSCORE(state, args, remoteAddr)

	case "ZPOPMAX":
		return h.handleZPOPMAX(state, args, remoteAddr)

	case "ZPOPMIN":
		return h.handleZPOPMIN(state, args, remoteAddr)

	case "BZPOPMAX":
		return h.handleBZPOPMAX(state, args, remoteAddr)

	case "BZPOPMIN":
		return h.handleBZPOPMIN(state, args, remoteAddr)

	case "ZCARD":
		return h.handleZCARD(state, args, remoteAddr)

	case "ZSCORE":
		return h.handleZSCORE(state, args, remoteAddr)

	case "ZRANK":
		return h.handleZRANK(state, args, remoteAddr)

	case "ZREVRANK":
		return h.handleZREVRANK(state, args, remoteAddr)

	case "ZCOUNT":
		return h.handleZCOUNT(state, args, remoteAddr)

	case "ZMSCORE":
		return h.handleZMSCORE(state, args, remoteAddr)

	case "ZRANGE":
		return h.handleZRANGE(state, args, remoteAddr)

	case "ZREVRANGE":
		return h.handleZREVRANGE(state, args, remoteAddr)

	case "ZRANGEBYSCORE":
		return h.handleZRANGEBYSCORE(state, args, remoteAddr)

	case "ZREVRANGEBYSCORE":
		return h.handleZREVRANGEBYSCORE(state, args, remoteAddr)

	case "ZINCRBY":
		return h.handleZINCRBY(state, args, remoteAddr)

	case "HRANDMEMBER":
		return h.handleHRANDMEMBER(state, args, remoteAddr)

	case "ZRANDMEMBER":
		return h.handleZRANDMEMBER(state, args, remoteAddr)

	case "LMPOP":
		return h.handleLMPOP(state, args, remoteAddr)

	case "ZMPOP":
		return h.handleZMPOP(state, args, remoteAddr)

	case "ZUNIONSTORE":
		return h.handleZUNIONSTORE(state, args, remoteAddr)

	case "ZINTERSTORE":
		return h.handleZINTERSTORE(state, args, remoteAddr)

	case "ZDIFFSTORE":
		return h.handleZDIFFSTORE(state, args, remoteAddr)

	case "ZDIFF":
		return h.handleZDIFF(state, args, remoteAddr)

	case "ZINTER":
		return h.handleZINTER(state, args, remoteAddr)

	case "ZUNION":
		return h.handleZUNION(state, args, remoteAddr)

	case "ZLEXCOUNT":
		return h.handleZLEXCOUNT(state, args, remoteAddr)

	case "ZRANGEBYLEX":
		return h.handleZRANGEBYLEX(state, args, remoteAddr)

	case "ZREVRANGEBYLEX":
		return h.handleZREVRANGEBYLEX(state, args, remoteAddr)

	case "ZREMRANGEBYLEX":
		return h.handleZREMRANGEBYLEX(state, args, remoteAddr)

	case "ZSCAN":
		return h.handleZSCAN(state, args, remoteAddr)

	case "ASKING":
		state.clusterAsking = true
		logger.Logger.Debug().Msg("收到 ASKING 命令")
		return proto.OK

	// Cluster命令
	case "CLUSTER":
		return h.handleCLUSTER(state, args, remoteAddr)
	case "CONFIG":
		return h.handleCONFIG(state, args, remoteAddr)

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
		return h.handleREPLCONF(state, args, remoteAddr)

	case "INFO":
		section := ""
		if len(args) >= 1 {
			section = strings.ToUpper(string(args[0]))
		}
		info := h.buildInfoResponse(section)
		return proto.NewBulkString([]byte(info))

	// 备份命令
	case "SAVE":
		return h.handleSAVE(state, args, remoteAddr)

	case "BGSAVE":
		return h.handleBGSAVE(state, args, remoteAddr)

	case "LASTSAVE":
		return h.handleLASTSAVE(state, args, remoteAddr)

	case "DBSIZE":
		return h.handleDBSIZE(state, args, remoteAddr)

	case "TIME":
		return h.handleTIME(state, args, remoteAddr)

	case "FLUSHDB":
		return h.handleFLUSHDB(state, args, remoteAddr)

	case "FLUSHALL":
		return h.handleFLUSHALL(state, args, remoteAddr)

	case "SELECT":
		return h.handleSELECT(state, args, remoteAddr)

	case "MOVE":
		return h.handleMOVE(state, args, remoteAddr)

	case "WAIT":
		return h.handleWAIT(state, args, remoteAddr)

	case "SLOWLOG":
		return h.handleSLOWLOG(state, args, remoteAddr)

	case "MEMORY":
		return h.handleMEMORY(state, args, remoteAddr)
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
		version := "redis.bolt." + Version
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
		return h.handleZRANGESTORE(state, args, remoteAddr)
	case "PUBLISH":
		return h.handlePUBLISH(state, args, remoteAddr)

	case "SUBSCRIBE":
		return h.handleSUBSCRIBE(state, args, remoteAddr)

	case "PSUBSCRIBE":
		return h.handlePSUBSCRIBE(state, args, remoteAddr)

	case "UNSUBSCRIBE":
		return h.handleUNSUBSCRIBE(state, args, remoteAddr)

	case "PUNSUBSCRIBE":
		return h.handlePUNSUBSCRIBE(state, args, remoteAddr)

	case "PUBSUB":
		return h.handlePUBSUB(state, args, remoteAddr)
	case "MULTI":
		return h.handleMULTI(state, args, remoteAddr)

	case "EXEC":
		return h.handleEXEC(state, args, remoteAddr)

	case "DISCARD":
		return h.handleDISCARD(state, args, remoteAddr)

	case "WATCH":
		return h.handleWATCH(state, args, remoteAddr)

	case "UNWATCH":
		return h.handleUNWATCH(state, args, remoteAddr)
	case "GEOADD":
		return h.handleGEOADD(state, args, remoteAddr)

	case "GEOPOS":
		return h.handleGEOPOS(state, args, remoteAddr)

	case "GEOHASH":
		return h.handleGEOHASH(state, args, remoteAddr)

	case "GEODIST":
		return h.handleGEODIST(state, args, remoteAddr)

	case "GEORADIUS":
		return h.handleGEORADIUS(state, args, remoteAddr)

	case "GEOSEARCH":
		return h.handleGEOSEARCH(state, args, remoteAddr)

	case "GEOSEARCHSTORE":
		return h.handleGEOSEARCHSTORE(state, args, remoteAddr)

	// ==================== Stream commands ====================
	case "XADD":
		return h.handleXADD(state, args, remoteAddr)

	case "XLEN":
		return h.handleXLEN(state, args, remoteAddr)

	case "XREAD":
		return h.handleXREAD(state, args, remoteAddr)

	case "XRANGE":
		return h.handleXRANGE(state, args, remoteAddr)

	case "XREVRANGE":
		return h.handleXREVRANGE(state, args, remoteAddr)

	case "XDEL":
		return h.handleXDEL(state, args, remoteAddr)

	case "XACK":
		return h.handleXACK(state, args, remoteAddr)

	case "XGROUP":
		return h.handleXGROUP(state, args, remoteAddr)

	case "XREADGROUP":
		return h.handleXREADGROUP(state, args, remoteAddr)

	case "XCLAIM":
		return h.handleXCLAIM(state, args, remoteAddr)

	case "XAUTOCLAIM":
		return h.handleXAUTOCLAIM(state, args, remoteAddr)

	case "XPENDING":
		return h.handleXPENDING(state, args, remoteAddr)

	case "XINFO":
		return h.handleXINFO(state, args, remoteAddr)

	case "XTRIM":
		return h.handleXTRIM(state, args, remoteAddr)

	// ==================== SORT ====================
	case "SORT":
		return h.handleSORT(state, args, remoteAddr)
	case "AUTH":
		return h.handleAUTH(state, args, remoteAddr)
	case "JSON.SET":
		return h.handleJSON_SET(state, args, remoteAddr)

	case "JSON.GET":
		return h.handleJSON_GET(state, args, remoteAddr)

	case "JSON.DEL":
		return h.handleJSON_DEL(state, args, remoteAddr)

	case "JSON.TYPE":
		return h.handleJSON_TYPE(state, args, remoteAddr)

	case "JSON.MGET":
		return h.handleJSON_MGET(state, args, remoteAddr)

	case "JSON.ARRAPPEND":
		return h.handleJSON_ARRAPPEND(state, args, remoteAddr)

	case "JSON.ARRLEN":
		return h.handleJSON_ARRLEN(state, args, remoteAddr)

	case "JSON.OBJKEYS":
		return h.handleJSON_OBJKEYS(state, args, remoteAddr)

	case "JSON.NUMINCRBY":
		return h.handleJSON_NUMINCRBY(state, args, remoteAddr)

	case "JSON.NUMMULTBY":
		return h.handleJSON_NUMMULTBY(state, args, remoteAddr)

	case "JSON.CLEAR":
		return h.handleJSON_CLEAR(state, args, remoteAddr)

	case "JSON.DEBUG":
		return h.handleJSON_DEBUG(state, args, remoteAddr)
	case "TS.CREATE":
		return h.handleTS_CREATE(state, args, remoteAddr)

	case "TS.ADD":
		return h.handleTS_ADD(state, args, remoteAddr)

	case "TS.GET":
		return h.handleTS_GET(state, args, remoteAddr)

	case "TS.RANGE":
		return h.handleTS_RANGE(state, args, remoteAddr)

	case "TS.DEL":
		return h.handleTS_DEL(state, args, remoteAddr)

	case "TS.INFO":
		return h.handleTS_INFO(state, args, remoteAddr)

	case "TS.LEN":
		return h.handleTS_LEN(state, args, remoteAddr)

	case "TS.MGET":
		return h.handleTS_MGET(state, args, remoteAddr)

	case "MIGRATE":
		return h.handleMIGRATE(state, args, remoteAddr)

	case "DEBUG":
		return h.handleDEBUG(state, args, remoteAddr)

	case "MONITOR":
		if len(args) > 0 {
			return proto.NewError("ERR wrong number of arguments for 'MONITOR' command")
		}
		state.mu.Lock()
		state.monitoring = true
		state.mu.Unlock()
		h.registerMonitorClient(state)
		return proto.OK

	default:
		if state.inTransaction {
			state.commands = append(state.commands, TransactionCommand{
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
