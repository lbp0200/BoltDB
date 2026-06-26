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
		// 返回角色信息，兼容 redis-sentinel
		// 格式: [master|slave|sentinel, master地址, 复制偏移量]
		// 对于主节点: ["master", "repl_offset"]
		// 对于从节点: ["slave", "master地址", master端口, 状态, 已同步偏移量]
		if h.Replication != nil {
			role := h.Replication.GetRole()
			if role == replication.RoleMaster {
				offset := h.Replication.GetMasterReplOffset()
				return &proto.Array{Args: [][]byte{
					[]byte(replication.RoleMaster),
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
					[]byte(replication.RoleSlave),
					[]byte(masterHost),
					[]byte(masterPort),
					[]byte("connected"),
					[]byte(strconv.FormatInt(offset, 10)),
				}}
			}
		}
		// 默认主节点
		return &proto.Array{Args: [][]byte{
			[]byte(replication.RoleMaster),
			[]byte("0"),
		}}

	case "ECHO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ECHO' command")
		}
		return proto.NewBulkString(args[0])

	case "ACL":
		return handleACL(args)

	case "CLIENT":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'CLIENT' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "LIST":
			return h.clientListRESP()
		case "GETNAME":
			if state.clientInfo != nil && state.clientInfo.Name != "" {
				return proto.NewBulkString([]byte(state.clientInfo.Name))
			}
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			nilResp := proto.NewBulkString(nil)
			return nilResp
		case "SETNAME":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT SETNAME' command")
			}
			name := string(args[1])
			if state.clientInfo == nil {
				state.clientInfo = &ClientInfo{}
			}
			state.clientInfo.Name = name
			return proto.OK
		case "ID":
			if state.clientInfo != nil && state.clientInfo.ID > 0 {
				return proto.NewInteger(state.clientInfo.ID)
			}
			return proto.NewInteger(1)
		case "KILL":
			// CLIENT KILL TYPE <type> — kill by connection type
			if len(args) >= 3 && strings.ToUpper(string(args[1])) == "TYPE" {
				killType := strings.ToUpper(string(args[2]))
				var killed int
				switch killType {
				case "SLAVE":
					if h.Replication == nil {
						return proto.NewInteger(0)
					}
					slaves := h.Replication.GetSlaves()
					for _, slave := range slaves {
						if err := slave.Close(); err != nil {
							logger.Logger.Debug().Err(err).Str("slave_id", slave.ID).Msg("close slave on CLIENT KILL TYPE slave")
						}
						h.Replication.RemoveSlave(slave.ID)
						killed++
					}
				case "BLOCKING":
					h.connsMu.RLock()
					var targets []struct {
						state *connState
						conn  net.Conn
					}
					for s, m := range h.conns {
						if s.blocking.Load() {
							targets = append(targets, struct {
								state *connState
								conn  net.Conn
							}{s, m.conn})
						}
					}
					h.connsMu.RUnlock()
					for _, t := range targets {
						t.state.mu.Lock()
						if t.state.cancel != nil {
							t.state.cancel()
						}
						t.state.mu.Unlock()
						if t.conn != nil {
							_ = t.conn.Close()
						}
					}
					killed = len(targets)
				case "NORMAL":
					h.connsMu.RLock()
					var targets []struct {
						state *connState
						conn  net.Conn
					}
					for s, m := range h.conns {
						targets = append(targets, struct {
							state *connState
							conn  net.Conn
						}{s, m.conn})
					}
					h.connsMu.RUnlock()
					for _, t := range targets {
						t.state.mu.Lock()
						if t.state.cancel != nil {
							t.state.cancel()
						}
						t.state.mu.Unlock()
						if t.conn != nil {
							_ = t.conn.Close()
						}
					}
					killed = len(targets)
				default:
					return proto.NewError(fmt.Sprintf("ERR unsupported CLIENT KILL TYPE '%s'", killType))
				}
				return proto.NewInteger(int64(killed))
			}

			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'CLIENT KILL' command")
			}
			addr := string(args[1])
			if addr == "" {
				return proto.NewError("ERR Invalid address")
			}

			h.connsMu.RLock()
			var targetState *connState
			var targetConn net.Conn
			for s, m := range h.conns {
				if m.remoteAddr == addr {
					targetState = s
					targetConn = m.conn
					break
				}
			}
			h.connsMu.RUnlock()

			if targetState == nil {
				return proto.NewInteger(0)
			}

			targetState.mu.Lock()
			if targetState.cancel != nil {
				targetState.cancel()
			}
			targetState.mu.Unlock()
			if targetConn != nil {
				_ = targetConn.Close()
			}
			return proto.NewInteger(1)
		case "PAUSE":
			logger.Logger.Warn().Msg("CLIENT PAUSE is not implemented (no-op)")
			return proto.OK
		case "UNPAUSE":
			logger.Logger.Warn().Msg("CLIENT UNPAUSE is not implemented (no-op)")
			return proto.OK
		case "INFO":
			addr := fmt.Sprintf("127.0.0.1:%d", h.Port)
			clientID := int64(0)
			fd := 0
			clientName := ""
			ci := state.clientInfo
			if ci != nil {
				if ci.Addr != "" {
					addr = ci.Addr
				}
				clientID = ci.ID
				fd = ci.FD
				clientName = ci.Name
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
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		h.markDirtyKeys(state, key)
		if err := h.Db.Set(key, value); err != nil {
			return wrapStoreError(err)
		}
		return proto.OK

	case "GET":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'GET' command")
		}
		key := string(args[0])
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		value, err := h.Db.Get(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(value))

	case "GETDEL":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'GETDEL' command")
		}
		gdKey := string(args[0])
		if resp := h.checkAndHandleRedirect(state, gdKey); resp != nil {
			return resp
		}
		gdValue, gdErr := h.Db.Get(gdKey)
		if gdErr != nil {
			if errors.Is(gdErr, store.ErrKeyNotFound) {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(gdErr, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", gdErr))
		}
		h.markDirtyKeys(state, gdKey)
		if _, err := h.Db.Del(gdKey); err != nil {
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(gdValue))

	case "GETEX":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'GETEX' command")
		}
		gexKey := string(args[0])
		if resp := h.checkAndHandleRedirect(state, gexKey); resp != nil {
			return resp
		}
		gexSeconds := 0
		if len(args) > 1 {
			opt := strings.ToUpper(string(args[1]))
			if opt == "EX" && len(args) > 2 {
				s, err := strconv.Atoi(string(args[2]))
				if err != nil {
					return proto.NewError("ERR value is not an integer or out of range")
				}
				gexSeconds = s
			} else if opt == "PX" && len(args) > 2 {
				s, err := strconv.Atoi(string(args[2]))
				if err != nil {
					return proto.NewError("ERR value is not an integer or out of range")
				}
				gexSeconds = s / 1000
			} else if opt == "PERSIST" {
				h.markDirtyKeys(state, gexKey)
				if _, err := h.Db.Persist(gexKey); err != nil {
					return wrapStoreError(err)
				}
			} else {
				return proto.NewError("ERR syntax error")
			}
		}
		gexValue, gexErr := h.Db.Get(gexKey)
		if gexErr != nil {
			if errors.Is(gexErr, store.ErrKeyNotFound) {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(gexErr, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", gexErr))
		}
		if gexSeconds > 0 {
			h.markDirtyKeys(state, gexKey)
			if _, err := h.Db.Expire(gexKey, gexSeconds); err != nil {
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
		}
		return proto.NewBulkString([]byte(gexValue))

	case "SETEX":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'SETEX' command")
		}
		key, value := string(args[0]), string(args[2])
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR invalid integer")
		}
		h.markDirtyKeys(state, key)
		if err := h.Db.SetEX(key, value, seconds); err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		if err := h.Db.PSETEX(key, value, milliseconds); err != nil {
			return wrapStoreError(err)
		}
		return proto.OK

	case "SETNX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SETNX' command")
		}
		key, value := string(args[0]), string(args[1])
		h.markDirtyKeys(state, key)
		success, err := h.Db.SetNX(key, value)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "GETSET":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'GETSET' command")
		}
		key, value := string(args[0]), string(args[1])
		h.markDirtyKeys(state, key)
		oldValue, err := h.Db.GetSet(key, value)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "INCR":
		return h.handleINCR(state, args, remoteAddr)

	case "INCRBY":
		return h.handleINCRBY(state, args, remoteAddr)

	case "DECR":
		return h.handleDECR(state, args, remoteAddr)

	case "DECRBY":
		return h.handleDECRBY(state, args, remoteAddr)

	case "INCRBYFLOAT":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'INCRBYFLOAT' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		increment, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		h.markDirtyKeys(state, key)
		value, err := h.Db.INCRBYFLOAT(key, increment)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))

	case "APPEND":
		return h.handleAPPEND(state, args, remoteAddr)

	case "STRLEN":
		return h.handleSTRLEN(state, args, remoteAddr)

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
		h.markDirtyKeys(state, key)
		newBit, err := h.Db.SetBit(key, int(offset), int(bit))
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, destKey)
		length, err := h.Db.BitOp(operation, destKey, sourceKeys...)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		length, err := h.Db.SetRange(key, offset, value)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, keys...)
		count := int64(0)
		for _, arg := range args {
			key := string(arg)
			deleted, err := h.Db.Del(key)
			if err != nil {
				return wrapStoreError(err)
			}
			count += deleted
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
			if err != nil {
				return wrapStoreError(err)
			}
			if exists {
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
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		elements := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			elements[i-1] = string(args[i])
		}
		h.markDirtyKeys(state, key)
		changed, err := h.Db.PFAdd(key, elements...)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
		if resp := h.checkAndHandleRedirect(state, destKey); resp != nil {
			return resp
		}
		h.markDirtyKeys(state, destKey)
		err := h.Db.PFMerge(destKey, sourceKeys...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.OK

	case "PFINFO":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PFINFO' command")
		}
		key := string(args[0])
		// 检查集群重定向
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		info, err := h.Db.PFInfo(key)
		if err != nil {
			return wrapStoreError(err)
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
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
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
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
				if len(args) < 3 {
					return proto.NewError("ERR wrong number of arguments for 'RESTORE' command")
				}
				// 序列化数据现在在 args[2]
				absttl := false
				for i := 3; i < len(args); i++ {
					upper := strings.ToUpper(string(args[i]))
					switch upper {
					case "REPLACE":
						replace = true
					case "ABSTTL":
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
					return wrapStoreError(err)
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
			return wrapStoreError(err)
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
				return wrapStoreError(err)
			}
			if refcount == 0 {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			return proto.NewInteger(refcount)
		case "ENCODING":
			encoding, err := h.Db.ObjectEncoding(key)
			if err != nil {
				return wrapStoreError(err)
			}
			if encoding == "" {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			return proto.NewBulkString([]byte(encoding))
		case "IDLETIME":
			idletime, err := h.Db.ObjectIdleTime(key)
			if err != nil {
				return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		success, err := h.Db.Expire(key, seconds)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		success, err := h.Db.ExpireAt(key, timestamp)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		success, err := h.Db.PExpire(key, milliseconds)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		success, err := h.Db.PExpireAt(key, timestamp)
		if err != nil {
			return wrapStoreError(err)
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

	case "EXPIRETIME":
		return h.handleExpireTime(state, args, remoteAddr)

	case "PEXPIRETIME":
		return h.handlePExpireTime(state, args, remoteAddr)

	case "PERSIST":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PERSIST' command")
		}
		key := string(args[0])
		h.markDirtyKeys(state, key)
		success, err := h.Db.Persist(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(boolToInt(success)))

	case "RENAME":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RENAME' command")
		}
		key, newKey := string(args[0]), string(args[1])
		h.markDirtyKeys(state, key, newKey)
		if err := h.Db.Rename(key, newKey); err != nil {
			return wrapStoreError(err)
		}
		return proto.OK

	case "RENAMENX":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RENAMENX' command")
		}
		key, newKey := string(args[0]), string(args[1])
		h.markDirtyKeys(state, key, newKey)
		success, err := h.Db.RenameNX(key, newKey)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
		}
		if srcType == "none" {
			return proto.NewInteger(0) // 源键不存在
		}
		// 检查目标键是否存在
		dstExists, err := h.Db.Exists(dstKey)
		if err != nil {
			return wrapStoreError(err)
		}
		if dstExists && !replace {
			return proto.NewInteger(0) // 目标存在且不替换
		}
		h.markDirtyKeys(state, srcKey, dstKey)
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
		count := int64(0)
		for _, arg := range args {
			key := string(arg)
			exists, err := h.Db.Exists(key)
			if err != nil {
				return wrapStoreError(err)
			}
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
		}
		// 返回嵌套数组格式: [cursor, [key1, key2, ...]]
		return proto.NewScanResponse(result.Cursor, result.Keys)

	case "RANDOMKEY":
		key, err := h.Db.RandomKey()
		if err != nil || key == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.LPush(key, values...)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.RPush(key, values...)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if value == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	case "RPOP":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'RPOP' command")
		}
		key := string(args[0])
		value, err := h.Db.RPop(key)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if value == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if value == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		if err := h.Db.LSet(key, index, value); err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		if err := h.Db.LTrim(key, start, stop); err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.LInsert(key, where, pivot, value)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
		}

		if len(positions) == 0 {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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

	case "LCS":
		return h.handleLCS(state, args, remoteAddr)

	case "LREM":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LREM' command")
		}
		key, value := string(args[0]), string(args[2])
		count, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		h.markDirtyKeys(state, key)
		removed, err := h.Db.LRem(key, count, value)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(removed))

	case "RPOPLPUSH":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'RPOPLPUSH' command")
		}
		source, destination := string(args[0]), string(args[1])
		h.markDirtyKeys(state, source, destination)
		value, err := h.Db.RPopLPush(source, destination)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if value == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
		h.markDirtyKeys(state, source, destination)
		value, err := h.Db.LMove(source, destination, sourceDirection, destinationDirection)
		if err != nil {
			return wrapStoreError(err)
		}
		if value == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
		h.markDirtyKeys(state, source, destination)
		state.blocking.Store(true)
		value, err := h.Db.BLMoveBlocking(state.ctx, source, destination, sourceDirection, destinationDirection, timeout)
		state.blocking.Store(false)
		if err != nil {
			return wrapStoreError(err)
		}
		if value == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.LPUSHX(key, values...)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.RPUSHX(key, values...)
		if err != nil {
			return wrapStoreError(err)
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
		state.blocking.Store(true)
		key, value, err := h.Db.BLPOPBlocking(state.ctx, keys, timeout)
		state.blocking.Store(false)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NilArray{}
		}
		if key == "" {
			return proto.NilArray{}
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
		state.blocking.Store(true)
		key, value, err := h.Db.BRPOPBlocking(state.ctx, keys, timeout)
		state.blocking.Store(false)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NilArray{}
		}
		if key == "" {
			return proto.NilArray{}
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
		state.blocking.Store(true)
		value, err := h.Db.BRPOPLPUSHBlocking(state.ctx, source, destination, timeout)
		state.blocking.Store(false)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if value == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(value))

	// Hash命令
	case "HSET":
		return h.handleHSET(state, args, remoteAddr)

	case "HGET":
		return h.handleHGET(state, args, remoteAddr)

	case "HDEL":
		return h.handleHDEL(state, args, remoteAddr)

	case "HELLO":
		protoLevel := 2
		if len(args) >= 1 {
			level, err := strconv.Atoi(string(args[0]))
			if err != nil || level < 2 || level > 3 {
				return proto.NewError("ERR Protocol version is not supported")
			}
			protoLevel = level
		}
		state.respVersion = protoLevel
		role := "master"
		if h.Replication != nil {
			role = h.Replication.GetRole()
		}
		mode := "standalone"
		if h.Cluster != nil {
			mode = "cluster"
		}
		id := int64(0)
		if state.clientInfo != nil {
			id = state.clientInfo.ID
		}
		elements := []proto.RESP{
			proto.NewBulkString([]byte("server")),
			proto.NewBulkString([]byte("boltdb")),
			proto.NewBulkString([]byte("version")),
			proto.NewBulkString([]byte(Version)),
			proto.NewBulkString([]byte("proto")),
			proto.NewInteger(int64(protoLevel)),
			proto.NewBulkString([]byte("id")),
			proto.NewInteger(id),
			proto.NewBulkString([]byte("mode")),
			proto.NewBulkString([]byte(mode)),
			proto.NewBulkString([]byte("role")),
			proto.NewBulkString([]byte(role)),
			proto.NewBulkString([]byte("modules")),
			&proto.NestedArray{Elems: []proto.RESP{}},
		}
		if protoLevel == 3 {
			return &proto.Map{Elems: elements}
		}
		return &proto.NestedArray{Elems: elements}

	case "HLEN":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HLEN' command")
		}
		key := string(args[0])
		length, err := h.Db.HLen(key)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		for i := 1; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			field, value := string(args[i]), string(args[i+1])
			if err := h.Db.HSet(key, field, value); err != nil {
				return wrapStoreError(err)
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
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		success, err := h.Db.HSetNX(key, field, value)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		value, err := h.Db.HIncrBy(key, field, increment)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		value, err := h.Db.HIncrByFloat(key, field, increment)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))

	case "HSTRLEN":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HSTRLEN' command")
		}
		key, field := string(args[0]), string(args[1])
		length, err := h.Db.HStrLen(key, field)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.SAdd(key, members...)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
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

		if len(args) >= 2 {
			count, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			members, err := h.Db.SPopN(key, count)
			if err != nil {
				return wrapStoreError(err)
			}
			if len(members) == 0 {
				return &proto.Array{Args: [][]byte{}}
			}
			h.markDirtyKeys(state, key)
			if h.Replication != nil && h.Replication.IsMaster() {
				propArgs := make([][]byte, 2, 2+len(members))
				propArgs[0] = []byte("SREM")
				propArgs[1] = args[0]
				for _, m := range members {
					propArgs = append(propArgs, []byte(m))
				}
				h.Replication.PropagateCommand(propArgs)
			}
			results := make([][]byte, len(members))
			for i, m := range members {
				results[i] = []byte(m)
			}
			return &proto.Array{Args: results}
		}

		member, err := h.Db.SPop(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if member == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		h.markDirtyKeys(state, key)
		if h.Replication != nil && h.Replication.IsMaster() {
			h.Replication.PropagateCommand([][]byte{[]byte("SREM"), args[0], []byte(member)})
		}
		return proto.NewBulkString([]byte(member))

	case "SRANDMEMBER":
		key := string(args[0])
		if len(args) == 1 {
			// SRANDMEMBER key - return single member
			member, err := h.Db.SRandMember(key)
			if err != nil {
				return wrapStoreError(err)
			}
			if member == "" {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, destination)
		count, err := h.Db.SInterStore(destination, keys...)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
		}
		elems := make([]proto.RESP, len(results))
		for i, v := range results {
			elems[i] = proto.Integer(v)
		}
		return &proto.NestedArray{Elems: elems}

	case "SINTERCARD":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SINTERCARD' command")
		}
		numkeys, err := strconv.Atoi(string(args[0]))
		if err != nil || numkeys < 1 || numkeys > len(args)-1 {
			return proto.NewError("ERR wrong number of arguments for 'SINTERCARD' command")
		}
		sinterKeys := make([]string, numkeys)
		for i := 0; i < numkeys; i++ {
			sinterKeys[i] = string(args[i+1])
		}
		count, err := h.Db.SInterCard(sinterKeys...)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, destination)
		count, err := h.Db.SUnionStore(destination, keys...)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, destination)
		count, err := h.Db.SDiffStore(destination, keys...)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
		}
		// 返回格式: [cursor, [member1, member2, ...]]
		memberElems := make([]proto.RESP, len(result.Members))
		for i, m := range result.Members {
			memberElems[i] = proto.NewBulkString([]byte(m))
		}
		return &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(strconv.FormatUint(result.Cursor, 10))),
				&proto.NestedArray{Elems: memberElems},
			},
		}

	// ==================== HSCAN ====================
	case "HSCAN":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'HSCAN' command")
		}
		hscanKey := string(args[0])
		hscanCursor, err := strconv.ParseUint(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		hscanPattern := ""
		hscanCount := 10
		if len(args) > 2 {
			for i := 2; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "MATCH" && i+1 < len(args) {
					hscanPattern = string(args[i+1])
					i++
				} else if opt == "COUNT" && i+1 < len(args) {
					hscanCount, err = strconv.Atoi(string(args[i+1]))
					if err != nil {
						return proto.NewError("ERR value is not an integer")
					}
					i++
				}
			}
		}
		hscanResult, err := h.Db.HScan(hscanKey, hscanCursor, hscanPattern, hscanCount)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		fieldElems := make([]proto.RESP, 0, len(hscanResult.Fields)*2)
		for fieldName, fieldVal := range hscanResult.Fields {
			fieldElems = append(fieldElems, proto.NewBulkString([]byte(fieldName)))
			fieldElems = append(fieldElems, proto.NewBulkString(fieldVal))
		}
		return &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(strconv.FormatUint(hscanResult.Cursor, 10))),
				&proto.NestedArray{Elems: fieldElems},
			},
		}

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
		h.markDirtyKeys(state, key)
		if err := h.Db.ZAdd(key, members); err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(len(members)))

	case "ZREM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZREM' command")
		}
		key := string(args[0])
		h.markDirtyKeys(state, key)
		count := 0
		for i := 1; i < len(args); i++ {
			member := string(args[i])
			deleted, err := h.Db.ZRem(key, member)
			if err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				if errors.Is(err, store.ErrMemberNotFound) {
					continue
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			count += int(deleted)
		}
		return proto.NewInteger(int64(count))

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
		h.markDirtyKeys(state, key)
		count, err := h.Db.ZRemRangeByRank(key, start, stop)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.ZRemRangeByScore(key, min, max, minExclusive, maxExclusive)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		members, err := h.Db.ZPopMax(key, count)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		members, err := h.Db.ZPopMin(key, count)
		if err != nil {
			return wrapStoreError(err)
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
		state.blocking.Store(true)
		key, member, err := h.Db.BZPopMaxBlocking(state.ctx, keys, timeout)
		state.blocking.Store(false)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NilArray{}
		}
		if key == "" {
			return proto.NilArray{}
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
		state.blocking.Store(true)
		key, member, err := h.Db.BZPopMinBlocking(state.ctx, keys, timeout)
		state.blocking.Store(false)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NilArray{}
		}
		if key == "" {
			return proto.NilArray{}
		}
		return &proto.Array{Args: [][]byte{[]byte(key), []byte(member.Member), []byte(fmt.Sprintf("%.10g", member.Score))}}

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
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(int64(count))

	case "ZSCORE":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZSCORE' command")
		}
		key, member := string(args[0]), string(args[1])
		score, exists, err := h.Db.ZScore(key, member)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if !exists {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))

	case "ZRANK":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANK' command")
		}
		key, member := string(args[0]), string(args[1])
		rank, err := h.Db.ZRank(key, member)
		if err != nil {
			return wrapStoreError(err)
		}
		if rank < 0 {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
			return wrapStoreError(err)
		}
		if rank < 0 {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return proto.NewInteger(rank)

	case "ZCOUNT":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZCOUNT' command")
		}
		key := string(args[0])
		minScore, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		maxScore, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		count, err := h.Db.ZCount(key, minScore, maxScore)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return proto.NewInteger(0)
			}
			return wrapStoreError(err)
		}
		return proto.NewInteger(count)

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
			return wrapStoreError(err)
		}
		results := make([][]byte, len(scores))
		for i, s := range scores {
			results[i] = []byte(strconv.FormatFloat(s, 'f', -1, 64))
		}
		return &proto.Array{Args: results}

	case "ZRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGE' command")
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
		withScores := false
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "WITHSCORES" {
				withScores = true
			}
		}
		members, err := h.Db.ZRange(key, start, stop)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return &proto.Array{Args: [][]byte{}}
			}
			return wrapStoreError(err)
		}
		if withScores {
			results := make([][]byte, 0, len(members)*2)
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
			}
			return &proto.Array{Args: results}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m.Member)
		}
		return &proto.Array{Args: results}

	case "ZREVRANGE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGE' command")
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
		withScores := false
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "WITHSCORES" {
				withScores = true
			}
		}
		members, err := h.Db.ZRevRange(key, start, stop)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return &proto.Array{Args: [][]byte{}}
			}
			return wrapStoreError(err)
		}
		if withScores {
			results := make([][]byte, 0, len(members)*2)
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
			}
			return &proto.Array{Args: results}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m.Member)
		}
		return &proto.Array{Args: results}

	case "ZRANGEBYSCORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANGEBYSCORE' command")
		}
		zsetName := string(args[0])
		minScore, minExclusive, err := parseScoreExclusive(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		maxScore, maxExclusive, err := parseScoreExclusive(string(args[2]))
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		offset := 0
		count := -1
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
				break
			}
		}
		members, err := h.Db.ZRangeByScore(zsetName, minScore, maxScore, offset, count, minExclusive, maxExclusive)
		if err != nil {
			return wrapStoreError(err)
		}
		if len(members) == 0 {
			return &proto.Array{Args: [][]byte{}}
		}
		withScores := false
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "WITHSCORES" {
				withScores = true
				break
			}
		}
		if withScores {
			results := make([][]byte, 0, len(members)*2)
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
			}
			return &proto.Array{Args: results}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m.Member)
		}
		return &proto.Array{Args: results}

	case "ZREVRANGEBYSCORE":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZREVRANGEBYSCORE' command")
		}
		zsetName := string(args[0])
		maxScore, maxExclusive, err := parseScoreExclusive(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		minScore, minExclusive, err := parseScoreExclusive(string(args[2]))
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		offset := 0
		count := -1
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
				break
			}
		}
		members, err := h.Db.ZRevRangeByScore(zsetName, maxScore, minScore, offset, count, minExclusive, maxExclusive)
		if err != nil {
			return wrapStoreError(err)
		}
		if len(members) == 0 {
			return &proto.Array{Args: [][]byte{}}
		}
		withScores := false
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(string(args[i])) == "WITHSCORES" {
				withScores = true
				break
			}
		}
		if withScores {
			results := make([][]byte, 0, len(members)*2)
			for _, m := range members {
				results = append(results, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
			}
			return &proto.Array{Args: results}
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m.Member)
		}
		return &proto.Array{Args: results}

	case "ZINCRBY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZINCRBY' command")
		}
		key, member := string(args[0]), string(args[2])
		delta, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		h.markDirtyKeys(state, key)
		newScore, err := h.Db.ZIncrBy(key, member, delta)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(newScore, 'f', -1, 64)))

	case "HRANDMEMBER":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'HRANDMEMBER' command")
		}
		key := string(args[0])
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		count := 0
		withValues := false
		if len(args) >= 2 {
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			count = c
		}
		for i := 2; i < len(args); i++ {
			if strings.EqualFold(string(args[i]), "WITHVALUES") {
				withValues = true
			}
		}
		entries, err := h.Db.HRandMember(key, count)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if len(entries) == 0 {
			if count == 0 {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			return &proto.Array{Args: [][]byte{}}
		}
		if count == 0 && !withValues {
			return proto.NewBulkString([]byte(entries[0].Field))
		}
		if count == 0 && withValues {
			return &proto.Array{Args: [][]byte{
				[]byte(entries[0].Field),
				entries[0].Value,
			}}
		}
		if withValues {
			result := make([][]byte, 0, len(entries)*2)
			for _, e := range entries {
				result = append(result, []byte(e.Field), e.Value)
			}
			return &proto.Array{Args: result}
		}
		result := make([][]byte, len(entries))
		for i, e := range entries {
			result[i] = []byte(e.Field)
		}
		return &proto.Array{Args: result}

	case "ZRANDMEMBER":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'ZRANDMEMBER' command")
		}
		key := string(args[0])
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		count := 0
		withScores := false
		if len(args) >= 2 {
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			count = c
		}
		for i := 2; i < len(args); i++ {
			if strings.EqualFold(string(args[i]), "WITHSCORES") {
				withScores = true
			}
		}
		members, err := h.Db.ZRandMember(key, count)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if len(members) == 0 {
			if count == 0 {
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			return &proto.Array{Args: [][]byte{}}
		}
		if count == 0 && !withScores {
			return proto.NewBulkString([]byte(members[0].Member))
		}
		if count == 0 && withScores {
			return &proto.Array{Args: [][]byte{
				[]byte(members[0].Member),
				[]byte(strconv.FormatFloat(members[0].Score, 'f', -1, 64)),
			}}
		}
		if withScores {
			result := make([][]byte, 0, len(members)*2)
			for _, m := range members {
				result = append(result, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
			}
			return &proto.Array{Args: result}
		}
		result := make([][]byte, len(members))
		for i, m := range members {
			result[i] = []byte(m.Member)
		}
		return &proto.Array{Args: result}

	case "LMPOP":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'LMPOP' command")
		}
		numKeys, kErr := strconv.Atoi(string(args[0]))
		if kErr != nil || numKeys < 1 || 1+numKeys+1 > len(args) {
			return proto.NewError("ERR syntax error")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[1+i])
		}
		modifier := strings.ToUpper(string(args[1+numKeys]))
		if modifier != "LEFT" && modifier != "RIGHT" {
			return proto.NewError("ERR syntax error")
		}
		count := 1
		if len(args) >= 3+numKeys {
			if strings.ToUpper(string(args[2+numKeys])) == "COUNT" {
				if len(args) < 4+numKeys {
					return proto.NewError("ERR syntax error")
				}
				c, cErr := strconv.Atoi(string(args[3+numKeys]))
				if cErr != nil || c < 1 {
					return proto.NewError("ERR value is not an integer or out of range")
				}
				count = c
			}
		}
		key, elements, err := h.Db.LMPop(keys, modifier, count)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if key == "" || len(elements) == 0 {
			return proto.NilArray{}
		}
		elemArgs := make([][]byte, len(elements))
		for i, e := range elements {
			elemArgs[i] = []byte(e)
		}
		return &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(key)),
				&proto.Array{Args: elemArgs},
			},
		}

	case "ZMPOP":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'ZMPOP' command")
		}
		numKeys, kErr := strconv.Atoi(string(args[0]))
		if kErr != nil || numKeys < 1 || 1+numKeys+1 > len(args) {
			return proto.NewError("ERR syntax error")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[1+i])
		}
		modifier := strings.ToUpper(string(args[1+numKeys]))
		if modifier != "MIN" && modifier != "MAX" {
			return proto.NewError("ERR syntax error")
		}
		count := 1
		if len(args) >= 3+numKeys {
			if strings.ToUpper(string(args[2+numKeys])) == "COUNT" {
				if len(args) < 4+numKeys {
					return proto.NewError("ERR syntax error")
				}
				c, cErr := strconv.Atoi(string(args[3+numKeys]))
				if cErr != nil || c < 1 {
					return proto.NewError("ERR value is not an integer or out of range")
				}
				count = c
			}
		}
		key, members, err := h.Db.ZMPop(keys, modifier, count)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if key == "" || len(members) == 0 {
			return proto.NilArray{}
		}
		result := make([][]byte, 0, 1+len(members)*2)
		result = append(result, []byte(key))
		for _, m := range members {
			result = append(result, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return &proto.Array{Args: result}

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
		h.markDirtyKeys(state, destination)
		count, err := h.Db.ZUnionStore(destination, keys, weights, aggregate)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, destination)
		count, err := h.Db.ZInterStore(destination, keys, weights, aggregate)
		if err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, destination)
		count, err := h.Db.ZDiffStore(destination, keys)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(count)

	case "ZDIFF":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'ZDIFF' command")
		}
		numKeys, err := strconv.Atoi(string(args[0]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		if numKeys < 1 {
			return proto.NewError("ERR syntax error")
		}
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = string(args[1+i])
		}
		withScores := false
		for i := 1 + numKeys; i < len(args); i++ {
			if strings.EqualFold(string(args[i]), "WITHSCORES") {
				withScores = true
			}
		}
		members, err := h.Db.ZDiff(keys)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if len(members) == 0 {
			return &proto.Array{Args: [][]byte{}}
		}
		if withScores {
			result := make([][]byte, 0, len(members)*2)
			for _, m := range members {
				result = append(result, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
			}
			return &proto.Array{Args: result}
		}
		result := make([][]byte, len(members))
		for i, m := range members {
			result[i] = []byte(m.Member)
		}
		return &proto.Array{Args: result}

	case "ZINTER":
		return h.handleZINTER(state, args, remoteAddr)

	case "ZUNION":
		return h.handleZUNION(state, args, remoteAddr)

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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, zSetName)
		removed, err := h.Db.ZRemRangeByLex(zSetName, min, max)
		if err != nil {
			return wrapStoreError(err)
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
			return wrapStoreError(err)
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

	case "ASKING":
		state.clusterAsking = true
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
		case [][]interface{}:
			// 对于CLUSTER SLOTS，槽位信息
			// 格式：[[startSlot, endSlot, [host, port, nodeId]], ...]
			slotsResp := make([]proto.RESP, len(v))
			for i, slotEntry := range v {
				entry := make([]proto.RESP, len(slotEntry))
				for j, item := range slotEntry {
					if sub, ok := item.([]interface{}); ok {
						subEntry := make([]proto.RESP, len(sub))
						for k, subItem := range sub {
							subEntry[k] = proto.NewBulkString([]byte(fmt.Sprintf("%v", subItem)))
						}
						entry[j] = &proto.NestedArray{Elems: subEntry}
					} else {
						entry[j] = proto.NewBulkString([]byte(fmt.Sprintf("%v", item)))
					}
				}
				slotsResp[i] = &proto.NestedArray{Elems: entry}
			}
			return &proto.NestedArray{Elems: slotsResp}

		case []interface{}:
			entries := make([]proto.RESP, len(v))
			for i, item := range v {
				if sub, ok := item.([]interface{}); ok {
					subEntry := make([]proto.RESP, len(sub))
					for k, subItem := range sub {
						subEntry[k] = proto.NewBulkString([]byte(fmt.Sprintf("%v", subItem)))
					}
					entries[i] = &proto.NestedArray{Elems: subEntry}
				} else {
					entries[i] = proto.NewBulkString([]byte(fmt.Sprintf("%v", item)))
				}
			}
			return &proto.NestedArray{Elems: entries}

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
		bgCtx := h.Ctx
		if bgCtx == nil {
			bgCtx = context.Background()
		}
		if err := h.Backup.BGSave(bgCtx); err != nil {
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
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(len(keys)))

	case "TIME":
		sec, usec, err := h.Db.Time()
		if err != nil {
			return wrapStoreError(err)
		}
		return &proto.Array{Args: [][]byte{
			[]byte(fmt.Sprintf("%d", sec)),
			[]byte(fmt.Sprintf("%d", usec)),
		}}

	case "FLUSHDB":
		err := h.Db.FlushDB()
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.OK

	case "FLUSHALL":
		err := h.Db.FlushDB()
		if err != nil {
			return wrapStoreError(err)
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
		// Return number of connected slaves (simplified WAIT — does not
		// block for acknowledgement, reports current count immediately).
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'WAIT' command")
		}
		count := 0
		if h.Replication != nil {
			count = h.Replication.GetSlaveCount()
		}
		return proto.NewInteger(int64(count))

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
					if state.respVersion == 3 {
						return &proto.Null{}
					}
					return proto.NewBulkString(nil)
				}
				return wrapStoreError(err)
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
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
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
				return wrapStoreError(lexErr)
			}
			// Convert []string to []ZSetMember (score=0 for all)
			members = make([]store.ZSetMember, len(lexMembers))
			for i, m := range lexMembers {
				members[i] = store.ZSetMember{Member: m, Score: 0}
			}
		} else if byScore {
			members, err = h.Db.ZRangeByScore(srcKey, minScore, maxScore, int(limitOffset), int(limitCount), false, false)
			if err != nil {
				return wrapStoreError(err)
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
				return wrapStoreError(rangeErr)
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
		if _, err := h.Db.Del(dstKey); err != nil {
			return wrapStoreError(err)
		}

		// Add members to destination
		if len(members) > 0 {
			err = h.Db.ZAdd(dstKey, members)
			if err != nil {
				return wrapStoreError(err)
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
		channels := make([]string, len(args))
		for i, arg := range args {
			channels[i] = string(arg)
		}
		state.mu.Lock()
		if state.subscriber == nil {
			state.subscriber = store.NewSubscriber(fmt.Sprintf("%s:%d", remoteAddr, time.Now().UnixNano()))
		}
		state.mu.Unlock()
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
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'PSUBSCRIBE' command")
		}
		patterns := make([]string, len(args))
		for i, arg := range args {
			patterns[i] = string(arg)
		}
		state.mu.Lock()
		if state.subscriber == nil {
			state.subscriber = store.NewSubscriber(fmt.Sprintf("%s:%d", remoteAddr, time.Now().UnixNano()))
		}
		state.mu.Unlock()
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
		if state.subscriber == nil {
			// Not in pubsub mode, return empty confirmation
			channel := ""
			if len(args) >= 1 {
				channel = string(args[0])
			}
			return makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("unsubscribe")),
				proto.NewBulkString([]byte(channel)),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		var unsubscribed []string
		if len(args) >= 1 {
			channels := make([]string, len(args))
			for i, arg := range args {
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
		if state.subscriber == nil {
			// Not in pubsub mode, return empty confirmation
			pattern := ""
			if len(args) >= 1 {
				pattern = string(args[0])
			}
			return makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("punsubscribe")),
				proto.NewBulkString([]byte(pattern)),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		var unsubscribed []string
		if len(args) >= 1 {
			patterns := make([]string, len(args))
			for i, arg := range args {
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
		if state.inTransaction {
			return proto.NewError("ERR MULTI calls can not be nested")
		}
		state.inTransaction = true
		state.commands = make([]TransactionCommand, 0)
		if state.transaction == nil {
			state.transaction = &TransactionState{
				Commands:  make([]TransactionCommand, 0),
				WatchKeys: make(map[string]struct{}),
				DirtyKeys: make(map[string]struct{}),
			}
		}
		state.transaction.InTransaction = true
		state.transaction.Commands = make([]TransactionCommand, 0)
		return proto.NewSimpleString("OK")

	case "EXEC":
		if !state.inTransaction {
			return proto.NewError("ERR EXEC without MULTI")
		}
		if len(state.watchedKeys) > 0 {
			h.watchMu.Lock()
			for watchKey := range state.watchedKeys {
				if _, dirty := state.dirtyKeys[watchKey]; dirty {
					h.watchMu.Unlock()
					state.inTransaction = false
					state.commands = nil
					return proto.NilArray{}
				}
			}
			h.watchMu.Unlock()
		}
		results := make([]proto.RESP, len(state.commands))
		for i, tc := range state.commands {
			results[i] = h.executeQueuedCommand(tc.Command, tc.Args, state.respVersion)
		}

		// 传播事务中的写命令到从节点
		if h.Replication != nil && h.Replication.IsMaster() {
			for _, tc := range state.commands {
				if isWriteCommand(tc.Command) {
					fullArgs := make([][]byte, 1, len(tc.Args)+1)
					fullArgs[0] = []byte(tc.Command)
					fullArgs = append(fullArgs, tc.Args...)
					h.Replication.PropagateCommand(fullArgs)
				}
			}
		}

		state.inTransaction = false
		state.commands = nil
		return &proto.NestedArray{Elems: results}

	case "DISCARD":
		if !state.inTransaction {
			return proto.NewError("ERR DISCARD without MULTI")
		}
		state.inTransaction = false
		state.commands = nil
		state.transaction = nil
		return proto.NewSimpleString("OK")

	case "WATCH":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'WATCH' command")
		}
		if state.inTransaction && len(state.commands) > 0 {
			return proto.NewError("ERR WATCH inside MULTI is not allowed")
		}
		if state.watchedKeys == nil {
			state.watchedKeys = make(map[string]struct{})
			state.dirtyKeys = make(map[string]struct{})
		}
		h.watchMu.Lock()
		if h.watchMonitors == nil {
			h.watchMonitors = make(map[string]map[*connState]struct{})
		}
		for _, arg := range args {
			key := string(arg)
			state.watchedKeys[key] = struct{}{}
			if h.watchMonitors[key] == nil {
				h.watchMonitors[key] = make(map[*connState]struct{})
			}
			h.watchMonitors[key][state] = struct{}{}
		}
		h.watchMu.Unlock()
		return proto.NewSimpleString("OK")

	case "UNWATCH":
		if state.watchedKeys != nil {
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
			state.watchedKeys = make(map[string]struct{})
			state.dirtyKeys = make(map[string]struct{})
		}
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
		h.markDirtyKeys(state, key)
		added, err := h.Db.GeoAdd(key, members)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		results := make([]proto.RESP, len(positions))
		for i, pos := range positions {
			if pos[0] == 0 && pos[1] == 0 {
				if state.respVersion == 3 {
					results[i] = &proto.Null{}
				} else {
					results[i] = proto.NewBulkString(nil)
				}
			} else {
				results[i] = &proto.NestedArray{
					Elems: []proto.RESP{
						proto.NewBulkString([]byte(fmt.Sprintf("%.6f", pos[1]))),
						proto.NewBulkString([]byte(fmt.Sprintf("%.6f", pos[0]))),
					},
				}
			}
		}
		return &proto.NestedArray{Elems: results}

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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewBulkString([]byte(fmt.Sprintf("%.4f", dist)))

	// ==================== GEORADIUS ====================
	case "GEORADIUS":
		if len(args) < 5 {
			return proto.NewError("ERR wrong number of arguments for 'GEORADIUS' command")
		}
		gKey := string(args[0])
		gLon, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		gLat, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		gRadius, err := strconv.ParseFloat(string(args[3]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		gUnit := strings.ToLower(string(args[4]))

		var gCount int
		var gWithDist, gWithHash, gWithCoord bool

		gI := 5
		for gI < len(args) {
			opt := strings.ToUpper(string(args[gI]))
			switch opt {
			case "WITHCOORD":
				gWithCoord = true
				gI++
			case "WITHDIST":
				gWithDist = true
				gI++
			case "WITHHASH":
				gWithHash = true
				gI++
			case "COUNT":
				if gI+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				c, err := strconv.Atoi(string(args[gI+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				gCount = c
				gI += 2
			case "ASC", "DESC":
				gI++
			default:
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}

		gResults, err := h.Db.GeoRadius(gKey, gLon, gLat, gRadius, gUnit, gCount, gWithDist, gWithHash, gWithCoord)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		if !gWithCoord && !gWithDist && !gWithHash {
			gResp := make([][]byte, len(gResults))
			for i, r := range gResults {
				gResp[i] = []byte(r.Member)
			}
			return &proto.Array{Args: gResp}
		}

		gResp := make([]proto.RESP, len(gResults))
		for i, r := range gResults {
			elems := []proto.RESP{proto.NewBulkString([]byte(r.Member))}
			if gWithDist {
				elems = append(elems, proto.NewBulkString([]byte(fmt.Sprintf("%.4f", r.Dist))))
			}
			if gWithHash {
				elems = append(elems, proto.NewBulkString([]byte(r.Hash)))
			}
			if gWithCoord {
				elems = append(elems, &proto.NestedArray{
					Elems: []proto.RESP{
						proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lon))),
						proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lat))),
					},
				})
			}
			gResp[i] = &proto.NestedArray{Elems: elems}
		}
		return &proto.NestedArray{Elems: gResp}

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
		var count int
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
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}

		results, err := h.Db.GeoSearch(key, centerLon, centerLat, radius, unit, count, withDist, withHash, withCoord)
		if err != nil {
			return wrapStoreError(err)
		}

		// Format results
		if !withCoord && !withDist && !withHash {
			resp := make([][]byte, len(results))
			for i, r := range results {
				resp[i] = []byte(r.Member)
			}
			return &proto.Array{Args: resp}
		}

		resp := make([]proto.RESP, len(results))
		for i, r := range results {
			elems := []proto.RESP{proto.NewBulkString([]byte(r.Member))}
			if withDist {
				elems = append(elems, proto.NewBulkString([]byte(fmt.Sprintf("%.4f", r.Dist))))
			}
			if withHash {
				elems = append(elems, proto.NewBulkString([]byte(r.Hash)))
			}
			if withCoord {
				elems = append(elems, &proto.NestedArray{
					Elems: []proto.RESP{
						proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lon))),
						proto.NewBulkString([]byte(fmt.Sprintf("%.6f", r.Lat))),
					},
				})
			}
			resp[i] = &proto.NestedArray{Elems: elems}
		}
		return &proto.NestedArray{Elems: resp}

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
		var count int
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

		h.markDirtyKeys(state, dstKey)
		stored, err := h.Db.GeoSearchStore(dstKey, srcKey, centerLon, centerLat, radius, unit, count, storeDist)
		if err != nil {
			return wrapStoreError(err)
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
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}

		// ID or field name
		idPos := i
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

		h.markDirtyKeys(state, key)
		resultID, err := h.Db.XAdd(key, opts, id, fields)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		if h.Replication != nil && h.Replication.IsMaster() && id == "*" {
			args[idPos] = []byte(resultID)
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(length)

	// ==================== XREAD ====================
	case "XREAD":
		var count int64 = 0
		var block int64 = -1

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

		if block >= 0 {
			state.blocking.Store(true)
		}
		results, err := h.Db.XRead(state.ctx, count, block, allArgs...)
		if block >= 0 {
			state.blocking.Store(false)
		}
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Format response
		if len(results) == 0 {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		streamResults := make([]proto.RESP, 0, len(results))
		for _, streamMap := range results {
			for streamKey, entries := range streamMap {
				entriesResp := make([]proto.RESP, 0, len(entries))
				for _, entry := range entries {
					fieldsResp := make([]proto.RESP, 0, len(entry.Fields)*2)
					for k, v := range entry.Fields {
						fieldsResp = append(fieldsResp,
							proto.NewBulkString([]byte(k)),
							proto.NewBulkString([]byte(v)),
						)
					}
					entriesResp = append(entriesResp, &proto.NestedArray{Elems: []proto.RESP{
						proto.NewBulkString([]byte(entry.ID)),
						&proto.NestedArray{Elems: fieldsResp},
					}})
				}
				streamResults = append(streamResults, &proto.NestedArray{Elems: []proto.RESP{
					proto.NewBulkString([]byte(streamKey)),
					&proto.NestedArray{Elems: entriesResp},
				}})
			}
		}
		return &proto.NestedArray{Elems: streamResults}

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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		deleted, err := h.Db.XDel(key, ids...)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			return wrapStoreError(err)
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
			h.markDirtyKeys(state, key)
			err := h.Db.XGroupCreate(key, group, startID)
			if err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.OK
		case "DESTROY":
			if len(args) < 3 {
				return proto.NewError("ERR wrong number of arguments for 'XGROUP DESTROY' command")
			}
			key := string(args[1])
			group := string(args[2])
			h.markDirtyKeys(state, key)
			err := h.Db.XGroupDestroy(key, group)
			if err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
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
			h.markDirtyKeys(state, key)
			err := h.Db.XGroupSetID(key, group, id)
			if err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
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
			h.markDirtyKeys(state, key)
			removed, err := h.Db.XGroupDelConsumer(key, group, consumer)
			if err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			return proto.NewInteger(removed)
		default:
			return proto.NewError("ERR syntax error")
		}

	// ==================== XREADGROUP ====================
	case "XREADGROUP":
		var count int64 = 0
		var block int64 = -1
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
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
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
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
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

		if block >= 0 {
			state.blocking.Store(true)
		}
		results, err := h.Db.XReadGroup(h.Ctx, group, consumer, count, block, streamKeys...)
		if block >= 0 {
			state.blocking.Store(false)
		}
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if state.respVersion == 3 {
				return &proto.Null{}
			}
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
		h.markDirtyKeys(state, key)
		claimed, err := h.Db.XClaim(key, group, consumer, minIdleTime, ids...)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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

		h.markDirtyKeys(state, key)
		result, err := h.Db.XAutoClaim(key, group, consumer, minIdleTime, start, opts)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}

		// Build response: [nextID, [entry1, entry2, ...]]
		if opts.JustID {
			entries := make([]proto.RESP, len(result.ClaimedIDs))
			for i, id := range result.ClaimedIDs {
				entries[i] = proto.NewBulkString([]byte(id))
			}
			return &proto.NestedArray{
				Elems: []proto.RESP{
					proto.NewBulkString([]byte(result.NextID)),
					&proto.NestedArray{Elems: entries},
				},
			}
		}

		entries := make([]proto.RESP, 0, len(result.Messages))
		for _, msg := range result.Messages {
			fields := make([]proto.RESP, 0, len(msg.Fields)*2)
			for k, v := range msg.Fields {
				fields = append(fields, proto.NewBulkString([]byte(k)), proto.NewBulkString([]byte(v)))
			}
			entries = append(entries, &proto.NestedArray{
				Elems: []proto.RESP{
					proto.NewBulkString([]byte(msg.ID)),
					&proto.NestedArray{Elems: fields},
				},
			})
		}
		return &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(result.NextID)),
				&proto.NestedArray{Elems: entries},
			},
		}

	// ==================== XPENDING ====================
	case "XPENDING":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XPENDING' command")
		}
		key := string(args[0])
		group := string(args[1])
		entries, err := h.Db.XPending(key, group)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			groupsCount := int64(0)
			if info.Groups != nil {
				groupsCount = int64(len(info.Groups))
			}
			// first-entry and last-entry require full entry data (nested arrays),
			// not just IDs. The store doesn't expose this directly, so we
			// provide scalar fields only. Redis-compatible clients should use
			// XRANGE for entry data.
			response := [][]byte{
				[]byte("length"),
				[]byte(strconv.FormatInt(info.Length, 10)),
				[]byte("radix-tree-keys"),
				[]byte(strconv.FormatInt(info.RadixTreeKeys, 10)),
				[]byte("radix-tree-nodes"),
				[]byte(strconv.FormatInt(info.RadixTreeNodes, 10)),
				[]byte("last-generated-id"),
				[]byte(info.LastID),
				[]byte("max-deleted-entry-id"),
				[]byte(info.MaxDeletedID),
				[]byte("entries-added"),
				[]byte(strconv.FormatInt(info.Length, 10)),
				[]byte("groups"),
				[]byte(strconv.FormatInt(groupsCount, 10)),
			}
			return &proto.Array{Args: response}
		case "GROUPS":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'XINFO GROUPS' command")
			}
			key := string(args[1])
			groups, err := h.Db.XInfoGroups(key)
			if err != nil {
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
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
				if errors.Is(err, store.ErrWrongType) {
					return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				return proto.NewError(fmt.Sprintf("ERR %v", err))
			}
			var response []proto.RESP
			for _, c := range consumers {
				consumerInfo := []proto.RESP{
					proto.NewBulkString([]byte("name")),
					proto.NewBulkString([]byte(c.Name)),
					proto.NewBulkString([]byte("seen")),
					proto.NewBulkString([]byte(strconv.FormatInt(c.LastSeen, 10))),
				}
				response = append(response, &proto.NestedArray{Elems: consumerInfo})
			}
			return &proto.NestedArray{Elems: response}
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

		i := 1
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "MAXLEN":
				if i+1 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				nextArg := strings.ToUpper(string(args[i+1]))
				if nextArg == "~" {
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
					return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
				}
			}
		}

		h.markDirtyKeys(state, key)
		trimmed, err := h.Db.XTrim(key, maxLen, minID)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		var asc = true
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
		keyType, err := h.Db.Type(key)
		if err != nil {
			return wrapStoreError(err)
		}
		var values []string
		var scores []float64

		switch keyType {
		case "list":
			listValues, err := h.Db.LRange(key, 0, -1)
			if err != nil {
				return wrapStoreError(err)
			}
			values = listValues
		case "set":
			setValues, err := h.Db.SMembers(key)
			if err != nil {
				return wrapStoreError(err)
			}
			values = setValues
		case "string":
			val, err := h.Db.Get(key)
			if err != nil {
				return wrapStoreError(err)
			}
			values = []string{val}
		case "zset":
			members, err := h.Db.ZRange(key, 0, -1)
			if err != nil {
				return wrapStoreError(err)
			}
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
				weightVal, err := h.Db.Get(targetKey)
				if err != nil {
					return wrapStoreError(err)
				}
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
			}
		}
		if count >= 0 && int64(len(values)) > count {
			values = values[:count]
		}

		// Apply GET patterns
		if len(getPatterns) > 0 {
			finalValues := make([]string, 0)
			for _, pattern := range getPatterns {
				for _, val := range values {
					targetKey := strings.Replace(pattern, "*", val, 1)
					targetVal, err := h.Db.Get(targetKey)
					if err != nil {
						return wrapStoreError(err)
					}
					finalValues = append(finalValues, targetVal)
				}
			}
			values = finalValues
		}

		// STORE
		if destKey != "" {
			h.markDirtyKeys(state, destKey)
			// Store as a list
			for idx, v := range values {
				if idx == 0 {
					if _, err := h.Db.Del(destKey); err != nil {
						return wrapStoreError(err)
					}
				}
				if _, err := h.Db.RPush(destKey, v); err != nil {
					return wrapStoreError(err)
				}
			}
			if h.Replication != nil && h.Replication.IsMaster() {
				h.Replication.PropagateCommand(args)
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
			state.authenticated = true
			return proto.NewSimpleString("OK")
		}

		// 格式: AUTH password 或 AUTH username password
		var inputPassword string
		if len(args) >= 1 {
			inputPassword = string(args[0])
		}

		if inputPassword == password {
			state.authenticated = true
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
			switch opt {
			case "NX":
				nx = true
			case "XX":
				xx = true
			}
		}
		h.markDirtyKeys(state, key)
		result, err := h.Db.JSONSet(key, path, value, nx, xx)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.JSONDel(key, paths...)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.JSONArrAppend(key, path, values...)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		result, err := h.Db.JSONNumIncrBy(key, path, increment)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		result, err := h.Db.JSONNumMultBy(key, path, multiplier)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		count, err := h.Db.JSONClear(key, path)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
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
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
			i++
		}
		if err := h.Db.TSCreate(key, opts); err != nil {
			return wrapStoreError(err)
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
		h.markDirtyKeys(state, key)
		ts, err := h.Db.TSAdd(key, timestamp, value, opts)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
		h.markDirtyKeys(state, key)
		deleted, err := h.Db.TSDel(key, start, stop)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
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
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NewBulkString(nil)
			}
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
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
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
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

	case "MIGRATE":
		// MIGRATE host port key|"" db timeout [COPY] [REPLACE] [KEYS key ...]
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'MIGRATE' command")
		}
		host := string(args[0])
		port := string(args[1])
		timeoutStr := string(args[4])
		timeoutMS, err := strconv.Atoi(timeoutStr)
		if err != nil || timeoutMS < 0 {
			return proto.NewError("ERR timeout is not an integer or out of range")
		}
		timeout := time.Duration(timeoutMS) * time.Millisecond
		if timeout < time.Second {
			timeout = time.Second
		}

		// Parse options
		copyKey := false
		replace := false
		var keysToMigrate []string

		// key is at args[2], can be "" meaning "use KEYS option"
		keyArg := string(args[2])
		if keyArg != "" {
			keysToMigrate = append(keysToMigrate, keyArg)
		}

		for i := 5; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "COPY":
				copyKey = true
			case "REPLACE":
				replace = true
			case "KEYS":
				for j := i + 1; j < len(args); j++ {
					keysToMigrate = append(keysToMigrate, string(args[j]))
				}
				i = len(args)
			}
		}

		if len(keysToMigrate) == 0 {
			return proto.NewError("ERR no keys to migrate")
		}

		targetAddr := net.JoinHostPort(host, port)

		// Connect once and send all RESTORE commands over the same connection
		conn, err := net.DialTimeout("tcp", targetAddr, timeout)
		if err != nil {
			return proto.NewError(fmt.Sprintf("ERR MIGRATE: connecting to %s: %v", targetAddr, err))
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(timeout))

		reader := bufio.NewReader(conn)
		var migratedKeys []string

		for _, migrateKey := range keysToMigrate {
			data, err := h.Db.Dump(migrateKey)
			if err != nil {
				if strings.Contains(err.Error(), "no such key") {
					continue
				}
				_ = conn.Close()
				return proto.NewError(fmt.Sprintf("ERR MIGRATE: dump key %s: %v", migrateKey, err))
			}

			// Build RESTORE command using raw RESP format
			var restoreData []byte
			if replace {
				restoreData = []byte(fmt.Sprintf("*5\r\n$7\r\nRESTORE\r\n$%d\r\n%s\r\n$1\r\n0\r\n$%d\r\n",
					len(migrateKey), migrateKey, len(data)))
			} else {
				restoreData = []byte(fmt.Sprintf("*4\r\n$7\r\nRESTORE\r\n$%d\r\n%s\r\n$1\r\n0\r\n$%d\r\n",
					len(migrateKey), migrateKey, len(data)))
			}
			restoreData = append(restoreData, data...)
			restoreData = append(restoreData, "\r\n"...)
			if replace {
				restoreData = append(restoreData, "$7\r\nREPLACE\r\n"...)
			}

			if _, err := conn.Write(restoreData); err != nil {
				_ = conn.Close()
				return proto.NewError(fmt.Sprintf("ERR MIGRATE: write to %s: %v", targetAddr, err))
			}

			resp, err := proto.ReadRESP(reader)
			if err != nil {
				_ = conn.Close()
				return proto.NewError(fmt.Sprintf("ERR MIGRATE: target response for key %s: %v", migrateKey, err))
			}

			targetErr := targetRespError(resp)
			if targetErr != "" {
				_ = conn.Close()
				return proto.NewError(fmt.Sprintf("ERR MIGRATE: target error for key %s: %s", migrateKey, targetErr))
			}

			migratedKeys = append(migratedKeys, migrateKey)
		}

		// Only delete local keys after all RESTOREs succeeded
		if !copyKey {
			for _, migrateKey := range migratedKeys {
				h.markDirtyKeys(state, migrateKey)
				_, _ = h.Db.Del(migrateKey)
			}
		}
		return proto.OK

	case "DEBUG":
		if len(args) < 1 {
			return proto.NewError("ERR wrong number of arguments for 'DEBUG' command")
		}
		subcommand := strings.ToUpper(string(args[0]))
		switch subcommand {
		case "SLEEP":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'DEBUG SLEEP' command")
			}
			duration, err := strconv.ParseFloat(string(args[1]), 64)
			if err != nil || duration < 0 {
				return proto.NewError("ERR invalid sleep duration")
			}
			time.Sleep(time.Duration(duration * float64(time.Second)))
			return proto.OK
		case "OBJECT":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'DEBUG OBJECT' command")
			}
			key := string(args[1])
			keyType, err := h.Db.Type(key)
			if err != nil {
				return wrapStoreError(err)
			}
			ttl, err := h.Db.TTL(key)
			if err != nil {
				return wrapStoreError(err)
			}
			info := fmt.Sprintf("Key: %s; Type: %s; TTL: %ds", key, keyType, ttl)
			return proto.NewBulkString([]byte(info))
		case "SEGFAULT":
			return proto.NewError("ERR DEBUG SEGFAULT requested (simulated)")
		case "ERROR":
			if len(args) < 2 {
				return proto.NewError("ERR wrong number of arguments for 'DEBUG ERROR' command")
			}
			message := string(args[1])
			return proto.NewError(fmt.Sprintf("ERR %s", message))
		default:
			return proto.NewError(fmt.Sprintf("ERR unknown DEBUG subcommand '%s'", subcommand))
		}

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

// executeQueuedCommand 执行事务队列中的命令
func (h *Handler) executeQueuedCommand(cmd string, args [][]byte, respVersion int) proto.RESP {
	nilBulk := func() proto.RESP {
		if respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	switch cmd {
	case "SET":
		key, value := string(args[0]), string(args[1])
		if err := h.Db.Set(key, value); err != nil {
			return wrapStoreError(err)
		}
		return proto.OK
	case "GET":
		key := string(args[0])
		value, err := h.Db.Get(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return nilBulk()
			}
			return wrapStoreError(err)
		}
		return proto.NewBulkString([]byte(value))
	case "DEL":
		count := int64(0)
		for _, arg := range args {
			deleted, err := h.Db.Del(string(arg))
			if err != nil {
				return wrapStoreError(err)
			}
			count += deleted
		}
		return proto.NewInteger(count)
	case "INCR":
		key := string(args[0])
		val, err := h.Db.INCR(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "DECR":
		key := string(args[0])
		val, err := h.Db.DECR(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "INCRBY":
		key := string(args[0])
		delta, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		val, err := h.Db.INCRBY(key, delta)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "DECRBY":
		key := string(args[0])
		delta, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		val, err := h.Db.DECRBY(key, delta)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "APPEND":
		key, value := string(args[0]), string(args[1])
		length, err := h.Db.APPEND(key, value)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(length))
	case "STRLEN":
		key := string(args[0])
		length, err := h.Db.StrLen(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(length))
	case "EXISTS":
		key := string(args[0])
		exists, err := h.Db.Exists(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "EXPIRE":
		key := string(args[0])
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		success, err := h.Db.Expire(key, seconds)
		if err != nil {
			return wrapStoreError(err)
		}
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TTL":
		key := string(args[0])
		ttl, err := h.Db.TTL(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(ttl)
	case "PERSIST":
		key := string(args[0])
		success, err := h.Db.Persist(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TYPE":
		key := string(args[0])
		keyType, err := h.Db.Type(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewSimpleString(keyType)
	case "LPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.LPush(key, values...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "RPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.RPush(key, values...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "LPOP":
		key := string(args[0])
		val, err := h.Db.LPop(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if val == "" {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(val))
	case "RPOP":
		key := string(args[0])
		val, err := h.Db.RPop(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if val == "" {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(val))
	case "LLEN":
		key := string(args[0])
		length, err := h.Db.LLen(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(length))
	case "LRANGE":
		key := string(args[0])
		start, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		stop, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		items, err := h.Db.LRange(key, start, stop)
		if err != nil {
			return wrapStoreError(err)
		}
		results := make([][]byte, len(items))
		for i, item := range items {
			results[i] = []byte(item)
		}
		return &proto.Array{Args: results}
	case "HSET":
		key := string(args[0])
		field, value := string(args[1]), string(args[2])
		if err := h.Db.HSet(key, field, value); err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(1)
	case "HGET":
		key, field := string(args[0]), string(args[1])
		val, err := h.Db.HGet(key, field)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil || val == nil {
			return nilBulk()
		}
		return proto.NewBulkString(val)
	case "HGETALL":
		key := string(args[0])
		data, err := h.Db.HGetAll(key)
		if err != nil {
			return wrapStoreError(err)
		}
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
		count, err := h.Db.HDel(key, fields...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "SADD":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, err := h.Db.SAdd(key, members...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "SMEMBERS":
		key := string(args[0])
		members, err := h.Db.SMembers(key)
		if err != nil {
			return wrapStoreError(err)
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}
	case "SISMEMBER":
		key, member := string(args[0]), string(args[1])
		exists, err := h.Db.SIsMember(key, member)
		if err != nil {
			return wrapStoreError(err)
		}
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "SCARD":
		key := string(args[0])
		count, err := h.Db.SCard(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "SREM":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, err := h.Db.SRem(key, members...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "ZADD":
		key := string(args[0])
		members := make([]store.ZSetMember, 0)
		for i := 1; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(string(args[i]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			members = append(members, store.ZSetMember{Score: score, Member: string(args[i+1])})
		}
		if err := h.Db.ZAdd(key, members); err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(len(members)))
	case "ZREM":
		key := string(args[0])
		member := string(args[1])
		count, err := h.Db.ZRem(key, member)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(count)
	case "ZCARD":
		key := string(args[0])
		count, err := h.Db.ZCard(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "ZSCORE":
		key, member := string(args[0]), string(args[1])
		score, exists, err := h.Db.ZScore(key, member)
		if err != nil {
			return wrapStoreError(err)
		}
		if !exists {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
	case "ZINCRBY":
		key, member := string(args[0]), string(args[2])
		delta, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		newScore, err := h.Db.ZIncrBy(key, member, delta)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(newScore, 'f', -1, 64)))
	case "SPOP":
		key := string(args[0])
		if len(args) >= 2 {
			count, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			members, err := h.Db.SPopN(key, count)
			if err != nil {
				return wrapStoreError(err)
			}
			if len(members) == 0 {
				return &proto.Array{Args: [][]byte{}}
			}
			results := make([][]byte, len(members))
			for i, m := range members {
				results[i] = []byte(m)
			}
			return &proto.Array{Args: results}
		}
		member, err := h.Db.SPop(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if member == "" {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(member))
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
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
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
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
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
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
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
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
	// 添加所有成员
	zMembers := make([]store.ZSetMember, len(members))
	for i, m := range members {
		zMembers[i] = store.ZSetMember{Score: m.Score, Member: m.Member}
	}
	err = h.Db.ZAdd(dstKey, zMembers)
	return err == nil
}
