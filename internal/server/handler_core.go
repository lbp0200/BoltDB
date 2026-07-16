package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"runtime/debug"
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
	// currentCmd is set for the duration of executeCommand so redirect/fence
	// helpers know whether the access is a write (IMPORTING write fence).
	currentCmd string
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

	// MaxInputBytes 是每个连接累计读取字节数的上限。
	// 防止单连接通过发送大量/大 bulk 请求耗尽服务器内存。
	// 达到上限后后续 ReadRESP 返回错误并断开连接。
	// 0 表示不限制。
	MaxInputBytes int64

	// MaxClients 是最大并发连接数限制（0 = 默认 10000）
	MaxClients int

	// Timeout 是空闲连接超时时间（0 = 不超时）
	Timeout time.Duration

	// wg 跟踪所有后台 goroutine，确保关闭时完整收束
	wg sync.WaitGroup

	// shuttingDown is set to 1 when Shutdown begins, so handleConnection
	// goroutines that register after the conns iteration can exit promptly
	// instead of blocking on ReadRESP with nobody to close their connection.
	shuttingDown atomic.Int32

	// cmdCounters 按命令名统计调用次数，用于使用率分析。
	// 添加新命令计数时在对应的 handleXXX 函数中调用 h.incrementCmdCounter("CMD")。
	cmdCounters   map[string]*atomic.Int64
	cmdCountersMu sync.Mutex
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

// checkAndHandleRedirect 检查键是否需要重定向到其他节点
// 返回 nil 表示不需要重定向，可以继续执行命令
// 返回非 nil 表示需要重定向，包含重定向信息
func (h *Handler) checkAndHandleRedirect(state *connState, key string) proto.RESP {
	if h.Cluster == nil {
		return nil
	}

	// Redis ASKING is one-shot: it applies only to the next key-routed command.
	asking := state.clusterAsking
	if asking {
		state.clusterAsking = false
	}

	if asking {
		slot := cluster.Slot(key)
		if h.Cluster.IsImportingSlot(slot) {
			// IMPORTING write fence: ASKING clients may read, but writes other
			// than RESTORE are blocked so Phase-1 DUMP→RESTORE cannot be
			// clobbered by concurrent client updates on the target.
			cmd := ""
			if state != nil {
				cmd = state.currentCmd
			}
			if cmd != "" && isWriteCommand(cmd) && cmd != "RESTORE" {
				return proto.NewError(fmt.Sprintf(
					"ERR slot %d is IMPORTING: client writes fenced during migration (RESTORE allowed)",
					slot))
			}
			return nil
		}
	}

	redirect := h.Cluster.CheckSlotRedirect(key)
	if redirect != nil {
		if redirect.Type == "MOVED" {
			return proto.NewError(redirect.Error())
		}
		// Redis MIGRATING semantics: serve locally if the key still exists;
		// only ASK when the key is missing (already migrated or never present).
		if redirect.Type == "ASK" {
			if exists, err := h.Db.Exists(key); err == nil && exists {
				// Source write fence: while MIGRATING, block client writes so
				// Phase-1 DUMP→RESTORE cannot be invalidated by concurrent SETs
				// that Phase-2 would then delete without re-copying (data loss).
				// Reads remain allowed. RESTORE is reserved for migration tooling.
				cmd := ""
				if state != nil {
					cmd = state.currentCmd
				}
				slot := cluster.Slot(key)
				if cmd != "" && isWriteCommand(cmd) && cmd != "RESTORE" &&
					h.Cluster.IsMigratingSlot(slot) {
					return proto.NewError(fmt.Sprintf(
						"ERR slot %d is MIGRATING: client writes fenced during migration",
						slot))
				}
				return nil
			}
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
	defer func() {
		if r := recover(); r != nil {
			logger.Logger.Error().
				Str("remote_addr", conn.RemoteAddr().String()).
				Interface("panic", r).
				Str("stack", string(debug.Stack())).
				Msg("recovered panic in handleConnection")
			_ = conn.Close()
		}
	}()
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
	// 如果配置了 MaxInputBytes，用 CumulativeLimitReader 包装输入流，
	// 防止单连接通过发送大 bulk 请求耗尽服务器内存。
	var limitReader *CumulativeLimitReader
	if h.MaxInputBytes > 0 {
		limitReader = NewCumulativeLimitReader(conn, h.MaxInputBytes)
		reader = bufio.NewReader(limitReader)
	}
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

	// maxclients 检查：超限立即拒绝
	if maxClients := h.GetMaxClients(); maxClients > 0 && h.ActiveClientCount() >= maxClients {
		cancel()
		logger.Logger.Warn().Str("remote_addr", remoteAddr).
			Int("current", h.ActiveClientCount()).
			Int("max", maxClients).
			Msg("maxclients 超限，拒绝新连接")
		_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
		return
	}

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
		// 空闲超时：在 ReadRESP 前设置读取 deadline
		if h.Timeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(h.Timeout))
		}

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

		// 成功读取后清除 deadline，避免命令处理期间误断开
		if h.Timeout > 0 {
			_ = conn.SetReadDeadline(time.Time{})
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
	// 失败回复不进入 backlog（避免 slave 上触发 apply 错误 / FULLRESYNC thrash）
	// SPOP 等已由 handler 内单路径规范化传播，见 shouldPropagateCommand
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
	if h.Replication != nil && h.Replication.IsMaster() && isWriteCommand(cmd) &&
		shouldPropagateCommand(cmd) && !isErrorResponse(resp) {
		h.Replication.PropagateCommand(propagateArgs)
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
