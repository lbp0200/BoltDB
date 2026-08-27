package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
)

type ReplicationTakeoverSignal struct{}

func (ReplicationTakeoverSignal) String() string { return "replication-takeover" }
func (ReplicationTakeoverSignal) Error() string  { return "replication takeover" }
func (ReplicationTakeoverSignal) IsError() bool  { return false }

// handlePSyncWithRDB 处理PSYNC命令并发送RDB数据（全量同步）
// 这是executeCommand的特例，用于在全量同步时直接发送RDB数据
// 返回 nil 表示正常处理
// 返回 ReplicationTakeoverSignal{} 表示连接已由复制接管，需要关闭处理循环但不关闭连接
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
		return wrapLogError(err)
	}

	if result.FullResync {
		// 线性 FULLRESYNC 边界（Issue #3）：用 store.snapshotMu 将
		// snapshotOffset 捕获与 MVCC View 原子绑定，消除重复窗口。
		//
		// 旧方案仅靠"先取 offset 再开 View"保证无丢失，但两者之间
		// 仍有微秒级重复窗口（badger commit 已发生但 offset 尚未递增的
		// 并发写会同时落在 RDB 与 backlog）。本锁使该窗口为零：
		// 写路径 retryUpdate() 持读锁，FULLRESYNC 持写锁覆盖整个
		//   snapshotOffset→View 区间，零并发写入可落入两边。
		//
		// 原序关系仍成立：
		//   store.Set()（badger commit）→ PropagateCommand()（offset 递增）
		// 因此 offset < snapshotOffset 的写入必在 RDB，offset >= 的
		// 必在 backlog，且无交集。
		//
		// 时序：
		//   [1] SnapshotMuLock()
		//   [2] snapshotOffset = GetMasterReplOffset()
		//   [3] GenerateRDB 内部 View 全程仍在写锁内（GenerateRDBWithSnapshotLock）
		//   [4] SnapshotMuUnlock()（RDB 生成完立即释放，不阻塞后续 I/O）
		//   [5] FULLRESYNC 响应使用 snapshotOffset
		//   [6] 发送 RDB + backlog [snapshotOffset, currentOffset) + AddSlave
		h.Db.SnapshotMuLock()
		snapshotOffset := h.Replication.GetMasterReplOffset()
		rdbData, err := replication.GenerateRDBWithSnapshotLock(h.Db)
		h.Db.SnapshotMuUnlock()
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
		// Ready 保持 false，直到 CatchUpAndEnableSlave 完成 gap-fill，
		// 避免 live SendCommand 与 gap-fill 对同一 offset 双发（非幂等命令翻倍）。
		//
		// 正确时序：
		//   [1] currentOffset = GetMasterReplOffset()（AddSlave 前捕获）
		//   [2] SendBacklogData(snapshotOffset → currentOffset)（AddSlave 前发送）
		//   [3] AddSlave(slaveConn) — Ready=false，PropagateCommand 只写 backlog
		//   [4] CatchUpAndEnableSlave：gap-fill 后原子 SetReady(true)
		slaveConn := replication.NewSlaveConnection(conn)
		backlog := h.Replication.GetBacklog()

		currentOffset := h.Replication.GetMasterReplOffset()

		if currentOffset > snapshotOffset {
			if err := replication.SendBacklogData(slaveConn, backlog, snapshotOffset, currentOffset); err != nil {
				logger.Logger.Error().Err(err).
					Int64("snapshot_offset", snapshotOffset).
					Int64("current_offset", currentOffset).
					Msg("发送FULLRESYNC backlog数据失败")
			}
		}

		h.Replication.AddSlave(slaveConn)
		if err := h.Replication.CatchUpAndEnableSlave(slaveConn, currentOffset); err != nil {
			logger.Logger.Error().Err(err).Msg("FULLRESYNC slave catch-up failed")
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

		// 创建从节点连接（Ready=false 直到 catch-up 完成）
		slaveConn := replication.NewSlaveConnection(conn)
		slaveConn.SetReplOffset(result.Offset)

		backlog := h.Replication.GetBacklog()

		// 先发送 backlog（[result.Offset, currentOffset)）再注册 slave。
		// 此时 slave 不在 ReplicationManager.slaves 中，PropagateCommand
		// 不会向其 live-push。
		currentOffset := h.Replication.GetMasterReplOffset()
		if err := replication.SendBacklogData(slaveConn, backlog, result.Offset, currentOffset); err != nil {
			logger.Logger.Error().Err(err).
				Int64("start_offset", result.Offset).
				Int64("end_offset", currentOffset).
				Msg("发送CONTINUE backlog数据失败")
			return proto.NewError("ERR failed to send CONTINUE backlog")
		}

		h.Replication.AddSlave(slaveConn)
		if err := h.Replication.CatchUpAndEnableSlave(slaveConn, currentOffset); err != nil {
			logger.Logger.Error().Err(err).Msg("CONTINUE slave catch-up failed")
			return proto.NewError("ERR failed to finish CONTINUE catch-up")
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

// handleSlaveReplicationConnection 处理从节点的复制连接
// 这个goroutine负责从节点连接的生命周期：
// 1. 接收 REPLCONF ACK 命令（从节点确认已接收的命令偏移量）
// 2. 保持连接打开，直到从节点断开
// 3. 负责关闭连接
func (h *Handler) handleSlaveReplicationConnection(ctx context.Context, slave *replication.SlaveConnection) {
	defer h.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			logger.Logger.Error().
				Str("slave_id", slave.ID).
				Str("slave_addr", slave.Addr).
				Interface("panic", r).
				Str("stack", string(debug.Stack())).
				Msg("recovered panic in handleSlaveReplicationConnection")
			_ = slave.Close()
		}
	}()
	if ctx == nil {
		ctx = h.Ctx
	}
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
