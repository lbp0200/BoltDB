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

		// 捕获 currentOffset 必须在 AddSlave 之前，否则 PropagateCommand
		// 在 AddSlave 之后 GetMasterReplOffset 之前写入的命令会被
		// 同时通过 SendCommand（直接推）和 SendBacklogData（从 backlog 重放）
		// 发送两次，导致从节点数据翻倍。
		//
		// 正确时序（与 CONTINUE 路径一致）：
		//   [1] currentOffset = GetMasterReplOffset()（AddSlave 前捕获）
		//   [2] SendBacklogData(snapshotOffset → currentOffset)（AddSlave 前发送）
		//   [3] AddSlave(slaveConn) — 此后 PropagateCommand 推新命令
		//   [4] 填充缺口：currentOffset 到 AddSlave 之间的写入
		currentOffset := h.Replication.GetMasterReplOffset()

		if currentOffset > snapshotOffset {
			if err := replication.SendBacklogData(slaveConn, backlog, snapshotOffset, currentOffset); err != nil {
				logger.Logger.Error().Err(err).
					Int64("snapshot_offset", snapshotOffset).
					Int64("current_offset", currentOffset).
					Msg("发送FULLRESYNC backlog数据失败")
			}
		}

		slaveConn.SetReady(true)
		h.Replication.AddSlave(slaveConn)
		slaveConn.SetReplOffset(currentOffset)

		// 填充竞态缺口：currentOffset 到 AddSlave 之间可能有多条
		// PropagateCommand 写入。这些写入的 offset >= currentOffset，
		// 不在已发送的 backlog 中。缺口通常为空或极窄（1-2 条命令）。
		afterOffset := h.Replication.GetMasterReplOffset()
		if afterOffset > currentOffset {
			if err := replication.SendBacklogData(slaveConn, backlog, currentOffset, afterOffset); err != nil {
				logger.Logger.Debug().Err(err).Msg("发送FULLRESYNC缺口backlog数据失败")
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
