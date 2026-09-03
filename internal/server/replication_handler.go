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
		// Issue #3：processRequest 持 snapshotMu 读锁跨越 executeCommand
		// （commit）与 PropagateCommand（offset = backlog.Append）。这里持写锁
		// 跨越 snapshotOffset 捕获与 MVCC View，因此看不到已提交未传播的写入。
		// 守卫：internal/replication/fullresync_boundary_test.go。
		//
		// 时序：
		//   [1] SnapshotMuLock()
		//   [2] snapshotOffset = GetMasterReplOffset()
		//   [3] GenerateRDB 内部 View 全程仍在写锁内
		//   [4] 发送 FULLRESYNC + RDB（仍持写锁，避免 1MB backlog 在
		//       网络发送期间被写穿，GetRange 变成 offset too old）
		//   [5] SnapshotMuUnlock()
		//   [6] AddSlave + CatchUpAndEnableSlave(snapshotOffset)
		h.Db.SnapshotMuLock()
		locked := true
		unlock := func() {
			if locked {
				h.Db.SnapshotMuUnlock()
				locked = false
			}
		}
		defer unlock()

		snapshotOffset := h.Replication.GetMasterReplOffset()
		rdbData, err := replication.GenerateRDBWithSnapshotLock(h.Db)
		if err != nil {
			logger.Logger.Error().Err(err).Msg("生成RDB数据失败")
			return proto.NewError("ERR failed to generate RDB")
		}

		response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", result.ReplId, snapshotOffset)
		if _, err := writer.WriteString(response); err != nil {
			logger.Logger.Error().Err(err).Msg("发送FULLRESYNC失败")
			return proto.NewError("ERR failed to send FULLRESYNC")
		}

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

		// Writes were fenced through the RDB send, so snapshotOffset is still
		// the live watermark. CatchUp covers commands that land after unlock.
		unlock()

		slaveConn := replication.NewSlaveConnection(conn)
		h.Replication.AddSlave(slaveConn)
		if err := h.Replication.CatchUpAndEnableSlave(slaveConn, snapshotOffset); err != nil {
			logger.Logger.Error().Err(err).Msg("FULLRESYNC slave catch-up failed")
			h.Replication.RemoveSlave(slaveConn.ID)
			return nil
		}

		currentOffset := h.Replication.GetMasterReplOffset()
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
			// +CONTINUE already flushed — do not write ERR after it.
			return nil
		}

		h.Replication.AddSlave(slaveConn)
		if err := h.Replication.CatchUpAndEnableSlave(slaveConn, currentOffset); err != nil {
			logger.Logger.Error().Err(err).Msg("CONTINUE slave catch-up failed")
			h.Replication.RemoveSlave(slaveConn.ID)
			return nil
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
				// S2 ACK-ts 双轨（向后兼容 len 判定）：第 4 参为从侧 lastAppliedTS
				// （applied 语义——排水判据 D2 的数据源）。
				if len(req.Args) >= 4 {
					if ackTS, tsErr := strconv.ParseUint(string(req.Args[3]), 10, 64); tsErr == nil {
						slave.UpdateReplAckTS(ackTS)
						h.Replication.UpdateSlaveAckTS(slave.ID, ackTS)
					}
				}
				continue
			}
		}

		// 处理 REPLCONF GETACK * — 从节点周期性查询主节点 offset，
		// 用于检测"命令已入 backlog 但投递静默中断"的尾巴缺口
		// （docs/plans/TODO.md §1c：dw 回归偶发 1 元素亏空，两个丢弃
		// 计数器均为 0 时仍可能发生）。回复与从节点 ACK 同构的
		// RESP 数组命令，使从节点 readCommandLoop 能直接解析。
		if cmd == "REPLCONF" && len(req.Args) >= 2 &&
			strings.ToUpper(string(req.Args[1])) == "GETACK" {
			masterOffset := h.Replication.GetMasterReplOffset()
			currentTS, _ := h.Replication.CurrentTS()
			ackResp := replication.EncodeReplconfAck(masterOffset, currentTS)
			if err := slave.WriteAndFlush([]byte(ackResp)); err != nil {
				logger.Logger.Debug().
					Str("slave_id", slave.ID).
					Err(err).
					Msg("回复 GETACK 失败")
				return
			}
			continue
		}

		// 其他命令（理论上不应该有）
		logger.Logger.Warn().
			Str("slave_id", slave.ID).
			Str("cmd", cmd).
			Msg("从节点发送了未知命令")
	}
}
