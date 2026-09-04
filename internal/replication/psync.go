package replication

import (
	"context"
	"fmt"
	"strings"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// PSyncResult PSYNC结果
type PSyncResult struct {
	FullResync bool   // 是否全量同步
	ReplId     string // 复制ID
	Offset     int64  // 复制偏移量（字节影子——S2 双轨）
	TS         uint64 // 主侧 ts 水位（S2 PSYNC-ts——④）
}

// HandlePSync 处理PSYNC命令（主节点端）
func HandlePSync(rm *ReplicationManager, replId string, offset int64, ts uint64) (*PSyncResult, error) {
	rm.mu.RLock()
	currentReplId := rm.replId
	backlog := rm.backlog
	rm.mu.RUnlock()

	// 偏移量即 backlog 的连续水位，与下面所有 range/boundary 判定同源；
	// 不能再读独立计数器，否则 CONTINUE 会用一个不在命令边界上的
	// currentOffset 去接受请求（该缺陷正是 repl_offset_boundary_test 证明的）。
	currentOffset := backlog.GetCurrentOffset()

	// 检查是否可以增量同步
	// replId 匹配时允许 CONTINUE（offset >= 0），确保重连时
	// 不会因为 offset == 0 触发不必要的 FULLRESYNC 造成数据丢失。
	// 初始连接时 replId 为 "?"，不会匹配，触发 FULLRESYNC。
	if replId == currentReplId && offset >= 0 {
		// S2 PSYNC-ts 模式（④——第 4 参 ts > 0）：整数边界判定——ts ∈ [logStartTS,
		// currentTS]——每个 ts 即命令边界（StartsAtCommandBoundary 字节映射在 ts 模式
		// 退役）。ts == 0 = 旧从节点（字节模式——原有判定保留——len 向后兼容）。
		if ts > 0 {
			logStartTS, _ := rm.store.ReplLogStartTS()
			currentTS, _ := rm.store.ReplLogCurrentTS()
			// S2 PSYNC-ts（④）：ts > 0 = feed 模式重连从节点。强制 FULLRESYNC——
			// CONTINUE 路径的 result.Offset 为从侧 lastOffset（feed 域 REPLLOG 帧字节），
			// 与主侧 backlog 原始命令 offset 域不一致，SendBacklogData(backlog,
			// result.Offset, currentOffset) 读错区间 → gap [ts+1, currentTS] 丢失。
			// FULLRESYNC 用主侧 currentOffset（合法域）+ RDB 点时快照 +
			// CatchUpAndEnableSlave；从侧在 reconnect.go:336 把 lastAppliedTS 重置为
			// currentTS，与 fresh 路径逐字节一致（TestFeedReconnectAfterFullresync），
			// 无 stale-dedup 风险。
			logger.Logger.Warn().
				Uint64("requested_ts", ts).
				Uint64("log_start_ts", logStartTS).
				Uint64("current_ts", currentTS).
				Msg("PSYNC-ts 重连强制 FULLRESYNC（feed offset 域与 backlog 不一致）")
		} else {
			// 检查backlog中是否有足够的数据（字节模式——旧从节点）
			backlogStart := backlog.GetCurrentOffset() - backlog.GetSize()
			if backlogStart < 0 {
				backlogStart = 0
			}

			// 重启后 backlog 为空（currentOffset == 0）但请求的 offset > 0：
			// backlog 中没有可发送的数据，必须降级为 FULLRESYNC。
			// 不能仅靠 backlogStart/offset 范围判断——空 backlog 的
			// GetCurrentOffset() == 0，range check 会误判为有效。
			if backlog.GetCurrentOffset() == 0 && offset > 0 {
				logger.Logger.Info().
					Int64("requested_offset", offset).
					Msg("backlog 为空（重启后），请求的 offset 不可用，降级为全量同步")
			} else if offset >= backlogStart && offset <= currentOffset {
				// 可以增量同步，但先做命令边界校验（纵深防御，见 backlog.StartsAtCommandBoundary）。
				// 若 offset 落在一个命令字节中间（从节点 offset 失步/错位续传），
				// 取到的字节流首字节不会是 '*'，此时降级 FULLRESYNC，
				// 避免从节点 ReadRESP 误帧（K:HASH:47 类 mis-frame → 无限重同步）。
				if offset < currentOffset && !backlog.StartsAtCommandBoundary(offset) {
					logger.Logger.Warn().
						Str("repl_id", replId).
						Int64("offset", offset).
						Int64("current_offset", currentOffset).
						Msg("PSYNC CONTINUE offset 非命令边界，降级为全量同步")
				} else {
					// 可以增量同步
					logger.Logger.Info().
						Str("repl_id", replId).
						Int64("offset", offset).
						Msg("执行增量同步")
					return &PSyncResult{
						FullResync: false,
						ReplId:     currentReplId,
						Offset:     offset,
					}, nil
				}
			}
		}
	}

	// 需要全量同步
	currentTS, _ := rm.store.ReplLogCurrentTS()
	logger.Logger.Info().
		Str("requested_repl_id", replId).
		Str("current_repl_id", currentReplId).
		Int64("requested_offset", offset).
		Int64("current_offset", currentOffset).
		Uint64("current_ts", currentTS).
		Msg("执行全量同步")
	return &PSyncResult{
		FullResync: true,
		ReplId:     currentReplId,
		Offset:     currentOffset,
		TS:         currentTS,
	}, nil
}

// SendFullResync 发送全量同步响应
func SendFullResync(slave *SlaveConnection, replId string, offset int64, ts uint64) error {
	// 发送 +FULLRESYNC <replid> <offset> <ts>（S2 PSYNC-ts——第 4 字段为 currentTS——
	// 旧从节点按字段数忽略 ts）
	response := fmt.Sprintf("+FULLRESYNC %s %d %d\r\n", replId, offset, ts)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send FULLRESYNC response failed: %w", err)
	}
	return nil
}

// SendContinueResync 发送增量同步响应
func SendContinueResync(slave *SlaveConnection, replId string, offset int64) error {
	// 发送 +CONTINUE <replid>
	response := fmt.Sprintf("+CONTINUE %s\r\n", replId)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send CONTINUE response failed: %w", err)
	}
	return nil
}

// SendBacklogData 发送backlog数据到从节点
func SendBacklogData(slave *SlaveConnection, backlog *ReplicationBacklog, startOffset, endOffset int64) error {
	if startOffset >= endOffset {
		return nil
	}
	data, err := backlog.GetRange(startOffset, endOffset)
	if err != nil {
		return fmt.Errorf("get backlog range failed: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	slave.writeMu.Lock()
	defer slave.writeMu.Unlock()

	if _, err := slave.Writer.Write(data); err != nil {
		return fmt.Errorf("write backlog data failed: %w", err)
	}

	if err := slave.Writer.Flush(); err != nil {
		return fmt.Errorf("flush backlog data failed: %w", err)
	}

	logger.Logger.Debug().
		Str("slave_id", slave.ID).
		Int64("start_offset", startOffset).
		Int64("end_offset", endOffset).
		Int("data_size", len(data)).
		Msg("发送backlog数据到从节点")

	return nil
}

// StartSlaveReplication 启动从节点复制（从节点端），包含自动重连
func StartSlaveReplication(rm *ReplicationManager, storeObj *store.BotreonStore, masterAddr string) error {
	rm.mu.Lock()
	if rm.slaveReconnector != nil {
		rm.slaveReconnector.Stop()
		rm.slaveReconnector = nil
	}
	rm.role = RoleSlave
	rm.masterAddr = masterAddr
	rm.mu.Unlock()

	reconnector := NewSlaveReconnector(rm, storeObj, masterAddr)
	rm.mu.Lock()
	rm.slaveReconnector = reconnector
	rm.mu.Unlock()

	reconnector.Start()
	return nil
}

// StopSlaveReplication 停止从节点复制
func StopSlaveReplication(rm *ReplicationManager) {
	rm.mu.Lock()
	reconnector := rm.slaveReconnector
	rm.slaveReconnector = nil
	rm.role = RoleMaster
	rm.masterAddr = ""

	if rm.masterConn != nil {
		if err := rm.masterConn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close master connection")
		}
		rm.masterConn = nil
	}
	rm.mu.Unlock()

	if reconnector != nil {
		reconnector.Stop()
	}
}

// executeReplicatedCommand 执行从节点收到的复制命令（薄包装，转发给 store.WriteCommand）。
// WriteCommand 的完整 switch 见 internal/store/write_command.go。
func executeReplicatedCommand(s *store.BotreonStore, args [][]byte, ctx context.Context) error {
	if len(args) == 0 {
		return nil
	}
	args[0] = []byte(strings.ToUpper(string(args[0])))
	return store.WriteCommand(s, args, ctx)
}
