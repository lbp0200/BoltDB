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
	TS         uint64 // 主侧 ts 水位（FULLRESYNC: currentTS; CONTINUE: 从侧 lastAppliedTS）
}

// HandlePSyncAfterTSRead 测试钩子（生产恒 nil——零开销）：FULLRESYNC 分支在
// HandlePSync 锁外读 currentTS 之后、返回之前调用——精确模拟
// 「锁外读 ts 与快照实际水位（SnapshotMuLock）之间」的提交窗口（TODO §6 ②
// 区分守卫注入点：窗口内提交 K 条非幂等命令 → pre-fix 通告旧 ts → 从侧断连重连
// 后重发已在 RDB 内的区间 → 双应用）。测试内设置 + defer 置回 nil。
var HandlePSyncAfterTSRead func()

// HandlePSync 处理 PSYNC 命令（主节点端——feed-only / ts 域）。
//
// 判定逻辑：
//   - ts == 0 → 从侧从未同步（首连）→ FULLRESYNC
//   - replId 不匹配 → 从侧来自不同主实例 → FULLRESYNC
//   - ts ∈ [logStartTS, currentTS] → CONTINUE（handler 做 ts 域 catch-up）
//   - 其余 → FULLRESYNC（ts 超出 log 键范围——retention 外 / 主侧重启后 log 为空）
func HandlePSync(rm *ReplicationManager, replId string, offset int64, ts uint64) (*PSyncResult, error) {
	rm.mu.RLock()
	currentReplId := rm.replId
	rm.mu.RUnlock()

	if ts == 0 || replId != currentReplId {
		return fullResyncResult(rm, replId, offset, ts), nil
	}

	logStartTS, _ := rm.store.ReplLogStartTS()
	currentTS, _ := rm.store.ReplLogCurrentTS()

	if ts >= logStartTS && ts <= currentTS {
		logger.Logger.Info().
			Uint64("requested_ts", ts).
			Uint64("log_start_ts", logStartTS).
			Uint64("current_ts", currentTS).
			Msg("PSYNC: 执行增量同步（ts 域 catch-up）")
		return &PSyncResult{
			FullResync: false,
			ReplId:     currentReplId,
			TS:         ts,
		}, nil
	}

	logger.Logger.Warn().
		Uint64("requested_ts", ts).
		Uint64("log_start_ts", logStartTS).
		Uint64("current_ts", currentTS).
		Msg("PSYNC: ts 不在日志键范围，降级为全量同步")
	return fullResyncResult(rm, replId, offset, ts), nil
}

func fullResyncResult(rm *ReplicationManager, replId string, offset int64, ts uint64) *PSyncResult {
	currentTS, _ := rm.store.ReplLogCurrentTS()
	if HandlePSyncAfterTSRead != nil {
		HandlePSyncAfterTSRead()
	}
	logger.Logger.Info().
		Str("requested_repl_id", replId).
		Int64("requested_offset", offset).
		Uint64("current_ts", currentTS).
		Msg("PSYNC: 执行全量同步")
	return &PSyncResult{
		FullResync: true,
		ReplId:     rm.replId,
		TS:         currentTS,
	}
}

// SendFullResync 发送全量同步响应
func SendFullResync(slave *SlaveConnection, replId string, ts uint64) error {
	response := fmt.Sprintf("+FULLRESYNC %s 0 %d\r\n", replId, ts)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send FULLRESYNC response failed: %w", err)
	}
	return nil
}

// SendContinueResync 发送增量同步响应
func SendContinueResync(slave *SlaveConnection, replId string) error {
	response := fmt.Sprintf("+CONTINUE %s\r\n", replId)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send CONTINUE response failed: %w", err)
	}
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
