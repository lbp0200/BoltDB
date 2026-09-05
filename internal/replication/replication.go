package replication

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/store"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

// walTruncateFactor: truncate the backlog WAL when its file exceeds this
// multiple of the in-memory backlog size. Keeps the WAL bounded to ~2x the
// ring buffer instead of growing without limit (see BacklogWAL.Truncate).
const walTruncateFactor int64 = 2

// walCheckIntervalDivisor: re-check the WAL file size after this fraction of
// a backlog-worth of bytes has been appended. Throttles the stat() syscall
// off the per-command hot path.
const walCheckIntervalDivisor int64 = 4

// ReplicationManager 管理主从复制
type ReplicationManager struct {
	mu               sync.RWMutex
	propMu           sync.RWMutex                // serializes live SendCommand vs slave install Ready flip
	role             string                      // RoleMaster | RoleSlave
	masterAddr       string                      // 主节点地址(当role=slave时)
	masterConn       *MasterConnection           // 到主节点的连接(当role=slave时)
	slaves           map[string]*SlaveConnection // 从节点连接(当role=master时)
	backlog          *ReplicationBacklog         // 复制积压缓冲区
	wal              *BacklogWAL                 // 持久化 WAL（nil = 不使用，向后兼容）
	walCheckBytes    atomic.Int64                // bytes appended since last WAL size check
	replId           string                      // 复制ID(主节点运行ID)
	store            *store.BotreonStore         // 数据存储
	stopped          bool                        // 是否已停止
	slaveReconnector *SlaveReconnector           // 从节点自动重连器
	tlsConfig        *tls.Config                 // TLS 配置（nil = 不使用 TLS）

	// Drop-path counters for diagnosing silent replica divergence
	// (docs/plans/TODO.md §1c). Live SendCommand failures leave the
	// command in the backlog (catch-up / FULLRESYNC can still recover);
	// apply skips advance replica offset without mutating the store.
	sendDropCount  atomic.Int64
	applySkipCount atomic.Int64

	// feedLoop: 全局 feed 模式开关（S2 backlog 退役首步）——开启后新激活的从侧
	// 走 REPLLOG 增量流（feed-mode——backlog 字节发送的双轨替代）——默认关闭
	//（backlog 字节路径保持现状——双轨并存可切换——回滚零成本）。
	feedLoop atomic.Bool
}

// NewReplicationManager 创建新的复制管理器
// 首次启动时生成新的复制 ID；重启时从 BadgerDB 读取已有的复制 ID，
// 使从节点可以通过 PSYNC CONTINUE 而非 FULLRESYNC 重新连接。
// 同时加载持久化的 masterReplOffset，确保重启后 offset 连续。
func NewReplicationManager(store *store.BotreonStore) *ReplicationManager {
	// 尝试加载已有的 replId
	replId, err := store.LoadReplID()
	if err != nil {
		logger.Logger.Warn().Err(err).Msg("Failed to load persisted replId, generating new one")
	}
	if replId == "" {
		replId, _ = generateReplicationID()
		// 持久化新生成的 replId
		if saveErr := store.SaveReplID(replId); saveErr != nil {
			logger.Logger.Warn().Err(saveErr).Msg("Failed to persist new replId")
		}
	}

	// 读取持久化的 masterReplOffset：仅用于与恢复出的 backlog 水位交叉校验，
	// 不再作为偏移量的来源。偏移量 = backlog 的连续写入水位（见 GetMasterReplOffset）；
	// 脱离了 backlog 内容的偏移量没有意义——若把它当作水位种回空的环，
	// HandlePSync 会用零填充的空环去满足 CONTINUE，等于给从节点发垃圾字节。
	offset, loadOffsetErr := store.LoadMasterReplOffset()
	if loadOffsetErr != nil {
		logger.Logger.Warn().Err(loadOffsetErr).Msg("Failed to load persisted masterReplOffset, starting from 0")
	}

	rm := &ReplicationManager{
		role:    RoleMaster,
		slaves:  make(map[string]*SlaveConnection),
		backlog: NewReplicationBacklog(DefaultBacklogSize),
		replId:  replId,
		store:   store,
	}

	// 尝试加载持久化的 backlog（干净重启时保留，避免 FULLRESYNC）
	backlogRestored := false
	if bOff, bBuf, bSize, loadErr := store.LoadBacklog(); loadErr != nil {
		logger.Logger.Warn().Err(loadErr).Msg("Failed to load persisted backlog")
	} else if bBuf != nil && int64(len(bBuf)) == bSize {
		backlogRestored = true
		rm.backlog.mu.Lock()
		rm.backlog.offset = bOff
		rm.backlog.size = bSize
		// Allocate buffer to persisted size (may differ from DefaultBacklogSize).
		if int64(len(rm.backlog.buffer)) != bSize {
			rm.backlog.buffer = make([]byte, bSize)
		}
		copy(rm.backlog.buffer, bBuf)
		rm.backlog.mu.Unlock()
		logger.Logger.Debug().
			Int64("offset", bOff).
			Int64("size", bSize).
			Int("bytes", len(bBuf)).
			Msg("Persisted backlog loaded on startup")
	}

	switch {
	case backlogRestored && offset != rm.backlog.GetCurrentOffset():
		logger.Logger.Warn().
			Int64("persisted_offset", offset).
			Int64("backlog_offset", rm.backlog.GetCurrentOffset()).
			Msg("persisted masterReplOffset disagrees with restored backlog; backlog wins")
	case !backlogRestored && offset > 0:
		logger.Logger.Info().
			Int64("persisted_offset", offset).
			Msg("no backlog restored (crash or truncated); starting from offset 0, reconnecting slaves will FULLRESYNC")
	}

	return rm
}

// SetBacklogSize 设置复制积压缓冲区大小。
// 若已有 backlog（含从 Badger 恢复的），迁移有效窗口而非丢弃历史，
// 避免 -repl-backlog-size 在 load 之后调用时抹掉 CONTINUE 资格。
func (rm *ReplicationManager) SetBacklogSize(size int64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if size <= 0 {
		size = DefaultBacklogSize
	}
	if size > MaxBacklogSize {
		size = MaxBacklogSize
	}
	if rm.backlog != nil && rm.backlog.GetSize() == size {
		return
	}
	rm.backlog = resizeBacklog(rm.backlog, size)
}

// SetTLSConfig 设置 TLS 配置（nil = 不使用 TLS）
func (rm *ReplicationManager) SetTLSConfig(cfg *tls.Config) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.tlsConfig = cfg
}

// GetTLSConfig 获取 TLS 配置
func (rm *ReplicationManager) GetTLSConfig() *tls.Config {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.tlsConfig
}

// SetBacklogWAL 设置 backlog 持久化 WAL。
// wal 为 nil 时禁用持久化（向后兼容）。
// 如果 WAL 中有未回放的条目，会立即回放到当前 backlog 中。
//
// 使用示例：
//
//	rm.SetBacklogWAL(wal)  // 启用 WAL，自动回放未处理条目
//	rm.SetBacklogWAL(nil)  // 禁用 WAL
func (rm *ReplicationManager) SetBacklogWAL(wal *BacklogWAL) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.wal = wal

	// 如果 WAL 非空，立即回放未处理的条目
	if wal != nil {
		if err := wal.Replay(rm.backlog); err != nil {
			logger.Logger.Warn().Err(err).Msg("failed to replay WAL on SetBacklogWAL")
		}
		// Replayed entries outside the live window were never truncated in
		// older builds, so the WAL can carry a stale multi-GB tail. Drop it
		// now so every restart doesn't re-read the whole file.
		if err := wal.Truncate(rm.backlog.AvailableStartOffset()); err != nil {
			logger.Logger.Warn().Err(err).Msg("failed to truncate backlog WAL after replay")
		}
	}
}

// GetBacklogWAL 获取 backlog 持久化 WAL。
func (rm *ReplicationManager) GetBacklogWAL() *BacklogWAL {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.wal
}

// generateReplicationID 生成40字符的十六进制复制ID
func generateReplicationID() (string, error) {
	bytes := make([]byte, 20)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// CurrentTS 返回当前传播日志键水位（主侧 currentTS——GETACK 回复的 ts 携带——
// S2 feed 协议相位 ③ ACK-ts 双轨）。
func (rm *ReplicationManager) CurrentTS() (uint64, error) {
	return rm.store.ReplLogCurrentTS()
}

// GetRole 获取当前角色
func (rm *ReplicationManager) GetRole() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.role
}

// GetReplicationID 获取复制ID
func (rm *ReplicationManager) GetReplicationID() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.replId
}

// GetMasterReplOffset 获取主节点复制偏移量。
//
// 偏移量就是 backlog 的连续写入水位，二者必须是同一个数：该值会被
//   - +FULLRESYNC 通告给从节点（从节点原样存为自己的 lastOffset，reconnect.go sendPSYNC）
//   - 当作 backlog.GetRange 的切片起点（从节点的字节流从它开始）
//   - 用于 PSYNC CONTINUE 的可用性与命令边界判定
//
// 这三处都要求它落在命令边界上。原先另设的 `masterReplOffset += len(cmdBytes)`
// 计数器做不到：Append 在环锁下连续推进，求和却按完成顺序累加，两者可以分叉
// （实测：并发传播下 13.5% 的采样读到落后值；在真实 FULLRESYNC 窗口内约 1% 的
// 通告值切不出整条命令，因为该临界区只阻塞提交、不阻塞传播）。让偏移量等于环
// 水位后，分叉在结构上不再可能。见 repl_offset_boundary_test.go 与
// docs/failures/repl-offset-boundary-drift.md。
func (rm *ReplicationManager) GetMasterReplOffset() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.backlog.GetCurrentOffset()
}

// SetMasterReplOffset 将偏移量（= backlog 水位）前移到 offset，不回退。
// 仅供启动恢复与测试使用；写入路径由 backlog.Append 推进。
func (rm *ReplicationManager) SetMasterReplOffset(offset int64) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	rm.backlog.SetOffset(offset)
}

// AddSlave 添加从节点连接
func (rm *ReplicationManager) AddSlave(slave *SlaveConnection) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.slaves[slave.ID] = slave
	logger.Logger.Info().
		Str("slave_id", slave.ID).
		Str("slave_addr", slave.Addr).
		Msg("添加从节点连接")
}

// RemoveSlave 移除从节点连接
// 注意：Close() 在释放 rm.mu 后调用，避免与 handlePSyncWithRDB 中
// slaveConn.Lock() + I/O 的锁链死锁（CLIENT KILL → RemoveSlave →
// Close → sc.mu 被 handlePSyncWithRDB 持有）。
func (rm *ReplicationManager) RemoveSlave(slaveID string) {
	rm.mu.Lock()
	slave, exists := rm.slaves[slaveID]
	if exists {
		delete(rm.slaves, slaveID)
	}
	rm.mu.Unlock()

	if exists {
		if err := slave.Close(); err != nil {
			logger.Logger.Debug().Err(err).Str("slave_id", slaveID).Msg("failed to close slave connection")
		}
		logger.Logger.Info().
			Str("slave_id", slaveID).
			Msg("移除从节点连接")
	}
}

// GetSlaves 获取所有从节点
func (rm *ReplicationManager) GetSlaves() []*SlaveConnection {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	slaves := make([]*SlaveConnection, 0, len(rm.slaves))
	for _, slave := range rm.slaves {
		slaves = append(slaves, slave)
	}
	return slaves
}

// GetSlaveCount 获取从节点数量
func (rm *ReplicationManager) GetSlaveCount() int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.slaves)
}

// GetSlaveReplOffset 获取从节点的复制偏移量（slave角色时有效）
func (rm *ReplicationManager) GetSlaveReplOffset() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.slaveReconnector != nil {
		return rm.slaveReconnector.GetLastOffset()
	}
	return 0
}

// GetSlaveApplyIdleMs 返回从侧最近一次成功应用复制命令后的空闲毫秒数
// （B2 应用进度探针——INFO repl_apply_idle_ms 数据源——§1c 冻结链的
// "收到数据但应用卡住"检测面）。0 = 从侧不在复制或从未应用。
func (rm *ReplicationManager) GetSlaveApplyIdleMs() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.slaveReconnector != nil {
		return int64(rm.slaveReconnector.GetApplyIdle() / time.Millisecond)
	}
	return 0
}

func (rm *ReplicationManager) GetReconnectCount() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.slaveReconnector != nil {
		return rm.slaveReconnector.GetReconnectCount()
	}
	return 0
}

// GetReplSendDropCount is the number of times PropagateCommand dropped a
// live push because SlaveConnection.SendCommand returned an error. The
// command is still in the backlog; this is not by itself a lost write.
func (rm *ReplicationManager) GetReplSendDropCount() int64 {
	return rm.sendDropCount.Load()
}

// GetReplApplySkipCount is the number of times readCommandLoop skipped
// executeReplicatedCommand (isTransientReplicationError) while still
// advancing lastOffset. That combination is silent data loss for any
// non-idempotent command.
func (rm *ReplicationManager) GetReplApplySkipCount() int64 {
	return rm.applySkipCount.Load()
}

// SetRole 设置角色
func (rm *ReplicationManager) SetRole(role string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.role = role
}

// SetMasterAddr 设置主节点地址
func (rm *ReplicationManager) SetMasterAddr(addr string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.masterAddr = addr
}

// GetMasterAddr 获取主节点地址
func (rm *ReplicationManager) GetMasterAddr() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.masterAddr
}

// SetMasterConnection 设置主节点连接
func (rm *ReplicationManager) SetMasterConnection(conn *MasterConnection) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.masterConn = conn
}

// GetMasterConnection 获取主节点连接
func (rm *ReplicationManager) GetMasterConnection() *MasterConnection {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.masterConn
}

// GetBacklog 获取复制积压缓冲区
func (rm *ReplicationManager) GetBacklog() *ReplicationBacklog {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.backlog
}

// PropagateCommand 传播命令到所有从节点
func (rm *ReplicationManager) PropagateCommand(cmd [][]byte) {
	rm.mu.RLock()
	slaves := make([]*SlaveConnection, 0, len(rm.slaves))
	for _, slave := range rm.slaves {
		slaves = append(slaves, slave)
	}
	backlog := rm.backlog
	wal := rm.wal
	rm.mu.RUnlock()

	// 总是将命令添加到backlog并更新offset，无论是否有从节点
	// 这使得断连期间的写操作不会丢失，重连后可通过PSYNC增量同步
	cmdBytes := serializeCommand(cmd)
	// Append 在环锁下把字节写入并前移水位；水位即复制偏移量，因此
	// "已入 backlog 的字节" 与 "对外通告的偏移量" 不可能再分叉，也不需要
	// 额外的计数器递增（原先的 IncrementReplOffset(len) 按完成顺序求和，
	// 并发时会落在命令中间）。
	cmdOffset := backlog.Append(cmdBytes)

	// 如果配置了 WAL，将命令写入持久化日志
	// 这提供了 crash 恢复能力：即使主节点崩溃，backlog 也可以从 WAL 重建
	// S2 backlog 退役双轨切换：feed-loop 开启时跳过 WAL 字节记账——log-key 为
	// 权威持久化源（commit 即写日志键——backlog 退役尾项）——backlog 内存环保留
	// （offset 水位 = PSYNC 判定/FULLRESYNC offset/字节从侧 ts=0 兼容的基础）——
	// 重启后 backlog 空 → PSYNC 安全降级 FULLRESYNC（ts 域重建——已治本）。
	// --feed-loop 关闭（字节模式）即完全恢复字节 WAL 记账（回滚开关）。
	if wal != nil && !rm.feedLoop.Load() {
		if err := wal.Append(cmdOffset, cmdBytes); err != nil {
			logger.Logger.Warn().Err(err).Int64("offset", cmdOffset).Msg("backlog WAL append failed")
		}
		rm.maybeTruncateBacklogWAL(int64(len(cmdBytes)))
	}

	// Live push under propMu so slave install (CatchUpAndEnableSlave) can
	// flip Ready without overlapping gap-fill and SendCommand for the same
	// offset range (non-idempotent double apply).
	rm.propMu.RLock()
	defer rm.propMu.RUnlock()
	for _, slave := range slaves {
		if slave.IsReady() {
			// feed-mode 从侧：走 REPLLOG 增量流（backlog 字节发送的双轨替代——S2
			// backlog 退役首步）——FeedSlave 只发增量（feedSinceTS 起的 log 键）——
			// 不发本命令的 backlog 字节（避免从侧双通道重复 apply）。
			if slave.FeedIsEnabled() {
				if err := rm.FeedSlave(slave); err != nil {
					rm.sendDropCount.Add(1)
					logger.Logger.Warn().
						Str("slave_id", slave.ID).
						Err(err).
						Int64("send_drop_count", rm.sendDropCount.Load()).
						Msg("feed 增量发送到从节点失败")
					// handleSlaveReplicationConnection 会在断连时清理
				}
				continue
			}
			if err := slave.SendCommand(cmdBytes, cmdOffset); err != nil {
				rm.sendDropCount.Add(1)
				logger.Logger.Warn().
					Str("slave_id", slave.ID).
					Err(err).
					Int64("send_drop_count", rm.sendDropCount.Load()).
					Msg("传播命令到从节点失败")
				// handleSlaveReplicationConnection 会在断连时清理
			}
		}
	}
}

// maybeTruncateBacklogWAL keeps the backlog WAL file bounded: once the file
// exceeds walTruncateFactor × backlog size, entries before the live window's
// start offset are dropped. The size check is throttled to roughly once per
// walCheckIntervalDivisor of a backlog-worth of writes so the stat() syscall
// stays off the per-command hot path. The counter is atomic and the check is
// CAS-gated so this never takes the manager write lock on the hot path.
func (rm *ReplicationManager) maybeTruncateBacklogWAL(written int64) {
	threshold := rm.backlog.GetSize() / walCheckIntervalDivisor
	if threshold <= 0 {
		return
	}
	acc := rm.walCheckBytes.Add(written)
	if acc < threshold {
		return
	}
	// Exactly one caller wins the reset; the others skip this round.
	if !rm.walCheckBytes.CompareAndSwap(acc, 0) {
		return
	}

	wal := rm.GetBacklogWAL()
	if wal == nil {
		return
	}
	retainStart := rm.backlog.AvailableStartOffset()
	sz, err := wal.GetFileSize()
	if err != nil {
		logger.Logger.Debug().Err(err).Msg("backlog WAL size check failed")
		return
	}
	if sz > walTruncateFactor*rm.backlog.GetSize() {
		if err := wal.Truncate(retainStart); err != nil {
			logger.Logger.Warn().Err(err).Msg("backlog WAL truncate failed")
		}
	}
}

// SetFeedLoop 全局切换 feed 模式（S2 backlog 退役首步）——开启后新激活的从侧走
// REPLLOG 增量流（feed-mode——backlog 字节发送的双轨替代）——默认关闭（回滚零成本）。
func (rm *ReplicationManager) SetFeedLoop(enabled bool) {
	rm.feedLoop.Store(enabled)
}

// CatchUpAndEnableSlaveTS 为 feed 模式重连从侧做 **ts 域增量 catch-up**（S2 分级-3——
// 重连改 ts 域——backlog 影子退役治本）：从 resumeTS+1 起经 FeedSlave 同步补发
// [resumeTS+1, curTS] 的 REPLLOG gap（log 键值源零对齐——不再走字节 SendBacklogData，
// byte 坐标错域问题结构性消除——见 9435523 根因记录），然后 SetReady(true)。
// 激活（FeedSetEnabled + SetReady）在 propMu 内原子完成（与字节路径
// CatchUpAndEnableSlave 的激活语义一致——极小窗口——不阻塞写路径）；gap 补发在
// propMu 外——FeedSlave 经 SendCommand 要求 Ready（slave.go:130），故先激活再补发。
// 补发与 live-push 不会各发一份：两者都只经 FeedSlave 读同一个每从侧游标
// feedSinceTS（PropagateCommand 的 feed 分支同样调 FeedSlave），补发完成后游标推进到
// 最后已发 ts+1——后续 live-push 无缝续传。
// 注意：**从侧没有 ts 去重**（历史注释曾称"ts <= lastAppliedTS 跳过"——该机制不存在；
// 从侧 apply 路径对每条命令无条件执行，全包仅 psync.go CONTINUE 区间判定与
// reconnect.go 收敛判据两处比较 ts）。因此防双发完全依赖上面那个共享游标：
// FeedSlave 的"读游标→发送→推进"（feed_source.go）不是原子的（writeMu 只串行化 socket
// 写），同一从侧上的并发 FeedSlave 调用可能重发同一 ts 区间而从侧重复 apply。
// 该风险的可达率未实测（feed-mode 规模守卫 TestRegressionPsyncReconnectNoLossFeed
// 零 EXTRA/MISMATCH 通过——与本推断相左，尚无解释）——视为开放项，勿当既成结论。
// On error Ready stays true（已激活——由调用方 RemoveSlave 清理）。
func (rm *ReplicationManager) CatchUpAndEnableSlaveTS(slave *SlaveConnection, resumeTS uint64) error {
	// 激活 + Ready 在 propMu 内原子（防与 live-push 交错——字节路径同构）。
	rm.propMu.Lock()
	slave.FeedSetEnabled(true, resumeTS+1)
	slave.SetReady(true)
	rm.propMu.Unlock()
	if err := rm.FeedSlave(slave); err != nil {
		return err
	}
	return nil
}

// CatchUpAndEnableSlave drains backlog [startOffset, masterOffset) while
// Ready=false, then sets Ready under propMu so gap-fill never races with
// live PropagateCommand for the same offsets.
// Slave must already be in rm.slaves (AddSlave) with Ready=false.
// On error Ready stays false; the caller must RemoveSlave — installing a
// not-ready slave at startOffset after a failed GetRange/write skips the
// failed range on the next loop and leaves a hole.
func (rm *ReplicationManager) CatchUpAndEnableSlave(slave *SlaveConnection, startOffset int64) error {
	backlog := rm.GetBacklog()
	for {
		endOffset := rm.GetMasterReplOffset()
		if endOffset > startOffset {
			if err := SendBacklogData(slave, backlog, startOffset, endOffset); err != nil {
				return err
			}
			startOffset = endOffset
			continue
		}
		// Appears caught up. Hold propMu so no live SendCommand runs while
		// we re-check offset and flip Ready.
		rm.propMu.Lock()
		end2 := rm.GetMasterReplOffset()
		if end2 > startOffset {
			rm.propMu.Unlock()
			continue
		}
		slave.SetReplOffset(startOffset)
		if rm.feedLoop.Load() {
			// feed-mode 激活：feedSinceTS = 当前 log 键水位+1——激活前（RDB 快照 +
			// backlog 字节 catch-up）已覆盖到 startOffset——从下一条起走 REPLLOG
			// 增量——propMu 内激活（写路径 RLock 阻塞——竞态窗口写经字节路径补发
			//（就绪翻转后 SendCommand 成功）——无丢失无重复）。
			curTS, err := rm.store.ReplLogCurrentTS()
			if err != nil {
				rm.propMu.Unlock()
				return err
			}
			slave.FeedSetEnabled(true, curTS+1)
		}
		slave.SetReady(true)
		rm.propMu.Unlock()
		return nil
	}
}

// resizeBacklog builds a new ring of size newSize, copying the overlapping
// valid window from old (if any). Logical offset is preserved for PSYNC.
func resizeBacklog(old *ReplicationBacklog, newSize int64) *ReplicationBacklog {
	nb := NewReplicationBacklog(newSize)
	if old == nil {
		return nb
	}
	old.mu.RLock()
	oldOff := old.offset
	oldSize := old.size
	availStart := oldOff - oldSize
	if availStart < 0 {
		availStart = 0
	}
	copyStart := oldOff - newSize
	if copyStart < availStart {
		copyStart = availStart
	}
	if copyStart < 0 {
		copyStart = 0
	}
	length := oldOff - copyStart
	var data []byte
	if length > 0 {
		data = make([]byte, length)
		for i := int64(0); i < length; i++ {
			data[i] = old.buffer[(copyStart+i)%oldSize]
		}
	}
	old.mu.RUnlock()

	nb.mu.Lock()
	nb.offset = oldOff
	if length > 0 {
		for i := int64(0); i < length; i++ {
			nb.buffer[(copyStart+i)%newSize] = data[i]
		}
	}
	nb.mu.Unlock()
	return nb
}

// serializeCommand 序列化命令为RESP格式
func serializeCommand(cmd [][]byte) []byte {
	var buf []byte
	buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(cmd)))...)
	for _, arg := range cmd {
		buf = append(buf, []byte(fmt.Sprintf("$%d\r\n", len(arg)))...)
		buf = append(buf, arg...)
		buf = append(buf, []byte("\r\n")...)
	}
	return buf
}

// Stop 停止复制管理器
func (rm *ReplicationManager) Stop() {
	rm.mu.Lock()
	if rm.stopped {
		rm.mu.Unlock()
		return
	}
	rm.stopped = true

	// 在关闭连接前持久化偏移量与 backlog，确保干净重启时二者一致连续。
	// DB 在 Stop() 之后才关闭（main.go 关闭序列），写入安全。
	// 偏移量即 backlog 的写入水位，所以先取水位再落盘；此处已持 rm.mu，
	// 不能再走 GetMasterReplOffset()（同一 goroutine 上 Lock 后 RLock 会自锁）。
	if rm.store != nil {
		rm.backlog.mu.RLock()
		bOff := rm.backlog.offset
		bBuf := make([]byte, len(rm.backlog.buffer))
		copy(bBuf, rm.backlog.buffer)
		bSize := rm.backlog.size
		rm.backlog.mu.RUnlock()

		if saveErr := rm.store.SaveMasterReplOffset(bOff); saveErr != nil {
			logger.Logger.Warn().Err(saveErr).Int64("offset", bOff).Msg("failed to persist masterReplOffset on shutdown")
		} else {
			logger.Logger.Debug().Int64("offset", bOff).Msg("masterReplOffset persisted on shutdown")
		}

		// 持久化 backlog（环形缓冲区），使干净重启后从节点可以通过
		// PSYNC CONTINUE 增量同步，避免不必要的 FULLRESYNC。
		if saveErr := rm.store.SaveBacklog(bOff, bBuf, bSize); saveErr != nil {
			logger.Logger.Warn().Err(saveErr).Msg("failed to persist backlog on shutdown")
		} else {
			logger.Logger.Debug().Int("backlog_bytes", len(bBuf)).Msg("backlog persisted on shutdown")
		}
	}

	// 关闭 WAL（flush 剩余数据到磁盘）
	if rm.wal != nil {
		if closeErr := rm.wal.Close(); closeErr != nil {
			logger.Logger.Warn().Err(closeErr).Msg("failed to close backlog WAL")
		}
	}

	slaves := make([]*SlaveConnection, 0, len(rm.slaves))
	for _, slave := range rm.slaves {
		slaves = append(slaves, slave)
	}
	rm.slaves = make(map[string]*SlaveConnection)

	reconnector := rm.slaveReconnector
	rm.slaveReconnector = nil

	masterConn := rm.masterConn
	rm.masterConn = nil
	rm.mu.Unlock()

	// 先停重连器再关连接：reconnectLoop 只认 stopCh，且 MaxRetries=0 意味着它
	// 会永久重试；每一轮 tryReplicate 都会走到 LoadRDB / executeReplicatedCommand，
	// 也就是在 db.Close() 之后继续访问 store，违反关机不变量
	// （replMgr.Stop() → cancel() → handler.Shutdown() → backupMgr.Wait() → db.Close()）。
	// sr.Stop() 内部 wg.Wait() 而重连循环会取 rm.mu，因此必须在释放 rm.mu 之后调用。
	// 最坏阻塞约等于一次 dialMaster 超时（5s）加一轮 RDB 应用。
	if reconnector != nil {
		reconnector.Stop()
	}

	// 在 锁外关闭连接，避免与 handlePSyncWithRDB 的锁链死锁
	for _, slave := range slaves {
		if err := slave.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close slave connection")
		}
	}
	if masterConn != nil {
		if err := masterConn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close master connection")
		}
	}
}

// IsMaster 检查是否是主节点
func (rm *ReplicationManager) IsMaster() bool {
	return rm.GetRole() == RoleMaster
}

// IsSlave 检查是否是从节点
func (rm *ReplicationManager) IsSlave() bool {
	return rm.GetRole() == RoleSlave
}

// UpdateSlaveAckOffset 更新从节点的ACK偏移量
// UpdateSlaveAckTS 更新从节点确认的主侧 ts 水位（S2 ACK-ts 双轨——从侧
// lastAppliedTS——applied 语义——排水判据 D2 的数据源）。
func (rm *ReplicationManager) UpdateSlaveAckTS(slaveID string, ts uint64) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if slave, exists := rm.slaves[slaveID]; exists {
		slave.UpdateReplAckTS(ts)
		logger.Logger.Debug().
			Str("slave_id", slaveID).
			Uint64("ack_ts", ts).
			Msg("更新从节点ACK ts 水位")
	}
}

func (rm *ReplicationManager) UpdateSlaveAckOffset(slaveID string, offset int64) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if slave, exists := rm.slaves[slaveID]; exists {
		slave.UpdateReplAck(offset)
		logger.Logger.Debug().
			Str("slave_id", slaveID).
			Int64("ack_offset", offset).
			Msg("更新从节点ACK偏移量")
	}
}

// GetSlaveByID 根据ID获取从节点
func (rm *ReplicationManager) GetSlaveByID(slaveID string) *SlaveConnection {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.slaves[slaveID]
}

// GetSlaveByAddr 根据地址获取从节点
func (rm *ReplicationManager) GetSlaveByAddr(addr string) *SlaveConnection {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	for _, slave := range rm.slaves {
		if slave.Addr == addr {
			return slave
		}
	}
	return nil
}
