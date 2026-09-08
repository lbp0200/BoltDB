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

// ReplicationManager 管理主从复制
type ReplicationManager struct {
	mu               sync.RWMutex
	propMu           sync.RWMutex                // serializes live SendCommand vs slave install Ready flip
	role             string                      // RoleMaster | RoleSlave
	masterAddr       string                      // 主节点地址(当role=slave时)
	masterConn       *MasterConnection           // 到主节点的连接(当role=slave时)
	slaves           map[string]*SlaveConnection // 从节点连接(当role=master时)
	replId           string                      // 复制ID(主节点运行ID)
	store            *store.BotreonStore         // 数据存储
	stopped          bool                        // 是否已停止
	slaveReconnector *SlaveReconnector           // 从节点自动重连器
	tlsConfig        *tls.Config                 // TLS 配置（nil = 不使用 TLS）

	// Drop-path counters for diagnosing silent replica divergence
	// (docs/plans/TODO.md §1c). Live SendCommand failures are counted;
	// catch-up / FULLRESYNC recover from the store.
	sendDropCount  atomic.Int64
	applySkipCount atomic.Int64
}

// NewReplicationManager 创建新的复制管理器。
// 首次启动时生成新的复制 ID；重启时从 BadgerDB 读取已有的复制 ID，
// 使从节点可以通过 PSYNC CONTINUE 而非 FULLRESYNC 重新连接。
func NewReplicationManager(store *store.BotreonStore) *ReplicationManager {
	replId, err := store.LoadReplID()
	if err != nil {
		logger.Logger.Warn().Err(err).Msg("Failed to load persisted replId, generating new one")
	}
	if replId == "" {
		replId, _ = generateReplicationID()
		if saveErr := store.SaveReplID(replId); saveErr != nil {
			logger.Logger.Warn().Err(saveErr).Msg("Failed to persist new replId")
		}
	}

	return &ReplicationManager{
		role:   RoleMaster,
		slaves: make(map[string]*SlaveConnection),
		replId: replId,
		store:  store,
	}
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

// GetMasterReplOffset 获取主节点复制水位（ts 域——store.ReplLogCurrentTS）。
func (rm *ReplicationManager) GetMasterReplOffset() int64 {
	ts, _ := rm.store.ReplLogCurrentTS()
	// #nosec G115——ts 单调小步增长，远低于 MaxInt64
	return int64(ts)
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

// GetSlaveLastAppliedTS 获取从节点已应用的直接主侧 ts 水位（slave 角色时有效——
// S2 feed 协议——阶段 1：framework.WaitForReplicaSync 的 feed 模式 ts 判据数据源
// ——与 GetSlaveReplOffset（字节域）错域——feed 部署下用 ts 面做同步判据）。
func (rm *ReplicationManager) GetSlaveLastAppliedTS() uint64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.slaveReconnector != nil {
		return rm.slaveReconnector.GetLastAppliedTS()
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

// PropagateCommand 传播命令到所有从节点（feed-only——REPLLOG ts 域增量流）。
// 每条写命令经 FeedSlave 把 log 键值源 [feedSinceTS+1, curTS] 推给各 Ready 从侧；
// 断连期间已 commit 的 log 键在重连时由 CatchUpAndEnableSlaveTS 补发（ts 域 catch-up）。
func (rm *ReplicationManager) PropagateCommand(cmd [][]byte) {
	rm.mu.RLock()
	slaves := make([]*SlaveConnection, 0, len(rm.slaves))
	for _, slave := range rm.slaves {
		slaves = append(slaves, slave)
	}
	rm.mu.RUnlock()

	// Live push under propMu so slave install (CatchUpAndEnableSlaveTS) can
	// flip Ready without overlapping gap-fill for the same ts range
	// (non-idempotent double apply).
	rm.propMu.RLock()
	defer rm.propMu.RUnlock()
	for _, slave := range slaves {
		if slave.IsReady() {
			if err := rm.FeedSlave(slave); err != nil {
				rm.sendDropCount.Add(1)
				logger.Logger.Warn().
					Str("slave_id", slave.ID).
					Err(err).
					Int64("send_drop_count", rm.sendDropCount.Load()).
					Msg("feed 增量发送到从节点失败")
			}
		}
	}
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

	// 偏移量与 log 键均持久化于 store（BadgerDB）——重启后 GetMasterReplOffset =
	// ReplLogCurrentTS 从 log 键重建，无需单独落盘 offset/backlog（环已退役）。

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
