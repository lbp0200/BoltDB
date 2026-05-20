package replication

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

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
	role             string                      // "master" | "slave"
	masterAddr       string                      // 主节点地址(当role=slave时)
	masterConn       *MasterConnection           // 到主节点的连接(当role=slave时)
	slaves           map[string]*SlaveConnection // 从节点连接(当role=master时)
	backlog          *ReplicationBacklog         // 复制积压缓冲区
	masterReplOffset int64                       // 主节点复制偏移量
	replId           string                      // 复制ID(主节点运行ID)
	store            *store.BotreonStore         // 数据存储
	stopCh           chan struct{}               // 停止信号
	closeOnce        sync.Once                   // 确保关闭只执行一次
	slaveReconnector *SlaveReconnector           // 从节点自动重连器
}

// NewReplicationManager 创建新的复制管理器
func NewReplicationManager(store *store.BotreonStore) *ReplicationManager {
	replId, _ := generateReplicationID()
	rm := &ReplicationManager{
		role:             RoleMaster,
		slaves:           make(map[string]*SlaveConnection),
		backlog:          NewReplicationBacklog(1024 * 1024), // 1MB backlog
		masterReplOffset: 0,
		replId:           replId,
		store:            store,
		stopCh:           make(chan struct{}),
	}
	return rm
}

// generateReplicationID 生成40字符的十六进制复制ID
func generateReplicationID() (string, error) {
	bytes := make([]byte, 20)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
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

// GetMasterReplOffset 获取主节点复制偏移量
func (rm *ReplicationManager) GetMasterReplOffset() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.masterReplOffset
}

// SetMasterReplOffset 设置主节点复制偏移量
func (rm *ReplicationManager) SetMasterReplOffset(offset int64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.masterReplOffset = offset
}

// IncrementReplOffset 增加复制偏移量
func (rm *ReplicationManager) IncrementReplOffset(delta int64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.masterReplOffset += delta
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
func (rm *ReplicationManager) RemoveSlave(slaveID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if slave, exists := rm.slaves[slaveID]; exists {
		delete(rm.slaves, slaveID)
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

func (rm *ReplicationManager) GetReconnectCount() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if rm.slaveReconnector != nil {
		return rm.slaveReconnector.GetReconnectCount()
	}
	return 0
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
	rm.mu.RUnlock()

	// 总是将命令添加到backlog并更新offset，无论是否有从节点
	// 这使得断连期间的写操作不会丢失，重连后可通过PSYNC增量同步
	cmdBytes := serializeCommand(cmd)
	cmdOffset := backlog.Append(cmdBytes)

	// 更新复制偏移量（在Slave建立前也必须有正确的offset）
	rm.IncrementReplOffset(int64(len(cmdBytes)))

	// 传播到所有从节点
	for _, slave := range slaves {
		if slave.IsReady() {
			if err := slave.SendCommand(cmdBytes, cmdOffset); err != nil {
				logger.Logger.Warn().
					Str("slave_id", slave.ID).
					Err(err).
					Msg("传播命令到从节点失败")
				// 连接可能已断开，稍后会清理
			}
		}
	}
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
	rm.closeOnce.Do(func() {
		close(rm.stopCh)
	})

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 关闭所有从节点连接
	for _, slave := range rm.slaves {
		if err := slave.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close slave connection")
		}
	}
	rm.slaves = make(map[string]*SlaveConnection)

	// 关闭主节点连接
	if rm.masterConn != nil {
		if err := rm.masterConn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close master connection")
		}
		rm.masterConn = nil
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
