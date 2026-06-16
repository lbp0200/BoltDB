package sentinel

import (
	"fmt"
	"net"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// FailoverManager 故障转移管理器
type FailoverManager struct {
	sentinel *Sentinel
}

// NewFailoverManager 创建新的故障转移管理器
func NewFailoverManager(sentinel *Sentinel) *FailoverManager {
	return &FailoverManager{
		sentinel: sentinel,
	}
}

// StartFailover 启动故障转移
func (fm *FailoverManager) StartFailover(masterName string) error {
	master := fm.sentinel.GetMaster(masterName)
	if master == nil {
		return fmt.Errorf("master %s not found", masterName)
	}

	// 检查是否已经在下线状态
	if !master.IsDown() {
		return fmt.Errorf("master %s is not down", masterName)
	}

	// 检查是否已经有其他哨兵在执行故障转移
	if master.GetState() == "failover" {
		return fmt.Errorf("failover already in progress for master %s", masterName)
	}

	// 设置状态为故障转移中
	master.SetState("failover")
	fm.sentinel.Metrics.RecordFailoverStart(masterName)

	logger.Logger.Info().
		Str("master_name", masterName).
		Str("master_addr", master.GetAddr()).
		Msg("开始故障转移")

	// 选择新的主节点
	newMaster := fm.selectNewMaster(master)
	if newMaster == nil {
		master.SetState("odown")
		return fmt.Errorf("no suitable slave found for failover")
	}

	logger.Logger.Info().
		Str("new_master", newMaster.Addr).
		Str("old_master", master.GetAddr()).
		Msg("选中的新主节点")

	// 执行故障转移
	if err := fm.executeFailover(master, newMaster); err != nil {
		master.SetState("odown")
		return fmt.Errorf("failover failed: %w", err)
	}

	// 更新配置
	fm.updateConfiguration(master, newMaster)

	logger.Logger.Info().
		Str("master_name", masterName).
		Str("new_master", newMaster.Addr).
		Msg("故障转移完成")

	return nil
}

// selectNewMaster 选择新的主节点
func (fm *FailoverManager) selectNewMaster(oldMaster *MasterInstance) *SlaveInstance {
	slaves := oldMaster.GetSlaves()

	if len(slaves) == 0 {
		return nil
	}

	// 按偏移量排序候选从节点
	type candidate struct {
		slave *SlaveInstance
		score int64
	}
	var candidates []candidate
	for _, slave := range slaves {
		if slave.State != "online" {
			continue
		}
		candidates = append(candidates, candidate{slave: slave, score: slave.Offset})
	}

	// 按偏移量降序排列
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].score > candidates[i].score {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// 遍历候选集，尝试存活探测，返回第一个可达的
	for _, c := range candidates {
		conn, err := net.DialTimeout("tcp", c.slave.Addr, 3*time.Second)
		if err != nil {
			logger.Logger.Warn().
				Str("slave_addr", c.slave.Addr).
				Int64("offset", c.slave.Offset).
				Err(err).
				Msg("候选从节点不可达，跳过")
			continue
		}
		_ = conn.Close()
		logger.Logger.Info().
			Str("slave_addr", c.slave.Addr).
			Int64("offset", c.slave.Offset).
			Msg("选中从节点作为新主节点")
		return c.slave
	}

	return nil
}

// executeFailover 执行故障转移
func (fm *FailoverManager) executeFailover(oldMaster *MasterInstance, newMaster *SlaveInstance) error {
	// 1. 将选中的从节点提升为主节点
	// 发送 SLAVEOF NO ONE 命令
	logger.Logger.Info().
		Str("slave_addr", newMaster.Addr).
		Msg("发送 SLAVEOF NO ONE 到新主节点")

	if err := SendSlaveOfNoOne(newMaster.Addr); err != nil {
		logger.Logger.Error().
			Str("slave_addr", newMaster.Addr).
			Err(err).
			Msg("发送 SLAVEOF NO ONE 失败")
		return fmt.Errorf("failed to promote slave: %w", err)
	}

	// 2. 等待新主节点就绪（轮询 ROLE，最长等待 5s）
	logger.Logger.Info().Msg("等待新主节点就绪...")
	fm.sentinel.Metrics.RecordNewMaster(oldMaster.GetName())
	role, err := waitForRole(newMaster.Addr, 5*time.Second, 100*time.Millisecond)
	if err != nil {
		logger.Logger.Warn().
			Str("addr", newMaster.Addr).
			Err(err).
			Msg("无法验证新主节点角色")
	} else {
		logger.Logger.Info().
			Str("addr", newMaster.Addr).
			Str("role", role).
			Msg("新主节点已就绪")
	}

	// 4. 将其他从节点重新配置为复制新主节点
	slaves := oldMaster.GetSlaves()
	for _, slave := range slaves {
		if slave.ID != newMaster.ID && slave.State == "online" {
			logger.Logger.Info().
				Str("slave_addr", slave.Addr).
				Str("new_master", newMaster.Addr).
				Msg("重新配置从节点")

			if err := SendReplicaOf(slave.Addr, newMaster.Addr); err != nil {
				logger.Logger.Warn().
					Str("slave", slave.Addr).
					Err(err).
					Msg("重新配置从节点失败")
			}
		}
	}

	return nil
}

// updateConfiguration 更新配置
func (fm *FailoverManager) updateConfiguration(oldMaster *MasterInstance, newMaster *SlaveInstance) {
	// 更新配置纪元
	fm.sentinel.IncrementConfigEpoch()

	// 更新主节点地址
	oldMaster.mu.Lock()
	oldMaster.addr = newMaster.Addr
	oldMaster.state = "ok"
	oldMaster.mu.Unlock()

	fm.sentinel.Metrics.RecordLeaderChange()

	logger.Logger.Info().
		Str("master_name", oldMaster.GetName()).
		Str("new_addr", newMaster.Addr).
		Msg("更新主节点配置")
}

// AutoFailover 自动故障转移（检测到客观下线时触发）
func (fm *FailoverManager) AutoFailover(masterName string) error {
	// 检查是否满足自动故障转移条件
	master := fm.sentinel.GetMaster(masterName)
	if master == nil {
		return fmt.Errorf("master %s not found", masterName)
	}

	// 检查是否满足客观下线条件
	if !master.IsODown() {
		return fmt.Errorf("master %s is not objectively down", masterName)
	}

	// 检查是否已经有其他哨兵在执行故障转移
	if master.GetState() == "failover" {
		return fmt.Errorf("failover already in progress for master %s", masterName)
	}

	logger.Logger.Info().
		Str("master_name", masterName).
		Str("master_addr", master.GetAddr()).
		Msg("自动故障转移条件满足，开始故障转移")

	return fm.StartFailover(masterName)
}

// waitForRole 轮询服务器角色，直到返回预期前缀或超时
func waitForRole(addr string, timeout, interval time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		role, err := GetRole(addr)
		if err == nil {
			return role, nil
		}
		lastErr = err
		time.Sleep(interval)
	}
	return "", fmt.Errorf("timed out waiting for role: %w", lastErr)
}
