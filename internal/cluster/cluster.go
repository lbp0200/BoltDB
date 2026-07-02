package cluster

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// Cluster 表示Redis集群
type Cluster struct {
	Myself *Node               // 当前节点
	Nodes  map[string]*Node    // 所有节点，key为节点ID
	Slots  [SlotCount]*Node    // 槽位到节点的映射
	Store  *store.BotreonStore // 数据存储
	Epoch  int64               // 当前配置纪元
	mu     sync.RWMutex        // 保护集群状态的锁
	Gossip *Gossiper           // 节点间 gossip 协议
	Bus    *ClusterBus         // 集群总线（节点间持久 TCP 连接）

	pfailReports map[string]map[string]struct{} // nodeID → set of reporters
}

// NewCluster 创建新集群
func NewCluster(store *store.BotreonStore, nodeID, addr string) (*Cluster, error) {
	if nodeID == "" {
		// 生成随机节点ID
		var err error
		nodeID, err = generateNodeID()
		if err != nil {
			return nil, fmt.Errorf("failed to generate node ID: %w", err)
		}
	}

	myself := NewNode(nodeID, addr)
	myself.SetMyself()
	myself.EnsureMasterFlag()

	cluster := &Cluster{
		Myself:       myself,
		Nodes:        make(map[string]*Node),
		Store:        store,
		Epoch:        0,
		pfailReports: make(map[string]map[string]struct{}),
	}
	cluster.Nodes[nodeID] = myself

	// 从持久化配置恢复（如果存在）
	found, err := cluster.LoadConfig()
	if err != nil {
		logger.Logger.Warn().Err(err).Msg("NewCluster: failed to load persisted config, starting fresh")
	}
	if found {
		// 恢复完持久化配置后，确保当前节点始终在节点表中
		if _, exists := cluster.Nodes[nodeID]; !exists {
			cluster.Nodes[nodeID] = myself
		}
	} else {
		// 初始化时，当前节点负责所有槽位
		for i := uint32(0); i < SlotCount; i++ {
			cluster.Slots[i] = myself
		}
		myself.AddSlotRange(0, SlotCount-1)
	}

	// 初始化 gossip（使用 background context；生产环境由 main.go 替换）
	cluster.Gossip = NewGossiper(context.Background(), cluster)

	// 初始化 cluster bus
	cluster.Bus = NewClusterBus(cluster)

	return cluster, nil
}

// generateNodeID 生成40字符的十六进制节点ID
func generateNodeID() (string, error) {
	bytes := make([]byte, 20)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// GetNodeBySlot 根据槽位获取负责该槽位的节点
func (c *Cluster) GetNodeBySlot(slot uint32) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if slot >= SlotCount {
		return nil
	}
	return c.Slots[slot]
}

// GetNodeByID 根据节点ID获取节点
func (c *Cluster) GetNodeByID(nodeID string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Nodes[nodeID]
}

// AddNode 添加节点到集群
func (c *Cluster) AddNode(node *Node) {
	c.mu.Lock()
	c.Nodes[node.ID] = node
	c.mu.Unlock()
	if err := c.SaveConfig(); err != nil {
		logger.Logger.Warn().Err(err).Str("nodeID", node.ID).Msg("AddNode: failed to persist config")
	}
}

// RemoveNode 从集群中移除节点
func (c *Cluster) RemoveNode(nodeID string) {
	c.mu.Lock()
	delete(c.Nodes, nodeID)
	// 将该节点负责的槽位重新分配给当前节点
	for i := uint32(0); i < SlotCount; i++ {
		if c.Slots[i] != nil && c.Slots[i].ID == nodeID {
			c.Slots[i] = c.Myself
			c.Myself.AddSlotRange(i, i)
		}
	}
	c.mu.Unlock()
	if err := c.SaveConfig(); err != nil {
		logger.Logger.Warn().Err(err).Str("nodeID", nodeID).Msg("RemoveNode: failed to persist config")
	}
}

// AssignSlot 将槽位分配给指定节点
func (c *Cluster) AssignSlot(slot uint32, nodeID string) error {
	c.mu.Lock()

	if slot >= SlotCount {
		c.mu.Unlock()
		return fmt.Errorf("slot %d out of range", slot)
	}

	node, exists := c.Nodes[nodeID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("node %s not found", nodeID)
	}

	c.Slots[slot] = node
	node.AddSlotRange(slot, slot)
	c.Epoch++
	node.SetEpoch(c.Epoch)

	// 清除当前节点的迁移状态（如果这个 slot 正在被迁移或导入）
	c.Myself.ClearSlotMigration(slot)

	c.mu.Unlock()

	return c.SaveConfig()
}

// AssignSlotRange 将槽位范围分配给指定节点
func (c *Cluster) AssignSlotRange(start, end uint32, nodeID string) error {
	c.mu.Lock()

	if start >= SlotCount || end >= SlotCount || start > end {
		c.mu.Unlock()
		return fmt.Errorf("invalid slot range: %d-%d", start, end)
	}

	node, exists := c.Nodes[nodeID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("node %s not found", nodeID)
	}

	for i := start; i <= end; i++ {
		c.Slots[i] = node
	}
	c.Epoch++
	node.SetEpoch(c.Epoch)
	node.AddSlotRange(start, end)
	c.mu.Unlock()

	return c.SaveConfig()
}

// RemoveSlot 移除槽位分配
func (c *Cluster) RemoveSlot(slot uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if slot >= SlotCount {
		return fmt.Errorf("slot %d out of range", slot)
	}

	owner := c.Slots[slot]
	if owner == nil {
		return nil
	}

	c.Slots[slot] = nil

	if owner.ID == c.Myself.ID {
		var updated []SlotRange
		for _, r := range c.Myself.GetSlotRanges() {
			if slot < r.Start || slot > r.End {
				updated = append(updated, r)
			} else if slot == r.Start && slot == r.End {
				continue
			} else if slot == r.Start {
				updated = append(updated, SlotRange{Start: slot + 1, End: r.End})
			} else if slot == r.End {
				updated = append(updated, SlotRange{Start: r.Start, End: slot - 1})
			} else {
				updated = append(updated, SlotRange{Start: r.Start, End: slot - 1})
				updated = append(updated, SlotRange{Start: slot + 1, End: r.End})
			}
		}
		c.Myself.SetSlots(updated)
	}

	return c.saveConfigLocked()
}

// GetSlotOwner 获取槽位的所有者节点
func (c *Cluster) GetSlotOwner(slot uint32) *Node {
	return c.GetNodeBySlot(slot)
}

// IsSlotLocal 检查槽位是否属于当前节点
func (c *Cluster) IsSlotLocal(slot uint32) bool {
	node := c.GetNodeBySlot(slot)
	return node != nil && node.ID == c.Myself.ID
}

// GetClusterNodes 获取所有节点的字符串表示（用于CLUSTER NODES命令）
func (c *Cluster) GetClusterNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []string
	for _, node := range c.Nodes {
		line := c.formatNodeLine(node)
		result = append(result, line)
	}
	return result
}

// formatNodeLine 格式化节点行为CLUSTER NODES格式
func (c *Cluster) formatNodeLine(node *Node) string {
	node.mu.RLock()
	flags := ""
	if len(node.Flags) > 0 {
		flags = node.Flags[0]
		for i := 1; i < len(node.Flags); i++ {
			flags += "," + node.Flags[i]
		}
	}

	masterID := "-"
	if node.MasterID != "" {
		masterID = node.MasterID
	}

	slots := ""
	if len(node.Slots) > 0 {
		slotStrs := []string{}
		for _, r := range node.Slots {
			if r.Start == r.End {
				slotStrs = append(slotStrs, fmt.Sprintf("%d", r.Start))
			} else {
				slotStrs = append(slotStrs, fmt.Sprintf("%d-%d", r.Start, r.End))
			}
		}
		slots = fmt.Sprintf(" %s", fmt.Sprintf("%v", slotStrs))
	}

	pingSent := node.PingSent
	pongRecv := node.PongRecv
	epoch := node.Epoch
	node.mu.RUnlock()

	return fmt.Sprintf("%s %s %s %s %d %d %d connected%s",
		node.ID, node.Addr, flags, masterID,
		pingSent, pongRecv, epoch, slots)
}

// GetClusterSlots 获取槽位分配信息（用于CLUSTER SLOTS命令）
func (c *Cluster) GetClusterSlots() [][]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 合并连续的槽位范围
	ranges := c.mergeSlotRanges()

	result := make([][]interface{}, 0, len(ranges))
	for _, r := range ranges {
		node := c.Slots[r.Start]
		if node == nil {
			continue
		}

		// 格式: [start, end, [ip, port, nodeid], ...]
		host, port, err := node.GetHostPort()
		if err != nil {
			continue
		}

		slotInfo := []interface{}{
			int64(r.Start),
			int64(r.End),
			[]interface{}{host, port, node.ID},
		}

		// 如果有replica，添加replica信息
		// 这里简化处理，实际应该查找该节点的slave节点

		result = append(result, slotInfo)
	}

	return result
}

// mergeSlotRanges 合并连续的槽位范围
func (c *Cluster) mergeSlotRanges() []SlotRange {
	// 按节点分组槽位
	nodeSlots := make(map[string][]uint32)
	for i := uint32(0); i < SlotCount; i++ {
		if c.Slots[i] != nil {
			nodeID := c.Slots[i].ID
			nodeSlots[nodeID] = append(nodeSlots[nodeID], i)
		}
	}

	// 为每个节点合并连续的槽位
	var allRanges []SlotRange
	for _, slots := range nodeSlots {
		ranges := mergeConsecutiveSlots(slots)
		allRanges = append(allRanges, ranges...)
	}

	return allRanges
}

// mergeConsecutiveSlots 合并连续的槽位
func mergeConsecutiveSlots(slots []uint32) []SlotRange {
	if len(slots) == 0 {
		return nil
	}

	// 排序槽位（这里假设已经排序，实际应该先排序）
	ranges := []SlotRange{}
	start := slots[0]
	end := slots[0]

	for i := 1; i < len(slots); i++ {
		if slots[i] == end+1 {
			end = slots[i]
		} else {
			ranges = append(ranges, SlotRange{Start: start, End: end})
			start = slots[i]
			end = slots[i]
		}
	}
	ranges = append(ranges, SlotRange{Start: start, End: end})

	return ranges
}

// GetMyself 获取当前节点
func (c *Cluster) GetMyself() *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Myself
}

// IncrementEpoch 增加配置纪元
func (c *Cluster) IncrementEpoch() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Epoch++
	return c.Epoch
}

// GetEpoch 获取当前配置纪元
func (c *Cluster) GetEpoch() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Epoch
}

// UpdateNodeEpoch 更新节点的配置纪元
func (c *Cluster) UpdateNodeEpoch(nodeID string, epoch int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if node, exists := c.Nodes[nodeID]; exists {
		node.SetEpoch(epoch)
		if epoch > c.Epoch {
			c.Epoch = epoch
		}
	}
}

// SlotMigrationState 槽位迁移状态
type SlotMigrationState struct {
	Slot       uint32
	SourceNode string
	TargetNode string
	State      string // "migrating" | "importing" | "stable"
}

// ImportingSlotInfo 导入中的槽信息
type ImportingSlotInfo struct {
	Slot       uint32
	SourceNode string
}

// MigratingSlotInfo 迁移中的槽信息
type MigratingSlotInfo struct {
	Slot       uint32
	TargetNode string
}

// IsImportingSlot 检查槽位是否正在导入到当前节点
func (c *Cluster) IsImportingSlot(slot uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if node := c.Slots[slot]; node != nil && node.ID == c.Myself.ID {
		// 检查当前节点是否标记为从其他节点导入此槽
		return c.Myself.IsImportingSlot(slot)
	}
	return false
}

// IsMigratingSlot 检查槽位是否正在从当前节点迁移出去
func (c *Cluster) IsMigratingSlot(slot uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if node := c.Slots[slot]; node != nil && node.ID == c.Myself.ID {
		// 检查当前节点是否标记为正在迁移此槽
		return c.Myself.IsMigratingSlot(slot)
	}
	return false
}

// GetImportingSlots 获取所有正在导入到当前节点的槽
func (c *Cluster) GetImportingSlots() []ImportingSlotInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Myself.GetImportingSlots()
}

// GetMigratingSlots 获取所有正在从当前节点迁移的槽
func (c *Cluster) GetMigratingSlots() []MigratingSlotInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Myself.GetMigratingSlots()
}

// SetSlotImporting 设置槽位正在导入
func (c *Cluster) SetSlotImporting(slot uint32, sourceNodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.Nodes[sourceNodeID]; exists {
		c.Myself.SetImportingSlot(slot, node.Addr)
		_ = c.saveConfigLocked()
	}
}

// SetSlotMigrating 设置槽位正在迁移
func (c *Cluster) SetSlotMigrating(slot uint32, targetNodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.Nodes[targetNodeID]; exists {
		c.Myself.SetMigratingSlot(slot, node.Addr)
		_ = c.saveConfigLocked()
	}
}

// MarkNodePFail 标记一个节点为 PFAIL
func (c *Cluster) MarkNodePFail(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.Nodes[nodeID]; ok && n.ID != c.Myself.ID {
		n.MarkPFail()
	}
}

// GetPFailReports 获取 PFAIL 报告计数（用于测试）
func (c *Cluster) GetPFailReports(nodeID string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	reports, ok := c.pfailReports[nodeID]
	if !ok {
		return 0
	}
	return len(reports)
}

// ClearSlotMigration 清除槽位迁移状态
func (c *Cluster) ClearSlotMigration(slot uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Myself.ClearSlotMigration(slot)
	_ = c.saveConfigLocked()
}

// MigrateSlot 迁移一个槽位的所有 key 到目标节点。
// 自动从源节点读取所有属于该槽位的 key，通过 DUMP/RESTORE 传输到目标节点，
// 迁移完成后更新 slot 归属到目标节点。
func (c *Cluster) MigrateSlot(slot uint32, targetNodeID string, copyKeys bool) error {
	// 警告：当前实现不是 crash-safe 的。逐 key 迁移（DUMP → TCP → RESTORE → DEL）
	// 没有两阶段提交、重做日志或回滚机制。连接中断或进程崩溃可能导致数据丢失、
	// 数据重复或 slot 归属不一致。
	// 完整的 crash-safe 迁移（WAL + 两阶段提交）预计需要 2-3 周开发。
	// 当前建议：slot 调整仅在集群初始化或重启时静态配置。参见 docs/plans/TODO.md E3。
	if slot >= SlotCount {
		return fmt.Errorf("slot %d out of range", slot)
	}

	// 验证 slot 在当前节点处于 MIGRATING 状态
	if !c.IsMigratingSlot(slot) {
		return fmt.Errorf("slot %d is not in MIGRATING state", slot)
	}

	// 找目标节点
	c.mu.RLock()
	targetNode, exists := c.Nodes[targetNodeID]
	c.mu.RUnlock()
	if !exists || targetNode == nil {
		return fmt.Errorf("target node %s not found", targetNodeID)
	}

	logger.Logger.Info().
		Uint32("slot", slot).
		Str("target", targetNodeID).
		Str("target_addr", targetNode.Addr).
		Bool("copy", copyKeys).
		Msg("MigrateSlot: starting slot migration")

	// 收集所有属于该 slot 的 key
	var keys []string
	err := c.Store.IterateRawKeys(func(rawKey string) bool {
		if Slot(rawKey) == slot {
			keys = append(keys, rawKey)
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("iterate keys for slot %d: %w", slot, err)
	}

	logger.Logger.Info().
		Uint32("slot", slot).
		Int("key_count", len(keys)).
		Msg("MigrateSlot: collected keys to migrate")

	// 连接目标节点
	conn, err := net.DialTimeout("tcp", targetNode.Addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect to target %s: %w", targetNode.Addr, err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	var migratedKeys []string

	// 逐个迁移 key
	for _, key := range keys {
		// DUMP key
		data, err := c.Store.Dump(key)
		if err != nil {
			if strings.Contains(err.Error(), "no such key") {
				continue
			}
			_ = conn.Close()
			return fmt.Errorf("dump key %s: %w", key, err)
		}

		// 构建 RESTORE 命令
		restoreCmd := &proto.Array{
			Args: [][]byte{
				[]byte("RESTORE"),
				[]byte(key),
				[]byte("0"),
				data,
				[]byte("REPLACE"),
			},
		}

		if err := proto.WriteRESP(conn, restoreCmd); err != nil {
			_ = conn.Close()
			return fmt.Errorf("write RESTORE for key %s: %w", key, err)
		}

		// 读取响应
		resp, err := proto.ReadRESP(reader)
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("read RESTORE response for key %s: %w", key, err)
		}

		// 检查错误响应（RESTORE 失败时服务器返回 -ERR）
		if len(resp.Args) > 0 && len(resp.Args[0]) > 0 && resp.Args[0][0] == 'E' {
			_ = conn.Close()
			return fmt.Errorf("target error for key %s: %s", key, string(resp.Args[0]))
		}

		migratedKeys = append(migratedKeys, key)
	}

	// 如果未指定 COPY，删除本地 key
	if !copyKeys {
		for _, key := range migratedKeys {
			if _, delErr := c.Store.Del(key); delErr != nil {
				logger.Logger.Warn().
					Err(delErr).
					Str("key", key).
					Msg("MigrateSlot: failed to delete local key after migration")
			}
		}
	}

	// 清理迁移状态并更新 slot 归属
	c.ClearSlotMigration(slot)
	_ = c.AssignSlot(slot, targetNodeID)

	// 通知目标节点清除 IMPORTING 状态（通过现有的 TCP 连接）
	if !copyKeys {
		stableCmd := &proto.Array{
			Args: [][]byte{
				[]byte("CLUSTER"),
				[]byte("SETSLOT"),
				[]byte(strconv.FormatUint(uint64(slot), 10)),
				[]byte("STABLE"),
			},
		}
		_ = proto.WriteRESP(conn, stableCmd)
		// 不检查响应——连接将在函数返回时关闭
	}

	logger.Logger.Info().
		Uint32("slot", slot).
		Str("target", targetNodeID).
		Int("migrated", len(migratedKeys)).
		Msg("MigrateSlot: slot migration completed")

	return nil
}

// GetAskRedirect 获取ASK重定向信息
func (c *Cluster) GetAskRedirect(key string) *RedirectError {
	slot := Slot(key)

	// 如果槽属于当前节点，检查是否正在导入
	if c.IsSlotLocal(slot) && c.IsImportingSlot(slot) {
		// 返回ASK重定向到源节点
		importingSlots := c.GetImportingSlots()
		for _, info := range importingSlots {
			if info.Slot == slot {
				return NewAskError(slot, info.SourceNode)
			}
		}
	}

	return nil
}
