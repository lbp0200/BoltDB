package cluster

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// 集群节点标志常量
const (
	FlagMaster = "master"
	FlagSlave  = "slave"
	FlagMyself = "myself"
	FlagFail   = "fail"
	FlagPFail  = "pfail"
)

// Node 表示集群中的一个节点
type Node struct {
	ID       string      // 节点ID（40字符的十六进制字符串）
	Addr     string      // 节点地址，格式: "host:port"
	Flags    []string    // 节点标志，如: FlagMaster, FlagSlave, FlagMyself, FlagFail
	MasterID string      // 如果是slave，指向master的ID
	PingSent int64       // 最后一次ping发送时间（Unix时间戳，毫秒）
	PongRecv int64       // 最后一次pong接收时间（Unix时间戳，毫秒）
	Epoch    int64       // 配置纪元（config epoch）
	Slots    []SlotRange // 该节点负责的槽位范围
	mu       sync.RWMutex
	// 发现时间（首次加入集群的时间戳，毫秒），用于判定无响应节点的清理
	DiscoveredAt int64
	// 槽位迁移状态
	importingSlots map[uint32]string // 正在导入的槽 -> 源节点地址
	migratingSlots map[uint32]string // 正在迁移的槽 -> 目标节点地址
}

// SlotRange 表示槽位范围
type SlotRange struct {
	Start uint32 // 起始槽位（包含）
	End   uint32 // 结束槽位（包含）
}

// NewNode 创建新节点
func NewNode(id, addr string) *Node {
	return &Node{
		ID:             id,
		Addr:           addr,
		Flags:          []string{},
		Slots:          []SlotRange{},
		PingSent:       0,
		PongRecv:       0,
		Epoch:          0,
		DiscoveredAt:   time.Now().UnixMilli(),
		importingSlots: make(map[uint32]string),
		migratingSlots: make(map[uint32]string),
	}
}

// AddSlotRange 添加槽位范围
func (n *Node) AddSlotRange(start, end uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Merge with last range if adjacent or overlapping (common case for sequential calls)
	if len(n.Slots) > 0 {
		last := &n.Slots[len(n.Slots)-1]
		if start >= last.Start && start <= last.End+1 {
			if end > last.End {
				last.End = end
			}
			return
		}
	}

	n.Slots = append(n.Slots, SlotRange{Start: start, End: end})
}

// HasSlot 检查节点是否负责指定的槽位
func (n *Node) HasSlot(slot uint32) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, r := range n.Slots {
		if slot >= r.Start && slot <= r.End {
			return true
		}
	}
	return false
}

// GetSlotRanges 获取所有槽位范围
func (n *Node) GetSlotRanges() []SlotRange {
	n.mu.RLock()
	defer n.mu.RUnlock()
	ranges := make([]SlotRange, len(n.Slots))
	copy(ranges, n.Slots)
	return ranges
}

// IsMaster 检查节点是否是master
func (n *Node) IsMaster() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == FlagMaster {
			return true
		}
	}
	return false
}

// IsSlave 检查节点是否是slave
func (n *Node) IsSlave() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == FlagSlave {
			return true
		}
	}
	return false
}

// IsMyself 检查节点是否是当前节点
func (n *Node) IsMyself() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == FlagMyself {
			return true
		}
	}
	return false
}

// SetMyself 设置节点为当前节点
func (n *Node) SetMyself() {
	n.mu.Lock()
	defer n.mu.Unlock()
	newFlags := []string{}
	for _, flag := range n.Flags {
		if flag != FlagMyself {
			newFlags = append(newFlags, flag)
		}
	}
	n.Flags = append(newFlags, FlagMyself)
}

// GetPongRecv 返回最后一次pong接收时间
func (n *Node) GetPongRecv() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.PongRecv
}

// SetPongRecv 设置pong接收时间（用于测试）
func (n *Node) SetPongRecv(t int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.PongRecv = t
}

// UpdatePong 更新pong接收时间
func (n *Node) UpdatePong() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.PongRecv = time.Now().UnixMilli()
}

// MergeGossipState updates epoch/pong from gossip when the payload is newer.
func (n *Node) MergeGossipState(epoch, pongRecv int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if epoch > n.Epoch {
		n.Epoch = epoch
	}
	if pongRecv > n.PongRecv {
		n.PongRecv = pongRecv
	}
}

// MarkPFail adds the PFAIL flag when the node is not already marked failed.
func (n *Node) MarkPFail() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hasFailFlag() {
		return
	}
	n.Flags = append(n.Flags, FlagPFail)
}

// PromotePFailToFail promotes PFAIL to FAIL. Returns true when promotion happened.
func (n *Node) PromotePFailToFail() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hasFailFlag() {
		return false
	}
	n.Flags = append(n.Flags, FlagFail)
	cleaned := make([]string, 0, len(n.Flags))
	for _, f := range n.Flags {
		if f != FlagPFail {
			cleaned = append(cleaned, f)
		}
	}
	n.Flags = cleaned
	return true
}

// UpdatePing 更新ping发送时间
func (n *Node) UpdatePing() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.PingSent = time.Now().UnixMilli()
}

// HasFailFlag 检查节点是否已有 FAIL 或 PFAIL 标记（线程安全）
func (n *Node) HasFailFlag() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.hasFailFlag()
}

// hasFailFlag 检查节点是否已有 FAIL 或 PFAIL 标记（不加锁，调用者需持有锁）
func (n *Node) hasFailFlag() bool {
	for _, flag := range n.Flags {
		if flag == FlagFail || flag == FlagPFail {
			return true
		}
	}
	return false
}

// IsFailed 检查节点是否失败
func (n *Node) IsFailed() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, flag := range n.Flags {
		if flag == FlagFail {
			return true
		}
	}
	// 如果超过一定时间没有收到pong，认为节点失败
	if n.PongRecv > 0 {
		elapsed := time.Now().UnixMilli() - n.PongRecv
		if elapsed > 5000 { // 5秒超时
			return true
		}
	}
	return false
}

// String 返回节点的字符串表示
func (n *Node) String() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return fmt.Sprintf("%s %s", n.ID, n.Addr)
}

// GetHostPort 解析地址，返回host和port
func (n *Node) GetHostPort() (string, string, error) {
	host, port, err := net.SplitHostPort(n.Addr)
	if err != nil {
		return "", "", err
	}
	return host, port, nil
}

// IsImportingSlot 检查槽位是否正在导入
func (n *Node) IsImportingSlot(slot uint32) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, exists := n.importingSlots[slot]
	return exists
}

// IsMigratingSlot 检查槽位是否正在迁移
func (n *Node) IsMigratingSlot(slot uint32) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, exists := n.migratingSlots[slot]
	return exists
}

// SetImportingSlot 设置槽位正在导入
func (n *Node) SetImportingSlot(slot uint32, sourceNodeAddr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.importingSlots[slot] = sourceNodeAddr
}

// SetMigratingSlot 设置槽位正在迁移
func (n *Node) SetMigratingSlot(slot uint32, targetNodeAddr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.migratingSlots[slot] = targetNodeAddr
}

// ClearSlotMigration 清除槽位迁移状态
func (n *Node) ClearSlotMigration(slot uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.importingSlots, slot)
	delete(n.migratingSlots, slot)
}

// GetImportingSlots 获取所有正在导入的槽信息
func (n *Node) GetImportingSlots() []ImportingSlotInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make([]ImportingSlotInfo, 0, len(n.importingSlots))
	for slot, sourceNode := range n.importingSlots {
		result = append(result, ImportingSlotInfo{
			Slot:       slot,
			SourceNode: sourceNode,
		})
	}
	return result
}

// GetMigratingSlots 获取所有正在迁移的槽信息
func (n *Node) GetMigratingSlots() []MigratingSlotInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make([]MigratingSlotInfo, 0, len(n.migratingSlots))
	for slot, targetNode := range n.migratingSlots {
		result = append(result, MigratingSlotInfo{
			Slot:       slot,
			TargetNode: targetNode,
		})
	}
	return result
}

// GetImportingSlotSource 获取槽的源节点地址
func (n *Node) GetImportingSlotSource(slot uint32) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.importingSlots[slot]
}

// GetMigratingSlotTarget 获取槽的目标节点地址
func (n *Node) GetMigratingSlotTarget(slot uint32) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.migratingSlots[slot]
}
