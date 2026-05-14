package cluster

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// ClusterCommands 处理CLUSTER命令
type ClusterCommands struct {
	cluster *Cluster
}

// NewClusterCommands 创建CLUSTER命令处理器
func NewClusterCommands(cluster *Cluster) *ClusterCommands {
	return &ClusterCommands{cluster: cluster}
}

// HandleCommand 处理CLUSTER命令
func (cc *ClusterCommands) HandleCommand(args []string) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("ERR wrong number of arguments for 'CLUSTER' command")
	}

	subcommand := strings.ToUpper(args[0])
	subArgs := args[1:]

	switch subcommand {
	case "NODES":
		return cc.handleNodes(subArgs)
	case "SLOTS":
		return cc.handleSlots(subArgs)
	case "INFO":
		return cc.handleInfo(subArgs)
	case "KEYSLOT":
		return cc.handleKeySlot(subArgs)
	case "GETKEYSINSLOT":
		return cc.handleGetKeysInSlot(subArgs)
	case "SETSLOT":
		return cc.handleSetSlot(subArgs)
	case "MEET":
		return cc.handleMeet(subArgs)
	case "FORGET":
		return cc.handleForget(subArgs)
	case "REPLICATE":
		return cc.handleReplicate(subArgs)
	case "SAVECONFIG":
		return cc.handleSaveConfig(subArgs)
	case "ADDSLOTS":
		return cc.handleAddSlots(subArgs)
	case "DELSLOTS":
		return cc.handleDelSlots(subArgs)
	case "FLUSHSLOTS":
		return cc.handleFlushSlots(subArgs)
	case "COUNTKEYSINSLOT":
		return cc.handleCountKeysInSlot(subArgs)
	case "MYID":
		return cc.handleMyID(subArgs)
	case "EPOCH":
		return cc.handleEpoch(subArgs)
	case "SLAVES":
		return cc.handleSlaves(subArgs)
	case "RESET":
		return cc.handleReset(subArgs)
	case "CALLS":
		return cc.handleCalls(subArgs)
	case "TOTALKEYS":
		return cc.handleTotalKeys(subArgs)
	default:
		return nil, fmt.Errorf("ERR unknown subcommand '%s'", subcommand)
	}
}

// handleNodes 处理CLUSTER NODES命令
func (cc *ClusterCommands) handleNodes(args []string) (string, error) {
	nodes := cc.cluster.GetClusterNodes()
	return strings.Join(nodes, "\n"), nil
}

// handleSlots 处理CLUSTER SLOTS命令
func (cc *ClusterCommands) handleSlots(args []string) (interface{}, error) {
	slots := cc.cluster.GetClusterSlots()
	return slots, nil
}

// handleInfo 处理CLUSTER INFO命令
func (cc *ClusterCommands) handleInfo(args []string) (string, error) {
	myself := cc.cluster.GetMyself()
	epoch := cc.cluster.GetEpoch()

	// 统计信息
	totalNodes := len(cc.cluster.Nodes)
	totalSlots := 0
	for i := uint32(0); i < SlotCount; i++ {
		if cc.cluster.Slots[i] == myself {
			totalSlots++
		}
	}

	info := fmt.Sprintf(`cluster_state:ok
cluster_slots_assigned:%d
cluster_slots_ok:%d
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:%d
cluster_size:1
cluster_current_epoch:%d
cluster_my_epoch:%d
cluster_stats_messages_sent:0
cluster_stats_messages_received:0`,
		totalSlots, totalSlots, totalNodes, epoch, myself.Epoch)

	return info, nil
}

// handleKeySlot 处理CLUSTER KEYSLOT命令
func (cc *ClusterCommands) handleKeySlot(args []string) (int64, error) {
	if len(args) < 1 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'CLUSTER KEYSLOT' command")
	}
	key := args[0]
	slot := Slot(key)
	return int64(slot), nil
}

// handleGetKeysInSlot 处理CLUSTER GETKEYSINSLOT命令
func (cc *ClusterCommands) handleGetKeysInSlot(args []string) ([]string, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("ERR wrong number of arguments for 'CLUSTER GETKEYSINSLOT' command")
	}

	slot, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("ERR invalid slot number")
	}

	count, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || count < 0 {
		return nil, fmt.Errorf("ERR invalid count")
	}

	if slot >= uint64(SlotCount) {
		return nil, fmt.Errorf("ERR slot out of range")
	}

	// 简化实现：返回空列表
	// 实际应该扫描数据库，找到属于该槽位的所有键
	return []string{}, nil
}

// handleSetSlot 处理CLUSTER SETSLOT命令
func (cc *ClusterCommands) handleSetSlot(args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER SETSLOT' command")
	}

	slot, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return "", fmt.Errorf("ERR invalid slot number")
	}

	subcommand := strings.ToUpper(args[1])
	subArgs := args[2:]

	switch subcommand {
	case "IMPORTING":
		// 导入槽位（迁移中）
		return "OK", nil
	case "MIGRATING":
		// 迁移槽位（迁移中）
		if len(subArgs) < 1 {
			return "", fmt.Errorf("ERR wrong number of arguments")
		}
		return "OK", nil
	case "STABLE":
		// 稳定状态
		return "OK", nil
	case "NODE":
		// 设置槽位所属节点
		if len(subArgs) < 1 {
			return "", fmt.Errorf("ERR wrong number of arguments")
		}
		nodeID := subArgs[0]
		err := cc.cluster.AssignSlot(uint32(slot), nodeID)
		if err != nil {
			return "", err
		}
		return "OK", nil
	default:
		return "", fmt.Errorf("ERR unknown subcommand '%s'", subcommand)
	}
}

// handleMeet 处理CLUSTER MEET命令
func (cc *ClusterCommands) handleMeet(args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER MEET' command")
	}

	ip := args[0]
	port, err := strconv.Atoi(args[1])
	if err != nil {
		return "", fmt.Errorf("ERR invalid port")
	}

	addr := fmt.Sprintf("%s:%d", ip, port)

	// 创建新节点（简化实现，实际应该通过握手获取节点ID）
	nodeID, err := generateNodeID()
	if err != nil {
		return "", err
	}
	node := NewNode(nodeID, addr)
	node.Flags = append(node.Flags, "master")
	cc.cluster.AddNode(node)

	return "OK", nil
}

// handleForget 处理CLUSTER FORGET命令
func (cc *ClusterCommands) handleForget(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER FORGET' command")
	}

	nodeID := args[0]
	if nodeID == cc.cluster.Myself.ID {
		return "", fmt.Errorf("ERR I can't forget myself")
	}

	cc.cluster.RemoveNode(nodeID)
	return "OK", nil
}

// handleReplicate 处理CLUSTER REPLICATE命令
func (cc *ClusterCommands) handleReplicate(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER REPLICATE' command")
	}

	masterID := args[0]
	master := cc.cluster.GetNodeByID(masterID)
	if master == nil {
		return "", fmt.Errorf("ERR unknown node %s", masterID)
	}

	// 将当前节点设置为slave
	cc.cluster.Myself.MasterID = masterID
	cc.cluster.Myself.Flags = []string{"slave", "myself"}

	// 移除当前节点的槽位分配
	for i := uint32(0); i < SlotCount; i++ {
		if cc.cluster.Slots[i] == cc.cluster.Myself {
			cc.cluster.Slots[i] = master
		}
	}
	cc.cluster.Myself.Slots = []SlotRange{}

	return "OK", nil
}

// handleSaveConfig 处理CLUSTER SAVECONFIG命令
func (cc *ClusterCommands) handleSaveConfig(args []string) (string, error) {
	// 简化实现：配置已持久化在数据库中
	return "OK", nil
}

// handleAddSlots 处理CLUSTER ADDSLOTS命令
func (cc *ClusterCommands) handleAddSlots(args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER ADDSLOTS' command")
	}

	for _, arg := range args {
		slot, err := strconv.ParseUint(arg, 10, 32)
		if err != nil {
			return "", fmt.Errorf("ERR invalid slot number: %s", arg)
		}

		if slot >= uint64(SlotCount) {
			return "", fmt.Errorf("ERR slot %d out of range", slot)
		}

		// 检查槽位是否已被分配
		currentOwner := cc.cluster.GetNodeBySlot(uint32(slot))
		if currentOwner != nil && currentOwner.ID != cc.cluster.Myself.ID {
			return "", fmt.Errorf("ERR Slot %d is already busy", slot)
		}

		err = cc.cluster.AssignSlot(uint32(slot), cc.cluster.Myself.ID)
		if err != nil {
			return "", err
		}
	}

	return "OK", nil
}

// handleDelSlots 处理CLUSTER DELSLOTS命令
func (cc *ClusterCommands) handleDelSlots(args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER DELSLOTS' command")
	}

	for _, arg := range args {
		slot, err := strconv.ParseUint(arg, 10, 32)
		if err != nil {
			return "", fmt.Errorf("ERR invalid slot number: %s", arg)
		}

		if slot >= uint64(SlotCount) {
			return "", fmt.Errorf("ERR slot %d out of range", slot)
		}

		// 移除槽位分配（设置为nil或重新分配）
		// 简化实现：不删除，只是标记
	}

	return "OK", nil
}

// handleFlushSlots 处理CLUSTER FLUSHSLOTS命令
func (cc *ClusterCommands) handleFlushSlots(args []string) (string, error) {
	// 清除当前节点的所有槽位
	myself := cc.cluster.Myself
	for i := uint32(0); i < SlotCount; i++ {
		if cc.cluster.Slots[i] == myself {
			cc.cluster.Slots[i] = nil
		}
	}
	myself.Slots = []SlotRange{}

	return "OK", nil
}

// handleCountKeysInSlot 处理CLUSTER COUNTKEYSINSLOT命令
func (cc *ClusterCommands) handleCountKeysInSlot(args []string) (int64, error) {
	if len(args) < 1 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'CLUSTER COUNTKEYSINSLOT' command")
	}

	slot, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("ERR invalid slot number")
	}

	if slot >= uint64(SlotCount) {
		return 0, fmt.Errorf("ERR slot out of range")
	}

	// 简化实现：返回0
	// 实际应该扫描数据库，统计属于该槽位的键数量
	return 0, nil
}

// handleMyID 处理CLUSTER MYID命令
func (cc *ClusterCommands) handleMyID(args []string) (string, error) {
	return cc.cluster.Myself.ID, nil
}

// handleEpoch 处理CLUSTER EPOCH命令
func (cc *ClusterCommands) handleEpoch(args []string) (int64, error) {
	return cc.cluster.GetEpoch(), nil
}

// handleSlaves 处理CLUSTER SLAVES命令
func (cc *ClusterCommands) handleSlaves(args []string) ([]string, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments for 'CLUSTER SLAVES' command")
	}

	nodeID := args[0]
	master := cc.cluster.GetNodeByID(nodeID)
	if master == nil {
		return nil, fmt.Errorf("ERR No such node %s", nodeID)
	}

	// 获取所有slave节点
	var slaves []string
	for _, node := range cc.cluster.Nodes {
		if node.MasterID == nodeID {
			line := cc.cluster.formatNodeLine(node)
			slaves = append(slaves, line)
		}
	}

	return slaves, nil
}

// handleReset 处理CLUSTER RESET命令
func (cc *ClusterCommands) handleReset(args []string) (string, error) {
	// 默认使用SOFT重置
	hard := false
	for _, arg := range args {
		if strings.ToUpper(arg) == "HARD" {
			hard = true
		}
	}

	cc.cluster.mu.Lock()
	defer cc.cluster.mu.Unlock()

	// 清除所有节点（除了自己）
	cc.cluster.Nodes = make(map[string]*Node)
	cc.cluster.Nodes[cc.cluster.Myself.ID] = cc.cluster.Myself

	// 重置槽位分配
	if hard {
		// HARD: 清除所有槽位
		for i := uint32(0); i < SlotCount; i++ {
			cc.cluster.Slots[i] = nil
		}
		cc.cluster.Myself.Slots = []SlotRange{}
	} else {
		// SOFT: 保留当前节点的槽位
		for i := uint32(0); i < SlotCount; i++ {
			if cc.cluster.Slots[i] != cc.cluster.Myself {
				cc.cluster.Slots[i] = nil
			}
		}
		// 重新计算自己的槽位范围
		var mySlots []uint32
		for i := uint32(0); i < SlotCount; i++ {
			if cc.cluster.Slots[i] == cc.cluster.Myself {
				mySlots = append(mySlots, i)
			}
		}
		ranges := mergeConsecutiveSlots(mySlots)
		cc.cluster.Myself.Slots = ranges
	}

	// 重置节点状态
	cc.cluster.Myself.MasterID = ""
	cc.cluster.Myself.Flags = []string{"master", "myself"}

	// 增加纪元
	cc.cluster.Epoch++

	return "OK", nil
}

// clusterStats 用于存储集群调用统计
type clusterStats struct {
	CommandsProcessed int64
	NetInputBytes     int64
	NetOutputBytes    int64
	mu                sync.RWMutex
}

var globalClusterStats = &clusterStats{
	CommandsProcessed: 0,
	NetInputBytes:     0,
	NetOutputBytes:    0,
}

// handleCalls 处理CLUSTER CALLS命令
func (cc *ClusterCommands) handleCalls(args []string) ([]interface{}, error) {
	globalClusterStats.mu.RLock()
	defer globalClusterStats.mu.RUnlock()

	// 返回集群调用统计信息
	// 格式: [nodename, num_commands, num_bytes_received, num_bytes_sent, ...]
	result := make([]interface{}, 0, 4*len(cc.cluster.Nodes))

	for _, node := range cc.cluster.Nodes {
		result = append(result, node.ID)
		result = append(result, globalClusterStats.CommandsProcessed)
		result = append(result, globalClusterStats.NetInputBytes)
		result = append(result, globalClusterStats.NetOutputBytes)
	}

	return result, nil
}

// handleTotalKeys 处理CLUSTER TOTALKEYS命令
func (cc *ClusterCommands) handleTotalKeys(args []string) (int64, error) {
	if len(args) < 1 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'CLUSTER TOTALKEYS' command")
	}

	slot, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("ERR invalid slot number")
	}

	if slot >= uint64(SlotCount) {
		return 0, fmt.Errorf("ERR slot out of range")
	}

	// 简化实现：返回0
	// 实际需要扫描整个数据库，统计属于该槽位的键数量
	// 由于BoltDB/BadgerDB的键不存储槽位信息，这里无法准确统计
	return 0, nil
}
