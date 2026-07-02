package cluster

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
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
	case "BUMPEPOCH":
		return cc.handleBumpEpoch(subArgs)
	case "COUNT-FAILURE-REPORTS":
		return cc.handleCountFailureReports(subArgs)
	case "LINKS":
		return cc.handleLinks(subArgs)
	case "MIGRATESLOT":
		return cc.handleMigrateSlot(subArgs)
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

	cc.cluster.mu.RLock()
	totalNodes := len(cc.cluster.Nodes)
	totalSlots := 0
	for i := uint32(0); i < SlotCount; i++ {
		if cc.cluster.Slots[i] == myself {
			totalSlots++
		}
	}
	cc.cluster.mu.RUnlock()

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

	var keys []string
	err = cc.cluster.Store.IterateRawKeys(func(rawKey string) bool {
		if Slot(rawKey) == uint32(slot) {
			keys = append(keys, rawKey)
			if len(keys) >= int(count) {
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
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
		if len(subArgs) < 1 {
			return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER SETSLOT IMPORTING'")
		}
		cc.cluster.SetSlotImporting(uint32(slot), subArgs[0])
		logger.Logger.Info().Uint32("slot", uint32(slot)).Str("source", subArgs[0]).Msg("cluster SETSLOT IMPORTING")
		return "OK", nil
	case "MIGRATING":
		if len(subArgs) < 1 {
			return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER SETSLOT MIGRATING'")
		}
		cc.cluster.SetSlotMigrating(uint32(slot), subArgs[0])
		logger.Logger.Info().Uint32("slot", uint32(slot)).Str("target", subArgs[0]).Msg("cluster SETSLOT MIGRATING")
		return "OK", nil
	case "STABLE":
		// 清除槽位迁移状态（MIGRATING 和 IMPORTING）
		cc.cluster.ClearSlotMigration(uint32(slot))
		logger.Logger.Info().Uint32("slot", uint32(slot)).Msg("cluster SETSLOT STABLE")
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

// handleMigrateSlot 处理CLUSTER MIGRATESLOT命令
// 将指定槽位的所有 key 从当前节点迁移到目标节点
// 语法: CLUSTER MIGRATESLOT <slot> <targetNodeID> [COPY]
func (cc *ClusterCommands) handleMigrateSlot(args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER MIGRATESLOT' command")
	}

	slot, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return "", fmt.Errorf("ERR invalid slot number")
	}

	targetNodeID := args[1]
	copyKeys := len(args) >= 3 && strings.ToUpper(args[2]) == "COPY"

	if err := cc.cluster.MigrateSlotCrashSafe(uint32(slot), targetNodeID, copyKeys); err != nil {
		return "", fmt.Errorf("ERR %v", err)
	}

	return "OK", nil
}

// handleMeet 处理CLUSTER MEET命令
// 2 个参数（ip, port）：用户发起的 MEET，连接到目标节点并交换信息
// 3 个参数（ip, port, peerNodeID）：其他节点发起的内部 MEET，添加该节点到本地表
func (cc *ClusterCommands) handleMeet(args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER MEET' command")
	}

	ip := args[0]
	portStr := args[1]
	if _, err := strconv.Atoi(portStr); err != nil {
		return "", fmt.Errorf("ERR invalid port")
	}

	addr := net.JoinHostPort(ip, portStr)

	// Internal MEET from another node: 3 args (ip, port, peerNodeID)
	if len(args) >= 3 {
		peerNodeID := args[2]
		node := NewNode(peerNodeID, addr)
		node.EnsureMasterFlag()
		cc.cluster.AddNode(node)
		logger.Logger.Info().
			Str("peer", peerNodeID).
			Str("addr", addr).
			Msg("cluster MEET: added peer from internal handshake")

		// Establish reverse bus connection to the peer
		if cc.cluster.Bus != nil {
			busAddr := busAddrForPeer(addr)
			if err := cc.cluster.Bus.Connect(busAddr, peerNodeID); err != nil {
				logger.Logger.Warn().Err(err).Str("peer", peerNodeID).Str("bus", busAddr).Msg("cluster MEET: reverse bus connect failed")
			}
		}

		// Return our own node ID so the caller can update its node table
		return cc.cluster.Myself.ID, nil
	}

	// User-facing MEET: connect to target and perform handshake
	// First, add a placeholder node for the target
	nodeID, err := generateNodeID()
	if err != nil {
		return "", err
	}
	placeholder := NewNode(nodeID, addr)
	placeholder.EnsureMasterFlag()
	cc.cluster.AddNode(placeholder)

	// Connect to the target and send handshake
	myIP, myPort, err := cc.cluster.Myself.GetHostPort()
	if err != nil {
		// Fallback: keep placeholder, no handshake
		logger.Logger.Warn().Err(err).Msg("cluster MEET: failed to parse own address, skipping handshake")
		return "OK", nil
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		logger.Logger.Warn().Err(err).Str("target", addr).Msg("cluster MEET: failed to connect to target")
		return "OK", nil
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send: CLUSTER MEET <myIP> <myPort> <myNodeID>
	myNodeID := cc.cluster.Myself.ID
	cmd := fmt.Sprintf("*5\r\n$7\r\nCLUSTER\r\n$4\r\nMEET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(myIP), myIP, len(myPort), myPort, len(myNodeID), myNodeID)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		logger.Logger.Warn().Err(err).Str("target", addr).Msg("cluster MEET: handshake write failed")
		return "OK", nil
	}

	// Read response: expect bulk string with target's node ID
	reader := bufio.NewReader(conn)
	resp, err := proto.ReadRESP(reader)
	if err != nil {
		logger.Logger.Warn().Err(err).Str("target", addr).Msg("cluster MEET: handshake read failed")
		return "OK", nil
	}

	if len(resp.Args) >= 1 {
		realNodeID := string(resp.Args[0])
		if realNodeID != "" && realNodeID != nodeID {
			// Update node table with the real node ID from the target
			cc.cluster.mu.Lock()
			delete(cc.cluster.Nodes, nodeID)
			if existing, ok := cc.cluster.Nodes[realNodeID]; ok {
				existing.UpdatePong()
			} else {
				node := NewNode(realNodeID, addr)
				node.EnsureMasterFlag()
				node.UpdatePong()
				cc.cluster.Nodes[realNodeID] = node
			}
			cc.cluster.mu.Unlock()

			// Clean up any stale bus connection under the old placeholder ID
			if cc.cluster.Bus != nil {
				cc.cluster.Bus.Disconnect(nodeID)
			}

			if err := cc.cluster.SaveConfig(); err != nil {
				logger.Logger.Warn().Err(err).Msg("cluster MEET: failed to persist config")
			}

			// Establish persistent bus connection
			if cc.cluster.Bus != nil {
				busAddr := busAddrForPeer(addr)
				if err := cc.cluster.Bus.Connect(busAddr, realNodeID); err != nil {
					logger.Logger.Warn().Err(err).Str("peer", realNodeID).Str("bus", busAddr).Msg("cluster MEET: bus connect failed")
				}
			}

			logger.Logger.Info().
				Str("peer", realNodeID).
				Str("addr", addr).
				Msg("cluster MEET: handshake complete")
		}
	}

	return "OK", nil
}

// handleForget 处理CLUSTER FORGET命令
func (cc *ClusterCommands) handleForget(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER FORGET' command")
	}

	nodeID := args[0]

	cc.cluster.mu.Lock()
	defer cc.cluster.mu.Unlock()

	if nodeID == cc.cluster.Myself.ID {
		return "", fmt.Errorf("ERR I can't forget myself")
	}

	delete(cc.cluster.Nodes, nodeID)
	for i := uint32(0); i < SlotCount; i++ {
		if cc.cluster.Slots[i] != nil && cc.cluster.Slots[i].ID == nodeID {
			cc.cluster.Slots[i] = cc.cluster.Myself
			cc.cluster.Myself.AddSlotRange(i, i)
		}
	}

	return "OK", nil
}

// handleReplicate 处理CLUSTER REPLICATE命令
func (cc *ClusterCommands) handleReplicate(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERR wrong number of arguments for 'CLUSTER REPLICATE' command")
	}

	masterID := args[0]

	cc.cluster.mu.Lock()

	master := cc.cluster.Nodes[masterID]
	if master == nil {
		cc.cluster.mu.Unlock()
		return "", fmt.Errorf("ERR unknown node %s", masterID)
	}

	cc.cluster.Myself.SetRoleAsSlave(masterID)

	for i := uint32(0); i < SlotCount; i++ {
		if cc.cluster.Slots[i] == cc.cluster.Myself {
			cc.cluster.Slots[i] = master
			master.AddSlotRange(i, i)
		}
	}
	cc.cluster.Myself.ClearSlots()
	cc.cluster.mu.Unlock()

	if err := cc.cluster.SaveConfig(); err != nil {
		logger.Logger.Warn().Err(err).Msg("CLUSTER SETSLOT: failed to persist config")
	}
	return "OK", nil
}

// handleSaveConfig 处理CLUSTER SAVECONFIG命令
func (cc *ClusterCommands) handleSaveConfig(args []string) (string, error) {
	if err := cc.cluster.SaveConfig(); err != nil {
		return "", fmt.Errorf("ERR saving cluster config: %v", err)
	}
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

		if err := cc.cluster.AssignSlot(uint32(slot), cc.cluster.Myself.ID); err != nil {
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

		if err := cc.cluster.RemoveSlot(uint32(slot)); err != nil {
			return "", err
		}
	}

	return "OK", nil
}

// handleFlushSlots 处理CLUSTER FLUSHSLOTS命令
func (cc *ClusterCommands) handleFlushSlots(args []string) (string, error) {
	cc.cluster.mu.Lock()
	defer cc.cluster.mu.Unlock()

	for i := uint32(0); i < SlotCount; i++ {
		if cc.cluster.Slots[i] == cc.cluster.Myself {
			cc.cluster.Slots[i] = nil
		}
	}
	cc.cluster.Myself.ClearSlots()

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

	var count int64
	err = cc.cluster.Store.IterateRawKeys(func(rawKey string) bool {
		if Slot(rawKey) == uint32(slot) {
			count++
		}
		return true
	})
	if err != nil {
		return 0, err
	}

	return count, nil
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

	cc.cluster.mu.RLock()
	defer cc.cluster.mu.RUnlock()

	master := cc.cluster.Nodes[nodeID]
	if master == nil {
		return nil, fmt.Errorf("ERR No such node %s", nodeID)
	}

	var slaves []string
	for _, node := range cc.cluster.Nodes {
		if node.GetMasterID() == nodeID {
			slaves = append(slaves, cc.cluster.formatNodeLine(node))
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
		cc.cluster.Myself.ClearSlots()
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
		cc.cluster.Myself.SetSlots(ranges)
	}

	cc.cluster.Myself.SetRoleAsMaster()

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
	cc.cluster.mu.RLock()
	defer cc.cluster.mu.RUnlock()

	globalClusterStats.mu.RLock()
	defer globalClusterStats.mu.RUnlock()

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

// handleBumpEpoch 处理CLUSTER BUMPEPOCH命令
func (cc *ClusterCommands) handleBumpEpoch(args []string) (string, error) {
	// Increment the cluster epoch
	cc.cluster.IncrementEpoch()
	return "BUMPED", nil
}

// handleCountFailureReports 处理CLUSTER COUNT-FAILURE-REPORTS命令
func (cc *ClusterCommands) handleCountFailureReports(args []string) (int64, error) {
	if len(args) < 1 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'CLUSTER COUNT-FAILURE-REPORTS' command")
	}
	// Simplified: return 0 (no failure tracking in BoltDB)
	return 0, nil
}

// handleLinks 处理CLUSTER LINKS命令
func (cc *ClusterCommands) handleLinks(args []string) ([][]string, error) {
	// Return inter-node connection info
	// Simplified: return empty array (no persistent cluster bus connections tracked)
	return [][]string{}, nil
}
