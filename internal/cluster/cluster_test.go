package cluster

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

func setupTestCluster(t *testing.T) (*Cluster, func()) {
	dbPath := t.TempDir()
	s, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)

	cluster, err := NewCluster(s, "", "127.0.0.1:6379", context.Background())
	assert.NoError(t, err)

	return cluster, func() {
		assert.NoError(t, s.Close())
		assert.NoError(t, os.RemoveAll(dbPath))
	}
}

func TestSlotCalculation(t *testing.T) {
	t.Parallel()
	// 测试基本槽位计算
	slot := Slot("testkey")
	assert.True(t, slot < SlotCount)

	// 测试hash tag
	slot1 := Slot("user{1000}:name")
	slot2 := Slot("user{1000}:age")
	assert.Equal(t, slot1, slot2) // 相同hash tag应该映射到相同槽位

	// 测试不同hash tag
	slot3 := Slot("user{1000}:name")
	slot4 := Slot("user{2000}:name")
	assert.NotEqual(t, slot3, slot4)
}

func TestClusterCreation(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	myself := cluster.GetMyself()
	assert.NotNil(t, myself)
	assert.True(t, myself.IsMyself())
	assert.True(t, myself.IsMaster())
	assert.Equal(t, 40, len(myself.ID)) // 节点ID应该是40字符
}

func TestClusterSlotMigration(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Test IsImportingSlot - initially should be false
	importing := cluster.IsImportingSlot(100)
	assert.False(t, importing)

	// Test IsMigratingSlot - initially should be false
	migrating := cluster.IsMigratingSlot(100)
	assert.False(t, migrating)

	// Test GetImportingSlots - initially should be empty
	importingSlots := cluster.GetImportingSlots()
	assert.Equal(t, 0, len(importingSlots))

	// Test GetMigratingSlots - initially should be empty
	migratingSlots := cluster.GetMigratingSlots()
	assert.Equal(t, 0, len(migratingSlots))
}

func TestClusterGetAskRedirect(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Test GetAskRedirect for non-migrating key
	redirect := cluster.GetAskRedirect("testkey")
	assert.Nil(t, redirect)
}

func TestSlotAssignment(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// 创建新节点
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	node.Flags = append(node.Flags, "master")
	cluster.AddNode(node)

	// 分配槽位给新节点
	err := cluster.AssignSlot(100, nodeID)
	assert.NoError(t, err)

	owner := cluster.GetNodeBySlot(100)
	assert.NotNil(t, owner)
	assert.Equal(t, nodeID, owner.ID)
}

func TestSlotRangeAssignment(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	node.Flags = append(node.Flags, "master")
	cluster.AddNode(node)

	// 分配槽位范围
	err := cluster.AssignSlotRange(0, 100, nodeID)
	assert.NoError(t, err)

	// 验证槽位分配
	for i := uint32(0); i <= 100; i++ {
		owner := cluster.GetNodeBySlot(i)
		assert.NotNil(t, owner)
		assert.Equal(t, nodeID, owner.ID)
	}
}

func TestIsSlotLocal(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// 初始时所有槽位都属于当前节点
	assert.True(t, cluster.IsSlotLocal(0))
	assert.True(t, cluster.IsSlotLocal(1000))
	assert.True(t, cluster.IsSlotLocal(SlotCount-1))

	// 分配槽位给其他节点后，应该返回false
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)
	_ = cluster.AssignSlot(100, nodeID)

	assert.False(t, cluster.IsSlotLocal(100))
}

func TestClusterCommands(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// 测试CLUSTER INFO
	info, err := cmd.HandleCommand([]string{"INFO"})
	assert.NoError(t, err)
	assert.NotNil(t, info)
	infoStr, ok := info.(string)
	assert.True(t, ok)
	assert.True(t, strings.Contains(infoStr, "cluster_state:ok"))

	// 测试CLUSTER MYID
	myid, err := cmd.HandleCommand([]string{"MYID"})
	assert.NoError(t, err)
	myidStr, ok := myid.(string)
	assert.True(t, ok)
	assert.Equal(t, 40, len(myidStr))

	// 测试CLUSTER KEYSLOT
	keyslot, err := cmd.HandleCommand([]string{"KEYSLOT", "testkey"})
	assert.NoError(t, err)
	slot, ok := keyslot.(int64)
	assert.True(t, ok)
	assert.True(t, slot >= 0 && slot < SlotCount)

	// 测试CLUSTER NODES
	nodes, err := cmd.HandleCommand([]string{"NODES"})
	assert.NoError(t, err)
	nodesStr, ok := nodes.(string)
	assert.True(t, ok)
	assert.True(t, strings.Contains(nodesStr, cluster.GetMyself().ID))

	// 测试CLUSTER SLOTS
	slots, err := cmd.HandleCommand([]string{"SLOTS"})
	assert.NoError(t, err)
	slotsArr, ok := slots.([][]interface{})
	assert.True(t, ok)
	assert.Equal(t, 1, len(slotsArr)) // 1 merged slot range (0-16383)
}

func TestClusterMeet(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// 测试CLUSTER MEET
	result, err := cmd.HandleCommand([]string{"MEET", "127.0.0.1", "6380"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 验证节点已添加
	assert.Equal(t, 2, len(cluster.Nodes)) // myself + meet target
}

func TestClusterAddSlots(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// 创建新节点并添加到集群
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)

	// 测试CLUSTER ADDSLOTS
	result, err := cmd.HandleCommand([]string{"ADDSLOTS", "100", "101", "102"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 注意：ADDSLOTS会将槽位分配给当前节点，不是新节点
	// 这里只是测试命令执行成功
}

func TestRedirectError(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// 创建新节点并分配槽位
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)
	_ = cluster.AssignSlot(100, nodeID)

	// 测试重定向检查
	redirect := cluster.CheckSlotRedirect("testkey")
	slot := Slot("testkey")
	if slot == 100 {
		// 如果槽位恰好是100，应该返回重定向
		assert.NotNil(t, redirect)
	}
	_ = node // suppress unused variable warning
}

func TestNewMovedError(t *testing.T) {
	t.Parallel()
	err := NewMovedError(100, "127.0.0.1:6380")
	assert.Equal(t, "MOVED", err.Type)
	assert.Equal(t, uint32(100), err.Slot)
	assert.Equal(t, "127.0.0.1:6380", err.Address)
	assert.Equal(t, "MOVED 100 127.0.0.1:6380", err.Error())
}

func TestNewAskError(t *testing.T) {
	t.Parallel()
	err := NewAskError(200, "127.0.0.1:6381")
	assert.Equal(t, "ASK", err.Type)
	assert.Equal(t, uint32(200), err.Slot)
	assert.Equal(t, "127.0.0.1:6381", err.Address)
	assert.Equal(t, "ASK 200 127.0.0.1:6381", err.Error())
}

func TestClusterGetRedirectAddress(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Add node and assign slot first
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)
	_ = cluster.AssignSlot(100, nodeID)

	// Test with assigned slot
	addr, err := cluster.GetRedirectAddress(100)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:6380", addr)
}

func TestClusterCheckSlotRedirect(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Test with key on unassigned slot - should return nil
	redirect := cluster.CheckSlotRedirect("testkey")
	// May or may not be nil depending on slot assignment
	_ = redirect
}

func TestHandleGetKeysInSlot(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER GETKEYSINSLOT
	result, err := cmd.HandleCommand([]string{"GETKEYSINSLOT", "100", "10"})
	assert.NoError(t, err)
	keys, ok := result.([]string)
	assert.True(t, ok)
	assert.Equal(t, 0, len(keys))

	// Test with invalid arguments
	_, err = cmd.HandleCommand([]string{"GETKEYSINSLOT"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"GETKEYSINSLOT", "abc", "10"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"GETKEYSINSLOT", "100", "-1"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"GETKEYSINSLOT", "20000", "10"})
	assert.Error(t, err)
}

func TestHandleSetSlot(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER SETSLOT <slot> IMPORTING <source-node-id>
	result, err := cmd.HandleCommand([]string{"SETSLOT", "100", "IMPORTING", "source-node-id"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Test CLUSTER SETSLOT <slot> MIGRATING <target-node-id>
	result, err = cmd.HandleCommand([]string{"SETSLOT", "101", "MIGRATING", "target-node-id"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Test CLUSTER SETSLOT <slot> MIGRATING without nodeid
	_, err = cmd.HandleCommand([]string{"SETSLOT", "102", "MIGRATING"})
	assert.Error(t, err)

	// Test CLUSTER SETSLOT <slot> STABLE
	result, err = cmd.HandleCommand([]string{"SETSLOT", "103", "STABLE"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Test CLUSTER SETSLOT <slot> NODE <nodeid>
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)
	result, err = cmd.HandleCommand([]string{"SETSLOT", "104", "NODE", nodeID})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify slot was assigned
	owner := cluster.GetNodeBySlot(104)
	assert.NotNil(t, owner)
	assert.Equal(t, nodeID, owner.ID)

	// Test CLUSTER SETSLOT with invalid arguments
	_, err = cmd.HandleCommand([]string{"SETSLOT"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"SETSLOT", "abc", "NODE"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"SETSLOT", "100", "INVALID"})
	assert.Error(t, err)
}

func TestHandleForget(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Add a new node
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)

	// Test CLUSTER FORGET <nodeid>
	result, err := cmd.HandleCommand([]string{"FORGET", nodeID})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify node was removed
	assert.Nil(t, cluster.GetNodeByID(nodeID))

	// Test CLUSTER FORGET with myself
	_, err = cmd.HandleCommand([]string{"FORGET", cluster.GetMyself().ID})
	assert.Error(t, err)

	// Test CLUSTER FORGET with missing nodeid
	_, err = cmd.HandleCommand([]string{"FORGET"})
	assert.Error(t, err)
}

func TestHandleReplicate(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Add a master node
	masterID, _ := generateNodeID()
	master := NewNode(masterID, "127.0.0.1:6380")
	master.Flags = []string{"master"}
	cluster.AddNode(master)

	// Assign a slot to myself first
	_ = cluster.AssignSlot(100, cluster.GetMyself().ID)

	// Test CLUSTER REPLICATE <masterid>
	result, err := cmd.HandleCommand([]string{"REPLICATE", masterID})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify this node is now a slave
	myself := cluster.GetMyself()
	assert.Equal(t, 2, len(myself.Flags)) // "slave" + "myself"
	assert.Equal(t, "slave", myself.Flags[0])

	// Test CLUSTER REPLICATE with unknown node
	_, err = cmd.HandleCommand([]string{"REPLICATE", "unknown-node-id"})
	assert.Error(t, err)

	// Test CLUSTER REPLICATE with missing nodeid
	_, err = cmd.HandleCommand([]string{"REPLICATE"})
	assert.Error(t, err)
}

func TestHandleSaveConfig(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER SAVECONFIG
	result, err := cmd.HandleCommand([]string{"SAVECONFIG"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

func TestHandleDelSlots(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER DELSLOTS
	result, err := cmd.HandleCommand([]string{"DELSLOTS", "100", "101", "102"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Test with invalid slot
	_, err = cmd.HandleCommand([]string{"DELSLOTS", "abc"})
	assert.Error(t, err)

	// Test with out of range slot
	_, err = cmd.HandleCommand([]string{"DELSLOTS", "20000"})
	assert.Error(t, err)

	// Test with no arguments
	_, err = cmd.HandleCommand([]string{"DELSLOTS"})
	assert.Error(t, err)
}

func TestHandleFlushSlots(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Assign some slots to myself first
	_ = cluster.AssignSlot(100, cluster.GetMyself().ID)
	_ = cluster.AssignSlot(101, cluster.GetMyself().ID)
	_ = cluster.AssignSlot(102, cluster.GetMyself().ID)

	// Verify slots are assigned
	assert.NotNil(t, cluster.GetNodeBySlot(100))

	// Test CLUSTER FLUSHSLOTS
	result, err := cmd.HandleCommand([]string{"FLUSHSLOTS"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify slots are cleared for myself
	myself := cluster.GetMyself()
	assert.Equal(t, 0, len(myself.Slots))
}

func TestHandleCountKeysInSlot(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER COUNTKEYSINSLOT
	result, err := cmd.HandleCommand([]string{"COUNTKEYSINSLOT", "100"})
	assert.NoError(t, err)
	count, ok := result.(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), count)

	// Test with invalid arguments
	_, err = cmd.HandleCommand([]string{"COUNTKEYSINSLOT"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"COUNTKEYSINSLOT", "abc"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"COUNTKEYSINSLOT", "20000"})
	assert.Error(t, err)
}

func TestHandleEpoch(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER EPOCH
	result, err := cmd.HandleCommand([]string{"EPOCH"})
	assert.NoError(t, err)
	epoch, ok := result.(int64)
	assert.True(t, ok)
	assert.True(t, epoch >= 0)
}

func TestHandleSlaves(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Add a master node
	masterID, _ := generateNodeID()
	master := NewNode(masterID, "127.0.0.1:6380")
	master.Flags = []string{"master"}
	cluster.AddNode(master)

	// Add a slave node
	slaveID, _ := generateNodeID()
	slave := NewNode(slaveID, "127.0.0.1:6381")
	slave.Flags = []string{"slave"}
	slave.MasterID = masterID
	cluster.AddNode(slave)

	// Test CLUSTER SLAVES <masterid>
	result, err := cmd.HandleCommand([]string{"SLAVES", masterID})
	assert.NoError(t, err)
	slaves, ok := result.([]string)
	assert.True(t, ok)
	assert.Equal(t, 1, len(slaves))

	// Test with unknown node
	_, err = cmd.HandleCommand([]string{"SLAVES", "unknown-node-id"})
	assert.Error(t, err)

	// Test with missing nodeid
	_, err = cmd.HandleCommand([]string{"SLAVES"})
	assert.Error(t, err)
}

func TestHandleReset(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Add another node and assign slots
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)
	_ = cluster.AssignSlot(100, cluster.GetMyself().ID)
	_ = cluster.AssignSlot(101, cluster.GetMyself().ID)

	// Test CLUSTER RESET (soft reset)
	result, err := cmd.HandleCommand([]string{"RESET"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// Verify myself is still there
	assert.NotNil(t, cluster.GetMyself())

	// Test CLUSTER RESET HARD
	_ = cluster.AssignSlot(200, cluster.GetMyself().ID)
	result, err = cmd.HandleCommand([]string{"RESET", "HARD"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// After hard reset, slots should be cleared
	myself := cluster.GetMyself()
	assert.Equal(t, 0, len(myself.Slots))
}

func TestHandleCalls(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER CALLS
	result, err := cmd.HandleCommand([]string{"CALLS"})
	assert.NoError(t, err)
	calls, ok := result.([]interface{})
	assert.True(t, ok)
	// Should have at least myself in the result
	assert.Equal(t, 4, len(calls)) // 1 node × 4 fields (ID, CommandsProcessed, NetInputBytes, NetOutputBytes)
}

func TestHandleTotalKeys(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// Test CLUSTER TOTALKEYS
	result, err := cmd.HandleCommand([]string{"TOTALKEYS", "100"})
	assert.NoError(t, err)
	keys, ok := result.(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(0), keys)

	// Test with invalid arguments
	_, err = cmd.HandleCommand([]string{"TOTALKEYS"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"TOTALKEYS", "abc"})
	assert.Error(t, err)

	_, err = cmd.HandleCommand([]string{"TOTALKEYS", "20000"})
	assert.Error(t, err)
}

func TestNodeUpdatePing(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	node := cluster.GetMyself()
	assert.NotNil(t, node)

	// Test UpdatePing sets PingSent timestamp
	oldPing := node.PingSent
	node.UpdatePing()

	node.mu.RLock()
	assert.True(t, node.PingSent > 0)
	assert.True(t, node.PingSent >= oldPing)
	node.mu.RUnlock()

	// Subsequent calls update the timestamp forward
	firstPing := node.PingSent
	node.UpdatePing()
	node.mu.RLock()
	assert.True(t, node.PingSent >= firstPing)
	node.mu.RUnlock()
}
