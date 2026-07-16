package cluster

import (
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// TestCluster_GetNodeBySlot tests GetNodeBySlot
func TestCluster_GetNodeBySlot(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Assign slot range 0-100 to the node (updates cluster slot table)
	err := cluster.AssignSlotRange(0, 100, "node1")
	assert.NoError(t, err)

	// Get node by slot 50
	found := cluster.GetNodeBySlot(50)
	assert.True(t, found != nil)
	assert.Equal(t, "node1", found.ID)
	assert.True(t, found.HasSlot(50))
}

// TestCluster_GetNodeByID tests GetNodeByID
func TestCluster_GetNodeByID(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Get node by ID
	found := cluster.GetNodeByID("node1")
	assert.True(t, found != nil)
	assert.Equal(t, "127.0.0.1:6379", found.Addr)
}

// TestCluster_RemoveNode tests RemoveNode
func TestCluster_RemoveNode(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Remove node
	cluster.RemoveNode("node1")

	// Node should be removed
	found := cluster.GetNodeByID("node1")
	assert.True(t, found == nil)
}

// TestCluster_AssignSlot tests AssignSlot
func TestCluster_AssignSlot(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Assign slot
	err := cluster.AssignSlot(100, "node1")
	assert.NoError(t, err)

	// Verify slot is assigned
	found := cluster.GetNodeBySlot(100)
	assert.True(t, found != nil)
}

// TestCluster_AssignSlotRange tests AssignSlotRange
func TestCluster_AssignSlotRange(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Assign slot range
	err := cluster.AssignSlotRange(200, 300, "node1")
	assert.NoError(t, err)

	// Verify slots are assigned
	found := cluster.GetNodeBySlot(250)
	assert.True(t, found != nil)
}

// TestCluster_GetSlotOwner tests GetSlotOwner
func TestCluster_GetSlotOwner(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Assign slot range 0-100 to the node
	err := cluster.AssignSlotRange(0, 100, "node1")
	assert.NoError(t, err)

	// Get slot owner
	owner := cluster.GetSlotOwner(50)
	assert.True(t, owner != nil)
	assert.Equal(t, "node1", owner.ID)
}

// TestCluster_GetClusterNodes tests GetClusterNodes
func TestCluster_GetClusterNodes(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Get cluster nodes (returns formatted node lines like Redis CLUSTER NODES)
	nodes := cluster.GetClusterNodes()
	assert.Equal(t, 2, len(nodes)) // myself + node1
	// Verify node1 appears in one of the node lines
	found := false
	for _, line := range nodes {
		if strings.Contains(line, "node1") {
			found = true
			break
		}
	}
	assert.True(t, found)
}

// TestCluster_GetClusterSlots tests GetClusterSlots
func TestCluster_GetClusterSlots(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	node.AddSlotRange(0, 100)
	cluster.AddNode(node)

	// Get cluster slots
	slots := cluster.GetClusterSlots()
	assert.Equal(t, 1, len(slots))
}

// TestCluster_GetMyself tests GetMyself
func TestCluster_GetMyself(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Get myself
	myself := cluster.GetMyself()
	assert.True(t, myself != nil)
}

// TestCluster_Epoch tests IncrementEpoch and GetEpoch
func TestCluster_Epoch(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Initial epoch
	epoch := cluster.GetEpoch()
	assert.True(t, epoch >= 0)

	// Increment epoch
	newEpoch := cluster.IncrementEpoch()
	assert.Equal(t, epoch+1, newEpoch)
}

// TestCluster_UpdateNodeEpoch tests UpdateNodeEpoch
func TestCluster_UpdateNodeEpoch(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Update node epoch
	cluster.UpdateNodeEpoch("node1", 100)

	// Verify epoch was updated
	updated := cluster.GetNodeByID("node1")
	assert.True(t, updated != nil)
	assert.Equal(t, uint64(100), updated.Epoch)
}

// TestCluster_ImportingSlots tests IsImportingSlot
func TestCluster_ImportingSlots(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Add a source node to the cluster (required by SetSlotImporting to resolve node ID to address)
	sourceNode := NewNode("source-node", "127.0.0.1:6380")
	cluster.AddNode(sourceNode)

	// IMPORTING does not require local ownership (target is not owner until AssignSlot).
	cluster.SetSlotImporting(100, "source-node")
	assert.True(t, cluster.IsImportingSlot(100))
	importing := cluster.GetImportingSlots()
	found := false
	for _, info := range importing {
		if info.Slot == 100 && info.SourceNode == "127.0.0.1:6380" {
			found = true
			break
		}
	}
	assert.True(t, found)
}

// TestDelKeysInSlot deletes keys that hash to a given slot.
func TestDelKeysInSlot(t *testing.T) {
	t.Parallel()
	c, cleanup := setupTestCluster(t)
	defer cleanup()

	// Find keys that land in different slots; delete only one slot.
	keyA := "dki:a"
	keyB := "dki:b"
	// Force known slots via hash tags if needed — just insert and collect.
	assert.NoError(t, c.Store.Set(keyA, "1"))
	assert.NoError(t, c.Store.Set(keyB, "2"))
	slotA := Slot(keyA)
	n, err := c.DelKeysInSlot(slotA)
	assert.NoError(t, err)
	assert.True(t, n >= 1)
	exists, err := c.Store.Exists(keyA)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// TestCluster_MigratingSlots tests IsMigratingSlot
func TestCluster_MigratingSlots(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Add a target node to the cluster (required by SetSlotMigrating to resolve node ID to address)
	targetNode := NewNode("target-node", "127.0.0.1:6381")
	cluster.AddNode(targetNode)

	// Ensure slot 100 is owned by myself (IsMigratingSlot checks this first)
	cluster.AssignSlot(100, cluster.Myself.ID)

	// Set migrating slot
	cluster.SetSlotMigrating(100, "target-node")
	assert.True(t, cluster.IsMigratingSlot(100))
	migrating := cluster.GetMigratingSlots()
	found := false
	for _, info := range migrating {
		if info.Slot == 100 && info.TargetNode == "127.0.0.1:6381" {
			found = true
			break
		}
	}
	assert.True(t, found)
}

// TestCluster_ClearSlotMigration tests ClearSlotMigration
func TestCluster_ClearSlotMigration(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Set and clear migrating slot
	cluster.SetSlotMigrating(100, "target-node")
	cluster.ClearSlotMigration(100)
	assert.False(t, cluster.IsMigratingSlot(100))
	migrating := cluster.GetMigratingSlots()
	for _, info := range migrating {
		if info.Slot == 100 {
			t.Fatalf("slot 100 should be cleared but still in migrating list")
		}
	}
}
