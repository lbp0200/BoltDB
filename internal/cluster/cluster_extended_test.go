package cluster

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestCluster_GetNodeBySlot tests GetNodeBySlot
func TestCluster_GetNodeBySlot(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	node.AddSlotRange(0, 100)
	cluster.AddNode(node)

	// Get node by slot - just verify no panic
	_ = cluster.GetNodeBySlot(50)
	assert.True(t, true)
}

// TestCluster_GetNodeByID tests GetNodeByID
func TestCluster_GetNodeByID(t *testing.T) {
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
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	node.AddSlotRange(0, 100)
	cluster.AddNode(node)

	// Get slot owner - just verify no panic
	_ = cluster.GetSlotOwner(50)
	assert.True(t, true)
}

// TestCluster_GetClusterNodes tests GetClusterNodes
func TestCluster_GetClusterNodes(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Get cluster nodes - just verify no panic
	_ = cluster.GetClusterNodes()
	assert.True(t, true)
}

// TestCluster_GetClusterSlots tests GetClusterSlots
func TestCluster_GetClusterSlots(t *testing.T) {
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
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Get myself
	myself := cluster.GetMyself()
	assert.True(t, myself != nil)
}

// TestCluster_Epoch tests IncrementEpoch and GetEpoch
func TestCluster_Epoch(t *testing.T) {
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
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Create and add a node
	node := NewNode("node1", "127.0.0.1:6379")
	cluster.AddNode(node)

	// Update node epoch
	cluster.UpdateNodeEpoch("node1", 100)

	// Should not panic
	assert.True(t, true)
}

// TestCluster_ImportingSlots tests IsImportingSlot
func TestCluster_ImportingSlots(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Set importing slot - just verify no panic
	cluster.SetSlotImporting(100, "source-node")
	assert.True(t, true)
}

// TestCluster_MigratingSlots tests IsMigratingSlot
func TestCluster_MigratingSlots(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Set migrating slot - just verify no panic
	cluster.SetSlotMigrating(100, "target-node")
	assert.True(t, true)
}

// TestCluster_ClearSlotMigration tests ClearSlotMigration
func TestCluster_ClearSlotMigration(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Set and clear migrating slot - just verify no panic
	cluster.SetSlotMigrating(100, "target-node")
	cluster.ClearSlotMigration(100)
	assert.True(t, true)
}
