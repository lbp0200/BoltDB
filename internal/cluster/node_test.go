package cluster

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestNode_New tests Node creation
func TestNode_New(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	assert.Equal(t, "node1", node.ID)
	assert.Equal(t, "127.0.0.1:6379", node.Addr)
}

// TestNode_SlotRange tests slot range management
func TestNode_SlotRange(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	// Initially no slots
	assert.False(t, node.HasSlot(0))
	assert.False(t, node.HasSlot(100))

	// Add slot range
	node.AddSlotRange(0, 100)
	assert.True(t, node.HasSlot(0))
	assert.True(t, node.HasSlot(50))
	assert.True(t, node.HasSlot(100))
	assert.False(t, node.HasSlot(101))

	// Get slot ranges
	ranges := node.GetSlotRanges()
	assert.Equal(t, 1, len(ranges))
	assert.Equal(t, uint32(0), ranges[0].Start)
	assert.Equal(t, uint32(100), ranges[0].End)
}

// TestNode_MultipleSlotRanges tests multiple slot ranges
func TestNode_MultipleSlotRanges(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	// Add multiple slot ranges
	node.AddSlotRange(0, 100)
	node.AddSlotRange(500, 1000)

	// Check both ranges
	assert.True(t, node.HasSlot(50))
	assert.True(t, node.HasSlot(750))

	// Get all ranges
	ranges := node.GetSlotRanges()
	assert.Equal(t, 2, len(ranges))
}

func TestNode_Role(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	assert.False(t, node.IsMaster())
	assert.False(t, node.IsSlave())
}

// TestNode_Myself tests IsMyself
func TestNode_Myself(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	// Initially not myself
	assert.False(t, node.IsMyself())

	// Set as myself
	node.SetMyself()
	assert.True(t, node.IsMyself())
}

// TestNode_String tests node string representation
func TestNode_String(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")
	node.AddSlotRange(0, 100)

	str := node.String()
	assert.True(t, len(str) > 0)
}

// TestNode_GetHostPort tests GetHostPort
func TestNode_GetHostPort(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	host, port, err := node.GetHostPort()
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1", host)
	assert.Equal(t, "6379", port)
}

// TestNode_ImportingSlots tests importing slot management
func TestNode_ImportingSlots(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	// Initially no importing slots
	assert.False(t, node.IsImportingSlot(100))

	// Set importing slot
	node.SetImportingSlot(100, "127.0.0.1:6380")
	assert.True(t, node.IsImportingSlot(100))
	assert.False(t, node.IsImportingSlot(200))

	// Get importing slot source
	source := node.GetImportingSlotSource(100)
	assert.Equal(t, "127.0.0.1:6380", source)

	// Get all importing slots
	importing := node.GetImportingSlots()
	assert.Equal(t, 1, len(importing))
}

// TestNode_MigratingSlots tests migrating slot management
func TestNode_MigratingSlots(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	// Initially no migrating slots
	assert.False(t, node.IsMigratingSlot(100))

	// Set migrating slot
	node.SetMigratingSlot(100, "127.0.0.1:6380")
	assert.True(t, node.IsMigratingSlot(100))
	assert.False(t, node.IsMigratingSlot(200))

	// Get migrating slot target
	target := node.GetMigratingSlotTarget(100)
	assert.Equal(t, "127.0.0.1:6380", target)

	// Get all migrating slots
	migrating := node.GetMigratingSlots()
	assert.Equal(t, 1, len(migrating))
}

// TestNode_ClearSlotMigration tests clearing slot migration
func TestNode_ClearSlotMigration(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	// Set migrating slot
	node.SetMigratingSlot(100, "127.0.0.1:6380")
	assert.True(t, node.IsMigratingSlot(100))

	// Clear migration
	node.ClearSlotMigration(100)
	assert.False(t, node.IsMigratingSlot(100))
}

func TestNode_UpdatePong(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")
	assert.Equal(t, int64(0), node.PongRecv)

	node.UpdatePong()

	assert.True(t, node.PongRecv > 0)
}

// TestNode_IsFailed tests IsFailed
func TestNode_IsFailed(t *testing.T) {
	t.Parallel()
	node := NewNode("node1", "127.0.0.1:6379")

	// Initially not failed
	assert.False(t, node.IsFailed())
}

// TestGenerateNodeID tests generateNodeID
func TestGenerateNodeID(t *testing.T) {
	t.Parallel()
	id1, err := generateNodeID()
	assert.NoError(t, err)
	assert.True(t, len(id1) > 0)

	// IDs should be unique
	id2, err := generateNodeID()
	assert.NoError(t, err)
	assert.True(t, id1 != id2)
}
