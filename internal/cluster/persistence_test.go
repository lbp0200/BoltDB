package cluster

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestClearSlots(t *testing.T) {
	t.Parallel()
	slots := make([]*Node, 10)
	for i := range slots {
		slots[i] = &Node{ID: "test"}
	}
	clearSlots(slots)
	for i := range slots {
		assert.Nil(t, slots[i])
	}
}

func TestClearSlots_Empty(t *testing.T) {
	t.Parallel()
	slots := make([]*Node, 0)
	clearSlots(slots)
	assert.Equal(t, 0, len(slots))
}

func TestClearSlots_NilSlice(t *testing.T) {
	t.Parallel()
	var slots []*Node
	clearSlots(slots)
	assert.Nil(t, slots)
}

func TestLoadState(t *testing.T) {
	cluster := &Cluster{
		Myself: NewNode("self-id", "127.0.0.1:6379"),
		Nodes:  make(map[string]*Node),
	}

	state := persistedClusterState{
		Epoch: 42,
		Nodes: map[string]*persistedNode{
			"self-id": {
				ID:    "self-id",
				Addr:  "127.0.0.1:6379",
				Flags: []string{"master", "myself"},
				Epoch: 1,
			},
			"other-id": {
				ID:    "other-id",
				Addr:  "127.0.0.1:6380",
				Flags: []string{"master"},
				Epoch: 2,
			},
		},
		Slots: []persistedSlotOwner{
			{Start: 0, End: 8191, NodeID: "self-id"},
			{Start: 8192, End: 16383, NodeID: "other-id"},
		},
	}

	cluster.loadState(state)

	assert.Equal(t, int64(42), cluster.Epoch)
	assert.Equal(t, 2, len(cluster.Nodes))

	selfNode := cluster.Nodes["self-id"]
	assert.NotNil(t, selfNode)

	otherNode := cluster.Nodes["other-id"]
	assert.NotNil(t, otherNode)
	assert.Equal(t, "127.0.0.1:6380", otherNode.Addr)
	assert.Equal(t, int64(2), otherNode.Epoch)
	assert.Equal(t, "other-id", otherNode.ID)

	assert.Equal(t, cluster.Nodes["self-id"], cluster.Slots[0])
	assert.Equal(t, cluster.Nodes["self-id"], cluster.Slots[8191])
	assert.Equal(t, cluster.Nodes["other-id"], cluster.Slots[8192])
	assert.Equal(t, cluster.Nodes["other-id"], cluster.Slots[16383])
}

func TestLoadState_Empty(t *testing.T) {
	cluster := &Cluster{
		Myself: NewNode("self-id", "127.0.0.1:6379"),
		Nodes:  make(map[string]*Node),
	}

	state := persistedClusterState{
		Epoch: 0,
		Nodes: nil,
		Slots: nil,
	}

	cluster.loadState(state)

	assert.Equal(t, int64(0), cluster.Epoch)
	assert.Equal(t, 0, len(cluster.Nodes))
	for i := range cluster.Slots {
		assert.Nil(t, cluster.Slots[i])
	}
}

func TestLoadState_FallbackToMyself(t *testing.T) {
	cluster := &Cluster{
		Myself: NewNode("self-id", "127.0.0.1:6379"),
		Nodes:  make(map[string]*Node),
	}

	state := persistedClusterState{
		Epoch: 1,
		Slots: []persistedSlotOwner{
			{Start: 0, End: 100, NodeID: "unknown-node"},
		},
	}

	cluster.loadState(state)

	for i := uint32(0); i <= 100; i++ {
		assert.Equal(t, cluster.Myself, cluster.Slots[i])
	}
}

func TestLoadState_UpdatesExistingNode(t *testing.T) {
	existingNode := NewNode("existing-id", "127.0.0.1:6379")
	existingNode.Flags = []string{"slave"}
	existingNode.Epoch = 1

	cluster := &Cluster{
		Myself: NewNode("self-id", "127.0.0.1:6379"),
		Nodes: map[string]*Node{
			"existing-id": existingNode,
		},
	}

	state := persistedClusterState{
		Epoch: 5,
		Nodes: map[string]*persistedNode{
			"existing-id": {
				ID:    "existing-id",
				Addr:  "127.0.0.1:6379",
				Flags: []string{"master"},
				Epoch: 10,
			},
		},
	}

	cluster.loadState(state)

	assert.Equal(t, int64(5), cluster.Epoch)
	updated := cluster.Nodes["existing-id"]
	assert.Equal(t, "existing-id", updated.ID)
	assert.Equal(t, "master", updated.Flags[0])
	assert.Equal(t, int64(10), updated.Epoch)
}
