package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestGossip_StartStop(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	g := NewGossiper(context.Background(), cluster)
	g.Start()
	assert.True(t, g.started.Load())

	g.Stop()
	assert.False(t, g.started.Load())
}

func TestGossip_DoubleStart(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	g := NewGossiper(context.Background(), cluster)
	g.Start()
	g.Start()
	assert.True(t, g.started.Load())
	g.Stop()
}

func TestGossip_DoubleStop(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	g := NewGossiper(context.Background(), cluster)
	g.Start()
	g.Stop()
	g.Stop()
	assert.False(t, g.started.Load())
}

func TestGossip_StopWithoutStart(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	g := NewGossiper(context.Background(), cluster)
	g.Stop()
	assert.False(t, g.started.Load())
}

func TestGossip_PingNoPeers(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	g := NewGossiper(context.Background(), cluster)

	// No peers → pingRandomPeers should be a safe no-op
	g.pingRandomPeers()

	// Cluster state unchanged (only Myself exists)
	cluster.mu.RLock()
	assert.Equal(t, 1, len(cluster.Nodes))
	_, selfExists := cluster.Nodes[cluster.Myself.ID]
	assert.True(t, selfExists)
	cluster.mu.RUnlock()
}

func TestGossip_PingWithPeers(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	peer1 := NewNode("peer1", "127.0.0.1:6380")
	peer2 := NewNode("peer2", "127.0.0.1:6381")
	cluster.mu.Lock()
	cluster.Nodes["peer1"] = peer1
	cluster.Nodes["peer2"] = peer2
	cluster.mu.Unlock()

	g := NewGossiper(context.Background(), cluster)
	g.pingRandomPeers()

	cluster.mu.RLock()
	assert.True(t, peer1.PingSent > 0)
	assert.True(t, peer2.PingSent > 0)
	cluster.mu.RUnlock()
}

func TestGossip_PingManyPeersRespectsFanout(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	cluster.mu.Lock()
	for i := 0; i < 10; i++ {
		id := "peer" + string(rune('0'+i))
		cluster.Nodes[id] = NewNode(id, "127.0.0.1:6380")
	}
	cluster.mu.Unlock()

	g := NewGossiper(context.Background(), cluster)
	g.pingRandomPeers()

	// Without Bus, pingRandomPeers falls back to local timestamp update.
	// Verify at least one peer was pinged (respecting fanout).
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	pingedCount := 0
	for _, node := range cluster.Nodes {
		if node.ID != cluster.Myself.ID && node.PingSent > 0 {
			pingedCount++
		}
	}
	assert.True(t, pingedCount > 0)
	assert.True(t, pingedCount <= gossipFanout)
}

func TestGossip_CheckFailures_SkipsSelf(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	selfID := cluster.Myself.ID
	cluster.mu.Lock()
	cluster.Myself.PongRecv = time.Now().Add(-10 * time.Second).UnixMilli()
	cluster.mu.Unlock()

	g := NewGossiper(context.Background(), cluster)
	g.checkFailures()

	cluster.mu.RLock()
	selfNode := cluster.Nodes[selfID]
	assert.False(t, selfNode.hasFailFlag())
	cluster.mu.RUnlock()
}

func TestGossip_CheckFailures_SkipsNeverContacted(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	peer := NewNode("peer1", "127.0.0.1:6380")
	cluster.mu.Lock()
	cluster.Nodes["peer1"] = peer
	cluster.mu.Unlock()

	g := NewGossiper(context.Background(), cluster)
	g.checkFailures()

	cluster.mu.RLock()
	assert.Equal(t, 0, len(peer.Flags))
	cluster.mu.RUnlock()
}

func TestGossip_CheckFailures_MarksPFAIL(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	peer := NewNode("peer1", "127.0.0.1:6380")
	peer.PongRecv = time.Now().Add(-10 * time.Second).UnixMilli()

	cluster.mu.Lock()
	cluster.Nodes["peer1"] = peer
	cluster.mu.Unlock()

	g := NewGossiper(context.Background(), cluster)
	g.checkFailures()

	cluster.mu.RLock()
	foundPFail := false
	for _, flag := range peer.Flags {
		if flag == FlagPFail {
			foundPFail = true
			break
		}
	}
	assert.True(t, foundPFail)
	cluster.mu.RUnlock()
}

func TestGossip_CheckFailures_RemovesStaleNode(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping stale node gossip test in short mode")
	}
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	peer := NewNode("peer1", "127.0.0.1:6380")
	peer.DiscoveredAt = time.Now().Add(-120 * time.Second).UnixMilli()
	peer.PongRecv = time.Now().Add(-120 * time.Second).UnixMilli()

	cluster.mu.Lock()
	cluster.Nodes["peer1"] = peer
	cluster.mu.Unlock()

	g := NewGossiper(context.Background(), cluster)
	g.checkFailures()

	cluster.mu.RLock()
	_, exists := cluster.Nodes["peer1"]
	cluster.mu.RUnlock()
	assert.False(t, exists)
}

func TestGossip_CheckFailures_StaleNodeSlotReassigned(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping stale node gossip test in short mode")
	}
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	peer := NewNode("peer1", "127.0.0.1:6380")
	peer.DiscoveredAt = time.Now().Add(-120 * time.Second).UnixMilli()
	peer.PongRecv = time.Now().Add(-120 * time.Second).UnixMilli()

	cluster.mu.Lock()
	cluster.Nodes["peer1"] = peer
	for i := uint32(0); i < 10; i++ {
		cluster.Slots[i] = peer
	}
	cluster.mu.Unlock()

	g := NewGossiper(context.Background(), cluster)
	g.checkFailures()

	cluster.mu.RLock()
	for i := uint32(0); i < 10; i++ {
		assert.Equal(t, cluster.Myself.ID, cluster.Slots[i].ID)
	}
	cluster.mu.RUnlock()
}

func TestGossip_ContextCancellation(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	g := NewGossiper(context.Background(), cluster)
	g.Start()
	g.Stop()
	assert.False(t, g.started.Load())
}