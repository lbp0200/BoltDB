package cluster

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestBusAddrForPeer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"127.0.0.1:6379", "127.0.0.1:16379"},
		{"10.0.0.1:3000", "10.0.0.1:13000"},
		{"[::1]:6379", "::1:16379"},
		{"no-port-string", "no-port-string"},
	}

	for _, tc := range tests {
		result := busAddrForPeer(tc.input)
		assert.Equal(t, tc.expected, result)
	}
}

func TestWriteBusMsgWithoutPayload(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	conn := &mockConn{w: &buf}

	err := writeBusMsg(conn, "PING", "node123", nil)
	assert.NoError(t, err)

	expected := "*2\r\n$4\r\nPING\r\n$7\r\nnode123\r\n"
	assert.Equal(t, expected, buf.String())
}

func TestWriteBusMsgWithPayload(t *testing.T) {
	t.Parallel()

	payload := &GossipPayload{
		Epoch: 42,
		Slots: "0-16383",
	}

	var buf bytes.Buffer
	conn := &mockConn{w: &buf}

	err := writeBusMsg(conn, "PONG", "node456", payload)
	assert.NoError(t, err)

	data := buf.String()
	assert.True(t, strings.HasPrefix(data, "*3\r\n"))

	// Message format: *3\r\n$4\r\nPONG\r\n$7\r\nnode456\r\n$<len>\r\n<binary>\r\n
	parts := strings.Split(data, "\r\n")
	for i, p := range parts {
		if strings.HasPrefix(p, "G") || strings.HasPrefix(p, "J") {
			decoded, err := unmarshalGossipPayload([]byte(p))
			assert.NoError(t, err)
			assert.NotNil(t, decoded)
			assert.Equal(t, int64(42), decoded.Epoch)
			assert.Equal(t, "0-16383", decoded.Slots)
			return
		}
		_ = i
	}
	t.Fatal("gossip payload not found in bus message")
}

func TestFormatSlotsBrief(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ranges   []SlotRange
		expected string
	}{
		{nil, ""},
		{[]SlotRange{}, ""},
		{[]SlotRange{{Start: 0, End: 16383}}, "0-16383"},
		{[]SlotRange{{Start: 100, End: 200}}, "100-200"},
		{[]SlotRange{{Start: 0, End: 0}}, "0-0"},
		{[]SlotRange{{Start: 0, End: 10}, {Start: 20, End: 30}}, "2 ranges"},
		{[]SlotRange{{Start: 0, End: 10}, {Start: 20, End: 30}, {Start: 40, End: 50}}, "3 ranges"},
	}

	for _, tc := range tests {
		result := formatSlotsBrief(tc.ranges)
		assert.Equal(t, tc.expected, result)
	}
}

func TestNewClusterBus(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())
	assert.NotNil(t, bus)
	assert.NotNil(t, bus.ctx)
	assert.NotNil(t, bus.peers)
	assert.Equal(t, cluster, bus.cluster)
}

func TestClusterBusStartStop(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	err := bus.Start("127.0.0.1", 0)
	assert.NoError(t, err)

	addr := bus.Addr()
	assert.True(t, strings.Contains(addr, "127.0.0.1:"))

	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	_ = conn.Close()

	bus.Stop()

	_, err = net.Dial("tcp", addr)
	assert.True(t, err != nil)
}

func TestClusterBusDoubleStop(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())
	err := bus.Start("127.0.0.1", 0)
	assert.NoError(t, err)

	bus.Stop()
	bus.Stop() // second Stop must be idempotent — no panic, no deadlock

	// Verify bus is stopped: listener address should be closed
	addr := bus.Addr()
	if addr != "" {
		_, err := net.Dial("tcp", addr)
		assert.Error(t, err)
	}
}

func TestBuildGossipPayload(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Clear default all-slots and assign a specific range
	cluster.Myself.Slots = []SlotRange{{Start: 0, End: 1000}}

	bus := NewClusterBus(cluster, context.Background())
	payload := bus.BuildGossipPayload()

	assert.NotNil(t, payload)
	assert.Equal(t, cluster.Epoch, payload.Epoch)
	assert.Equal(t, "0-1000", payload.Slots)

	foundMyself := false
	for _, entry := range payload.SlotOwners {
		if entry.NodeID == cluster.Myself.ID {
			foundMyself = true
			assert.Equal(t, 1, len(entry.Ranges))
			assert.Equal(t, uint32(0), entry.Ranges[0].Start)
			assert.Equal(t, uint32(1000), entry.Ranges[0].End)
		}
	}
	assert.True(t, foundMyself)

	assert.Equal(t, 0, len(payload.PFail))
}

func TestBuildGossipPayloadWithPFail(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	peer := NewNode("peer1", "127.0.0.1:6380")
	peer.Flags = []string{FlagPFail}
	peer.Epoch = 5
	cluster.Nodes["peer1"] = peer

	bus := NewClusterBus(cluster, context.Background())
	payload := bus.BuildGossipPayload()

	assert.NotNil(t, payload)
	// PFail list
	assert.Equal(t, 1, len(payload.PFail))
	assert.Equal(t, "peer1", payload.PFail[0])

	// Gossip section contains the PFAIL node with flags
	assert.Equal(t, 1, len(payload.Nodes))
	assert.Equal(t, "peer1", payload.Nodes[0].ID)
	assert.Equal(t, "127.0.0.1:6380", payload.Nodes[0].Addr)
	assert.Equal(t, int64(5), payload.Nodes[0].Epoch)
	foundPFailFlag := false
	for _, flag := range payload.Nodes[0].Flags {
		if flag == FlagPFail {
			foundPFailFlag = true
			break
		}
	}
	assert.True(t, foundPFailFlag)
}

func TestApplyGossipPayloadSlotReconciliation(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	payload := &GossipPayload{
		Epoch: 1,
		SlotOwners: []SlotOwnerEntry{
			{
				NodeID: "peer1",
				Epoch:  1,
				Ranges: []SlotOwnerRange{
					{Start: 0, End: 100},
				},
			},
		},
	}

	peer1 := NewNode("peer1", "127.0.0.1:6380")
	cluster.Nodes["peer1"] = peer1

	dirty := bus.ApplyGossipPayloadFrom("peer1", payload)
	assert.True(t, dirty)

	assert.Equal(t, peer1, cluster.Slots[0])
	assert.Equal(t, peer1, cluster.Slots[100])
	assert.Equal(t, cluster.Myself, cluster.Slots[101])
}

func TestApplyGossipPayloadEpoch(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	payload := &GossipPayload{Epoch: 100}
	dirty := bus.ApplyGossipPayloadFrom("peer1", payload)
	assert.False(t, dirty)
	assert.Equal(t, int64(100), cluster.Epoch)

	payload2 := &GossipPayload{Epoch: 50}
	dirty = bus.ApplyGossipPayloadFrom("peer1", payload2)
	assert.False(t, dirty)
	assert.Equal(t, int64(100), cluster.Epoch)
}

func TestApplyGossipPayloadPfailPromotion(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	peer1 := NewNode("peer1", "127.0.0.1:6380")
	peer2 := NewNode("peer2", "127.0.0.1:6381")
	cluster.Nodes["peer1"] = peer1
	cluster.Nodes["peer2"] = peer2

	// Assign peer2 some slots and clear them from Myself
	for i := uint32(1000); i <= 2000; i++ {
		cluster.Slots[i] = peer2
	}
	peer2.AddSlotRange(1000, 2000)
	cluster.Myself.Slots = []SlotRange{{Start: 0, End: 999}, {Start: 2001, End: 16383}}

	// With 2 non-self nodes, threshold=1, so single report promotes to FAIL.
	payload := &GossipPayload{PFail: []string{"peer2"}}

	dirty := bus.ApplyGossipPayloadFrom("peer1", payload)
	assert.True(t, dirty)
	assert.True(t, peer2.hasFailFlag())

	// Verify peer2's slots were reassigned to Myself
	for i := uint32(1000); i <= 2000; i++ {
		assert.Equal(t, cluster.Myself, cluster.Slots[i])
	}
}

func TestHasPeer(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())
	assert.False(t, bus.HasPeer("peer1"))

	bus.registerPeer("peer1", &mockConn{}, nil)
	assert.True(t, bus.HasPeer("peer1"))

	bus.registerPeer("peer2", &mockConn{}, nil)
	assert.True(t, bus.HasPeer("peer1"))
	assert.True(t, bus.HasPeer("peer2"))

	bus.removePeerIfMatch("unknown", &mockConn{})
	assert.True(t, bus.HasPeer("peer1"))

	bus.removePeerIfMatch("peer1", &mockConn{})
	assert.True(t, bus.HasPeer("peer1")) // wrong conn, not removed
}

func TestApplyGossipPayloadGossipSection(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	payload := &GossipPayload{
		Nodes: []GossipNodeInfo{
			{
				ID:    "newpeer",
				Addr:  "127.0.0.1:6390",
				Epoch: 5,
				Flags: []string{"master"},
			},
		},
	}

	dirty := bus.ApplyGossipPayloadFrom("peer1", payload)
	assert.False(t, dirty)

	node := cluster.GetNodeByID("newpeer")
	assert.NotNil(t, node)
	assert.Equal(t, "127.0.0.1:6390", node.Addr)

	payload2 := &GossipPayload{
		Nodes: []GossipNodeInfo{
			{
				ID:    "newpeer",
				Addr:  "127.0.0.1:6390",
				Epoch: 10,
				Flags: []string{"master"},
			},
		},
	}
	dirty = bus.ApplyGossipPayloadFrom("peer1", payload2)
	assert.False(t, dirty)
	assert.Equal(t, int64(10), node.Epoch)
}

func TestApplyGossipPayloadSlotsHigherEpoch(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	peer1 := NewNode("peer1", "127.0.0.1:6380")
	cluster.Nodes["peer1"] = peer1

	cluster.Slots[0] = peer1
	peer1.Epoch = 5

	peer2 := NewNode("peer2", "127.0.0.1:6381")
	cluster.Nodes["peer2"] = peer2

	// Payload with higher epoch than current owner
	payload := &GossipPayload{
		SlotOwners: []SlotOwnerEntry{
			{
				NodeID: "peer2",
				Epoch:  10,
				Ranges: []SlotOwnerRange{
					{Start: 0, End: 0},
				},
			},
		},
	}

	dirty := bus.ApplyGossipPayloadFrom("peer2", payload)
	assert.True(t, dirty)
	assert.Equal(t, peer2, cluster.Slots[0])

	// Set peer2's epoch to reflect the high epoch assignment
	peer2.Epoch = 10

	// Payload with lower epoch should not overwrite
	payload2 := &GossipPayload{
		SlotOwners: []SlotOwnerEntry{
			{
				NodeID: "peer1",
				Epoch:  5,
				Ranges: []SlotOwnerRange{
					{Start: 0, End: 0},
				},
			},
		},
	}
	dirty = bus.ApplyGossipPayloadFrom("peer1", payload2)
	assert.False(t, dirty)
	assert.Equal(t, peer2, cluster.Slots[0])
}

func TestBusPeerCount(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())
	assert.Equal(t, 0, bus.PeerCount())

	bus.registerPeer("peer1", &mockConn{}, nil)
	assert.Equal(t, 1, bus.PeerCount())

	bus.registerPeer("peer1", &mockConn{}, nil)
	assert.Equal(t, 1, bus.PeerCount())

	bus.removePeerIfMatch("peer1", &mockConn{})
	assert.Equal(t, 1, bus.PeerCount())

	conn := &mockConn{}
	bus.registerPeer("peer2", conn, nil)
	bus.removePeerIfMatch("peer2", conn)
	assert.Equal(t, 1, bus.PeerCount())
}

// mockConn implements net.Conn using bytes.Buffer for testing.
type mockConn struct {
	w      *bytes.Buffer
	r      *bytes.Buffer
	closed bool
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.r == nil {
		return 0, nil
	}
	return m.r.Read(b)
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.w == nil {
		return len(b), nil
	}
	return m.w.Write(b)
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr              { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234} }
func (m *mockConn) RemoteAddr() net.Addr             { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5678} }
func (m *mockConn) SetDeadline(time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(time.Time) error { return nil }

// TestApplyGossipPayloadFailRecoveryDirect verifies a FAIL node recovering via
// direct PONG gets its fail flag cleared and its usurped slots returned (P3).
func TestApplyGossipPayloadFailRecoveryDirect(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	peer1 := NewNode("peer1", "127.0.0.1:6380")
	peer2 := NewNode("peer2", "127.0.0.1:6381")
	cluster.Nodes["peer1"] = peer1
	cluster.Nodes["peer2"] = peer2

	// Assign peer2 some slots and clear them from Myself
	for i := uint32(1000); i <= 2000; i++ {
		cluster.Slots[i] = peer2
	}
	peer2.AddSlotRange(1000, 2000)
	cluster.Myself.Slots = []SlotRange{{Start: 0, End: 999}, {Start: 2001, End: 16383}}

	// FAIL promotion (single reporter is enough for a 2-peer cluster)
	payload := &GossipPayload{PFail: []string{"peer2"}}
	dirty := bus.ApplyGossipPayloadFrom("peer1", payload)
	assert.True(t, dirty)
	assert.True(t, peer2.hasFailFlag())
	for i := uint32(1000); i <= 2000; i++ {
		assert.Equal(t, cluster.Myself, cluster.Slots[i])
	}
	assert.Equal(t, 1, len(cluster.usurpedSlots["peer2"]))

	// peer2 recovers: sends a PONG over the bus
	aliveDirty := bus.handlePONG("peer2")
	assert.True(t, aliveDirty)

	assert.False(t, peer2.hasFailFlag())
	for i := uint32(1000); i <= 2000; i++ {
		assert.Equal(t, peer2, cluster.Slots[i])
	}
	// Usurp record cleared
	_, ok := cluster.usurpedSlots["peer2"]
	assert.False(t, ok)
	// Myself's slot ranges rebuilt: 1000-2000 no longer owned by self
	assert.False(t, cluster.Myself.HasSlot(1000))
	assert.False(t, cluster.Myself.HasSlot(2000))
	assert.True(t, cluster.Myself.HasSlot(999))
	assert.True(t, cluster.Myself.HasSlot(2001))
}

// TestApplyGossipPayloadFailRecoveryViaGossip verifies indirect recovery:
// a peer's gossip reporting a fresh PongRecv for a FAIL node clears the flag
// and returns the usurped slots (P3).
func TestApplyGossipPayloadFailRecoveryViaGossip(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	peer1 := NewNode("peer1", "127.0.0.1:6380")
	peer2 := NewNode("peer2", "127.0.0.1:6381")
	cluster.Nodes["peer1"] = peer1
	cluster.Nodes["peer2"] = peer2

	for i := uint32(1000); i <= 2000; i++ {
		cluster.Slots[i] = peer2
	}
	peer2.AddSlotRange(1000, 2000)
	cluster.Myself.Slots = []SlotRange{{Start: 0, End: 999}, {Start: 2001, End: 16383}}

	// FAIL promotion
	dirty := bus.ApplyGossipPayloadFrom("peer1", &GossipPayload{PFail: []string{"peer2"}})
	assert.True(t, dirty)
	assert.True(t, peer2.hasFailFlag())

	// peer1 gossips that peer2 just responded (fresh PongRecv)
	now := time.Now().UnixMilli()
	dirty = bus.ApplyGossipPayloadFrom("peer1", &GossipPayload{
		Nodes: []GossipNodeInfo{
			{ID: "peer2", Addr: "127.0.0.1:6381", PongRecv: now},
		},
	})
	assert.True(t, dirty)
	assert.False(t, peer2.hasFailFlag())
	for i := uint32(1000); i <= 2000; i++ {
		assert.Equal(t, peer2, cluster.Slots[i])
	}
	_, ok := cluster.usurpedSlots["peer2"]
	assert.False(t, ok)
}

// TestApplyGossipPayloadStalePongNoRecovery verifies that a stale PongRecv in
// gossip does NOT trigger recovery (keeps FAIL flag and usurped slots).
func TestApplyGossipPayloadStalePongNoRecovery(t *testing.T) {
	t.Parallel()
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	bus := NewClusterBus(cluster, context.Background())

	peer1 := NewNode("peer1", "127.0.0.1:6380")
	peer2 := NewNode("peer2", "127.0.0.1:6381")
	cluster.Nodes["peer1"] = peer1
	cluster.Nodes["peer2"] = peer2

	for i := uint32(1000); i <= 2000; i++ {
		cluster.Slots[i] = peer2
	}
	peer2.AddSlotRange(1000, 2000)
	cluster.Myself.Slots = []SlotRange{{Start: 0, End: 999}, {Start: 2001, End: 16383}}

	// FAIL promotion
	dirty := bus.ApplyGossipPayloadFrom("peer1", &GossipPayload{PFail: []string{"peer2"}})
	assert.True(t, dirty)

	// Stale PongRecv (older than failTimeout) must not recover the node
	stale := time.Now().UnixMilli() - failTimeout.Milliseconds() - 1000
	dirty = bus.ApplyGossipPayloadFrom("peer1", &GossipPayload{
		Nodes: []GossipNodeInfo{
			{ID: "peer2", Addr: "127.0.0.1:6381", PongRecv: stale},
		},
	})
	assert.False(t, dirty)
	assert.True(t, peer2.hasFailFlag())
	for i := uint32(1000); i <= 2000; i++ {
		assert.Equal(t, cluster.Myself, cluster.Slots[i])
	}
}
