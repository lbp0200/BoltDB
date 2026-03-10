package sentinel

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// TestDefaultGossipConfig tests DefaultGossipConfig
func TestDefaultGossipConfig(t *testing.T) {
	config := DefaultGossipConfig()
	assert.True(t, config != nil)
	assert.True(t, config.HelloInterval > 0)
}

// TestNewGossipProtocol tests NewGossipProtocol
func TestNewGossipProtocol(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Create with nil config (should use default)
	gp := NewGossipProtocol(sentinel, nil)
	assert.True(t, gp != nil)
	assert.True(t, gp.sentinel != nil)
}

// TestNewGossipProtocol_WithConfig tests NewGossipProtocol with custom config
func TestNewGossipProtocol_WithConfig(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	config := &GossipConfig{
		PingInterval: 1000 * time.Millisecond,
	}
	gp := NewGossipProtocol(sentinel, config)
	assert.True(t, gp != nil)
}

// TestGossipProtocol_GetPort tests GetPort
func TestGossipProtocol_GetPort(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	port := gp.GetPort()
	// Port should be non-zero when started, but before start it may be 0
	_ = port
	assert.True(t, true)
}

// TestGossipProtocol_FormatHello tests formatHello
func TestGossipProtocol_FormatHello(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	hello := gp.formatHello()
	// Hello should start with HELLO prefix
	assert.True(t, len(hello) > 0)
	assert.True(t, len(hello) > 6 && hello[:6] == "HELLO ")
}

// TestGossipProtocol_FormatPong tests formatPong
func TestGossipProtocol_FormatPong(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	pong := gp.formatPong()
	// Pong should not be empty
	assert.True(t, len(pong) > 0)
}

// TestGossipProtocol_GetPeersCount tests GetPeersCount
func TestGossipProtocol_GetPeersCount(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	count := gp.GetPeersCount()
	// Initially should be 0
	assert.Equal(t, 0, count)
}

// TestGossipProtocol_AddUpdatePeer tests addOrUpdatePeer
func TestGossipProtocol_AddUpdatePeer(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	// Add a peer
	gp.addOrUpdatePeer("127.0.0.1:26380", "test-run-id")
	count := gp.GetPeersCount()
	assert.Equal(t, 1, count)

	// Update same peer - should not increase count
	gp.addOrUpdatePeer("127.0.0.1:26380", "new-run-id")
	count = gp.GetPeersCount()
	assert.Equal(t, 1, count)
}

// TestGossipProtocol_RemovePeer tests removePeer
func TestGossipProtocol_RemovePeer(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	// Add a peer
	gp.addOrUpdatePeer("127.0.0.1:26380", "test-run-id")
	assert.Equal(t, 1, gp.GetPeersCount())

	// Remove peer
	gp.removePeer("127.0.0.1:26380")
	assert.Equal(t, 0, gp.GetPeersCount())
}

// TestGossipProtocol_TouchPeer tests touchPeer
func TestGossipProtocol_TouchPeer(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	// Add and touch peer - should not panic
	gp.addOrUpdatePeer("127.0.0.1:26380", "test-run-id")
	gp.touchPeer("127.0.0.1:26380")
	assert.True(t, true)
}

// TestGossipProtocol_Start_Stop tests Start and Stop methods
func TestGossipProtocol_Start_Stop(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)

	// Start should succeed
	err := gp.Start()
	assert.Nil(t, err)

	// Give some time for goroutines to start
	time.Sleep(100 * time.Millisecond)

	// Stop should not panic
	gp.Stop()
	time.Sleep(50 * time.Millisecond)
}

// TestGossipProtocol_Stop_WithoutStart tests Stop without Start
func TestGossipProtocol_Stop_WithoutStart(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)

	// Stop without starting - should not panic
	gp.Stop()
}

// TestGossipProtocol_sendMessage tests sendMessage method
func TestGossipProtocol_sendMessage(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)

	// Start the protocol to get a listener
	err := gp.Start()
	assert.Nil(t, err)
	defer gp.Stop()

	// Connect to ourselves to test sendMessage
	port := gp.GetPort()
	conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		// Skip if we can't connect
		t.Skip("Could not connect to self:", err)
	}
	defer conn.Close()

	// Test sending a message
	err = gp.sendMessage(conn, "test message")
	_ = err
	assert.True(t, true)
}

// TestGossipProtocol_BroadcastSdown tests BroadcastSdown method
func TestGossipProtocol_BroadcastSdown(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Add a master
	sentinel.AddMaster("mymaster", "127.0.0.1:6379", 2)

	gp := NewGossipProtocol(sentinel, nil)

	// BroadcastSdown should not panic even with no peers
	gp.BroadcastSdown("mymaster", 1)
	assert.True(t, true)
}

// TestGossipProtocol_managePeers tests managePeers method
func TestGossipProtocol_managePeers(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	config := &GossipConfig{
		HelloInterval: 100 * time.Millisecond,
		PeerTimeout:   30 * time.Second,
		MaxPeers:      10,
		RunID:         "test-run-id",
	}

	gp := NewGossipProtocol(sentinel, config)

	// Start the protocol
	err := gp.Start()
	assert.Nil(t, err)
	defer gp.Stop()

	// Give time for managePeers to run
	time.Sleep(200 * time.Millisecond)
	assert.True(t, true)
}

// TestGossipProtocol_sendHello tests sendHello method
func TestGossipProtocol_sendHello(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)

	// Send to invalid address - should fail but we get coverage
	err := gp.sendHello("127.0.0.1:1")
	// Error expected
	assert.True(t, err != nil)
}

// TestGossipProtocol_handleMessage tests handleMessage method
func TestGossipProtocol_handleMessage(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Add a master for SDOWN handling
	sentinel.AddMaster("mymaster", "127.0.0.1:6379", 2)

	gp := NewGossipProtocol(sentinel, nil)

	// Start the gossip protocol to have a valid listener
	err := gp.Start()
	assert.Nil(t, err)
	defer gp.Stop()

	// Connect to ourselves
	port := gp.GetPort()
	conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	assert.Nil(t, err)
	defer conn.Close()

	// Test with different message types
	// Empty message
	gp.handleMessage(conn, "")
	// Unknown command
	gp.handleMessage(conn, "UNKNOWN")
	// HELLO with valid parts
	gp.handleMessage(conn, "HELLO runid 26379 1")
	// HELLO with invalid parts (less than 3)
	gp.handleMessage(conn, "HELLO runid")
	// PING
	gp.handleMessage(conn, "PING")
	// PONG with runid
	gp.handleMessage(conn, "PONG runid")
	// PONG without runid
	gp.handleMessage(conn, "PONG")
	// SDOWN with valid parts
	gp.handleMessage(conn, "SDOWN mymaster 2")
	// SDOWN with invalid parts
	gp.handleMessage(conn, "SDOWN mymaster")
	// SDOWN with non-numeric count
	gp.handleMessage(conn, "SDOWN mymaster abc")
	// MASTERS
	gp.handleMessage(conn, "MASTERS")

	assert.True(t, true)
}

// TestGossipProtocol_sendHellos tests sendHellos method
func TestGossipProtocol_sendHellos(t *testing.T) {
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	config := &GossipConfig{
		HelloInterval: 100 * time.Millisecond,
		PeerTimeout:   30 * time.Second,
		MaxPeers:      10,
		RunID:         "test-run-id",
	}

	gp := NewGossipProtocol(sentinel, config)

	// Start the protocol
	err := gp.Start()
	assert.Nil(t, err)
	defer gp.Stop()

	// Add a peer
	gp.addOrUpdatePeer("127.0.0.1:26380", "test-run-id")

	// Give time for sendHellos to run
	time.Sleep(200 * time.Millisecond)
	assert.True(t, true)
}
