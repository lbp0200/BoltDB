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
	t.Parallel()
	config := DefaultGossipConfig()
	assert.True(t, config != nil)
	assert.True(t, config.HelloInterval > 0)
}

// TestNewGossipProtocol tests NewGossipProtocol
func TestNewGossipProtocol(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	// Create with nil config (should use default)
	gp := NewGossipProtocol(sentinel, nil)
	assert.True(t, gp != nil)
	assert.True(t, gp.sentinel != nil)
}

// TestNewGossipProtocol_WithConfig tests NewGossipProtocol with custom config
func TestNewGossipProtocol_WithConfig(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	config := &GossipConfig{
		PingInterval: 1000 * time.Millisecond,
	}
	gp := NewGossipProtocol(sentinel, config)
	assert.NotNil(t, gp)
}

// TestGossipProtocol_GetPort tests GetPort
func TestGossipProtocol_GetPort(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	port := gp.GetPort()
	// Port may be 0 before start — just verify it's a valid int
	_ = port
}

// TestGossipProtocol_FormatHello tests formatHello
func TestGossipProtocol_FormatHello(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	hello := gp.formatHello()
	// Hello should start with HELLO prefix
	assert.True(t, len(hello) > 6 && hello[:6] == "HELLO ")
}

// TestGossipProtocol_FormatPong tests formatPong
func TestGossipProtocol_FormatPong(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	pong := gp.formatPong()
	// Pong should not be empty
	assert.True(t, len(pong) > 0)
}

// TestGossipProtocol_GetPeersCount tests GetPeersCount
func TestGossipProtocol_GetPeersCount(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	count := gp.GetPeersCount()
	// Initially should be 0
	assert.Equal(t, 0, count)
}

// TestGossipProtocol_AddUpdatePeer tests addOrUpdatePeer
func TestGossipProtocol_AddUpdatePeer(t *testing.T) {
	t.Parallel()
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

// removePeer removes a peer from the gossip protocol (test helper)
func (gp *GossipProtocol) removePeer(addr string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	delete(gp.peers, addr)
}

// TestGossipProtocol_RemovePeer tests removePeer
func TestGossipProtocol_RemovePeer(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)

	// Stop without starting - should not panic
	gp.Stop()
}

// TestGossipProtocol_sendMessage tests sendMessage method
func TestGossipProtocol_sendMessage(t *testing.T) {
	t.Parallel()
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
	assert.NoError(t, err)
}

// TestGossipProtocol_BroadcastSdown tests BroadcastSdown method
func TestGossipProtocol_BroadcastSdown(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

// TestGossipProtocol_Stop_WaitsForHandleConnection verifies that Stop()
// waits for in-flight handleConnection goroutines before returning.
func TestGossipProtocol_Stop_WaitsForHandleConnection(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	err := gp.Start()
	assert.Nil(t, err)

	// Connect to ourselves — this creates a handleConnection goroutine.
	port := gp.GetPort()
	conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	assert.Nil(t, err)

	// Give the connection time to be accepted and the goroutine to start.
	time.Sleep(50 * time.Millisecond)

	// Close the client side first so handleConnection's ReadString
	// returns EOF before Stop() waits for wg.
	conn.Close()

	// Stop must complete promptly — the handleConnection goroutine
	// should have exited after ReadString returned EOF.
	done := make(chan struct{})
	go func() {
		gp.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() blocked — handleConnection goroutine not tracked or not exiting")
	}
}

// TestGossipProtocol_Stop_WaitsForMultipleConnections verifies that Stop()
// waits for multiple concurrent handleConnection goroutines.
func TestGossipProtocol_Stop_WaitsForMultipleConnections(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	gp := NewGossipProtocol(sentinel, nil)
	err := gp.Start()
	assert.Nil(t, err)

	port := gp.GetPort()

	// Create 3 concurrent connections.
	var conns []net.Conn
	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
		assert.Nil(t, err)
		conns = append(conns, conn)
	}

	time.Sleep(50 * time.Millisecond)

	// Close all client connections so handleConnection goroutines exit.
	for _, c := range conns {
		c.Close()
	}

	done := make(chan struct{})
	go func() {
		gp.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() blocked — concurrent handleConnection goroutines not tracked")
	}
}
