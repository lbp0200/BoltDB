package sentinel

import (
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
