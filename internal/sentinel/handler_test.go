package sentinel

import (
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// isError checks if a RESP is an error type
func isError(r proto.RESP) bool {
	_, ok := r.(*proto.Error)
	return ok
}

// TestSentinelHandler_New tests NewSentinelHandler
func TestSentinelHandler_New(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	assert.True(t, handler != nil)
	assert.True(t, handler.sentinel != nil)
}

// TestSentinelHandler_HandleConnection_PING tests HandleConnection with PING command
func TestSentinelHandler_HandleConnection_PING(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)

	// Create a mock connection using pipe
	server, client := net.Pipe()
	defer server.Close()

	go handler.HandleConnection(client)

	// Send PING command
	cmd := &proto.Array{
		Args: [][]byte{[]byte("PING")},
	}
	var buf bytes.Buffer
	err := proto.WriteRESP(&buf, cmd)
	assert.NoError(t, err)

	_, err = server.Write(buf.Bytes())
	assert.NoError(t, err)

	// Read response using bufio.Reader
	reader := bufio.NewReader(server)
	resp, err := proto.ReadRESP(reader)
	assert.NoError(t, err)
	assert.True(t, resp.Args[0] != nil)
}

// TestSentinelHandler_executeCommand_Unknown tests executeCommand with unknown command
func TestSentinelHandler_executeCommand_Unknown(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	resp := handler.executeCommand("UNKNOWN", nil)
	assert.True(t, isError(resp))
}

// TestSentinelHandler_executeCommand_Sentinel tests executeCommand with SENTINEL subcommand
func TestSentinelHandler_executeCommand_Sentinel(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)

	// Test SENTINEL with no args
	resp := handler.executeCommand("SENTINEL", nil)
	assert.True(t, isError(resp))
}

// TestSentinelHandler_handleSentinelCommand_Masters tests handleSentinelCommand with MASTERS subcommand
func TestSentinelHandler_handleSentinelCommand_Masters(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	resp := handler.handleSentinelCommand("MASTERS", nil)
	// Should return empty array since no masters configured
	assert.True(t, resp != nil)
}

// TestSentinelHandler_handleSentinelCommand_GetMasterAddrByName tests with empty args
func TestSentinelHandler_handleSentinelCommand_GetMasterAddrByName(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	// Get-Master-Addr-By-Name with no args should return error
	resp := handler.handleSentinelCommand("GET-MASTER-ADDR-BY-NAME", nil)
	assert.True(t, isError(resp))
}

// TestSentinelHandler_handleSentinelCommand_Slaves tests with no args
func TestSentinelHandler_handleSentinelCommand_Slaves(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	// SLAVES with no args should return error
	resp := handler.handleSentinelCommand("SLAVES", nil)
	assert.True(t, isError(resp))
}

// TestSentinelHandler_handleSentinelCommand_Failover tests with no args
func TestSentinelHandler_handleSentinelCommand_Failover(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	// FAILOVER with no args should return error
	resp := handler.handleSentinelCommand("FAILOVER", nil)
	assert.True(t, isError(resp))
}

// TestSentinelHandler_handleSentinelCommand_UnknownSubcommand tests unknown subcommand
func TestSentinelHandler_handleSentinelCommand_UnknownSubcommand(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	resp := handler.handleSentinelCommand("UNKNOWN", nil)
	assert.True(t, isError(resp))
}

// TestSentinelHandler_HandleConnection_EmptyArgs tests HandleConnection with empty command args
func TestSentinelHandler_HandleConnection_EmptyArgs(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)

	server, client := net.Pipe()
	defer server.Close()

	go handler.HandleConnection(client)

	// Send empty args
	cmd := &proto.Array{
		Args: [][]byte{},
	}
	var buf bytes.Buffer
	err := proto.WriteRESP(&buf, cmd)
	assert.NoError(t, err)

	_, err = server.Write(buf.Bytes())
	assert.NoError(t, err)
}

// TestSentinelHandler_HandleConnection_ReadError tests HandleConnection with read error
func TestSentinelHandler_HandleConnection_ReadError(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)

	// Create a connection that closes immediately
	server, client := net.Pipe()
	server.Close()

	// This should not panic
	handler.HandleConnection(client)
}

// TestSentinelHandler_handleSentinelCommand_Monitor tests MONITOR subcommand
func TestSentinelHandler_handleSentinelCommand_Monitor(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)

	// Test MONITOR with invalid quorum
	resp := handler.handleSentinelCommand("MONITOR", [][]byte{
		[]byte("mymaster"),
		[]byte("127.0.0.1"),
		[]byte("6379"),
		[]byte("invalid"),
	})
	assert.True(t, isError(resp))
}

// TestSentinelHandler_handleSentinelCommand_MonitorInsufficientArgs tests MONITOR with insufficient args
func TestSentinelHandler_handleSentinelCommand_MonitorInsufficientArgs(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)

	// Test MONITOR with insufficient args
	resp := handler.handleSentinelCommand("MONITOR", [][]byte{
		[]byte("mymaster"),
	})
	assert.True(t, isError(resp))
}

// TestSentinelHandler_executeCommand_PING tests executeCommand with PING
func TestSentinelHandler_executeCommand_PING(t *testing.T) {
	t.Parallel()
	sentinel := NewSentinel(1, 30000)
	defer sentinel.Stop()

	handler := NewSentinelHandler(sentinel)
	resp := handler.executeCommand("PING", nil)
	assert.False(t, isError(resp))
}
