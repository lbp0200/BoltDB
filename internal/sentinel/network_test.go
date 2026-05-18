package sentinel

import (
	"bufio"
	"net"
	"testing"

	"github.com/zeebo/assert"
)

func TestNetwork_SendPing(t *testing.T) {
	t.Parallel()
	ok, err := SendPing("invalid-address")
	assert.Error(t, err)
	assert.False(t, ok)
}

func TestNetwork_SendInfoReplication(t *testing.T) {
	t.Parallel()
	info, err := SendInfoReplication("invalid-address")
	assert.Error(t, err)
	assert.Equal(t, "", info)
}

func TestNetwork_GetRole(t *testing.T) {
	t.Parallel()
	role, err := GetRole("invalid-address")
	assert.Error(t, err)
	assert.Equal(t, "", role)
}

func TestNetwork_SendSlaveOfNoOne(t *testing.T) {
	t.Parallel()
	err := SendSlaveOfNoOne("invalid-address")
	assert.Error(t, err)
}

func TestNetwork_SendReplicaOf(t *testing.T) {
	t.Parallel()
	err := SendReplicaOf("invalid-address", "invalid-master")
	assert.Error(t, err)
}

// TestSentinelConnection_Close tests Close method
func TestSentinelConnection_Close(t *testing.T) {
	t.Parallel()
	// Create a listener
	listener, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	defer listener.Close()

	// Connect to the listener
	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	// Create SentinelConnection and close it
	sc := &SentinelConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
	err = sc.Close()
	assert.NoError(t, err)
}

func TestSentinelConnection_SendCommand(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	defer listener.Close()

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		c, e := listener.Accept()
		accepted <- acceptResult{c, e}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	r := <-accepted
	assert.NoError(t, r.err)
	defer r.conn.Close()

	sc := &SentinelConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}

	err = sc.SendCommand("*1\r\n$4\r\nPING\r\n")
	assert.NoError(t, err)
}

func TestSentinelConnection_ReadResponse(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	defer listener.Close()

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		c, e := listener.Accept()
		accepted <- acceptResult{c, e}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	r := <-accepted
	assert.NoError(t, r.err)
	serverConn := r.conn
	defer serverConn.Close()

	_, err = serverConn.Write([]byte("+PONG\r\n"))
	assert.NoError(t, err)

	sc := &SentinelConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}

	resp, err := sc.ReadResponse()
	assert.NoError(t, err)
	assert.True(t, resp == "+PONG")
}

// TestSentinelConnection_ReadResponse_Error tests ReadResponse with error
func TestSentinelConnection_ReadResponse_Error(t *testing.T) {
	t.Parallel()
	// Create a listener
	listener, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	defer listener.Close()

	// Connect client
	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)

	// Close the connection to cause read error
	conn.Close()

	// Create SentinelConnection with reader
	sc := &SentinelConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: nil,
	}

	// Test ReadResponse should return error
	_, err = sc.ReadResponse()
	assert.True(t, err != nil)
}

// TestSentinelConnection_SendCommand_WriteError tests SendCommand with write error
func TestSentinelConnection_SendCommand_WriteError(t *testing.T) {
	t.Parallel()
	// Create a listener
	listener, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	defer listener.Close()

	// Connect client
	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	defer conn.Close()

	// Close the connection to cause write error
	conn.Close()

	// Create SentinelConnection with writer
	sc := &SentinelConnection{
		conn:   conn,
		reader: nil,
		writer: bufio.NewWriter(conn),
	}

	// Test SendCommand should return error
	err = sc.SendCommand("*1\r\n$4\r\nPING\r\n")
	assert.True(t, err != nil)
}
