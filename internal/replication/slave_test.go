package replication

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

// mockConn 是一个用于测试的 mock net.Conn
type mockConn struct {
	readBuffer  []byte
	writeBuffer []byte
	writeErr    error
	closed      bool
	localAddr   net.Addr
	remoteAddr  net.Addr
}

func newMockConn() *mockConn {
	return &mockConn{
		localAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6379},
		remoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6380},
	}
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if len(m.readBuffer) == 0 {
		// 阻塞直到有数据或关闭
		time.Sleep(10 * time.Millisecond)
		return 0, nil
	}
	n = copy(b, m.readBuffer)
	m.readBuffer = m.readBuffer[n:]
	return n, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.writeBuffer = append(m.writeBuffer, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return m.localAddr }
func (m *mockConn) RemoteAddr() net.Addr               { return m.remoteAddr }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestSlaveConnection_New(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	assert.NotEqual(t, "", slave.ID)
	assert.NotEqual(t, "", slave.Addr)
	assert.Equal(t, int64(0), slave.GetReplOffset())
	assert.False(t, slave.IsReady())
}

func TestSlaveConnection_ReadyState(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 初始状态是 false
	assert.False(t, slave.IsReady())

	// 设置为 true
	slave.SetReady(true)
	assert.True(t, slave.IsReady())

	// 设置为 false
	slave.SetReady(false)
	assert.False(t, slave.IsReady())
}

func TestSlaveConnection_ReplOffset(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 初始 offset 是 0
	assert.Equal(t, int64(0), slave.GetReplOffset())

	// 设置 offset
	slave.SetReplOffset(100)
	assert.Equal(t, int64(100), slave.GetReplOffset())

	// 更新 ack offset
	slave.UpdateReplAck(50)
}

func TestSlaveConnection_Write(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 测试写入
	data := []byte("test data")
	n, err := slave.Conn.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	// 验证写入的数据
	assert.Equal(t, data, conn.writeBuffer)
}

func TestSlaveConnection_Close(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 初始未关闭
	assert.False(t, conn.closed)

	// 关闭
	err := slave.Close()
	assert.NoError(t, err)
	assert.True(t, conn.closed)

	// 再次关闭应该没问题
	err = slave.Close()
	assert.NoError(t, err)
}

func TestSlaveConnection_GetLastAckTime(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 初始应该有最近的时间戳
	lastAck := slave.GetLastAckTime()
	assert.True(t, lastAck > 0)

	// 更新 ack
	slave.UpdateReplAck(100)
	newLastAck := slave.GetLastAckTime()
	assert.True(t, newLastAck >= lastAck)
}

// 确保 SlaveConnection 实现了必要的接口
var _ net.Conn = (*mockConn)(nil)

func TestSlaveConnection_BufferedWriter(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 验证 Writer 是 bufio.Writer
	assert.NotEqual(t, nil, slave.Writer)
}

func TestSlaveConnection_IDGeneration(t *testing.T) {
	t.Parallel()
	conn1 := newMockConn()
	conn2 := newMockConn()

	// 给一点时间确保 ID 不同
	time.Sleep(time.Millisecond)

	slave1 := NewSlaveConnection(conn1)
	slave2 := NewSlaveConnection(conn2)

	// ID 应该不同
	assert.NotEqual(t, slave1.ID, slave2.ID)
}

// TestSendFullResync tests SendFullResync function
func TestSendFullResync(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	err := SendFullResync(slave, "test-repl-id", 100)
	assert.NoError(t, err)

	// 验证写入的数据包含 FULLRESYNC
	assert.True(t, strings.Contains(string(conn.writeBuffer), "FULLRESYNC"))
}

// TestSendContinueResync tests SendContinueResync function
func TestSendContinueResync(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	err := SendContinueResync(slave, "test-repl-id", 100)
	assert.NoError(t, err)

	// 验证写入的数据包含 CONTINUE
	assert.True(t, strings.Contains(string(conn.writeBuffer), "CONTINUE"))
}

// TestSendBacklogData tests SendBacklogData function
func TestSendBacklogData(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 创建一个 backlog 并添加数据
	backlog := NewReplicationBacklog(1000)
	backlog.Append([]byte("test command"))

	err := SendBacklogData(slave, backlog, 0, 100)
	assert.NoError(t, err)

	// 验证有数据写入
	assert.True(t, len(conn.writeBuffer) > 0)
}

// TestSendBacklogData_EmptyRange tests SendBacklogData with empty range
func TestSendBacklogData_EmptyRange(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// 创建一个 backlog 并添加数据
	backlog := NewReplicationBacklog(1000)
	backlog.Append([]byte("test command"))

	// 请求超出范围的偏移量
	err := SendBacklogData(slave, backlog, 1000, 2000)
	// 可能返回错误或空数据
	_ = err
}

// TestSlaveConnection_SendRDB tests SendRDB function
func TestSlaveConnection_SendRDB(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	rdbData := []byte("RDB_DATA_CONTENT")
	err := slave.SendRDB(rdbData)
	assert.NoError(t, err)

	// 验证写入的数据包含 RESP 格式的 bulk string
	// $16\r\nRDB_DATA_CONTENT\r\n (16 bytes)
	expected := "$16\r\nRDB_DATA_CONTENT\r\n"
	assert.Equal(t, expected, string(conn.writeBuffer))
}

// TestSlaveConnection_SendResponse tests SendResponse function
func TestSlaveConnection_SendResponse(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// Test sending a simple string response
	resp := proto.NewSimpleString("OK")
	err := slave.SendResponse(resp)
	assert.NoError(t, err)

	// Verify data was written
	assert.True(t, strings.Contains(string(conn.writeBuffer), "+OK"))
}

// TestSlaveConnection_SendRDB_Empty tests SendRDB with empty data
func TestSlaveConnection_SendRDB_Empty(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	rdbData := []byte{}
	err := slave.SendRDB(rdbData)
	assert.NoError(t, err)

	// $0\r\n\r\n
	expected := "$0\r\n\r\n"
	assert.Equal(t, expected, string(conn.writeBuffer))
}

// TestSlaveConnection_ReadCommand tests ReadCommand function
func TestSlaveConnection_ReadCommand(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	conn.readBuffer = []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	slave := NewSlaveConnection(conn)

	cmd, err := slave.ReadCommand()
	assert.NoError(t, err)
	assert.NotEqual(t, nil, cmd)
	assert.Equal(t, 3, len(cmd.Args))
}

func TestSlaveConnection_LockUnlock(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	// Lock 应该不会阻塞（未锁状态）
	slave.Lock()
	// 在 Lock 下操作是安全的
	// Unlock 应该不会 panic
	slave.Unlock()
}

func TestSlaveConnection_WriteAndFlush(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	data := []byte("+OK\r\n")
	err := slave.WriteAndFlush(data)
	assert.NoError(t, err)
	assert.Equal(t, data, conn.writeBuffer)
}

func TestSlaveConnection_WriteAndFlush_EmptyData(t *testing.T) {
	t.Parallel()
	conn := newMockConn()
	slave := NewSlaveConnection(conn)

	err := slave.WriteAndFlush([]byte{})
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, conn.writeBuffer)
}
