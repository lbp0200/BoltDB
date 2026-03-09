package replication

import (
	"net"
	"testing"
	"time"
)

// mockMasterConn 是一个用于测试的 mock net.Conn
type mockMasterConn struct {
	readBuffer  []byte
	writeBuffer []byte
	closed      bool
	localAddr   net.Addr
	remoteAddr  net.Addr
}

func newMockMasterConn() *mockMasterConn {
	return &mockMasterConn{
		localAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6380},
		remoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6379},
	}
}

func (m *mockMasterConn) Read(b []byte) (n int, err error) {
	if len(m.readBuffer) == 0 {
		time.Sleep(10 * time.Millisecond)
		return 0, nil
	}
	n = copy(b, m.readBuffer)
	m.readBuffer = m.readBuffer[n:]
	return n, nil
}

func (m *mockMasterConn) Write(b []byte) (n int, err error) {
	m.writeBuffer = append(m.writeBuffer, b...)
	return len(b), nil
}

func (m *mockMasterConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockMasterConn) LocalAddr() net.Addr  { return m.localAddr }
func (m *mockMasterConn) RemoteAddr() net.Addr { return m.remoteAddr }
func (m *mockMasterConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockMasterConn) SetReadDeadline(t time.Time) error   { return nil }
func (m *mockMasterConn) SetWriteDeadline(t time.Time) error { return nil }

// 确保实现了接口
var _ net.Conn = (*mockMasterConn)(nil)

func TestMasterConnection_New(t *testing.T) {
	// 注意：NewMasterConnection 会尝试建立真实连接
	// 这里我们测试它的初始化逻辑可能需要其他方式
	// 由于 master.go 的 NewMasterConnection 内部创建连接，我们跳过直接测试
	// 而是测试其他方法
	t.Skip("NewMasterConnection requires real connection, skipping")
}

func TestMasterConnection_ReplOffset(t *testing.T) {
	// 直接创建 MasterConnection 结构体来测试导出的字段
	mc := &MasterConnection{
		Addr:       "127.0.0.1:6379",
		ReplOffset: 0,
		ReplId:     "",
	}
	// 测试字段可以直接设置
	mc.SetReplOffset(100)
	mc.SetReplId("test-repl-id")
	if mc.GetReplOffset() != 100 {
		t.Errorf("expected ReplOffset 100, got %d", mc.GetReplOffset())
	}
	if mc.GetReplId() != "test-repl-id" {
		t.Errorf("expected ReplId test-repl-id, got %s", mc.GetReplId())
	}
}

func TestMasterConnection_ReplId(t *testing.T) {
	mc := &MasterConnection{
		Addr:       "127.0.0.1:6379",
		ReplOffset: 0,
		ReplId:     "",
	}
	mc.SetReplId("abc123")
	if mc.GetReplId() != "abc123" {
		t.Errorf("expected ReplId abc123, got %s", mc.GetReplId())
	}
}

func TestMasterConnection_IsClosed(t *testing.T) {
	mc := &MasterConnection{
		Addr:       "127.0.0.1:6379",
		ReplOffset: 0,
		ReplId:     "",
		stopCh:     make(chan struct{}),
	}
	if mc.IsClosed() {
		t.Error("expected not closed initially")
	}
	// 关闭 stopCh 来模拟关闭
	close(mc.stopCh)
	if !mc.IsClosed() {
		t.Error("expected closed after stopCh closed")
	}
}
