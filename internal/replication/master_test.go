package replication

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

type errReadMock struct {
	err error
}

func (e *errReadMock) Read(b []byte) (int, error) { return 0, e.err }
func (e *errReadMock) Write(b []byte) (int, error) { return len(b), nil }
func (e *errReadMock) Close() error                { return nil }
func (e *errReadMock) LocalAddr() net.Addr         { return &net.TCPAddr{Port: 1} }
func (e *errReadMock) RemoteAddr() net.Addr        { return &net.TCPAddr{Port: 2} }
func (e *errReadMock) SetDeadline(t time.Time) error      { return nil }
func (e *errReadMock) SetReadDeadline(t time.Time) error  { return nil }
func (e *errReadMock) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = (*errReadMock)(nil)

// Helper to create a test server for NewMasterConnection test
func startTestServer(t *testing.T) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	addr := ln.Addr().String()

	// Accept one connection and close
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}()

	return addr, func() { ln.Close() }
}

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

func (m *mockMasterConn) LocalAddr() net.Addr                { return m.localAddr }
func (m *mockMasterConn) RemoteAddr() net.Addr               { return m.remoteAddr }
func (m *mockMasterConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockMasterConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockMasterConn) SetWriteDeadline(t time.Time) error { return nil }

// 确保实现了接口
var _ net.Conn = (*mockMasterConn)(nil)

func TestMasterConnection_New(t *testing.T) {
	t.Parallel()
	// Start a test server
	addr, cleanup := startTestServer(t)
	defer cleanup()

	// Test NewMasterConnection
	mc, err := NewMasterConnection(addr)
	if err != nil {
		t.Errorf("NewMasterConnection failed: %v", err)
	}
	if mc == nil {
		t.Error("expected non-nil MasterConnection")
	}
	if mc.Addr != addr {
		t.Errorf("expected addr %s, got %s", addr, mc.Addr)
	}
	// Cleanup
	mc.Close()
}

func TestMasterConnection_ReplOffset(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestMasterConnection_Close(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		stopCh: make(chan struct{}),
	}
	err := mc.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if !mock.closed {
		t.Error("expected mock connection to be closed")
	}
}

func TestMasterConnection_Close_Double(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		stopCh: make(chan struct{}),
	}
	// First close
	err := mc.Close()
	if err != nil {
		t.Errorf("First Close failed: %v", err)
	}
	// Second close should not panic
	err = mc.Close()
	if err != nil {
		t.Errorf("Second Close failed: %v", err)
	}
}

func TestMasterConnection_SendCommand(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	err := mc.SendCommand([][]byte{[]byte("PING")})
	if err != nil {
		t.Errorf("SendCommand failed: %v", err)
	}
	// Verify the command was written in RESP format
	expected := "*1\r\n$4\r\nPING\r\n"
	if string(mock.writeBuffer) != expected {
		t.Errorf("expected %q, got %q", expected, string(mock.writeBuffer))
	}
}

func TestMasterConnection_ReadResponse_SimpleString(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte("+PONG\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	resp, err := mc.ReadResponse()
	if err != nil {
		t.Errorf("ReadResponse failed: %v", err)
	}
	// SimpleString.String() returns "+PONG\r\n"
	if resp.String() != "+PONG\r\n" {
		t.Errorf("expected +PONG\\r\\n, got %s", resp.String())
	}
}

func TestMasterConnection_ReadResponse_Error(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte("-ERR test error\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	resp, err := mc.ReadResponse()
	if err != nil {
		t.Errorf("ReadResponse failed: %v", err)
	}
	_, ok := resp.(*proto.Error)
	if !ok {
		t.Error("expected error response")
	}
}

func TestMasterConnection_ReadResponse_Integer(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte(":1000\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	resp, err := mc.ReadResponse()
	if err != nil {
		t.Errorf("ReadResponse failed: %v", err)
	}
	// Integer response becomes SimpleString in our implementation
	// so it will be "+1000\r\n"
	if resp.String() != "+1000\r\n" {
		t.Errorf("expected +1000\\r\\n, got %s", resp.String())
	}
}

func TestMasterConnection_ReadBulkString(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte("$5\r\nhello\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	data, err := mc.ReadBulkString()
	if err != nil {
		t.Errorf("ReadBulkString failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected hello, got %s", string(data))
	}
}

func TestMasterConnection_ReadBulkString_Null(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte("$-1\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	data, err := mc.ReadBulkString()
	if err != nil {
		t.Errorf("ReadBulkString failed: %v", err)
	}
	if data != nil {
		t.Error("expected nil for null bulk string")
	}
}

func TestMasterConnection_ReadBulkString_EOF(t *testing.T) {
	t.Parallel()
	// Header says 10 bytes, but only 3 bytes available → io.ReadFull returns io.ErrUnexpectedEOF
	partialData := append([]byte("$10\r\n"), []byte("hel")...)
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   &net.TCPConn{}, // not used, Reader is set directly
		Reader: bufio.NewReader(bytes.NewReader(partialData)),
		Writer: bufio.NewWriter(&bytes.Buffer{}),
		stopCh: make(chan struct{}),
	}
	_, err := mc.ReadBulkString()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "failed") || strings.Contains(err.Error(), "unexpected EOF"))
}

func TestMasterConnection_readUntilEOF_WithHexMarker(t *testing.T) {
	t.Parallel()
	rdbData := []byte("REDIS0009some-rdb-data-content")
	eofMarker := "abcdef0123456789abcdef0123456789abcdef01"
	fullData := append(append([]byte{}, rdbData...), []byte(eofMarker)...)

	mc := &MasterConnection{
		Reader: bufio.NewReader(bytes.NewReader(fullData)),
		Writer: bufio.NewWriter(&bytes.Buffer{}),
		stopCh: make(chan struct{}),
	}

	result, err := mc.readUntilEOF()
	assert.NoError(t, err)
	assert.Equal(t, rdbData, result)
}

func TestMasterConnection_readUntilEOF_LargeRDB(t *testing.T) {
	t.Parallel()
	// Large RDB (> 4096 bytes) to exercise multiple bufio reads
	rdbData := make([]byte, 10000)
	for i := range rdbData {
		rdbData[i] = 'A' + byte(i%26)
	}
	eofMarker := "abcdef0123456789abcdef0123456789abcdef01"
	fullData := append([]byte{}, rdbData...)
	fullData = append(fullData, []byte(eofMarker)...)

	mc := &MasterConnection{
		Reader: bufio.NewReader(bytes.NewReader(fullData)),
		Writer: bufio.NewWriter(&bytes.Buffer{}),
		stopCh: make(chan struct{}),
	}

	result, err := mc.readUntilEOF()
	assert.NoError(t, err)
	assert.Equal(t, rdbData, result)
}

func TestMasterConnection_readUntilEOF_NoMarker(t *testing.T) {
	t.Parallel()
	data := []byte("small-data-without-hex-marker")
	mc := &MasterConnection{
		Reader: bufio.NewReader(bytes.NewReader(data)),
		Writer: bufio.NewWriter(&bytes.Buffer{}),
		stopCh: make(chan struct{}),
	}

	result, err := mc.readUntilEOF()
	assert.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestMasterConnection_readUntilEOF_ReadError(t *testing.T) {
	t.Parallel()
	errMock := &errReadMock{err: fmt.Errorf("connection reset")}
	mc := &MasterConnection{
		Reader: bufio.NewReader(errMock),
		Writer: bufio.NewWriter(&bytes.Buffer{}),
		stopCh: make(chan struct{}),
	}

	_, err := mc.readUntilEOF()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "read RDB data failed"))
}

func TestMasterConnection_ReadResponse_BulkString(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte("$5\r\nhello\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	resp, err := mc.ReadResponse()
	assert.NoError(t, err)
	bs, ok := resp.(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "hello", string(*bs))
}

func TestMasterConnection_SendCommand_MultipleArgs(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	err := mc.SendCommand([][]byte{[]byte("SET"), []byte("key"), []byte("value")})
	if err != nil {
		t.Errorf("SendCommand failed: %v", err)
	}
	// Verify the command was written in RESP format
	expected := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	if string(mock.writeBuffer) != expected {
		t.Errorf("expected %q, got %q", expected, string(mock.writeBuffer))
	}
}

func TestMasterConnection_ReadResponse_Array(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	// Array response: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
	mock.readBuffer = []byte("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	resp, err := mc.ReadResponse()
	assert.NoError(t, err)
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 2, len(arr.Args))
	assert.Equal(t, "foo", string(arr.Args[0]))
	assert.Equal(t, "bar", string(arr.Args[1]))
}

func TestMasterConnection_ReadResponse_NilArray(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte("*-1\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	// ReadResponse does not support nil arrays; returns an error
	_, err := mc.ReadResponse()
	assert.Error(t, err)
}

func TestMasterConnection_ReadResponse_EmptyLine(t *testing.T) {
	t.Parallel()
	mock := newMockMasterConn()
	mock.readBuffer = []byte("\r\n")
	mc := &MasterConnection{
		Addr:   "127.0.0.1:6379",
		Conn:   mock,
		Reader: bufio.NewReader(mock),
		Writer: bufio.NewWriter(mock),
		stopCh: make(chan struct{}),
	}
	resp, err := mc.ReadResponse()
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.True(t, strings.Contains(err.Error(), "empty response"))
}
