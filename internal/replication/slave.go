package replication

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// SlaveConnection 表示一个从节点连接
type SlaveConnection struct {
	ID            string
	Addr          string
	Conn          net.Conn
	Reader        *bufio.Reader
	Writer        *bufio.Writer
	ReplOffset    int64 // 从节点的复制偏移量
	ReplAckOffset int64 // 从节点确认的偏移量
	Ready         bool  // 是否准备好接收命令
	LastAckTime   int64 // 最后一次ACK时间
	mu            sync.RWMutex
	closeOnce     sync.Once
}

// NewSlaveConnection 创建新的从节点连接
func NewSlaveConnection(conn net.Conn) *SlaveConnection {
	addr := conn.RemoteAddr().String()
	slaveID := generateSlaveID(addr)
	return &SlaveConnection{
		ID:            slaveID,
		Addr:          addr,
		Conn:          conn,
		Reader:        bufio.NewReader(conn),
		Writer:        bufio.NewWriter(conn),
		ReplOffset:    0,
		ReplAckOffset: 0,
		Ready:         false,
		LastAckTime:   time.Now().Unix(),
	}
}

// generateSlaveID 生成从节点ID
func generateSlaveID(addr string) string {
	return fmt.Sprintf("slave-%s-%d", addr, time.Now().UnixNano())
}

// SetReady 设置就绪状态
func (sc *SlaveConnection) SetReady(ready bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.Ready = ready
}

// IsReady 检查是否就绪
func (sc *SlaveConnection) IsReady() bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.Ready
}

// SetReplOffset 设置复制偏移量
func (sc *SlaveConnection) SetReplOffset(offset int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.ReplOffset = offset
}

// GetReplOffset 获取复制偏移量
func (sc *SlaveConnection) GetReplOffset() int64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.ReplOffset
}

// UpdateReplAck 更新确认偏移量
func (sc *SlaveConnection) UpdateReplAck(offset int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.ReplAckOffset = offset
	sc.LastAckTime = time.Now().Unix()
}

// SendCommand 发送命令到从节点
func (sc *SlaveConnection) SendCommand(cmdBytes []byte, offset int64) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.Ready {
		return fmt.Errorf("slave not ready")
	}

	// 写入命令
	if _, err := sc.Writer.Write(cmdBytes); err != nil {
		return fmt.Errorf("write command failed: %w", err)
	}

	// 刷新缓冲区
	if err := sc.Writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	// 更新偏移量
	sc.ReplOffset = offset

	return nil
}

// SendRDB 发送RDB数据到从节点（RESP协议格式）
func (sc *SlaveConnection) SendRDB(rdbData []byte) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 发送RDB数据长度（使用RESP Bulk String格式）
	header := fmt.Sprintf("$%d\r\n", len(rdbData))
	if _, err := sc.Writer.WriteString(header); err != nil {
		return fmt.Errorf("write RDB header failed: %w", err)
	}

	// 发送RDB数据
	if _, err := sc.Writer.Write(rdbData); err != nil {
		return fmt.Errorf("write RDB data failed: %w", err)
	}

	// 发送\r\n结尾
	if _, err := sc.Writer.WriteString("\r\n"); err != nil {
		return fmt.Errorf("write RDB trailing CRLF failed: %w", err)
	}

	// 刷新缓冲区
	if err := sc.Writer.Flush(); err != nil {
		return fmt.Errorf("flush RDB failed: %w", err)
	}

	logger.Logger.Info().
		Str("slave_id", sc.ID).
		Int("rdb_size", len(rdbData)).
		Msg("发送RDB数据到从节点")

	return nil
}

// SendResponse 发送响应
func (sc *SlaveConnection) SendResponse(resp proto.RESP) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if err := proto.WriteRESP(sc.Writer, resp); err != nil {
		return fmt.Errorf("write response failed: %w", err)
	}

	if err := sc.Writer.Flush(); err != nil {
		return fmt.Errorf("flush response failed: %w", err)
	}

	return nil
}

// ReadCommand 从连接读取命令(用于REPLCONF ACK)
func (sc *SlaveConnection) ReadCommand() (*proto.Array, error) {
	sc.mu.RLock()
	reader := sc.Reader
	sc.mu.RUnlock()

	return proto.ReadRESP(reader)
}

// Close 关闭连接
func (sc *SlaveConnection) Close() error {
	var err error
	sc.closeOnce.Do(func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()

		if sc.Conn != nil {
			err = sc.Conn.Close()
		}
	})
	return err
}

// GetLastAckTime 获取最后ACK时间
func (sc *SlaveConnection) GetLastAckTime() int64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.LastAckTime
}

// Lock 锁定从节点连接（防止 PropagateCommand 并发写入）
func (sc *SlaveConnection) Lock() {
	sc.mu.Lock()
}

// Unlock 解锁从节点连接
func (sc *SlaveConnection) Unlock() {
	sc.mu.Unlock()
}

// WriteAndFlush 在锁保护下直接写入并刷新数据
func (sc *SlaveConnection) WriteAndFlush(data []byte) error {
	if _, err := sc.Writer.Write(data); err != nil {
		return err
	}
	return sc.Writer.Flush()
}
