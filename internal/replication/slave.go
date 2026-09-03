package replication

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
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
	ReplOffset    atomic.Int64  // 从节点的复制偏移量
	ReplAckOffset atomic.Int64  // 从节点确认的偏移量
	ReplAckTS     atomic.Uint64 // 从节点确认的主侧 ts 水位（S2 ACK-ts 双轨——applied 语义）
	Ready         atomic.Bool   // 是否准备好接收命令
	LastAckTime   int64         // 最后一次ACK时间
	mu            sync.RWMutex
	closeOnce     sync.Once
	writeMu       sync.Mutex // 写锁：SendCommand/SendBacklogData 互斥，与 Close 不冲突
}

// NewSlaveConnection 创建新的从节点连接
func NewSlaveConnection(conn net.Conn) *SlaveConnection {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(10 * time.Second)
	}
	addr := conn.RemoteAddr().String()
	slaveID := generateSlaveID(addr)
	sc := &SlaveConnection{
		ID:          slaveID,
		Addr:        addr,
		Conn:        conn,
		Reader:      bufio.NewReader(conn),
		Writer:      bufio.NewWriter(conn),
		LastAckTime: time.Now().Unix(),
	}
	sc.Ready.Store(false)
	sc.ReplOffset.Store(0)
	return sc
}

// generateSlaveID 生成从节点ID
func generateSlaveID(addr string) string {
	return fmt.Sprintf("slave-%s-%d", addr, time.Now().UnixNano())
}

// SetReady 设置就绪状态
func (sc *SlaveConnection) SetReady(ready bool) {
	sc.Ready.Store(ready)
}

// IsReady 检查是否就绪
func (sc *SlaveConnection) IsReady() bool {
	return sc.Ready.Load()
}

// SetReplOffset 设置复制偏移量
func (sc *SlaveConnection) SetReplOffset(offset int64) {
	sc.ReplOffset.Store(offset)
}

// GetReplOffset 获取复制偏移量
func (sc *SlaveConnection) GetReplOffset() int64 {
	return sc.ReplOffset.Load()
}

// UpdateReplAck 更新确认偏移量
func (sc *SlaveConnection) UpdateReplAck(offset int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.ReplAckOffset.Store(offset)
	sc.LastAckTime = time.Now().Unix()
}

// UpdateReplAckTS 更新从节点确认的主侧 ts 水位（S2 ACK-ts 双轨——从侧 lastAppliedTS
// ——applied 语义——排水判据 D2 的数据源）。
func (sc *SlaveConnection) UpdateReplAckTS(ts uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.ReplAckTS.Store(ts)
	sc.LastAckTime = time.Now().Unix()
}

// GetReplAckOffset 获取确认偏移量
func (sc *SlaveConnection) GetReplAckOffset() int64 {
	return sc.ReplAckOffset.Load()
}

// SendCommand 发送命令到从节点
// 使用 writeMu 而非 sc.mu，避免与 Close() 的锁链死锁。
func (sc *SlaveConnection) SendCommand(cmdBytes []byte, offset int64) error {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()

	if !sc.Ready.Load() {
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
	sc.ReplOffset.Store(offset)

	return nil
}

// SendRDB 发送RDB数据到从节点（RESP协议格式）
func (sc *SlaveConnection) SendRDB(rdbData []byte) error {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()

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
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()

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
	defer sc.mu.RUnlock()
	return proto.ReadRESP(sc.Reader)
}

// Close 关闭连接。
// 先关闭底层 TCP 连接（unblock 正在 writeMu 下执行 I/O 的 goroutine），
// 再获取 writeMu 确保所有写操作已完成。
// 这种顺序（close → drain writeMu）与 handlePSyncWithRDB / SendBacklogData
// 中先持有 writeMu 再做 I/O 的顺序不冲突，彻底消除死锁。
func (sc *SlaveConnection) Close() error {
	var err error
	sc.closeOnce.Do(func() {
		// 步骤 1：先关闭连接，unblock 任何正在进行的 I/O
		if sc.Conn != nil {
			err = sc.Conn.Close()
		}
		// 步骤 2：等待所有写 goroutine 退出（它们会在 I/O 失败后释放 writeMu）
		// Lock/unlock acts as a memory barrier: any goroutine holding writeMu
		// will release it and exit once the conn above is closed, and this
		// acquisition ensures we don't return before they've finished.
		sc.writeMu.Lock()
		//nolint:staticcheck // SA2001: empty critical section intentional — memory barrier
		sc.writeMu.Unlock()
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
	sc.writeMu.Lock()
}

// Unlock 解锁从节点连接
func (sc *SlaveConnection) Unlock() {
	sc.writeMu.Unlock()
}

// WriteAndFlush 在 writeMu 保护下直接写入并刷新数据
func (sc *SlaveConnection) WriteAndFlush(data []byte) error {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	if _, err := sc.Writer.Write(data); err != nil {
		return err
	}
	return sc.Writer.Flush()
}
