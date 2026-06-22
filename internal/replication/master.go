package replication

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// maxRDBBufferSize 是 readUntilEOF 读取 RDB 数据的最大缓冲区大小（256 MB）。
// 超过此上限时断开连接，防止恶意/异常大 RDB 导致 OOM。
const maxRDBBufferSize = 256 * 1024 * 1024

// MasterConnection 表示到主节点的连接
type MasterConnection struct {
	Addr       string
	Conn       net.Conn
	Reader     *bufio.Reader
	Writer     *bufio.Writer
	ReplOffset int64
	ReplId     string
	mu         sync.RWMutex
	stopCh     chan struct{}
	closeOnce  sync.Once
}

// NewMasterConnection 创建新的主节点连接
func NewMasterConnection(addr string) (*MasterConnection, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial master failed: %w", err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(10 * time.Second)
	}

	mc := &MasterConnection{
		Addr:   addr,
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		Writer: bufio.NewWriter(conn),
		stopCh: make(chan struct{}),
	}

	logger.Logger.Info().
		Str("master_addr", addr).
		Msg("连接到主节点")

	logger.Logger.Debug().Msg("MasterConnection created, about to send commands")
	return mc, nil
}

// SendCommand 发送命令到主节点
func (mc *MasterConnection) SendCommand(cmd [][]byte) error {
	logger.Logger.Debug().Str("addr", mc.Addr).Msg("SendCommand called")
	mc.mu.Lock()
	defer mc.mu.Unlock()

	req := &proto.Array{Args: cmd}
	if err := proto.WriteRESP(mc.Writer, req); err != nil {
		return fmt.Errorf("write command failed: %w", err)
	}

	if err := mc.Writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	return nil
}

// ReadResponse 读取响应
func (mc *MasterConnection) ReadResponse() (proto.RESP, error) {
	logger.Logger.Debug().Str("addr", mc.Addr).Msg("ReadResponse called")
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	reader := mc.Reader

	// 读取响应类型
	line, err := reader.ReadBytes('\n')
	if err != nil {
		logger.Logger.Error().Err(err).Str("addr", mc.Addr).Msg("Master connection read failed")
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	logger.Logger.Debug().Str("addr", mc.Addr).Str("line", string(line)).Msg("Read response line")

	// 去掉\r\n
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) == 0 {
		logger.Logger.Error().Msg("Master connection read empty line - connection may be closed")
		return nil, fmt.Errorf("empty response")
	}

	// 根据响应类型解析
	switch line[0] {
	case '+': // Simple String
		return proto.NewSimpleString(string(line[1:])), nil
	case '-': // Error
		return proto.NewError(string(line[1:])), nil
	case ':': // Integer
		// 简化处理，实际应该解析整数
		return proto.NewSimpleString(string(line[1:])), nil
	case '$': // Bulk String
		// 需要读取长度和数据
		return proto.ReadRESP(reader)
	case '*': // Array
		return proto.ReadRESP(reader)
	default:
		return proto.NewSimpleString(string(line)), nil
	}
}

// ReadBulkString 读取Bulk String(用于RDB传输)
func (mc *MasterConnection) ReadBulkString() ([]byte, error) {
	mc.mu.RLock()
	reader := mc.Reader
	mc.mu.RUnlock()

	// 读取长度行
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read bulk string length failed: %w", err)
	}

	// 去掉\r\n
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	if len(line) == 0 || line[0] != '$' {
		return nil, fmt.Errorf("invalid bulk string format")
	}

	// 检查是否是 EOF-aware 格式 (Redis 8+): $EOF:<checksum>
	if len(line) > 5 && string(line[1:5]) == "EOF:" {
		logger.Logger.Info().Msg("Detected Redis 8+ EOF-aware replication format")
		return mc.readUntilEOF()
	}

	// 解析长度
	var length int
	if _, err := fmt.Sscanf(string(line[1:]), "%d", &length); err != nil {
		return nil, fmt.Errorf("parse bulk string length failed: %w", err)
	}

	if length < 0 {
		return nil, nil // NULL bulk string
	}

	if length > proto.MaxBulkLen {
		return nil, fmt.Errorf("bulk string length too large: %d", length)
	}

	// 读取数据
	data := make([]byte, length+2) // +2 for \r\n
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, fmt.Errorf("read bulk string data failed: %w", err)
	}

	return data[:length], nil
}

// readUntilEOF 读取 EOF-aware 格式的 RDB 数据 (Redis 8+)
func (mc *MasterConnection) readUntilEOF() ([]byte, error) {
	mc.mu.RLock()
	reader := mc.Reader
	mc.mu.RUnlock()

	// EOF 标记长度（40 字节 hex MD5）
	eofMarkLen := 40

	// 使用 buffer 累积所有数据
	var buffer bytes.Buffer
	buf := make([]byte, 8192)

	for {
		n, err := reader.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read RDB data failed: %w", err)
		}

		if n > 0 {
			buffer.Write(buf[:n])
		}

		data := buffer.Bytes()
		dataLen := len(data)

		// 在数据中查找 EOF 标记（40 字节十六进制字符串）
		// EOF 标记应该在数据末尾
		if dataLen >= eofMarkLen {
			// 检查最后 40 个字节是否是有效的十六进制
			isHex := true
			for j := 0; j < eofMarkLen; j++ {
				c := data[dataLen-eofMarkLen+j]
				if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
					isHex = false
					break
				}
			}

			if isHex {
				// 找到 EOF 标记
				// RDB 数据是 EOF 标记之前的所有内容
				rdbData := data[:dataLen-eofMarkLen]

				// 跳过 EOF 标记后的 \r\n
				cmdStart := dataLen - eofMarkLen
				for cmdStart < len(data) && (data[cmdStart] == '\r' || data[cmdStart] == '\n') {
					cmdStart++
				}

				// 剩余数据是后续命令
				cmdsData := data[cmdStart:]

				// 重置 Reader
				if len(cmdsData) > 0 {
					mc.mu.Lock()
					mc.Reader = bufio.NewReader(bytes.NewReader(cmdsData))
					mc.mu.Unlock()
				}

				return rdbData, nil
			}
		}

		if errors.Is(err, io.EOF) {
			// 连接关闭，返回所有数据
			return data, nil
		}

		// 如果 buffer 太大，发出警告
		if buffer.Len() > 10*1024*1024 {
			logger.Logger.Warn().Int("buffer_size", buffer.Len()).Msg("Buffer growing large in readUntilEOF")
		}

		// 硬上限：超过 256MB 时报错断开，防止 OOM
		if buffer.Len() > maxRDBBufferSize {
			return nil, fmt.Errorf("RDB data too large: %d bytes (max %d)", buffer.Len(), maxRDBBufferSize)
		}
	}
}

// SetReplOffset 设置复制偏移量
func (mc *MasterConnection) SetReplOffset(offset int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.ReplOffset = offset
}

// GetReplOffset 获取复制偏移量
func (mc *MasterConnection) GetReplOffset() int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.ReplOffset
}

// SetReplId 设置复制ID
func (mc *MasterConnection) SetReplId(replId string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.ReplId = replId
}

// GetReplId 获取复制ID
func (mc *MasterConnection) GetReplId() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.ReplId
}

// Close 关闭连接
func (mc *MasterConnection) Close() error {
	mc.closeOnce.Do(func() {
		close(mc.stopCh)
	})

	// Close the connection first to unblock any readers holding RLock.
	var err error
	mc.mu.RLock()
	conn := mc.Conn
	mc.mu.RUnlock()
	if conn != nil {
		err = conn.Close()
	}

	// Memory barrier: ensure no goroutine is still using the connection.
	mc.mu.Lock()
	//nolint:staticcheck // SA2001: empty critical section intentional — memory barrier
	mc.mu.Unlock()
	return err
}

// IsClosed 检查连接是否已关闭
func (mc *MasterConnection) IsClosed() bool {
	select {
	case <-mc.stopCh:
		return true
	default:
		return false
	}
}
