package sentinel

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// SentinelConnection 哨兵连接
type SentinelConnection struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// NewSentinelConnection 创建新的哨兵连接
func NewSentinelConnection(addr string) (*SentinelConnection, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to %s failed: %w", addr, err)
	}

	return &SentinelConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

// Close 关闭连接
func (sc *SentinelConnection) Close() error {
	return sc.conn.Close()
}

// SendCommand 发送命令
func (sc *SentinelConnection) SendCommand(cmd string) error {
	_, err := sc.writer.WriteString(cmd)
	if err != nil {
		return err
	}
	return sc.writer.Flush()
}

// ReadResponse 读取响应
func (sc *SentinelConnection) ReadResponse() (string, error) {
	line, err := sc.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

// SendSlaveOfNoOne 发送 SLAVEOF NO ONE 命令将节点提升为主节点
func SendSlaveOfNoOne(addr string) error {
	sc, err := NewSentinelConnection(addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := sc.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close sentinel connection")
		}
	}()

	// 发送 SLAVEOF NO ONE (SLAVEOF = 7 字节)
	cmd := "*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n"
	if err := sc.SendCommand(cmd); err != nil {
		return fmt.Errorf("send SLAVEOF NO ONE failed: %w", err)
	}

	// 读取响应
	resp, err := sc.ReadResponse()
	if err != nil {
		return fmt.Errorf("read SLAVEOF NO ONE response failed: %w", err)
	}

	if !strings.HasPrefix(resp, "+OK") && resp != "+OK" {
		return fmt.Errorf("SLAVEOF NO ONE failed: %s", resp)
	}

	logger.Logger.Info().Str("addr", addr).Msg("Successfully sent SLAVEOF NO ONE")
	return nil
}

// SendReplicaOf 发送 REPLICAOF 命令配置从节点复制新主节点
func SendReplicaOf(slaveAddr, masterAddr string) error {
	sc, err := NewSentinelConnection(slaveAddr)
	if err != nil {
		return err
	}
	defer func() {
		if err := sc.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close sentinel connection")
		}
	}()

	// 解析master地址
	masterHost, masterPort, err := net.SplitHostPort(masterAddr)
	if err != nil {
		return fmt.Errorf("invalid master address %s: %w", masterAddr, err)
	}

	// 发送 REPLICAOF <host> <port> (REPLICAOF = 9 字节)
	cmd := fmt.Sprintf("*4\r\n$9\r\nREPLICAOF\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(masterHost), masterHost, len(masterPort), masterPort)

	if err := sc.SendCommand(cmd); err != nil {
		return fmt.Errorf("send REPLICAOF failed: %w", err)
	}

	// 读取响应
	resp, err := sc.ReadResponse()
	if err != nil {
		return fmt.Errorf("read REPLICAOF response failed: %w", err)
	}

	if !strings.HasPrefix(resp, "+OK") && resp != "+OK" {
		return fmt.Errorf("REPLICAOF failed: %s", resp)
	}

	logger.Logger.Info().
		Str("slave", slaveAddr).
		Str("master", masterAddr).
		Msg("Successfully sent REPLICAOF")
	return nil
}

// SendPing 发送PING命令检查节点是否存活
func SendPing(addr string) (bool, error) {
	sc, err := NewSentinelConnection(addr)
	if err != nil {
		return false, err
	}
	defer func() {
		if err := sc.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close sentinel connection")
		}
	}()

	// 发送 PING
	cmd := "*1\r\n$4\r\nPING\r\n"
	if err := sc.SendCommand(cmd); err != nil {
		return false, err
	}

	// 读取响应
	resp, err := sc.ReadResponse()
	if err != nil {
		return false, err
	}

	return strings.HasPrefix(resp, "+PONG") || resp == "+PONG", nil
}

// SendInfoReplication 发送 INFO replication 命令获取复制信息
func SendInfoReplication(addr string) (string, error) {
	sc, err := NewSentinelConnection(addr)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := sc.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close sentinel connection")
		}
	}()

	// 发送 INFO replication
	cmd := "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n"
	if err := sc.SendCommand(cmd); err != nil {
		return "", err
	}

	// 读取响应（多行）
	var resp strings.Builder
	for {
		line, err := sc.reader.ReadString('\n')
		if err != nil {
			break
		}
		resp.WriteString(line)
		if line == "\r\n" || line == "\n" {
			break
		}
	}

	return resp.String(), nil
}

// GetRole 获取节点角色
func GetRole(addr string) (string, error) {
	sc, err := NewSentinelConnection(addr)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := sc.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close sentinel connection")
		}
	}()

	// 发送 ROLE
	cmd := "*1\r\n$4\r\nROLE\r\n"
	if err := sc.SendCommand(cmd); err != nil {
		return "", err
	}

	// 读取响应
	resp, err := sc.ReadResponse()
	if err != nil {
		return "", err
	}

	return resp, nil
}
