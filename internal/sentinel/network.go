package sentinel

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// sentinelPassword is the AUTH password read from BOLTDB_PASSWORD env var.
// When set, the sentinel sends AUTH after establishing every connection.
var sentinelPassword string

func init() {
	sentinelPassword = os.Getenv("BOLTDB_PASSWORD")
}

// SetSentinelPassword overrides the global sentinel AUTH password.
// Used by cmd/sentinel when --password flag is provided.
func SetSentinelPassword(password string) {
	sentinelPassword = password
}

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

	sc := &SentinelConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}

	// If a password is configured, authenticate immediately.
	if sentinelPassword != "" {
		if err := sc.authenticate(); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("auth to %s failed: %w", addr, err)
		}
	}

	return sc, nil
}

// authenticate sends AUTH <password> and checks the response.
func (sc *SentinelConnection) authenticate() error {
	authCmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n",
		len(sentinelPassword), sentinelPassword)
	if err := sc.SendCommand(authCmd); err != nil {
		return fmt.Errorf("send AUTH failed: %w", err)
	}
	resp, err := sc.ReadResponse()
	if err != nil {
		return fmt.Errorf("read AUTH response failed: %w", err)
	}
	if resp != "+OK" {
		return fmt.Errorf("AUTH failed: %s", resp)
	}
	return nil
}

// Ping sends PING and verifies +PONG.
func (sc *SentinelConnection) Ping() error {
	if err := sc.SendCommand("*1\r\n$4\r\nPING\r\n"); err != nil {
		return fmt.Errorf("send PING failed: %w", err)
	}
	resp, err := sc.ReadResponse()
	if err != nil {
		return fmt.Errorf("read PING response failed: %w", err)
	}
	if resp != "+PONG" {
		return fmt.Errorf("unexpected PING response: %s", resp)
	}
	return nil
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
	if err := sc.conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		logger.Logger.Debug().Err(err).Msg("Failed to set read deadline")
	}
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

// pingCheck connects to addr, optionally sends AUTH if password is non-empty, sends PING,
// and returns nil on +PONG. Used by checkMaster for proper health checks.
func pingCheck(addr, password string) error {
	sc, err := NewSentinelConnection(addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := sc.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("Failed to close pingCheck connection")
		}
	}()

	// If a per-master password is provided, send AUTH (overrides global sentinelPassword)
	if password != "" {
		authCmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(password), password)
		if err := sc.SendCommand(authCmd); err != nil {
			return fmt.Errorf("send AUTH failed: %w", err)
		}
		resp, err := sc.ReadResponse()
		if err != nil {
			return fmt.Errorf("read AUTH response failed: %w", err)
		}
		if resp != "+OK" {
			return fmt.Errorf("AUTH failed: %s", resp)
		}
	}

	return sc.Ping()
}
