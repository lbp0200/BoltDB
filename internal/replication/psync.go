package replication

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// PSyncResult PSYNC结果
type PSyncResult struct {
	FullResync bool   // 是否全量同步
	ReplId     string // 复制ID
	Offset     int64  // 复制偏移量
}

// HandlePSync 处理PSYNC命令（主节点端）
func HandlePSync(rm *ReplicationManager, replId string, offset int64) (*PSyncResult, error) {
	rm.mu.RLock()
	currentReplId := rm.replId
	currentOffset := rm.masterReplOffset
	backlog := rm.backlog
	rm.mu.RUnlock()

	// 检查是否可以增量同步
	if replId == currentReplId && offset > 0 {
		// 检查backlog中是否有足够的数据
		backlogStart := backlog.GetCurrentOffset() - backlog.GetSize()
		if backlogStart < 0 {
			backlogStart = 0
		}

		if offset >= backlogStart && offset < currentOffset {
			// 可以增量同步
			logger.Logger.Info().
				Str("repl_id", replId).
				Int64("offset", offset).
				Msg("执行增量同步")
			return &PSyncResult{
				FullResync: false,
				ReplId:     currentReplId,
				Offset:     offset,
			}, nil
		}
	}

	// 需要全量同步
	logger.Logger.Info().
		Str("requested_repl_id", replId).
		Str("current_repl_id", currentReplId).
		Int64("requested_offset", offset).
		Int64("current_offset", currentOffset).
		Msg("执行全量同步")
	return &PSyncResult{
		FullResync: true,
		ReplId:     currentReplId,
		Offset:     currentOffset,
	}, nil
}

// SendFullResync 发送全量同步响应
func SendFullResync(slave *SlaveConnection, replId string, offset int64) error {
	// 发送 +FULLRESYNC <replid> <offset>
	response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", replId, offset)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send FULLRESYNC response failed: %w", err)
	}
	return nil
}

// SendContinueResync 发送增量同步响应
func SendContinueResync(slave *SlaveConnection, replId string, offset int64) error {
	// 发送 +CONTINUE <replid>
	response := fmt.Sprintf("+CONTINUE %s\r\n", replId)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send CONTINUE response failed: %w", err)
	}
	return nil
}

// SendBacklogData 发送backlog数据到从节点
func SendBacklogData(slave *SlaveConnection, backlog *ReplicationBacklog, startOffset, endOffset int64) error {
	data, err := backlog.GetRange(startOffset, endOffset)
	if err != nil {
		return fmt.Errorf("get backlog range failed: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	// 发送数据
	if _, err := slave.Writer.Write(data); err != nil {
		return fmt.Errorf("write backlog data failed: %w", err)
	}

	if err := slave.Writer.Flush(); err != nil {
		return fmt.Errorf("flush backlog data failed: %w", err)
	}

	logger.Logger.Debug().
		Str("slave_id", slave.ID).
		Int64("start_offset", startOffset).
		Int64("end_offset", endOffset).
		Int("data_size", len(data)).
		Msg("发送backlog数据到从节点")

	return nil
}

// StartSlaveReplication 启动从节点复制（从节点端）
func StartSlaveReplication(rm *ReplicationManager, store *store.BotreonStore, masterAddr string) error {
	rm.mu.Lock()
	rm.role = RoleSlave
	rm.masterAddr = masterAddr
	rm.mu.Unlock()

	// 连接到主节点
	masterConn, err := NewMasterConnection(masterAddr)
	if err != nil {
		return fmt.Errorf("connect to master failed: %w", err)
	}

	rm.SetMasterConnection(masterConn)

	// 启动复制goroutine
	go func() {
		defer func() {
			if err := masterConn.Close(); err != nil {
				logger.Logger.Debug().Err(err).Msg("failed to close master connection")
			}
		}()

		// 发送PING
		if err := masterConn.SendCommand([][]byte{[]byte("PING")}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送PING到主节点失败")
			return
		}

		resp, err := masterConn.ReadResponse()
		if err != nil {
			logger.Logger.Error().Err(err).Msg("读取PING响应失败")
			return
		}

		if _, ok := resp.(*proto.SimpleString); !ok || resp.String() != "+PONG\r\n" {
			logger.Logger.Error().Msg("主节点PING响应异常")
			return
		}

		// 发送REPLCONF listening-port (Redis 要求必须有此命令)
		if err := masterConn.SendCommand([][]byte{
			[]byte("REPLCONF"),
			[]byte("listening-port"),
			[]byte("6380"),
		}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送REPLCONF listening-port失败")
			return
		}
		_, _ = masterConn.ReadResponse()

		// 发送REPLCONF capa eof
		if err := masterConn.SendCommand([][]byte{
			[]byte("REPLCONF"),
			[]byte("capa"),
			[]byte("eof"),
		}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送REPLCONF capa eof失败")
			return
		}
		_, _ = masterConn.ReadResponse()

		// 发送REPLCONF capa psync2
		if err := masterConn.SendCommand([][]byte{
			[]byte("REPLCONF"),
			[]byte("capa"),
			[]byte("psync2"),
		}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送REPLCONF capa psync2失败")
			return
		}
		_, _ = masterConn.ReadResponse()

		// 发送PSYNC
		if err := masterConn.SendCommand([][]byte{
			[]byte("PSYNC"),
			[]byte("?"),
			[]byte("-1"),
		}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送PSYNC失败")
			return
		}

		// 读取PSYNC响应
		resp, err = masterConn.ReadResponse()
		if err != nil {
			logger.Logger.Error().Err(err).Msg("读取PSYNC响应失败")
			return
		}

		respStr := resp.String()
		logger.Logger.Info().Str("psync_response", respStr).Msg("收到PSYNC响应")

		// 解析响应
		if strings.HasPrefix(respStr, "+FULLRESYNC") {
			// 全量同步
			// 解析replid和offset
			parts := strings.Fields(respStr)
			if len(parts) >= 3 {
				newReplId := parts[1]
				offset, _ := strconv.ParseInt(parts[2], 10, 64)
				rm.mu.Lock()
				rm.replId = newReplId
				rm.masterReplOffset = offset
				rm.mu.Unlock()
			}

			// 读取RDB数据
			rdbData, err := masterConn.ReadBulkString()
			if err != nil {
				logger.Logger.Error().Err(err).Msg("读取RDB数据失败")
				return
			}

			logger.Logger.Info().
				Int("rdb_size", len(rdbData)).
				Msg("收到RDB数据，开始加载")

			// 加载RDB数据
			if err := rm.LoadRDB(rdbData); err != nil {
				logger.Logger.Error().Err(err).Msg("加载RDB数据失败")
			}

			logger.Logger.Info().Msg("RDB数据加载完成，开始接收命令流")

		} else if strings.HasPrefix(respStr, "+CONTINUE") {
			// 增量同步
			parts := strings.Fields(respStr)
			if len(parts) >= 2 {
				newReplId := parts[1]
				rm.mu.Lock()
				rm.replId = newReplId
				rm.mu.Unlock()
			}

			// 开始接收命令流
			logger.Logger.Info().Msg("开始增量同步，接收命令流")
		}

		for {
			req, err := proto.ReadRESP(masterConn.Reader)
			if err != nil {
				logger.Logger.Warn().Err(err).Msg("从主节点读取命令失败")
				break
			}

			logger.Logger.Debug().
				Int("arg_count", len(req.Args)).
				Str("cmd", string(req.Args[0])).
				Msg("从主节点收到命令")

			if err := executeReplicatedCommand(store, req.Args); err != nil {
				logger.Logger.Error().Err(err).
					Str("cmd", string(req.Args[0])).
					Msg("执行复制命令失败")
				break
			}
			cmdBytes := serializeCommand(req.Args)
			rm.IncrementReplOffset(int64(len(cmdBytes)))
		}

		logger.Logger.Info().Msg("从节点复制连接关闭")
	}()

	return nil
}

// StopSlaveReplication 停止从节点复制
func StopSlaveReplication(rm *ReplicationManager) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.role = RoleMaster
	rm.masterAddr = ""

	if rm.masterConn != nil {
		if err := rm.masterConn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close master connection")
		}
		rm.masterConn = nil
	}
}

func executeReplicatedCommand(s *store.BotreonStore, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}

	cmd := strings.ToUpper(string(args[0]))

	switch cmd {
	case "SET":
		if len(args) >= 3 {
			key := string(args[1])
			value := string(args[2])
			if len(args) > 3 {
				opt := strings.ToUpper(string(args[3]))
				if opt == "EX" && len(args) >= 5 {
					if sec, err := strconv.ParseInt(string(args[4]), 10, 64); err == nil {
						return s.SetWithTTL(key, value, time.Duration(sec)*time.Second)
					}
				} else if opt == "PX" && len(args) >= 5 {
					if ms, err := strconv.ParseInt(string(args[4]), 10, 64); err == nil {
						return s.SetWithTTL(key, value, time.Duration(ms)*time.Millisecond)
					}
				}
			}
			return s.Set(key, value)
		}

	case "DEL":
		if len(args) >= 2 {
			for i := 1; i < len(args); i++ {
				if _, err := s.Del(string(args[i])); err != nil {
					return fmt.Errorf("DEL %s: %w", string(args[i]), err)
				}
			}
			return nil
		}

	case "INCR", "INCRBY":
		if len(args) >= 2 {
			key := string(args[1])
			var delta int64 = 1
			if cmd == "INCRBY" && len(args) >= 3 {
				if d, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
					delta = d
				}
			}
			_, err := s.INCRBY(key, delta)
			return err
		}

	case "DECR", "DECRBY":
		if len(args) >= 2 {
			key := string(args[1])
			var delta int64 = 1
			if cmd == "DECRBY" && len(args) >= 3 {
				if d, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
					delta = d
				}
			}
			_, err := s.DECRBY(key, delta)
			return err
		}

	case "APPEND":
		if len(args) >= 3 {
			key := string(args[1])
			value := string(args[2])
			_, err := s.APPEND(key, value)
			return err
		}

	case "RPUSH":
		if len(args) >= 3 {
			key := string(args[1])
			for i := 2; i < len(args); i++ {
				if _, err := s.RPush(key, string(args[i])); err != nil {
					return fmt.Errorf("RPUSH %s: %w", key, err)
				}
			}
			return nil
		}

	case "LPUSH":
		if len(args) >= 3 {
			key := string(args[1])
			for i := 2; i < len(args); i++ {
				if _, err := s.LPush(key, string(args[i])); err != nil {
					return fmt.Errorf("LPUSH %s: %w", key, err)
				}
			}
			return nil
		}

	case "LSET":
		if len(args) >= 4 {
			key := string(args[1])
			if index, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				return s.LSet(key, index, string(args[3]))
			}
			return nil
		}

	case "LREM":
		if len(args) >= 4 {
			key := string(args[1])
			if count, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				_, err := s.LRem(key, count, string(args[3]))
				return err
			}
			return nil
		}

	case "LTRIM":
		if len(args) >= 4 {
			key := string(args[1])
			if start, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				if stop, err := strconv.ParseInt(string(args[3]), 10, 64); err == nil {
					return s.LTrim(key, start, stop)
				}
			}
			return nil
		}

	case "SADD":
		if len(args) >= 3 {
			key := string(args[1])
			for i := 2; i < len(args); i++ {
				if _, err := s.SAdd(key, string(args[i])); err != nil {
					return fmt.Errorf("SADD %s: %w", key, err)
				}
			}
			return nil
		}

	case "SREM":
		if len(args) >= 3 {
			key := string(args[1])
			for i := 2; i < len(args); i++ {
				if _, err := s.SRem(key, string(args[i])); err != nil {
					return fmt.Errorf("SREM %s: %w", key, err)
				}
			}
			return nil
		}

	case "SMOVE":
		if len(args) >= 4 {
			src := string(args[1])
			dst := string(args[2])
			member := string(args[3])
			_, err := s.SMove(src, dst, member)
			return err
		}

	case "SPOP":
		if len(args) >= 2 {
			key := string(args[1])
			_, err := s.SPop(key)
			return err
		}

	case "HSET":
		if len(args) >= 4 {
			key := string(args[1])
			field := string(args[2])
			value := string(args[3])
			return s.HSet(key, field, value)
		}

	case "HMSET":
		if len(args) >= 4 {
			key := string(args[1])
			for i := 2; i+1 < len(args); i += 2 {
				if err := s.HSet(key, string(args[i]), string(args[i+1])); err != nil {
					return fmt.Errorf("HMSET %s: %w", key, err)
				}
			}
			return nil
		}

	case "HINCRBY":
		if len(args) >= 4 {
			key := string(args[1])
			field := string(args[2])
			if delta, err := strconv.ParseInt(string(args[3]), 10, 64); err == nil {
				_, err := s.HIncrBy(key, field, delta)
				return err
			}
			return nil
		}

	case "HINCRBYFLOAT":
		if len(args) >= 4 {
			key := string(args[1])
			field := string(args[2])
			if delta, err := strconv.ParseFloat(string(args[3]), 64); err == nil {
				_, err := s.HIncrByFloat(key, field, delta)
				return err
			}
			return nil
		}

	case "HDEL":
		if len(args) >= 3 {
			key := string(args[1])
			fields := make([]string, 0, len(args)-2)
			for i := 2; i < len(args); i++ {
				fields = append(fields, string(args[i]))
			}
			_, err := s.HDel(key, fields...)
			return err
		}

	case "ZADD":
		if len(args) >= 4 {
			key := string(args[1])
			members := make([]store.ZSetMember, 0, (len(args)-2)/2)
			for i := 2; i+1 < len(args); i += 2 {
				if score, err := strconv.ParseFloat(string(args[i]), 64); err == nil {
					members = append(members, store.ZSetMember{
						Member: string(args[i+1]),
						Score:  score,
					})
				}
			}
			if len(members) > 0 {
				return s.ZAdd(key, members)
			}
			return nil
		}

	case "ZINCRBY":
		if len(args) >= 4 {
			key := string(args[1])
			if delta, err := strconv.ParseFloat(string(args[2]), 64); err == nil {
				member := string(args[3])
				_, err := s.ZIncrBy(key, member, delta)
				return err
			}
			return nil
		}

	case "ZREM":
		if len(args) >= 3 {
			key := string(args[1])
			for i := 2; i < len(args); i++ {
				if _, err := s.ZRem(key, string(args[i])); err != nil {
					return fmt.Errorf("ZREM %s: %w", key, err)
				}
			}
			return nil
		}

	case "ZPOPMAX", "ZPOPMIN":
		logger.Logger.Debug().Str("cmd", cmd).Msg("忽略不支持的复制命令")
		return nil

	default:
		logger.Logger.Debug().Str("cmd", cmd).Msg("收到未处理的复制命令")
		return nil
	}
	return nil
}
