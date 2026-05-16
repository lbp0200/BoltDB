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

// StartSlaveReplication 启动从节点复制（从节点端），包含自动重连
func StartSlaveReplication(rm *ReplicationManager, storeObj *store.BotreonStore, masterAddr string) error {
	rm.mu.Lock()
	if rm.slaveReconnector != nil {
		rm.slaveReconnector.Stop()
		rm.slaveReconnector = nil
	}
	rm.role = RoleSlave
	rm.masterAddr = masterAddr
	rm.mu.Unlock()

	reconnector := NewSlaveReconnector(rm, storeObj, masterAddr)
	rm.mu.Lock()
	rm.slaveReconnector = reconnector
	rm.mu.Unlock()

	reconnector.Start()
	return nil
}

// StopSlaveReplication 停止从节点复制
func StopSlaveReplication(rm *ReplicationManager) {
	rm.mu.Lock()
	reconnector := rm.slaveReconnector
	rm.slaveReconnector = nil
	rm.role = RoleMaster
	rm.masterAddr = ""

	if rm.masterConn != nil {
		if err := rm.masterConn.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close master connection")
		}
		rm.masterConn = nil
	}
	rm.mu.Unlock()

	if reconnector != nil {
		reconnector.Stop()
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

	case "SETEX":
		if len(args) >= 4 {
			key := string(args[1])
			seconds, _ := strconv.ParseInt(string(args[2]), 10, 64)
			value := string(args[3])
			return s.SetEX(key, value, int(seconds))
		}

	case "PSETEX":
		if len(args) >= 4 {
			key := string(args[1])
			millis, _ := strconv.ParseInt(string(args[2]), 10, 64)
			value := string(args[3])
			return s.PSETEX(key, value, millis)
		}

	case "SETNX":
		if len(args) >= 3 {
			key := string(args[1])
			value := string(args[2])
			_, err := s.SetNX(key, value)
			return err
		}

	case "GETSET":
		if len(args) >= 3 {
			key := string(args[1])
			value := string(args[2])
			_, err := s.GetSet(key, value)
			return err
		}

	case "MSET", "MSETNX":
		if len(args) >= 3 && (len(args)-1)%2 == 0 {
			for i := 1; i < len(args); i += 2 {
				if err := s.Set(string(args[i]), string(args[i+1])); err != nil {
					return fmt.Errorf("%s %s: %w", cmd, string(args[i]), err)
				}
			}
			return nil
		}

	case "INCRBYFLOAT":
		if len(args) >= 3 {
			key := string(args[1])
			delta, _ := strconv.ParseFloat(string(args[2]), 64)
			_, err := s.INCRBYFLOAT(key, delta)
			return err
		}

	case "SETRANGE":
		if len(args) >= 4 {
			key := string(args[1])
			offset, _ := strconv.Atoi(string(args[2]))
			value := string(args[3])
			_, err := s.SetRange(key, offset, value)
			return err
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

	case "EXPIRE":
		if len(args) >= 3 {
			key := string(args[1])
			seconds, _ := strconv.Atoi(string(args[2]))
			_, err := s.Expire(key, seconds)
			return err
		}

	case "EXPIREAT":
		if len(args) >= 3 {
			key := string(args[1])
			timestamp, _ := strconv.ParseInt(string(args[2]), 10, 64)
			_, err := s.ExpireAt(key, timestamp)
			return err
		}

	case "PEXPIRE":
		if len(args) >= 3 {
			key := string(args[1])
			millis, _ := strconv.ParseInt(string(args[2]), 10, 64)
			_, err := s.PExpire(key, millis)
			return err
		}

	case "PEXPIREAT":
		if len(args) >= 3 {
			key := string(args[1])
			millis, _ := strconv.ParseInt(string(args[2]), 10, 64)
			_, err := s.PExpireAt(key, millis)
			return err
		}

	case "PERSIST":
		if len(args) >= 2 {
			key := string(args[1])
			_, err := s.Persist(key)
			return err
		}

	case "RENAME":
		if len(args) >= 3 {
			key := string(args[1])
			newKey := string(args[2])
			return s.Rename(key, newKey)
		}

	case "RENAMENX":
		if len(args) >= 3 {
			key := string(args[1])
			newKey := string(args[2])
			_, err := s.RenameNX(key, newKey)
			return err
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

	case "LPOP":
		if len(args) >= 2 {
			key := string(args[1])
			_, err := s.LPop(key)
			return err
		}

	case "RPOP":
		if len(args) >= 2 {
			key := string(args[1])
			_, err := s.RPop(key)
			return err
		}

	case "LPUSHX":
		if len(args) >= 3 {
			key := string(args[1])
			for i := 2; i < len(args); i++ {
				if _, err := s.LPUSHX(key, string(args[i])); err != nil {
					return fmt.Errorf("LPUSHX %s: %w", key, err)
				}
			}
			return nil
		}

	case "RPUSHX":
		if len(args) >= 3 {
			key := string(args[1])
			for i := 2; i < len(args); i++ {
				if _, err := s.RPUSHX(key, string(args[i])); err != nil {
					return fmt.Errorf("RPUSHX %s: %w", key, err)
				}
			}
			return nil
		}

	case "LINSERT":
		if len(args) >= 5 {
			key := string(args[1])
			where := string(args[2])
			pivot := string(args[3])
			value := string(args[4])
			_, err := s.LInsert(key, where, pivot, value)
			return err
		}

	case "RPOPLPUSH":
		if len(args) >= 3 {
			source := string(args[1])
			dest := string(args[2])
			_, err := s.RPopLPush(source, dest)
			return err
		}

	case "LMOVE":
		if len(args) >= 5 {
			_, err := s.LMove(string(args[1]), string(args[2]), string(args[3]), string(args[4]))
			return err
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

	case "SINTERSTORE":
		if len(args) >= 3 {
			dest := string(args[1])
			keys := make([]string, len(args)-2)
			for i := 2; i < len(args); i++ {
				keys[i-2] = string(args[i])
			}
			_, err := s.SInterStore(dest, keys...)
			return err
		}

	case "SUNIONSTORE":
		if len(args) >= 3 {
			dest := string(args[1])
			keys := make([]string, len(args)-2)
			for i := 2; i < len(args); i++ {
				keys[i-2] = string(args[i])
			}
			_, err := s.SUnionStore(dest, keys...)
			return err
		}

	case "SDIFFSTORE":
		if len(args) >= 3 {
			dest := string(args[1])
			keys := make([]string, len(args)-2)
			for i := 2; i < len(args); i++ {
				keys[i-2] = string(args[i])
			}
			_, err := s.SDiffStore(dest, keys...)
			return err
		}

	case "HSET":
		if len(args) >= 4 {
			key := string(args[1])
			for i := 2; i+1 < len(args); i += 2 {
				if err := s.HSet(key, string(args[i]), string(args[i+1])); err != nil {
					return fmt.Errorf("HSET %s: %w", key, err)
				}
			}
			return nil
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

	case "ZPOPMAX":
		if len(args) >= 2 {
			key := string(args[1])
			count := 1
			if len(args) >= 3 {
				if c, err := strconv.Atoi(string(args[2])); err == nil && c > 0 {
					count = c
				}
			}
			_, err := s.ZPopMax(key, count)
			return err
		}

	case "ZPOPMIN":
		if len(args) >= 2 {
			key := string(args[1])
			count := 1
			if len(args) >= 3 {
				if c, err := strconv.Atoi(string(args[2])); err == nil && c > 0 {
					count = c
				}
			}
			_, err := s.ZPopMin(key, count)
			return err
		}

	case "ZREMRANGEBYRANK":
		if len(args) >= 4 {
			key := string(args[1])
			start, _ := strconv.ParseInt(string(args[2]), 10, 64)
			stop, _ := strconv.ParseInt(string(args[3]), 10, 64)
			_, err := s.ZRemRangeByRank(key, start, stop)
			return err
		}

	case "ZREMRANGEBYSCORE":
		if len(args) >= 4 {
			key := string(args[1])
			min, _ := strconv.ParseFloat(string(args[2]), 64)
			max, _ := strconv.ParseFloat(string(args[3]), 64)
			_, err := s.ZRemRangeByScore(key, min, max, false, false)
			return err
		}

	case "ZREMRANGEBYLEX":
		if len(args) >= 4 {
			key := string(args[1])
			min := string(args[2])
			max := string(args[3])
			_, err := s.ZRemRangeByLex(key, min, max)
			return err
		}

	case "GEOADD":
		if len(args) >= 5 && (len(args)-2)%3 == 0 {
			key := string(args[1])
			members := make([]store.GeoMember, 0, (len(args)-2)/3)
			for i := 2; i+2 < len(args); i += 3 {
				lon, _ := strconv.ParseFloat(string(args[i]), 64)
				lat, _ := strconv.ParseFloat(string(args[i+1]), 64)
				members = append(members, store.GeoMember{
					Member: string(args[i+2]),
					Lon:    lon,
					Lat:    lat,
				})
			}
			_, err := s.GeoAdd(key, members)
			return err
		}

	case "XADD":
		if len(args) >= 4 {
			key := string(args[1])
			id := string(args[2])
			fields := make(map[string]string)
			for i := 3; i+1 < len(args); i += 2 {
				fields[string(args[i])] = string(args[i+1])
			}
			_, err := s.XAdd(key, store.StreamXAddOptions{}, id, fields)
			return err
		}

	case "XDEL":
		if len(args) >= 3 {
			key := string(args[1])
			ids := make([]string, len(args)-2)
			for i := 2; i < len(args); i++ {
				ids[i-2] = string(args[i])
			}
			_, err := s.XDel(key, ids...)
			return err
		}

	case "XTRIM":
		if len(args) >= 3 {
			key := string(args[1])
			maxLen, _ := strconv.ParseInt(string(args[2]), 10, 64)
			minID := ""
			if len(args) >= 4 {
				minID = string(args[3])
			}
			_, err := s.XTrim(key, maxLen, minID)
			return err
		}

	default:
		logger.Logger.Debug().Str("cmd", cmd).Msg("收到未处理的复制命令")
		return nil
	}
	return nil
}
