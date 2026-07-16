package replication

import (
	"fmt"
	"sort"
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
	// replId 匹配时允许 CONTINUE（offset >= 0），确保重连时
	// 不会因为 offset == 0 触发不必要的 FULLRESYNC 造成数据丢失。
	// 初始连接时 replId 为 "?"，不会匹配，触发 FULLRESYNC。
	if replId == currentReplId && offset >= 0 {
		// 检查backlog中是否有足够的数据
		backlogStart := backlog.GetCurrentOffset() - backlog.GetSize()
		if backlogStart < 0 {
			backlogStart = 0
		}

		// 重启后 backlog 为空（currentOffset == 0）但请求的 offset > 0：
		// backlog 中没有可发送的数据，必须降级为 FULLRESYNC。
		// 不能仅靠 backlogStart/offset 范围判断——空 backlog 的
		// GetCurrentOffset() == 0，range check 会误判为有效。
		if backlog.GetCurrentOffset() == 0 && offset > 0 {
			logger.Logger.Info().
				Int64("requested_offset", offset).
				Msg("backlog 为空（重启后），请求的 offset 不可用，降级为全量同步")
		} else if offset >= backlogStart && offset <= currentOffset {
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
	if startOffset >= endOffset {
		return nil
	}
	data, err := backlog.GetRange(startOffset, endOffset)
	if err != nil {
		return fmt.Errorf("get backlog range failed: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	slave.writeMu.Lock()
	defer slave.writeMu.Unlock()

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

	case "GETDEL":
		if len(args) >= 2 {
			key := string(args[1])
			_, gErr := s.Get(key)
			if gErr != nil {
				return nil
			}
			_, dErr := s.Del(key)
			return dErr
		}

	case "GETEX":
		if len(args) >= 2 {
			key := string(args[1])
			if len(args) >= 4 {
				switch strings.ToUpper(string(args[2])) {
				case "EX":
					if seconds, pErr := strconv.Atoi(string(args[3])); pErr == nil {
						_, gErr := s.Get(key)
						if gErr != nil {
							return nil
						}
						_, eErr := s.Expire(key, seconds)
						return eErr
					}
					return nil
				case "PX":
					if millis, pErr := strconv.Atoi(string(args[3])); pErr == nil {
						_, gErr := s.Get(key)
						if gErr != nil {
							return nil
						}
						_, eErr := s.PExpire(key, int64(millis))
						return eErr
					}
					return nil
				}
			}
			if len(args) >= 3 && strings.ToUpper(string(args[2])) == "PERSIST" {
				_, gErr := s.Get(key)
				if gErr != nil {
					return nil
				}
				_, pErr := s.Persist(key)
				return pErr
			}
			// No option: just a GET
			_, gErr := s.Get(key)
			if gErr != nil {
				return nil
			}
		}

	case "DEL", "UNLINK":
		if len(args) >= 2 {
			for i := 1; i < len(args); i++ {
				if _, err := s.Del(string(args[i])); err != nil {
					return fmt.Errorf("%s %s: %w", cmd, string(args[i]), err)
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

	// BLPOP/BRPOP in replication are non-blocking equivalents.
	// Master already resolved the blocking pop; replica just pops from
	// the first key that has elements, mirroring the master's key order.
	case "BLPOP":
		if len(args) >= 3 {
			for i := 1; i < len(args)-1; i++ {
				key := string(args[i])
				if val, err := s.LPop(key); err == nil && val != "" {
					return nil
				} else if err != nil {
					return fmt.Errorf("BLPOP %s: %w", key, err)
				}
			}
			return nil
		}

	case "BRPOP":
		if len(args) >= 3 {
			for i := 1; i < len(args)-1; i++ {
				key := string(args[i])
				if val, err := s.RPop(key); err == nil && val != "" {
					return nil
				} else if err != nil {
					return fmt.Errorf("BRPOP %s: %w", key, err)
				}
			}
			return nil
		}

	case "BZPOPMAX":
		if len(args) >= 2 {
			for i := 1; i < len(args)-1; i++ {
				key := string(args[i])
				if members, err := s.ZPopMax(key, 1); err == nil && len(members) > 0 {
					return nil
				} else if err != nil {
					return fmt.Errorf("BZPOPMAX %s: %w", key, err)
				}
			}
			return nil
		}

	case "BZPOPMIN":
		if len(args) >= 2 {
			for i := 1; i < len(args)-1; i++ {
				key := string(args[i])
				if members, err := s.ZPopMin(key, 1); err == nil && len(members) > 0 {
					return nil
				} else if err != nil {
					return fmt.Errorf("BZPOPMIN %s: %w", key, err)
				}
			}
			return nil
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

	case "BRPOPLPUSH":
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

	case "BLMOVE":
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

	case "HSETNX":
		if len(args) >= 4 {
			key := string(args[1])
			field := string(args[2])
			value := string(args[3])
			_, err := s.HSetNX(key, field, value)
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

	case "LMPOP":
		if len(args) >= 4 {
			numKeys, kErr := strconv.Atoi(string(args[1]))
			if kErr != nil || numKeys < 1 || 2+numKeys > len(args) {
				return nil
			}
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = string(args[2+i])
			}
			modifier := strings.ToUpper(string(args[2+numKeys]))
			if modifier != "LEFT" && modifier != "RIGHT" {
				return nil
			}
			count := 1
			if len(args) >= 4+numKeys && strings.ToUpper(string(args[3+numKeys])) == "COUNT" {
				if c, cErr := strconv.Atoi(string(args[4+numKeys])); cErr == nil && c > 0 {
					count = c
				}
			}
			_, _, err := s.LMPop(keys, modifier, count)
			return err
		}

	case "ZMPOP":
		if len(args) >= 4 {
			numKeys, kErr := strconv.Atoi(string(args[1]))
			if kErr != nil || numKeys < 1 || 2+numKeys > len(args) {
				return nil
			}
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = string(args[2+i])
			}
			modifier := strings.ToUpper(string(args[2+numKeys]))
			if modifier != "MIN" && modifier != "MAX" {
				return nil
			}
			count := 1
			if len(args) >= 4+numKeys && strings.ToUpper(string(args[3+numKeys])) == "COUNT" {
				if c, cErr := strconv.Atoi(string(args[4+numKeys])); cErr == nil && c > 0 {
					count = c
				}
			}
			_, _, err := s.ZMPop(keys, modifier, count)
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

	case "PFADD":
		if len(args) >= 2 {
			key := string(args[1])
			elements := make([]string, 0, len(args)-2)
			for i := 2; i < len(args); i++ {
				elements = append(elements, string(args[i]))
			}
			_, err := s.PFAdd(key, elements...)
			return err
		}

	case "PFMERGE":
		if len(args) >= 3 {
			destKey := string(args[1])
			sourceKeys := make([]string, 0, len(args)-2)
			for i := 2; i < len(args); i++ {
				sourceKeys = append(sourceKeys, string(args[i]))
			}
			err := s.PFMerge(destKey, sourceKeys...)
			return err
		}

	case "SETBIT":
		if len(args) >= 4 {
			key := string(args[1])
			offset, oErr := strconv.Atoi(string(args[2]))
			bit, bErr := strconv.Atoi(string(args[3]))
			if oErr == nil && bErr == nil {
				_, err := s.SetBit(key, offset, bit)
				return err
			}
			return nil
		}

	case "BITOP":
		if len(args) >= 4 {
			op := strings.ToUpper(string(args[1]))
			destKey := string(args[2])
			keys := make([]string, len(args)-3)
			for i := 3; i < len(args); i++ {
				keys[i-3] = string(args[i])
			}
			_, err := s.BitOp(op, destKey, keys...)
			return err
		}

	case "BITFIELD":
		if len(args) >= 3 {
			key := string(args[1])
			operations := make([]string, 0, len(args)-2)
			for i := 2; i < len(args); i++ {
				operations = append(operations, string(args[i]))
			}
			_, err := s.BitField(key, operations)
			return err
		}

	case "ZUNIONSTORE":
		if len(args) >= 4 {
			destination := string(args[1])
			numKeys, nErr := strconv.Atoi(string(args[2]))
			if nErr != nil || numKeys < 1 || 3+numKeys > len(args) {
				return nil
			}
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = string(args[3+i])
			}
			weights := []float64{}
			aggregate := "SUM"
			idx := 3 + numKeys
			for idx < len(args) {
				opt := strings.ToUpper(string(args[idx]))
				switch opt {
				case "WEIGHTS":
					if idx+numKeys >= len(args) {
						return nil
					}
					weights = make([]float64, numKeys)
					for j := 0; j < numKeys; j++ {
						w, wErr := strconv.ParseFloat(string(args[idx+1+j]), 64)
						if wErr != nil {
							return nil
						}
						weights[j] = w
					}
					idx += 1 + numKeys
				case "AGGREGATE":
					if idx+1 >= len(args) {
						return nil
					}
					aggregate = strings.ToUpper(string(args[idx+1]))
					idx += 2
				default:
					idx++
				}
			}
			_, err := s.ZUnionStore(destination, keys, weights, aggregate)
			return err
		}

	case "ZINTERSTORE":
		if len(args) >= 4 {
			destination := string(args[1])
			numKeys, nErr := strconv.Atoi(string(args[2]))
			if nErr != nil || numKeys < 1 || 3+numKeys > len(args) {
				return nil
			}
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = string(args[3+i])
			}
			weights := []float64{}
			aggregate := "SUM"
			idx := 3 + numKeys
			for idx < len(args) {
				opt := strings.ToUpper(string(args[idx]))
				switch opt {
				case "WEIGHTS":
					if idx+numKeys >= len(args) {
						return nil
					}
					weights = make([]float64, numKeys)
					for j := 0; j < numKeys; j++ {
						w, wErr := strconv.ParseFloat(string(args[idx+1+j]), 64)
						if wErr != nil {
							return nil
						}
						weights[j] = w
					}
					idx += 1 + numKeys
				case "AGGREGATE":
					if idx+1 >= len(args) {
						return nil
					}
					aggregate = strings.ToUpper(string(args[idx+1]))
					idx += 2
				default:
					idx++
				}
			}
			_, err := s.ZInterStore(destination, keys, weights, aggregate)
			return err
		}

	case "ZDIFFSTORE":
		if len(args) >= 4 {
			destination := string(args[1])
			numKeys, nErr := strconv.Atoi(string(args[2]))
			if nErr != nil || numKeys < 1 || 3+numKeys > len(args) {
				return nil
			}
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = string(args[3+i])
			}
			_, err := s.ZDiffStore(destination, keys)
			return err
		}

	case "XACK":
		if len(args) >= 4 {
			key := string(args[1])
			group := string(args[2])
			ids := make([]string, 0, len(args)-3)
			for i := 3; i < len(args); i++ {
				ids = append(ids, string(args[i]))
			}
			_, err := s.XAck(key, group, ids...)
			return err
		}

	case "XCLAIM":
		if len(args) >= 6 {
			key := string(args[1])
			group := string(args[2])
			consumer := string(args[3])
			minIdleTime, _ := strconv.ParseInt(string(args[4]), 10, 64)
			ids := make([]string, 0, len(args)-5)
			for i := 5; i < len(args); i++ {
				ids = append(ids, string(args[i]))
			}
			_, err := s.XClaim(key, group, consumer, minIdleTime, ids...)
			return err
		}

	case "XGROUP":
		if len(args) >= 2 {
			sub := strings.ToUpper(string(args[1]))
			switch sub {
			case "CREATE":
				if len(args) >= 5 {
					key := string(args[2])
					group := string(args[3])
					startID := string(args[4])
					err := s.XGroupCreate(key, group, startID)
					return err
				}
			case "DESTROY":
				if len(args) >= 4 {
					key := string(args[2])
					group := string(args[3])
					err := s.XGroupDestroy(key, group)
					return err
				}
			case "SETID":
				if len(args) >= 5 {
					key := string(args[2])
					group := string(args[3])
					id := string(args[4])
					err := s.XGroupSetID(key, group, id)
					return err
				}
			case "DELCONSUMER":
				if len(args) >= 5 {
					key := string(args[2])
					group := string(args[3])
					consumer := string(args[4])
					_, err := s.XGroupDelConsumer(key, group, consumer)
					return err
				}
			default:
				logger.Logger.Debug().Str("sub", sub).Msg("收到未处理的 XGROUP 子命令")
			}
		}

	case "ZRANGESTORE":
		if len(args) >= 5 {
			dstKey := string(args[1])
			srcKey := string(args[2])
			min := string(args[3])
			max := string(args[4])
			byScore := false
			byLex := false
			rev := false
			var limitOffset, limitCount int64 = 0, -1
			i := 5
			for i < len(args) {
				opt := strings.ToUpper(string(args[i]))
				switch opt {
				case "BYSCORE":
					byScore = true
					i++
				case "BYLEX":
					byLex = true
					i++
				case "REV":
					rev = true
					i++
				case "LIMIT":
					if i+2 >= len(args) {
						return nil
					}
					var parseErr error
					limitOffset, parseErr = strconv.ParseInt(string(args[i+1]), 10, 64)
					if parseErr != nil {
						return fmt.Errorf("invalid LIMIT offset %q: %w", args[i+1], parseErr)
					}
					limitCount, parseErr = strconv.ParseInt(string(args[i+2]), 10, 64)
					if parseErr != nil {
						return fmt.Errorf("invalid LIMIT count %q: %w", args[i+2], parseErr)
					}
					i += 3
				default:
					i++
				}
			}
			var members []store.ZSetMember
			var zErr error
			if byLex {
				lexMembers, lErr := s.ZRangeByLex(srcKey, min, max, int(limitOffset), int(limitCount))
				if lErr != nil {
					return lErr
				}
				members = make([]store.ZSetMember, len(lexMembers))
				for i, m := range lexMembers {
					members[i] = store.ZSetMember{Member: m, Score: 0}
				}
			} else if byScore {
				minScore, _ := strconv.ParseFloat(min, 64)
				maxScore, _ := strconv.ParseFloat(max, 64)
				members, zErr = s.ZRangeByScore(srcKey, minScore, maxScore, int(limitOffset), int(limitCount), false, false)
				if zErr != nil {
					return zErr
				}
			} else {
				start, _ := strconv.ParseInt(min, 10, 64)
				stop, _ := strconv.ParseInt(max, 10, 64)
				if rev {
					start, stop = stop, start
				}
				ptrMembers, rErr := s.ZRange(srcKey, start, stop)
				if rErr != nil {
					return rErr
				}
				if limitCount >= 0 && int64(len(ptrMembers)) > limitOffset {
					if limitCount == 0 || limitOffset+int64(limitCount) > int64(len(ptrMembers)) {
						ptrMembers = ptrMembers[limitOffset:]
					} else {
						ptrMembers = ptrMembers[limitOffset : limitOffset+int64(limitCount)]
					}
				}
				members = make([]store.ZSetMember, len(ptrMembers))
				for i, m := range ptrMembers {
					members[i] = store.ZSetMember{Member: m.Member, Score: m.Score}
				}
			}
			if rev && (byScore || byLex) {
				for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
					members[i], members[j] = members[j], members[i]
				}
			}
			if _, dErr := s.Del(dstKey); dErr != nil {
				logger.Logger.Warn().Err(dErr).Str("dstKey", dstKey).Msg("ZRANGESTORE: failed to clear destination before ZAdd")
			}
			if len(members) > 0 {
				return s.ZAdd(dstKey, members)
			}
			return nil
		}

	case "COPY":
		if len(args) >= 3 {
			srcKey := string(args[1])
			dstKey := string(args[2])
			replace := false
			i := 3
			for i < len(args) {
				opt := strings.ToUpper(string(args[i]))
				switch opt {
				case "REPLACE":
					replace = true
					i++
				case "DB":
					i += 2
				default:
					i++
				}
			}
			srcType, tErr := s.Type(srcKey)
			if tErr != nil || srcType == "none" {
				return nil
			}
			dstExists, dErr := s.Exists(dstKey)
			if dErr != nil {
				logger.Logger.Warn().Err(dErr).Str("dstKey", dstKey).Msg("COPY: Exists check failed, proceeding with copy")
			}
			if dstExists && !replace {
				return nil
			}
			switch srcType {
			case "string":
				val, gErr := s.Get(srcKey)
				if gErr != nil {
					return gErr
				}
				return s.Set(dstKey, val)
			case "list":
				length, lErr := s.LLen(srcKey)
				if lErr != nil {
					return lErr
				}
				if length == 0 {
					if _, dErr := s.Del(dstKey); dErr != nil {
						logger.Logger.Warn().Err(dErr).Str("dstKey", dstKey).Msg("COPY: failed to clear empty list destination")
					}
					return nil
				}
				items, lrErr := s.LRange(srcKey, 0, int64(length))
				if lrErr != nil {
					return lrErr
				}
				if len(items) == 0 {
					return nil
				}
				if _, dErr := s.Del(dstKey); dErr != nil {
					logger.Logger.Warn().Err(dErr).Str("dstKey", dstKey).Msg("COPY: failed to clear list destination before RPush")
				}
				_, rErr := s.RPush(dstKey, items...)
				return rErr
			case "hash":
				data, hErr := s.HGetAll(srcKey)
				if hErr != nil {
					return hErr
				}
				if len(data) == 0 {
					return nil
				}
				if _, dErr := s.Del(dstKey); dErr != nil {
					logger.Logger.Warn().Err(dErr).Str("dstKey", dstKey).Msg("COPY: failed to clear hash destination before HMSet")
				}
				fv := make(map[string]interface{}, len(data))
				for k, v := range data {
					fv[k] = string(v)
				}
				return s.HMSet(dstKey, fv)
			case "set":
				members, mErr := s.SMembers(srcKey)
				if mErr != nil {
					return mErr
				}
				if len(members) == 0 {
					return nil
				}
				if _, dErr := s.Del(dstKey); dErr != nil {
					logger.Logger.Warn().Err(dErr).Str("dstKey", dstKey).Msg("COPY: failed to clear set destination before SAdd")
				}
				_, addErr := s.SAdd(dstKey, members...)
				return addErr
			case "zset":
				zMembers, zErr := s.ZRange(srcKey, 0, -1)
				if zErr != nil {
					return zErr
				}
				if len(zMembers) == 0 {
					return nil
				}
				if _, dErr := s.Del(dstKey); dErr != nil {
					logger.Logger.Warn().Err(dErr).Str("dstKey", dstKey).Msg("COPY: failed to clear zset destination before ZAdd")
				}
				zAdd := make([]store.ZSetMember, len(zMembers))
				for i, m := range zMembers {
					zAdd[i] = store.ZSetMember{Score: m.Score, Member: m.Member}
				}
				return s.ZAdd(dstKey, zAdd)
			}
			return nil
		}

	case "GEOSEARCHSTORE":
		if len(args) >= 6 {
			dstKey := string(args[1])
			srcKey := string(args[2])
			var centerLon, centerLat float64
			var radius float64
			var unit string
			var count int
			var storeDist bool
			i := 3
			if i < len(args) && strings.ToUpper(string(args[i])) == "FROMMEMBER" {
				if i+1 >= len(args) {
					return nil
				}
				member := string(args[i+1])
				positions, gpErr := s.GeoPos(srcKey, member)
				if gpErr != nil || len(positions) == 0 || (positions[0][0] == 0 && positions[0][1] == 0) {
					return nil
				}
				centerLon = positions[0][1]
				centerLat = positions[0][0]
				i += 2
			} else if i < len(args) && strings.ToUpper(string(args[i])) == "FROMLONLAT" {
				if i+2 >= len(args) {
					return nil
				}
				var parseErr error
				centerLon, parseErr = strconv.ParseFloat(string(args[i+1]), 64)
				if parseErr != nil {
					return fmt.Errorf("invalid GEO FROMLONLAT longitude %q: %w", args[i+1], parseErr)
				}
				centerLat, parseErr = strconv.ParseFloat(string(args[i+2]), 64)
				if parseErr != nil {
					return fmt.Errorf("invalid GEO FROMLONLAT latitude %q: %w", args[i+2], parseErr)
				}
				i += 3
			} else {
				return nil
			}
			if i >= len(args) {
				return nil
			}
			if strings.ToUpper(string(args[i])) == "BYRADIUS" {
				if i+2 >= len(args) {
					return nil
				}
				var parseErr error
				radius, parseErr = strconv.ParseFloat(string(args[i+1]), 64)
				if parseErr != nil {
					return fmt.Errorf("invalid GEO BYRADIUS radius %q: %w", args[i+1], parseErr)
				}
				unit = string(args[i+2])
				i += 3
			} else {
				return nil
			}
			for i < len(args) {
				opt := strings.ToUpper(string(args[i]))
				switch opt {
				case "ASC", "DESC":
					i++
				case "COUNT":
					if i+1 >= len(args) {
						return nil
					}
					var parseErr error
					count, parseErr = strconv.Atoi(string(args[i+1]))
					if parseErr != nil {
						return fmt.Errorf("invalid GEO COUNT %q: %w", args[i+1], parseErr)
					}
					i += 2
				case "STOREDIST":
					storeDist = true
					i++
				default:
					i++
				}
			}
			_, gsErr := s.GeoSearchStore(dstKey, srcKey, centerLon, centerLat, radius, unit, count, storeDist)
			return gsErr
		}

	// JSON commands
	case "JSON.SET":
		if len(args) >= 4 {
			key := string(args[1])
			path := string(args[2])
			value := string(args[3])
			nx, xx := false, false
			for i := 4; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				if opt == "NX" {
					nx = true
				}
				if opt == "XX" {
					xx = true
				}
			}
			_, jErr := s.JSONSet(key, path, value, nx, xx)
			return jErr
		}

	case "JSON.DEL":
		if len(args) >= 2 {
			key := string(args[1])
			paths := make([]string, 0)
			for i := 2; i < len(args); i++ {
				paths = append(paths, string(args[i]))
			}
			_, jErr := s.JSONDel(key, paths...)
			return jErr
		}

	case "JSON.ARRAPPEND":
		if len(args) >= 4 {
			key := string(args[1])
			path := string(args[2])
			values := make([]string, 0)
			for i := 3; i < len(args); i++ {
				values = append(values, string(args[i]))
			}
			_, jErr := s.JSONArrAppend(key, path, values...)
			return jErr
		}

	case "JSON.NUMINCRBY":
		if len(args) >= 4 {
			key := string(args[1])
			path := string(args[2])
			increment, _ := strconv.ParseFloat(string(args[3]), 64)
			_, jErr := s.JSONNumIncrBy(key, path, increment)
			return jErr
		}

	case "JSON.NUMMULTBY":
		if len(args) >= 4 {
			key := string(args[1])
			path := string(args[2])
			multiplier, _ := strconv.ParseFloat(string(args[3]), 64)
			_, jErr := s.JSONNumMultBy(key, path, multiplier)
			return jErr
		}

	case "JSON.CLEAR":
		if len(args) >= 2 {
			key := string(args[1])
			path := "$"
			if len(args) >= 3 {
				path = string(args[2])
			}
			_, jErr := s.JSONClear(key, path)
			return jErr
		}

	// TimeSeries commands
	case "TS.CREATE":
		if len(args) >= 2 {
			key := string(args[1])
			opts := store.TSCreateOptions{}
			i := 2
			for i < len(args) {
				opt := strings.ToUpper(string(args[i]))
				switch opt {
				case "RETENTION":
					i++
					if i < len(args) {
						var parseErr error
						opts.Retention, parseErr = strconv.ParseInt(string(args[i]), 10, 64)
						if parseErr != nil {
							return fmt.Errorf("invalid TS.CREATE RETENTION %q: %w", args[i], parseErr)
						}
					}
				case "ENCODING":
					i++
					if i < len(args) {
						opts.Encoding = string(args[i])
					}
				case "DUPLICATE_POLICY":
					i++
					if i < len(args) {
						opts.DuplicatePolicy = string(args[i])
					}
				}
				i++
			}
			return s.TSCreate(key, opts)
		}

	case "TS.ADD":
		if len(args) >= 4 {
			key := string(args[1])
			var timestamp int64
			if string(args[2]) == "*" {
				timestamp = time.Now().UnixNano() / int64(time.Millisecond)
			} else {
				var parseErr error
				timestamp, parseErr = strconv.ParseInt(string(args[2]), 10, 64)
				if parseErr != nil {
					return fmt.Errorf("invalid TS.ADD timestamp %q: %w", args[2], parseErr)
				}
			}
			value, valueErr := strconv.ParseFloat(string(args[3]), 64)
			if valueErr != nil {
				return fmt.Errorf("invalid TS.ADD value %q: %w", args[3], valueErr)
			}
			opts := store.TSAddOptions{}
			if len(args) > 4 {
				opt := strings.ToUpper(string(args[4]))
				if opt == "ON_DUPLICATE" && len(args) > 5 {
					opts.OnDuplicate = string(args[5])
				}
			}
			_, tsErr := s.TSAdd(key, timestamp, value, opts)
			return tsErr
		}

	case "TS.DEL":
		if len(args) >= 4 {
			key := string(args[1])
			start := string(args[2])
			stop := string(args[3])
			_, tsErr := s.TSDel(key, start, stop)
			return tsErr
		}

	case "TS.MADD":
		// TS.MADD key ts val [key ts val ...]
		if len(args) >= 4 && (len(args)-1)%3 == 0 {
			for i := 1; i+2 < len(args); i += 3 {
				key := string(args[i])
				var timestamp int64
				if string(args[i+1]) == "*" {
					timestamp = time.Now().UnixNano() / int64(time.Millisecond)
				} else {
					var parseErr error
					timestamp, parseErr = strconv.ParseInt(string(args[i+1]), 10, 64)
					if parseErr != nil {
						return fmt.Errorf("invalid TS.MADD timestamp %q: %w", args[i+1], parseErr)
					}
				}
				value, valueErr := strconv.ParseFloat(string(args[i+2]), 64)
				if valueErr != nil {
					return fmt.Errorf("invalid TS.MADD value %q: %w", args[i+2], valueErr)
				}
				if _, err := s.TSAdd(key, timestamp, value, store.TSAddOptions{}); err != nil {
					return fmt.Errorf("TS.MADD %s: %w", key, err)
				}
			}
			return nil
		}

	case "TS.INCRBY":
		if len(args) >= 3 {
			key := string(args[1])
			value, valueErr := strconv.ParseFloat(string(args[2]), 64)
			if valueErr != nil {
				return fmt.Errorf("invalid TS.INCRBY value %q: %w", args[2], valueErr)
			}
			var timestamp int64
			if len(args) >= 5 && strings.ToUpper(string(args[3])) == "TIMESTAMP" {
				var parseErr error
				timestamp, parseErr = strconv.ParseInt(string(args[4]), 10, 64)
				if parseErr != nil {
					return fmt.Errorf("invalid TS.INCRBY timestamp %q: %w", args[4], parseErr)
				}
			}
			_, err := s.TSIncrBy(key, timestamp, value)
			return err
		}

	case "TS.CREATERULE":
		// TS.CREATERULE source dest AGGREGATION aggregator bucketDuration
		if len(args) >= 6 && strings.ToUpper(string(args[3])) == "AGGREGATION" {
			sourceKey := string(args[1])
			destKey := string(args[2])
			aggregator := string(args[4])
			bucketDuration, parseErr := strconv.ParseInt(string(args[5]), 10, 64)
			if parseErr != nil {
				return fmt.Errorf("invalid TS.CREATERULE bucket: %w", parseErr)
			}
			return s.TSAddRule(sourceKey, destKey, aggregator, bucketDuration)
		}

	case "TS.DELETERULE":
		if len(args) >= 6 && strings.ToUpper(string(args[3])) == "AGGREGATION" {
			sourceKey := string(args[1])
			destKey := string(args[2])
			aggregator := string(args[4])
			bucketDuration, parseErr := strconv.ParseInt(string(args[5]), 10, 64)
			if parseErr != nil {
				return fmt.Errorf("invalid TS.DELETERULE bucket: %w", parseErr)
			}
			return s.TSDelRule(sourceKey, destKey, aggregator, bucketDuration)
		}

	case "XACKDEL":
		// XACKDEL key group [KEEPREF|DELREF|ACKED] IDS n id [id ...]
		if len(args) >= 5 {
			key := string(args[1])
			group := string(args[2])
			mode := "KEEPREF"
			idsStart := 3
			next := strings.ToUpper(string(args[3]))
			if next == "KEEPREF" || next == "DELREF" || next == "ACKED" {
				mode = next
				idsStart = 4
			}
			if idsStart >= len(args) || strings.ToUpper(string(args[idsStart])) != "IDS" {
				return nil
			}
			if idsStart+1 >= len(args) {
				return nil
			}
			numIDs, nErr := strconv.Atoi(string(args[idsStart+1]))
			if nErr != nil || numIDs < 1 || idsStart+2+numIDs > len(args) {
				return nil
			}
			ids := make([]string, numIDs)
			for i := 0; i < numIDs; i++ {
				ids[i] = string(args[idsStart+2+i])
			}
			if _, err := s.XAck(key, group, ids...); err != nil {
				return fmt.Errorf("XACKDEL XAck: %w", err)
			}
			switch mode {
			case "KEEPREF":
				if _, err := s.XDel(key, ids...); err != nil {
					return fmt.Errorf("XACKDEL XDel: %w", err)
				}
			case "DELREF":
				for _, id := range ids {
					if _, err := s.XDel(key, id); err != nil {
						return fmt.Errorf("XACKDEL XDel: %w", err)
					}
					if err := s.XAckDelRemoveRefs(key, id); err != nil {
						return fmt.Errorf("XACKDEL remove refs: %w", err)
					}
				}
			case "ACKED":
				for _, id := range ids {
					allAcked, aErr := s.XIsAckedByAllGroups(key, id)
					if aErr != nil {
						return fmt.Errorf("XACKDEL IsAcked: %w", aErr)
					}
					if allAcked {
						if _, err := s.XDel(key, id); err != nil {
							return fmt.Errorf("XACKDEL XDel: %w", err)
						}
					}
				}
			}
			return nil
		}

	case "XDELEX":
		// XDELEX key id [id ...] — same as XDEL
		if len(args) >= 3 {
			key := string(args[1])
			ids := make([]string, 0, len(args)-2)
			for i := 2; i < len(args); i++ {
				ids = append(ids, string(args[i]))
			}
			_, err := s.XDel(key, ids...)
			return err
		}

	case "XNACK":
		// XNACK key group consumer id [id ...]
		if len(args) >= 5 {
			key := string(args[1])
			group := string(args[2])
			consumer := string(args[3])
			ids := make([]string, 0, len(args)-4)
			for i := 4; i < len(args); i++ {
				ids = append(ids, string(args[i]))
			}
			_, err := s.XNack(key, group, consumer, ids...)
			return err
		}

	case "XSETID":
		// XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]
		if len(args) >= 3 {
			key := string(args[1])
			lastID := string(args[2])
			var entriesAdded int64 = -1
			var maxDeletedID string
			for i := 3; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				switch opt {
				case "ENTRIESADDED":
					if i+1 >= len(args) {
						return nil
					}
					n, pErr := strconv.ParseInt(string(args[i+1]), 10, 64)
					if pErr != nil {
						return fmt.Errorf("invalid ENTRIESADDED: %w", pErr)
					}
					entriesAdded = n
					i++
				case "MAXDELETEDID":
					if i+1 >= len(args) {
						return nil
					}
					maxDeletedID = string(args[i+1])
					i++
				}
			}
			return s.XSetID(key, lastID, entriesAdded, maxDeletedID)
		}

	case "XCFGSET":
		// Internal no-op config command — accept for wire compatibility.
		return nil

	case "BZMPOP":
		// Non-blocking equivalent: BZMPOP timeout numkeys key... MIN|MAX [COUNT n]
		// Same approach as BZPOPMAX/BZPOPMIN — do not block on replica.
		if len(args) >= 5 {
			numKeys, kErr := strconv.Atoi(string(args[2]))
			if kErr != nil || numKeys < 1 || 3+numKeys > len(args) {
				return nil
			}
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = string(args[3+i])
			}
			modifier := strings.ToUpper(string(args[3+numKeys]))
			if modifier != "MIN" && modifier != "MAX" {
				return nil
			}
			count := 1
			if len(args) >= 5+numKeys && strings.ToUpper(string(args[4+numKeys])) == "COUNT" {
				if c, cErr := strconv.Atoi(string(args[5+numKeys])); cErr == nil && c > 0 {
					count = c
				}
			}
			_, _, err := s.ZMPop(keys, modifier, count)
			return err
		}

	case "MOVE", "SWAPDB":
		// Single-DB server: no-ops that still advance offset on master.
		return nil

	// ========== P1.5: 修复复制数据丢失 ==========

	case "RESTORE":
		if len(args) >= 3 {
			key := string(args[1])
			replace := false

			// 尝试新格式：RESTORE key ttl serializedData [REPLACE] [ABSTTL]
			if len(args) >= 4 {
				ttlMS, parseErr := strconv.ParseInt(string(args[2]), 10, 64)
				if parseErr == nil {
					serializedData := string(args[3])
					absttl := false
					for i := 4; i < len(args); i++ {
						upper := strings.ToUpper(string(args[i]))
						switch upper {
						case "REPLACE":
							replace = true
						case "ABSTTL":
							absttl = true
						}
					}
					var ttl time.Duration
					if absttl {
						now := time.Now().UnixMilli()
						if ttlMS > now {
							ttl = time.Duration(ttlMS-now) * time.Millisecond
						}
					} else {
						ttl = time.Duration(ttlMS) * time.Millisecond
					}
					err := s.Restore(key, []byte(serializedData), ttl, replace)
					return err
				}
			}

			// 旧格式：RESTORE key serializedData [REPLACE]
			serializedData := string(args[2])
			for i := 3; i < len(args); i++ {
				if strings.ToUpper(string(args[i])) == "REPLACE" {
					replace = true
				}
			}
			err := s.Restore(key, []byte(serializedData), 0, replace)
			return err
		}

	case "FLUSHDB", "FLUSHALL":
		err := s.FlushDB()
		s.ClearCaches()
		return err

	case "XAUTOCLAIM":
		if len(args) >= 7 {
			key := string(args[1])
			group := string(args[2])
			consumer := string(args[3])
			minIdleTime, _ := strconv.ParseInt(string(args[4]), 10, 64)
			start := string(args[5])
			opts := store.XAutoClaimOptions{Count: 100, JustID: false}
			for i := 6; i < len(args); i++ {
				opt := strings.ToUpper(string(args[i]))
				switch opt {
				case "COUNT":
					if i+1 < len(args) {
						count, parseErr := strconv.ParseInt(string(args[i+1]), 10, 64)
						if parseErr == nil {
							opts.Count = count
						}
						i++
					}
				case "JUSTID":
					opts.JustID = true
				}
			}
			_, err := s.XAutoClaim(key, group, consumer, minIdleTime, start, opts)
			return err
		}

	case "SORT":
		if len(args) < 2 {
			return nil
		}
		key := string(args[1])
		// 解析 SORT 选项
		var offset, count int64 = 0, -1
		var asc = true
		var alpha bool
		var destKey string
		var byPattern string
		i := 2
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "BY":
				if i+1 < len(args) {
					byPattern = string(args[i+1])
					i += 2
				} else {
					i++
				}
			case "LIMIT":
				if i+2 < len(args) {
					var parseErr error
					offset, parseErr = strconv.ParseInt(string(args[i+1]), 10, 64)
					if parseErr != nil {
						return fmt.Errorf("invalid SORT LIMIT offset %q: %w", args[i+1], parseErr)
					}
					count, parseErr = strconv.ParseInt(string(args[i+2]), 10, 64)
					if parseErr != nil {
						return fmt.Errorf("invalid SORT LIMIT count %q: %w", args[i+2], parseErr)
					}
					i += 3
				} else {
					i++
				}
			case "ASC":
				asc = true
				i++
			case "DESC":
				asc = false
				i++
			case "ALPHA":
				alpha = true
				i++
			case "STORE":
				if i+1 < len(args) {
					destKey = string(args[i+1])
					i += 2
				} else {
					i++
				}
			default:
				i++
			}
		}

		// 只处理带 STORE 选项的 SORT（否则是只读操作）
		if destKey == "" {
			return nil
		}

		// 获取源数据
		keyType, tErr := s.Type(key)
		if tErr != nil {
			return fmt.Errorf("SORT: failed to get type for key %s: %w", key, tErr)
		}
		var values []string
		var scores []float64

		switch keyType {
		case "list":
			listValues, err := s.LRange(key, 0, -1)
			if err == nil {
				values = listValues
			} else {
				values = []string{}
			}
		case "set":
			setValues, err := s.SMembers(key)
			if err == nil {
				values = setValues
			} else {
				values = []string{}
			}
		case "string":
			val, gErr := s.Get(key)
			if gErr != nil {
				return fmt.Errorf("SORT: failed to get string key %s: %w", key, gErr)
			}
			values = []string{val}
		case "zset":
			members, zErr := s.ZRange(key, 0, -1)
			if zErr != nil {
				return fmt.Errorf("SORT: failed to ZRange key %s: %w", key, zErr)
			}
			for _, m := range members {
				values = append(values, m.Member)
				scores = append(scores, m.Score)
			}
		default:
			return nil
		}

		// 应用 BY pattern
		if byPattern != "" && len(values) > 0 {
			weights := make([]float64, len(values))
			for idx, val := range values {
				targetKey := strings.Replace(byPattern, "*", val, 1)
				weightVal, wErr := s.Get(targetKey)
				if wErr != nil {
					logger.Logger.Warn().Err(wErr).Str("key", targetKey).Msg("SORT BY: failed to get weight key, using default")
				}
				if weightVal != "" {
					if f, parseErr := strconv.ParseFloat(weightVal, 64); parseErr == nil {
						weights[idx] = f
					} else {
						weights[idx] = float64(idx)
					}
				} else {
					weights[idx] = float64(idx)
				}
			}
			scores = weights
			alpha = false
		}

		// 数值排序
		if len(scores) == 0 && !alpha && len(values) > 0 {
			scores = make([]float64, len(values))
			for idx, v := range values {
				if f, parseErr := strconv.ParseFloat(v, 64); parseErr == nil {
					scores[idx] = f
				} else {
					scores[idx] = 0
				}
			}
		}

		// 排序 — 使用标准库（O(n log n)），保持 values/scores 同步
		type sortItem struct {
			value string
			score float64
		}
		items := make([]sortItem, len(values))
		for i := range values {
			items[i] = sortItem{value: values[i]}
			if i < len(scores) {
				items[i].score = scores[i]
			}
		}
		sort.Slice(items, func(i, j int) bool {
			if alpha {
				if asc {
					return items[i].value < items[j].value
				}
				return items[i].value > items[j].value
			}
			if asc {
				return items[i].score < items[j].score
			}
			return items[i].score > items[j].score
		})
		for i := range items {
			values[i] = items[i].value
		}
		if len(scores) > 0 {
			scores = make([]float64, len(items))
			for i := range items {
				scores[i] = items[i].score
			}
		}

		// LIMIT
		if offset > 0 {
			if offset >= int64(len(values)) {
				values = []string{}
			} else if offset < int64(len(values)) {
				values = values[offset:]
			}
		}
		if count >= 0 && int64(len(values)) > count {
			values = values[:count]
		}

		// STORE — 存为 list
		if _, dErr := s.Del(destKey); dErr != nil {
			logger.Logger.Warn().Err(dErr).Str("destKey", destKey).Msg("SORT STORE: failed to clear destination")
		}
		for _, v := range values {
			if _, rErr := s.RPush(destKey, v); rErr != nil {
				return fmt.Errorf("SORT STORE: failed to RPush to %s: %w", destKey, rErr)
			}
		}

	default:
		logger.Logger.Warn().Str("cmd", cmd).
			Msg("收到未处理的复制命令，触发重新同步")
		return fmt.Errorf("unknown replicated command: %s", cmd)
	}
	return nil
}
