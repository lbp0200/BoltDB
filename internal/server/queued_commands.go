package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// executeQueuedCommand 执行事务队列中的命令
func (h *Handler) executeQueuedCommand(cmd string, args [][]byte, respVersion int) proto.RESP {
	nilBulk := func() proto.RESP {
		if respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	switch cmd {
	case "SET":
		key, value := string(args[0]), string(args[1])
		ttl, nx, xx, get, keepTTL, err := parseSetOptions(args[2:])
		if err != nil {
			return wrapLogError(err)
		}
		var oldVal string
		exists := false
		if nx || xx || get || keepTTL {
			if v, err := h.Db.Get(key); err == nil {
				exists = true
				oldVal = v
			} else if !errors.Is(err, store.ErrKeyNotFound) && !errors.Is(err, store.ErrWrongType) {
				return wrapStoreError(err)
			}
			if nx && exists {
				return nilBulk()
			}
			if xx && !exists {
				return nilBulk()
			}
		}
		if keepTTL {
			ttlSec, ttlErr := h.Db.TTL(key)
			if err := h.Db.Set(key, value); err != nil {
				return wrapStoreError(err)
			}
			if ttlErr == nil && ttlSec > 0 {
				if _, err := h.Db.Expire(key, int(ttlSec)); err != nil {
					return wrapStoreError(err)
				}
			}
		} else if ttl > 0 {
			if err := h.Db.SetWithTTL(key, value, ttl); err != nil {
				return wrapStoreError(err)
			}
		} else {
			if err := h.Db.Set(key, value); err != nil {
				return wrapStoreError(err)
			}
		}
		if get {
			if !exists {
				return nilBulk()
			}
			return proto.NewBulkString([]byte(oldVal))
		}
		return proto.OK
	case "GET":
		key := string(args[0])
		value, err := h.Db.Get(key)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				return nilBulk()
			}
			return wrapStoreError(err)
		}
		return proto.NewBulkString([]byte(value))
	case "DEL":
		count := int64(0)
		for _, arg := range args {
			deleted, err := h.Db.Del(string(arg))
			if err != nil {
				return wrapStoreError(err)
			}
			count += deleted
		}
		return proto.NewInteger(count)
	case "INCR":
		key := string(args[0])
		val, err := h.Db.INCR(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "DECR":
		key := string(args[0])
		val, err := h.Db.DECR(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "INCRBY":
		key := string(args[0])
		delta, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		val, err := h.Db.INCRBY(key, delta)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "DECRBY":
		key := string(args[0])
		delta, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		val, err := h.Db.DECRBY(key, delta)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(val)
	case "APPEND":
		key, value := string(args[0]), string(args[1])
		length, err := h.Db.APPEND(key, value)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(length))
	case "STRLEN":
		key := string(args[0])
		length, err := h.Db.StrLen(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(length))
	case "EXISTS":
		key := string(args[0])
		exists, err := h.Db.Exists(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "EXPIRE":
		key := string(args[0])
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		success, err := h.Db.Expire(key, seconds)
		if err != nil {
			return wrapStoreError(err)
		}
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TTL":
		key := string(args[0])
		ttl, err := h.Db.TTL(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(ttl)
	case "PERSIST":
		key := string(args[0])
		success, err := h.Db.Persist(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if success {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "TYPE":
		key := string(args[0])
		keyType, err := h.Db.Type(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewSimpleString(keyType)
	case "LPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.LPush(key, values...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "RPUSH":
		key := string(args[0])
		values := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			values[i-1] = string(args[i])
		}
		count, err := h.Db.RPush(key, values...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "LPOP":
		key := string(args[0])
		val, err := h.Db.LPop(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if val == "" {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(val))
	case "RPOP":
		key := string(args[0])
		val, err := h.Db.RPop(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if val == "" {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(val))
	case "LLEN":
		key := string(args[0])
		length, err := h.Db.LLen(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(length))
	case "LRANGE":
		key := string(args[0])
		start, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		stop, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		items, err := h.Db.LRange(key, start, stop)
		if err != nil {
			return wrapStoreError(err)
		}
		results := make([][]byte, len(items))
		for i, item := range items {
			results[i] = []byte(item)
		}
		return &proto.Array{Args: results}
	case "HSET":
		key := string(args[0])
		field, value := string(args[1]), string(args[2])
		if err := h.Db.HSet(key, field, value); err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(1)
	case "HGET":
		key, field := string(args[0]), string(args[1])
		val, err := h.Db.HGet(key, field)
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if err != nil || val == nil {
			return nilBulk()
		}
		return proto.NewBulkString(val)
	case "HGETALL":
		key := string(args[0])
		data, err := h.Db.HGetAll(key)
		if err != nil {
			return wrapStoreError(err)
		}
		flatArgs := make([][]byte, 0)
		for k, v := range data {
			flatArgs = append(flatArgs, []byte(k), []byte(v))
		}
		return &proto.Array{Args: flatArgs}
	case "HDEL":
		key := string(args[0])
		fields := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			fields[i-1] = string(args[i])
		}
		count, err := h.Db.HDel(key, fields...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "SADD":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, err := h.Db.SAdd(key, members...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "SMEMBERS":
		key := string(args[0])
		members, err := h.Db.SMembers(key)
		if err != nil {
			return wrapStoreError(err)
		}
		results := make([][]byte, len(members))
		for i, m := range members {
			results[i] = []byte(m)
		}
		return &proto.Array{Args: results}
	case "SISMEMBER":
		key, member := string(args[0]), string(args[1])
		exists, err := h.Db.SIsMember(key, member)
		if err != nil {
			return wrapStoreError(err)
		}
		if exists {
			return proto.NewInteger(1)
		}
		return proto.NewInteger(0)
	case "SCARD":
		key := string(args[0])
		count, err := h.Db.SCard(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "SREM":
		key := string(args[0])
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = string(args[i])
		}
		count, err := h.Db.SRem(key, members...)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "ZADD":
		key := string(args[0])
		members := make([]store.ZSetMember, 0)
		for i := 1; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(string(args[i]), 64)
			if err != nil {
				return proto.NewError("ERR value is not a valid float")
			}
			members = append(members, store.ZSetMember{Score: score, Member: string(args[i+1])})
		}
		if err := h.Db.ZAdd(key, members); err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(len(members)))
	case "ZREM":
		key := string(args[0])
		member := string(args[1])
		count, err := h.Db.ZRem(key, member)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(count)
	case "ZCARD":
		key := string(args[0])
		count, err := h.Db.ZCard(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(count))
	case "ZSCORE":
		key, member := string(args[0]), string(args[1])
		score, exists, err := h.Db.ZScore(key, member)
		if err != nil {
			return wrapStoreError(err)
		}
		if !exists {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
	case "ZINCRBY":
		key, member := string(args[0]), string(args[2])
		delta, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		newScore, err := h.Db.ZIncrBy(key, member, delta)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(newScore, 'f', -1, 64)))
	case "SPOP":
		key := string(args[0])
		if len(args) >= 2 {
			count, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			members, err := h.Db.SPopN(key, count)
			if err != nil {
				return wrapStoreError(err)
			}
			if len(members) == 0 {
				return &proto.Array{Args: [][]byte{}}
			}
			results := make([][]byte, len(members))
			for i, m := range members {
				results[i] = []byte(m)
			}
			return &proto.Array{Args: results}
		}
		member, err := h.Db.SPop(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if member == "" {
			return nilBulk()
		}
		return proto.NewBulkString([]byte(member))
	default:
		return proto.NewError(fmt.Sprintf("ERR command '%s' not supported in transaction", cmd))
	}
}

// copyList 复制列表
func (h *Handler) copyList(srcKey, dstKey string) bool {
	length, err := h.Db.LLen(srcKey)
	if err != nil {
		return false
	}
	if length == 0 {
		return true
	}
	items, err := h.Db.LRange(srcKey, 0, int64(length-1))
	if err != nil {
		return false
	}
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
	_, err = h.Db.RPush(dstKey, items...)
	return err == nil
}

// copyHash 复制Hash
func (h *Handler) copyHash(srcKey, dstKey string) bool {
	data, err := h.Db.HGetAll(srcKey)
	if err != nil {
		return false
	}
	if len(data) == 0 {
		return true
	}
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
	for k, v := range data {
		if err := h.Db.HSet(dstKey, k, v); err != nil {
			return false
		}
	}
	return true
}

// copySet 复制Set
func (h *Handler) copySet(srcKey, dstKey string) bool {
	members, err := h.Db.SMembers(srcKey)
	if err != nil {
		return false
	}
	if len(members) == 0 {
		return true
	}
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
	_, err = h.Db.SAdd(dstKey, members...)
	return err == nil
}

// copySortedSet 复制SortedSet
func (h *Handler) copySortedSet(srcKey, dstKey string) bool {
	members, err := h.Db.ZRange(srcKey, 0, -1)
	if err != nil {
		return false
	}
	if len(members) == 0 {
		return true
	}
	if _, err := h.Db.Del(dstKey); err != nil {
		return false
	}
	zMembers := make([]store.ZSetMember, len(members))
	for i, m := range members {
		zMembers[i] = store.ZSetMember{Score: m.Score, Member: m.Member}
	}
	err = h.Db.ZAdd(dstKey, zMembers)
	return err == nil
}

// parseSetOptions 解析 SET 命令的可选修饰符（EX/PX/EXAT/PXAT/NX/XX/GET/KEEPTTL）
func parseSetOptions(opts [][]byte) (ttl time.Duration, nx, xx, get, keepTTL bool, err error) {
	for i := 0; i < len(opts); i++ {
		opt := strings.ToUpper(string(opts[i]))
		switch opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			keepTTL = true
		case "EX":
			i++
			if i >= len(opts) {
				return 0, false, false, false, false, fmt.Errorf("syntax error")
			}
			sec, parseErr := strconv.Atoi(string(opts[i]))
			if parseErr != nil {
				return 0, false, false, false, false, fmt.Errorf("value is not an integer or out of range")
			}
			if sec <= 0 {
				return 0, false, false, false, false, fmt.Errorf("invalid expire time in 'set' command")
			}
			ttl = time.Duration(sec) * time.Second
		case "PX":
			i++
			if i >= len(opts) {
				return 0, false, false, false, false, fmt.Errorf("syntax error")
			}
			ms, parseErr := strconv.ParseInt(string(opts[i]), 10, 64)
			if parseErr != nil {
				return 0, false, false, false, false, fmt.Errorf("value is not an integer or out of range")
			}
			if ms <= 0 {
				return 0, false, false, false, false, fmt.Errorf("invalid expire time in 'set' command")
			}
			ttl = time.Duration(ms) * time.Millisecond
		case "EXAT":
			i++
			if i >= len(opts) {
				return 0, false, false, false, false, fmt.Errorf("syntax error")
			}
			ts, parseErr := strconv.ParseInt(string(opts[i]), 10, 64)
			if parseErr != nil {
				return 0, false, false, false, false, fmt.Errorf("value is not an integer or out of range")
			}
			remaining := ts - time.Now().Unix()
			if remaining < 0 {
				// Redis 语义：过去时间戳也是合法值，key 立即过期。
				// 使用极小正 TTL 使 key 在写入后立即过期删除。
				ttl = 1 * time.Nanosecond
			} else {
				ttl = time.Duration(remaining) * time.Second
			}
		case "PXAT":
			i++
			if i >= len(opts) {
				return 0, false, false, false, false, fmt.Errorf("syntax error")
			}
			ts, parseErr := strconv.ParseInt(string(opts[i]), 10, 64)
			if parseErr != nil {
				return 0, false, false, false, false, fmt.Errorf("value is not an integer or out of range")
			}
			remaining := ts - time.Now().UnixMilli()
			if remaining < 0 {
				// Redis 语义：过去时间戳也是合法值，key 立即过期。
				ttl = 1 * time.Nanosecond
			} else {
				ttl = time.Duration(remaining) * time.Millisecond
			}
		default:
			return 0, false, false, false, false, fmt.Errorf("syntax error")
		}
	}
	if nx && xx {
		return 0, false, false, false, false, fmt.Errorf("syntax error")
	}
	return
}

// setKeyWithOpts 执行 SET 命令的键值写入，处理所有可选修饰符。
func (h *Handler) setKeyWithOpts(state *connState, key, value string, ttl time.Duration, nx, xx, get, keepTTL bool) proto.RESP {
	var oldVal string
	exists := false
	oldVal, err := h.Db.Get(key)
	if err == nil {
		exists = true
	} else if !errors.Is(err, store.ErrKeyNotFound) && !errors.Is(err, store.ErrWrongType) {
		return wrapStoreError(err)
	}
	// SET ... GET performs a read of the old value; redis counts that lookup
	// (plain SET without GET records no keyspace stats).
	if get {
		h.recordKeyspaceLookup(key)
	}

	if nx && exists {
		if get {
			return nilBulkString(state.respVersion)
		}
		return nilBulkString(state.respVersion)
	}

	if xx && !exists {
		if get {
			return nilBulkString(state.respVersion)
		}
		return nilBulkString(state.respVersion)
	}

	// 成功写入路径：先标脏再落盘，与其它条件写"成功才脏"一致。
	h.markDirtyKeys(state, key)
	if keepTTL {
		ttlSec, ttlErr := h.Db.TTL(key)
		if err := h.Db.Set(key, value); err != nil {
			return wrapStoreError(err)
		}
		if ttlErr == nil && ttlSec > 0 {
			if _, err := h.Db.Expire(key, int(ttlSec)); err != nil {
				return wrapStoreError(err)
			}
		}
	} else if ttl > 0 {
		if ttl < time.Second {
			// 极短 TTL（如过去 EXAT/PXAT 的 1ns），BadgerDB 秒级存储无法保留。
			// 先写入值，再设过去过期时间戳强制立即过期。
			if err := h.Db.Set(key, value); err != nil {
				return wrapStoreError(err)
			}
			if _, err := h.Db.ExpireAt(key, 1); err != nil {
				return wrapLogError(err)
			}
		} else {
			if err := h.Db.SetWithTTL(key, value, ttl); err != nil {
				return wrapStoreError(err)
			}
		}
	} else {
		if err := h.Db.Set(key, value); err != nil {
			return wrapStoreError(err)
		}
	}

	if get {
		if !exists {
			return nilBulkString(state.respVersion)
		}
		return proto.NewBulkString([]byte(oldVal))
	}

	return proto.OK
}

// nilBulkString 返回 RESP2 或 RESP3 格式的 null bulk string
func nilBulkString(respVersion int) proto.RESP {
	if respVersion == 3 {
		return &proto.Null{}
	}
	return proto.NewBulkString(nil)
}
