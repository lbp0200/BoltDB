package server

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleUNLINK 实现 UNLINK 命令（异步删除，语义等同 DEL 但立即返回）
func (h *Handler) handleUNLINK(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'UNLINK' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	// 检查集群重定向
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, keys...)
	count := int64(0)
	for _, arg := range args {
		key := string(arg)
		deleted, err := h.Db.Del(key)
		if err != nil {
			return wrapStoreError(err)
		}
		count += deleted
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(count)
}

// handleDEL 实现 DEL 命令
func (h *Handler) handleDEL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'DEL' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	// 检查集群重定向
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, keys...)
	count := int64(0)
	for _, arg := range args {
		key := string(arg)
		deleted, err := h.Db.Del(key)
		if err != nil {
			return wrapStoreError(err)
		}
		count += deleted
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(count)
}

// handleEXISTS 实现 EXISTS 命令
func (h *Handler) handleEXISTS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'EXISTS' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	// 检查集群重定向
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	count := 0
	for _, arg := range args {
		key := string(arg)
		exists, err := h.Db.Exists(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if exists {
			count++
			h.recordKeyspaceHit()
		} else {
			h.recordKeyspaceMiss()
		}
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleTYPE 实现 TYPE 命令
func (h *Handler) handleTYPE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TYPE' command")
	}
	key := string(args[0])
	// 检查集群重定向
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	keyType, err := h.Db.Type(key)
	if err != nil {
		return proto.NewSimpleString("none")
	}
	if keyType == "none" {
		h.recordKeyspaceMiss()
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewSimpleString(keyType)
}

// handleDUMP 实现 DUMP 命令
func (h *Handler) handleDUMP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'DUMP' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	data, err := h.Db.Dump(key)
	if err != nil {
		h.recordKeyspaceMiss()
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	h.recordKeyspaceHit()
	return proto.NewBulkString(data)
}

// handleRESTORE 实现 RESTORE 命令
func (h *Handler) handleRESTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'RESTORE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	// 必须在成功写入后才污染 WATCH，失败路径（参数错误、Restore 报错）不脏。
	// 这里先不标记，改为在每次 Restore 成功后标记。
	// 解析 TTL（毫秒）
	var ttl time.Duration = 0
	replace := false
	if len(args) > 2 {
		ttlArg := string(args[1])
		// 检查是否是 TTL（数字）而不是序列化数据
		if ttlMS, err := strconv.ParseInt(ttlArg, 10, 64); err == nil {
			// 参数位置偏移：key, ttl, serializedData, [REPLACE|ABSTTL]
			if len(args) < 3 {
				return proto.NewError("ERR wrong number of arguments for 'RESTORE' command")
			}
			// 序列化数据现在在 args[2]
			absttl := false
			for i := 3; i < len(args); i++ {
				upper := strings.ToUpper(string(args[i]))
				switch upper {
				case "REPLACE":
					replace = true
				case "ABSTTL":
					absttl = true
				}
			}
			if absttl {
				// ABSTTL: TTL 是绝对时间戳（毫秒）
				now := time.Now().UnixMilli()
				if ttlMS > now {
					ttl = time.Duration(ttlMS-now) * time.Millisecond
				}
			} else {
				// TTL 是相对时间（毫秒）
				ttl = time.Duration(ttlMS) * time.Millisecond
			}
			serializedData := string(args[2])
			err := h.Db.Restore(key, []byte(serializedData), ttl, replace)
			if err != nil {
				return wrapStoreError(err)
			}
			h.markDirtyKeys(state, key)
			return proto.OK
		}
	}
	// 旧格式：key, serializedData, [REPLACE]
	serializedData := string(args[1])
	for i := 2; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "REPLACE" {
			replace = true
			break
		}
	}
	err := h.Db.Restore(key, []byte(serializedData), ttl, replace)
	if err != nil {
		return wrapStoreError(err)
	}
	h.markDirtyKeys(state, key)
	return proto.OK
}

// handleOBJECT 实现 OBJECT 命令
func (h *Handler) handleOBJECT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'OBJECT' command")
	}
	subcommand := strings.ToUpper(string(args[0]))

	// HELP doesn't require a key argument
	if subcommand == "HELP" {
		response := [][]byte{
			[]byte("OBJECT <subcommand> [<arg> ...]"),
			[]byte("Subcommands:"),
			[]byte("ENCODING"),
			[]byte("  -- Return the internal encoding of an object."),
			[]byte("FREQ"),
			[]byte("  -- Return the access frequency of an object (always 0; no LFU)."),
			[]byte("HELP"),
			[]byte("  -- Return this help text."),
			[]byte("IDLETIME"),
			[]byte("  -- Return the idle time of an object."),
			[]byte("REFCOUNT"),
			[]byte("  -- Return the reference count of an object."),
		}
		return &proto.Array{Args: response}
	}

	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'OBJECT' command")
	}
	key := string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}

	switch subcommand {
	case "REFCOUNT":
		refcount, err := h.Db.ObjectRefCount(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if refcount == 0 {
			h.recordKeyspaceMiss()
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		h.recordKeyspaceHit()
		return proto.NewInteger(refcount)
	case "ENCODING":
		encoding, err := h.Db.ObjectEncoding(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if encoding == "" {
			h.recordKeyspaceMiss()
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		h.recordKeyspaceHit()
		return proto.NewBulkString([]byte(encoding))
	case "IDLETIME":
		// BadgerDB 不维护访问时间，空闲时间恒为 0；但与其他 OBJECT
		// 子命令保持一致：不存在的 key 返回 nil（RESP3）/ 空 bulk（RESP2）。
		exists, err := h.Db.Exists(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if !exists {
			h.recordKeyspaceMiss()
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		h.recordKeyspaceHit()
		idletime, err := h.Db.ObjectIdleTime(key)
		if err != nil {
			return wrapStoreError(err)
		}
		return proto.NewInteger(idletime)
	case "FREQ":
		// BoltDB doesn't support LFU, so the frequency is always 0.
		// Keep consistency with other OBJECT subcommands: a non-existent
		// key returns nil (RESP3) / empty bulk (RESP2), not an integer.
		exists, err := h.Db.Exists(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if !exists {
			h.recordKeyspaceMiss()
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		h.recordKeyspaceHit()
		return proto.NewInteger(0)
	default:
		return proto.NewError("ERR syntax error")
	}
}

// handleEXPIRE 实现 EXPIRE 命令
func (h *Handler) handleEXPIRE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'EXPIRE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	seconds, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	// 可选 NX/XX/GT/LT 条件（Redis 7 语义）
	if len(args) >= 3 {
		cond := strings.ToUpper(string(args[2]))
		switch cond {
		case "NX", "XX", "GT", "LT":
			// 条件判断：基于当前 TTL（-1 = 无过期时间）
			curTTL, ttlErr := h.Db.TTL(key)
			if ttlErr != nil {
				return wrapStoreError(ttlErr)
			}
			switch cond {
			case "NX":
				if curTTL != -1 {
					return proto.NewInteger(0) // 已有过期时间，不设置
				}
			case "XX":
				if curTTL == -1 {
					return proto.NewInteger(0) // 无过期时间，不设置
				}
			case "GT":
				// Redis 语义：key 无 TTL（curTTL==-1）视为无限 TTL，
				// GT 要求新 TTL 大于当前，任何有限 TTL 都不大于无限 → 不设置。
				if curTTL == -1 || int64(seconds) <= curTTL {
					return proto.NewInteger(0) // key 无 TTL 或新 TTL 不晚于当前，不设置
				}
			case "LT":
				if curTTL != -1 && int64(seconds) >= curTTL {
					return proto.NewInteger(0) // 新 TTL 不早于当前
				}
			}
		default:
			return proto.NewError(fmt.Sprintf("ERR unsupported option '%s'", string(args[2])))
		}
	}
	h.markDirtyKeys(state, key)
	success, err := h.Db.Expire(key, seconds)
	if err != nil {
		return wrapStoreError(err)
	}
	if success && seconds <= 0 {
		h.recordExpired()
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleEXPIREAT 实现 EXPIREAT 命令
func (h *Handler) handleEXPIREAT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'EXPIREAT' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	success, err := h.Db.ExpireAt(key, timestamp)
	if err != nil {
		return wrapStoreError(err)
	}
	if success && timestamp <= time.Now().Unix() {
		h.recordExpired()
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handlePEXPIRE 实现 PEXPIRE 命令
func (h *Handler) handlePEXPIRE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'PEXPIRE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	success, err := h.Db.PExpire(key, milliseconds)
	if err != nil {
		return wrapStoreError(err)
	}
	if success && milliseconds <= 0 {
		h.recordExpired()
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handlePEXPIREAT 实现 PEXPIREAT 命令
func (h *Handler) handlePEXPIREAT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'PEXPIREAT' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	success, err := h.Db.PExpireAt(key, timestamp)
	if err != nil {
		return wrapStoreError(err)
	}
	if success && timestamp <= time.Now().UnixMilli() {
		h.recordExpired()
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleTTL 实现 TTL 命令
func (h *Handler) handleTTL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TTL' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	ttl, err := h.Db.TTL(key)
	if err != nil {
		return proto.NewInteger(-2)
	}
	if ttl == -2 {
		h.recordKeyspaceMiss()
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewInteger(ttl)
}

// handlePTTL 实现 PTTL 命令
func (h *Handler) handlePTTL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PTTL' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	pttl, err := h.Db.PTTL(key)
	if err != nil {
		return proto.NewInteger(-2)
	}
	if pttl == -2 {
		h.recordKeyspaceMiss()
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewInteger(pttl)
}

// handleEXPIRETIME 实现 EXPIRETIME 命令
func (h *Handler) handleEXPIRETIME(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	return h.handleExpireTime(state, args, remoteAddr)
}

// handlePEXPIRETIME 实现 PEXPIRETIME 命令
func (h *Handler) handlePEXPIRETIME(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	return h.handlePExpireTime(state, args, remoteAddr)
}

// handlePERSIST 实现 PERSIST 命令
func (h *Handler) handlePERSIST(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PERSIST' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key)
	success, err := h.Db.Persist(key)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleRENAME 实现 RENAME 命令
func (h *Handler) handleRENAME(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'RENAME' command")
	}
	key, newKey := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key, newKey)
	if err := h.Db.Rename(key, newKey); err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleRENAMENX 实现 RENAMENX 命令
func (h *Handler) handleRENAMENX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'RENAMENX' command")
	}
	key, newKey := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key, newKey)
	success, err := h.Db.RenameNX(key, newKey)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleCOPY 实现 COPY 命令
func (h *Handler) handleCOPY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'COPY' command")
	}
	srcKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, srcKey); resp != nil {
		return resp
	}
	dstKey := string(args[1])
	replace := false
	db := int(0)
	i := 2
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "REPLACE":
			replace = true
			i++
		case "DB":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			dbNum, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			db = dbNum
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}
	// 单 DB 服务器：DB 0 合法（显式指定默认库），>0 不支持
	if db > 0 {
		return proto.NewError("ERR DB option not supported")
	}
	// 获取源键类型
	srcType, err := h.Db.Type(srcKey)
	if err != nil {
		return wrapStoreError(err)
	}
	if srcType == "none" {
		h.recordKeyspaceMiss()
		return proto.NewInteger(0) // 源键不存在
	}
	h.recordKeyspaceHit()
	// 检查目标键是否存在
	dstExists, err := h.Db.Exists(dstKey)
	if err != nil {
		return wrapStoreError(err)
	}
	if dstExists && !replace {
		return proto.NewInteger(0) // 目标存在且不替换
	}
	h.markDirtyKeys(state, srcKey, dstKey)
	// 根据类型复制
	var copied bool
	switch srcType {
	case "string":
		val, err := h.Db.Get(srcKey)
		if err == nil {
			err = h.Db.Set(dstKey, val)
		}
		copied = err == nil
	case "list":
		copied = h.copyList(srcKey, dstKey)
	case "hash":
		copied = h.copyHash(srcKey, dstKey)
	case "set":
		copied = h.copySet(srcKey, dstKey)
	case "zset":
		copied = h.copySortedSet(srcKey, dstKey)
	default:
		return proto.NewError("ERR unknown type")
	}
	if !copied {
		return proto.NewError("ERR copy failed")
	}
	// Preserve source key's TTL on the destination (Redis compat)
	srcTTL, ttlErr := h.Db.TTL(srcKey)
	if ttlErr == nil && srcTTL > 0 {
		// srcTTL is in seconds; set the same expiry on dstKey
		_, _ = h.Db.Expire(dstKey, int(srcTTL))
	}
	return proto.NewInteger(1)
}

// handleSWAPDB 实现 SWAPDB 命令
func (h *Handler) handleSWAPDB(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SWAPDB' command")
	}
	// BoltDB 是单数据库实现，SWAPDB 是空操作
	return proto.NewSimpleString("OK")
}

// handleTOUCH 实现 TOUCH 命令
func (h *Handler) handleTOUCH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TOUCH' command")
	}
	count := int64(0)
	for _, arg := range args {
		key := string(arg)
		if resp := h.checkAndHandleRedirect(state, key); resp != nil {
			return resp
		}
		exists, err := h.Db.Exists(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if exists {
			count++
			h.recordKeyspaceHit()
		} else {
			h.recordKeyspaceMiss()
		}
	}
	return proto.NewInteger(count)
}

// handleSHUTDOWN 实现 SHUTDOWN 命令
// SHUTDOWN [NOSAVE|SAVE] [NOW|FORCE|ABORT]
// 触发优雅关闭：通过 OnShutdown 钩子（main 注入的 cancel()）让
// ServeTCP 返回，随后 main 执行 replMgr.Stop → handler.Shutdown →
// backupMgr.Wait → db.Close 的完整关闭序列。数据本身由 BadgerDB
// 持久化，SAVE/NOSAVE 不改变行为（无独立 RDB 快照需要落盘）。
func (h *Handler) handleSHUTDOWN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// 参数校验：可选 [NOSAVE|SAVE]，Redis 7+ 追加 [NOW|FORCE|ABORT]
	for _, arg := range args {
		opt := strings.ToUpper(string(arg))
		switch opt {
		case "NOSAVE", "SAVE", "NOW", "FORCE", "ABORT":
			// 接受的选项（语义等价：触发优雅关闭）
		default:
			return proto.NewError(fmt.Sprintf("ERR Unknown option %s", opt))
		}
	}
	logger.Logger.Warn().Str("remote_addr", remoteAddr).Msg("SHUTDOWN command received, initiating graceful shutdown")
	if h.OnShutdown != nil {
		// 触发关闭序列。注意不能在当前连接 goroutine 里直接等待
		// handler.Shutdown()（wg.Wait 会等自己），而是通过 cancel()
		// 让 ServeTCP 返回，由 main 主流程执行关闭序列。
		h.OnShutdown()
	}
	return proto.OK
}

// handleKEYS 实现 KEYS 命令
func (h *Handler) handleKEYS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'KEYS' command")
	}
	pattern := string(args[0])
	if resp := h.checkAndHandleRedirect(state, pattern); resp != nil {
		return resp
	}
	keys, err := h.Db.Keys(pattern)
	if err != nil {
		return wrapStoreError(err)
	}
	if len(keys) == 0 {
		h.recordKeyspaceMiss()
	} else {
		h.recordKeyspaceHit()
	}
	results := make([][]byte, len(keys))
	for i, k := range keys {
		results[i] = []byte(k)
	}
	return &proto.Array{Args: results}
}

// handleSCAN 实现 SCAN 命令
func (h *Handler) handleSCAN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	cursor := uint64(0)
	pattern := "*"
	count := 10
	if len(args) >= 1 {
		var err error
		cursor, err = strconv.ParseUint(string(args[0]), 10, 64)
		if err != nil {
			return proto.NewError("ERR invalid cursor")
		}
	}
	if len(args) >= 3 && strings.ToUpper(string(args[1])) == "MATCH" {
		pattern = string(args[2])
	}
	if len(args) >= 5 && strings.ToUpper(string(args[3])) == "COUNT" {
		var err error
		count, err = strconv.Atoi(string(args[4]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
	}
	result, err := h.Db.Scan(cursor, pattern, count)
	if err != nil {
		return wrapStoreError(err)
	}
	if len(result.Keys) == 0 {
		if cursor == 0 {
			h.recordKeyspaceMiss()
		} else {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	// 返回嵌套数组格式: [cursor, [key1, key2, ...]]
	return proto.NewScanResponse(result.Cursor, result.Keys)
}

// handleRANDOMKEY 实现 RANDOMKEY 命令
func (h *Handler) handleRANDOMKEY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	key, err := h.Db.RandomKey()
	if err != nil || key == "" {
		h.recordKeyspaceMiss()
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	h.recordKeyspaceHit()
	return proto.NewBulkString([]byte(key))

	// List命令
}

// handleSORT_RO 实现 SORT_RO 命令（只读变体，禁止 STORE 选项）。
func (h *Handler) handleSORT_RO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	for _, a := range args[1:] {
		if strings.ToUpper(string(a)) == "STORE" {
			return proto.NewError("ERR syntax error")
		}
	}
	return h.handleSORT(state, args, remoteAddr)
}

// handleSORT 实现 SORT 命令
func (h *Handler) handleSORT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SORT' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}

	// Parse options
	var offset, count int64 = 0, -1
	var getPatterns []string
	var asc = true
	var alpha bool
	var destKey string
	var byPattern string

	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "BY":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			byPattern = string(args[i+1])
			i += 2
		case "LIMIT":
			if i+2 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			parseResult, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			offset = parseResult
			count, err = strconv.ParseInt(string(args[i+2]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			i += 3
		case "GET":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			getPatterns = append(getPatterns, string(args[i+1]))
			i += 2
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
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			destKey = string(args[i+1])
			i += 2
		default:
			i++
		}
	}

	// Get source type
	keyType, err := h.Db.Type(key)
	if err != nil {
		return wrapStoreError(err)
	}
	// Redis: SORT on non-existent key returns empty array
	if keyType == "none" {
		h.recordKeyspaceMiss()
		if destKey != "" {
			_, _ = h.Db.Del(destKey)
		}
		return &proto.Array{Args: [][]byte{}}
	}
	h.recordKeyspaceHit()
	var values []string
	var scores []float64

	switch keyType {
	case "list":
		listValues, err := h.Db.LRange(key, 0, -1)
		if err != nil {
			return wrapStoreError(err)
		}
		values = listValues
	case "set":
		setValues, err := h.Db.SMembers(key)
		if err != nil {
			return wrapStoreError(err)
		}
		values = setValues
	case "string":
		val, err := h.Db.Get(key)
		if err != nil {
			return wrapStoreError(err)
		}
		values = []string{val}
	case "zset":
		members, err := h.Db.ZRange(key, 0, -1)
		if err != nil {
			return wrapStoreError(err)
		}
		for _, m := range members {
			values = append(values, m.Member)
			scores = append(scores, m.Score)
		}
	default:
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Apply BY pattern - get weights from external keys
	if byPattern != "" && len(values) > 0 {
		weights := make([]float64, len(values))
		for idx, val := range values {
			targetKey := strings.Replace(byPattern, "*", val, 1)
			weightVal, err := h.Db.Get(targetKey)
			if err != nil {
				// Redis: BY key not found → weight = 0 (skip, don't error)
				weights[idx] = 0
				continue
			}
			if weightVal != "" {
				if f, err := strconv.ParseFloat(weightVal, 64); err == nil {
					weights[idx] = f
				} else {
					weights[idx] = float64(idx)
				}
			} else {
				weights[idx] = float64(idx)
			}
		}
		scores = weights
		// When using BY, sort by scores (numeric)
		alpha = false
	}

	// Sort values
	if len(scores) == 0 && !alpha && len(values) > 0 {
		// Numeric sort
		scores = make([]float64, len(values))
		for idx, v := range values {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				scores[idx] = f
			} else {
				scores[idx] = 0
			}
		}
	}

	// Sort values using standard library (O(n log n))
	// Use helper struct to keep values and scores in sync
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

	// Apply LIMIT
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

	// Apply GET patterns
	if len(getPatterns) > 0 {
		finalValues := make([]string, 0)
		for _, pattern := range getPatterns {
			for _, val := range values {
				targetKey := strings.Replace(pattern, "*", val, 1)
				targetVal, err := h.Db.Get(targetKey)
				if err != nil {
					// Redis: GET key not found → empty string (don't error)
					finalValues = append(finalValues, "")
					continue
				}
				finalValues = append(finalValues, targetVal)
			}
		}
		values = finalValues
	}

	// STORE
	if destKey != "" {
		h.markDirtyKeys(state, destKey)
		// Store as a list
		for idx, v := range values {
			if idx == 0 {
				if _, err := h.Db.Del(destKey); err != nil {
					return wrapStoreError(err)
				}
			}
			if _, err := h.Db.RPush(destKey, v); err != nil {
				return wrapStoreError(err)
			}
		}
		// SORT STORE is propagated once via processRequest (full command args).
		// Do not PropagateCommand here — that double-fired and grew offset twice.
		return proto.NewInteger(int64(len(values)))
	}

	// Return result
	results := make([][]byte, len(values))
	for idx, v := range values {
		results[idx] = []byte(v)
	}
	return &proto.Array{Args: results}

	// ==================== AUTH ====================
}
