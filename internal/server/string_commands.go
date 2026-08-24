package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleLCS 实现 LCS 命令 - 最长公共子序列
// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
func (h *Handler) handleLCS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'LCS' command")
	}

	key1 := string(args[0])
	key2 := string(args[1])
	if resp := h.checkAndHandleMultiKeyRedirect([]string{key1, key2}); resp != nil {
		return resp
	}

	// Parse optional modifiers
	withLen := false
	withIdx := false
	withMatchLen := false
	minMatchLen := 1

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "LEN":
			withLen = true
		case "IDX":
			withIdx = true
		case "WITHMATCHLEN":
			withMatchLen = true
		case "MINMATCHLEN":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error, MINMATCHLEN requires an argument")
			}
			ml, err := strconv.Atoi(string(args[i+1]))
			if err != nil || ml < 0 {
				return proto.NewError("ERR MINMATCHLEN must be a non-negative integer")
			}
			minMatchLen = ml
			i++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", string(args[i])))
		}
	}

	// LEN mode: only need the length
	if withLen && !withIdx {
		length, err := h.Db.GetLCSLength(key1, key2)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return proto.NewError("ERR no such key")
			}
			return wrapStoreError(err)
		}
		return proto.NewInteger(int64(length))
	}

	// IDX mode: need the full match data
	if withIdx {
		val1, val2, err := h.Db.GetLCSWithValues(key1, key2)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return proto.NewError("ERR no such key")
			}
			return wrapStoreError(err)
		}

		matches := store.ComputeLCSMatches(val1, val2, minMatchLen)

		// Build response: [matches..., [val1_len, val2_len]]
		elems := make([]proto.RESP, 0, len(matches)+1)
		for _, m := range matches {
			var matchElems []proto.RESP
			if withMatchLen {
				matchElems = make([]proto.RESP, 5)
				matchElems[0] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.StartA)))
				matchElems[1] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.EndA)))
				matchElems[2] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.StartB)))
				matchElems[3] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.EndB)))
				matchElems[4] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.MatchLen)))
			} else {
				matchElems = make([]proto.RESP, 4)
				matchElems[0] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.StartA)))
				matchElems[1] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.EndA)))
				matchElems[2] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.StartB)))
				matchElems[3] = proto.NewBulkString([]byte(fmt.Sprintf("%d", m.EndB)))
			}

			na := &proto.NestedArray{Elems: matchElems}
			elems = append(elems, na)
		}

		// Add [len_a, len_b] at the end
		lenArr := &proto.Array{Args: [][]byte{
			[]byte(fmt.Sprintf("%d", len(val1))),
			[]byte(fmt.Sprintf("%d", len(val2))),
		}}
		elems = append(elems, lenArr)

		return &proto.NestedArray{Elems: elems}
	}

	// Default: return the LCS string
	lcs, err := h.Db.GetLCS(key1, key2)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return proto.NewError("ERR no such key")
		}
		return wrapStoreError(err)
	}

	if lcs == "" {
		return &proto.BulkString{}
	}
	return proto.NewBulkString([]byte(lcs))
}

// handleAPPEND 实现 APPEND 命令
func (h *Handler) handleAPPEND(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'APPEND' command")
	}
	key, value := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key)
	length, err := h.Db.APPEND(key, value)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(length))
}

// handleSTRLEN 实现 STRLEN 命令
func (h *Handler) handleSTRLEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'STRLEN' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	length, err := h.Db.StrLen(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewInteger(0)
	}
	return proto.NewInteger(int64(length))
}

// handleINCR 实现 INCR 命令
func (h *Handler) handleINCR(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'INCR' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key)
	value, err := h.Db.INCR(key)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(value)
}

// handleINCRBY 实现 INCRBY 命令
func (h *Handler) handleINCRBY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'INCRBY' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	increment, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	value, err := h.Db.INCRBY(key, increment)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(value)
}

// handleDECR 实现 DECR 命令
func (h *Handler) handleDECR(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'DECR' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key)
	value, err := h.Db.DECR(key)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(value)
}

// handleDECRBY 实现 DECRBY 命令
func (h *Handler) handleDECRBY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'DECRBY' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	decrement, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	value, err := h.Db.DECRBY(key, decrement)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(value)
}

// handleSET 实现 SET 命令
func (h *Handler) handleSET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SET' command")
	}
	key, value := string(args[0]), string(args[1])

	// 检查集群重定向
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}

	ttl, nx, xx, get, keepTTL, err := parseSetOptions(args[2:])
	if err != nil {
		return wrapLogError(err)
	}

	h.markDirtyKeys(state, key)
	return h.setKeyWithOpts(state, key, value, ttl, nx, xx, get, keepTTL)
}

// handleGET 实现 GET 命令
func (h *Handler) handleGET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'GET' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	value, err := h.Db.Get(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			h.recordKeyspaceMiss()
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	h.recordKeyspaceHit()
	return proto.NewBulkString([]byte(value))
}

// handleGETDEL 实现 GETDEL 命令
func (h *Handler) handleGETDEL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'GETDEL' command")
	}
	gdKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, gdKey); resp != nil {
		return resp
	}
	gdValue, gdErr := h.Db.Get(gdKey)
	if gdErr != nil {
		if errors.Is(gdErr, store.ErrKeyNotFound) {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if errors.Is(gdErr, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(gdErr)
	}
	h.markDirtyKeys(state, gdKey)
	if _, err := h.Db.Del(gdKey); err != nil {
		return wrapLogError(err)
	}
	return proto.NewBulkString([]byte(gdValue))
}

// handleGETEX 实现 GETEX 命令
func (h *Handler) handleGETEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'GETEX' command")
	}
	gexKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, gexKey); resp != nil {
		return resp
	}
	gexSeconds := 0
	if len(args) > 1 {
		opt := strings.ToUpper(string(args[1]))
		if opt == "EX" && len(args) > 2 {
			s, err := strconv.Atoi(string(args[2]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			if s <= 0 {
				return proto.NewError("ERR invalid expire time in 'getex' command")
			}
			gexSeconds = s
		} else if opt == "PX" && len(args) > 2 {
			s, err := strconv.Atoi(string(args[2]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			if s <= 0 {
				return proto.NewError("ERR invalid expire time in 'getex' command")
			}
			gexSeconds = s / 1000
		} else if opt == "PERSIST" {
			h.markDirtyKeys(state, gexKey)
			if _, err := h.Db.Persist(gexKey); err != nil {
				return wrapStoreError(err)
			}
		} else {
			return proto.NewError("ERR syntax error")
		}
	}
	gexValue, gexErr := h.Db.Get(gexKey)
	if gexErr != nil {
		if errors.Is(gexErr, store.ErrKeyNotFound) {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		if errors.Is(gexErr, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(gexErr)
	}
	if gexSeconds > 0 {
		h.markDirtyKeys(state, gexKey)
		if _, err := h.Db.Expire(gexKey, gexSeconds); err != nil {
			return wrapLogError(err)
		}
	}
	return proto.NewBulkString([]byte(gexValue))
}

// handleSETEX 实现 SETEX 命令
func (h *Handler) handleSETEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'SETEX' command")
	}
	key, value := string(args[0]), string(args[2])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	seconds, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR invalid integer")
	}
	// Redis compatibility: SETEX rejects TTL <= 0
	if seconds <= 0 {
		return proto.NewError("ERR invalid expire time in 'setex' command")
	}
	h.markDirtyKeys(state, key)
	if err := h.Db.SetEX(key, value, seconds); err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handlePSETEX 实现 PSETEX 命令
func (h *Handler) handlePSETEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'PSETEX' command")
	}
	key, value := string(args[0]), string(args[2])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR invalid integer")
	}
	// Redis compatibility: PSETEX rejects TTL <= 0
	if milliseconds <= 0 {
		return proto.NewError("ERR invalid expire time in 'psetex' command")
	}
	h.markDirtyKeys(state, key)
	if err := h.Db.PSETEX(key, value, milliseconds); err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleSETNX 实现 SETNX 命令
func (h *Handler) handleSETNX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SETNX' command")
	}
	key, value := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key)
	success, err := h.Db.SetNX(key, value)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleGETSET 实现 GETSET 命令
func (h *Handler) handleGETSET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'GETSET' command")
	}
	key, value := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key)
	oldValue, err := h.Db.GetSet(key, value)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewBulkString([]byte(oldValue))
}

// handleMGET 实现 MGET 命令
func (h *Handler) handleMGET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'MGET' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	// 检查集群重定向
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	values, err := h.Db.MGet(keys...)
	if err != nil {
		return wrapStoreError(err)
	}
	results := make([][]byte, len(values))
	for i, v := range values {
		if v == "" {
			results[i] = nil
		} else {
			results[i] = []byte(v)
		}
	}
	// RESP3: missing keys must use the Null type ('_'); redis-py 8's RESP3
	// parser blocks forever on a RESP2 null bulk ('$-1') inside an array.
	if state.respVersion == 3 {
		elems := make([]proto.RESP, len(results))
		for i, v := range results {
			if v == nil {
				elems[i] = &proto.Null{}
			} else {
				elems[i] = proto.NewBulkString(v)
			}
		}
		return &proto.NestedArray{Elems: elems}
	}
	return &proto.Array{Args: results}
}

// handleMSET 实现 MSET 命令
func (h *Handler) handleMSET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 || len(args)%2 != 0 {
		return proto.NewError("ERR wrong number of arguments for 'MSET' command")
	}
	pairs := make([]string, len(args))
	for i, arg := range args {
		pairs[i] = string(arg)
	}
	// 检查集群重定向（取所有键中的第一个作为检查点）
	keys := make([]string, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, string(args[i]))
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	if err := h.Db.MSet(pairs...); err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleMSETNX 实现 MSETNX 命令
func (h *Handler) handleMSETNX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 || len(args)%2 != 0 {
		return proto.NewError("ERR wrong number of arguments for 'MSETNX' command")
	}
	pairs := make([]string, len(args))
	for i, arg := range args {
		pairs[i] = string(arg)
	}
	// 检查集群重定向（取所有键中的第一个作为检查点）
	keys := make([]string, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, string(args[i]))
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	success, err := h.Db.MSetNX(pairs...)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleINCRBYFLOAT 实现 INCRBYFLOAT 命令
func (h *Handler) handleINCRBYFLOAT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'INCRBYFLOAT' command")
	}
	key := string(args[0])
	// 检查集群重定向
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	increment, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	h.markDirtyKeys(state, key)
	value, err := h.Db.INCRBYFLOAT(key, increment)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))
}

// handleGETRANGE 实现 GETRANGE 命令
func (h *Handler) handleGETRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'GETRANGE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	start, err1 := strconv.Atoi(string(args[1]))
	end, err2 := strconv.Atoi(string(args[2]))
	if err1 != nil || err2 != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	value, err := h.Db.GetRange(key, start, end)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewBulkString([]byte(""))
	}
	return proto.NewBulkString([]byte(value))
}

// handleSETRANGE 实现 SETRANGE 命令
func (h *Handler) handleSETRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'SETRANGE' command")
	}
	key, value := string(args[0]), string(args[2])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	offset, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	length, err := h.Db.SetRange(key, offset, value)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - length is bounded by practical data size limits
	return proto.NewInteger(int64(length))

	// 通用键管理命令
}
