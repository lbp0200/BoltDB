package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleLPUSH 实现 LPUSH 命令
func (h *Handler) handleLPUSH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'LPUSH' command")
	}
	key := string(args[0])
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.LPush(key, values...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleRPUSH 实现 RPUSH 命令
func (h *Handler) handleRPUSH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'RPUSH' command")
	}
	key := string(args[0])
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.RPush(key, values...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleLPOP 实现 LPOP 命令
func (h *Handler) handleLPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'LPOP' command")
	}
	key := string(args[0])
	value, err := h.Db.LPop(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	if value == "" {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString([]byte(value))
}

// handleRPOP 实现 RPOP 命令
func (h *Handler) handleRPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'RPOP' command")
	}
	key := string(args[0])
	value, err := h.Db.RPop(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	if value == "" {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString([]byte(value))
}

// handleLLEN 实现 LLEN 命令
func (h *Handler) handleLLEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'LLEN' command")
	}
	key := string(args[0])
	length, err := h.Db.LLen(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewInteger(0)
	}
	// #nosec G115 - length is bounded by practical data size limits
	return proto.NewInteger(int64(length))
}

// handleLINDEX 实现 LINDEX 命令
func (h *Handler) handleLINDEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'LINDEX' command")
	}
	key := string(args[0])
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	value, err := h.Db.LIndex(key, index)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	if value == "" {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString([]byte(value))
}

// handleLRANGE 实现 LRANGE 命令
func (h *Handler) handleLRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'LRANGE' command")
	}
	key := string(args[0])
	start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
	stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err1 != nil || err2 != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	values, err := h.Db.LRange(key, start, stop)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, len(values))
	for i, v := range values {
		results[i] = []byte(v)
	}
	return &proto.Array{Args: results}
}

// handleLSET 实现 LSET 命令
func (h *Handler) handleLSET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'LSET' command")
	}
	key, value := string(args[0]), string(args[2])
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	if err := h.Db.LSet(key, index, value); err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleLTRIM 实现 LTRIM 命令
func (h *Handler) handleLTRIM(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'LTRIM' command")
	}
	key := string(args[0])
	start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
	stop, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err1 != nil || err2 != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	if err := h.Db.LTrim(key, start, stop); err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleLINSERT 实现 LINSERT 命令
func (h *Handler) handleLINSERT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'LINSERT' command")
	}
	key, pivot, value := string(args[0]), string(args[2]), string(args[3])
	where := strings.ToUpper(string(args[1]))
	if where != "BEFORE" && where != "AFTER" {
		return proto.NewError("ERR syntax error")
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.LInsert(key, where, pivot, value)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleLPOS 实现 LPOS 命令
func (h *Handler) handleLPOS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'LPOS' command")
	}
	key := string(args[0])
	element := string(args[1])
	rank := int64(0)
	count := int64(0)
	maxlen := int64(0)

	// 解析可选参数
	i := 2
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		if opt == "RANK" && i+1 < len(args) {
			r, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
			rank = r
			i += 2
		} else if opt == "COUNT" && i+1 < len(args) {
			c, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
			count = c
			i += 2
		} else if opt == "MAXLEN" && i+1 < len(args) {
			m, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
			maxlen = m
			i += 2
		} else {
			return proto.NewError("ERR syntax error")
		}
	}

	positions, err := h.Db.LPos(key, element, rank, count, maxlen)
	if err != nil {
		return wrapStoreError(err)
	}

	if len(positions) == 0 {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}

	if count == 0 && rank == 0 {
		// 返回单个位置
		return proto.NewInteger(positions[0])
	}

	// 返回多个位置
	result := make([][]byte, len(positions))
	for j, pos := range positions {
		result[j] = []byte(fmt.Sprintf("%d", pos))
	}
	return &proto.Array{Args: result}
}

// handleLREM 实现 LREM 命令
func (h *Handler) handleLREM(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'LREM' command")
	}
	key, value := string(args[0]), string(args[2])
	count, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	removed, err := h.Db.LRem(key, count, value)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(removed))
}

// handleRPOPLPUSH 实现 RPOPLPUSH 命令
func (h *Handler) handleRPOPLPUSH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'RPOPLPUSH' command")
	}
	source, destination := string(args[0]), string(args[1])
	h.markDirtyKeys(state, source, destination)
	value, err := h.Db.RPopLPush(source, destination)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	if value == "" {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString([]byte(value))
}

// handleLMOVE 实现 LMOVE 命令
func (h *Handler) handleLMOVE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'LMOVE' command")
	}
	source := string(args[0])
	destination := string(args[1])
	sourceDirection := strings.ToUpper(string(args[2]))
	destinationDirection := strings.ToUpper(string(args[3]))
	h.markDirtyKeys(state, source, destination)
	value, err := h.Db.LMove(source, destination, sourceDirection, destinationDirection)
	if err != nil {
		return wrapStoreError(err)
	}
	if value == "" {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString([]byte(value))
}

// handleBLMOVE 实现 BLMOVE 命令
func (h *Handler) handleBLMOVE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 5 {
		return proto.NewError("ERR wrong number of arguments for 'BLMOVE' command")
	}
	source := string(args[0])
	destination := string(args[1])
	sourceDirection := strings.ToUpper(string(args[2]))
	destinationDirection := strings.ToUpper(string(args[3]))
	timeout, err := strconv.ParseFloat(string(args[4]), 64)
	if err != nil {
		return proto.NewError("ERR timeout is not a float")
	}
	h.markDirtyKeys(state, source, destination)
	state.blocking.Store(true)
	value, err := h.Db.BLMoveBlocking(state.ctx, source, destination, sourceDirection, destinationDirection, timeout)
	state.blocking.Store(false)
	if err != nil {
		return wrapStoreError(err)
	}
	if value == "" {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString([]byte(value))
}

// handleLPUSHX 实现 LPUSHX 命令
func (h *Handler) handleLPUSHX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'LPUSHX' command")
	}
	key := string(args[0])
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.LPUSHX(key, values...)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleRPUSHX 实现 RPUSHX 命令
func (h *Handler) handleRPUSHX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'RPUSHX' command")
	}
	key := string(args[0])
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.RPUSHX(key, values...)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleBLPOP 实现 BLPOP 命令
func (h *Handler) handleBLPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'BLPOP' command")
	}
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i])
	}
	timeout, err := strconv.Atoi(string(args[len(args)-1]))
	if err != nil {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	state.blocking.Store(true)
	key, value, err := h.Db.BLPOPBlocking(state.ctx, keys, timeout)
	state.blocking.Store(false)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NilArray{}
	}
	if key == "" {
		return proto.NilArray{}
	}
	return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}
}

// handleBRPOP 实现 BRPOP 命令
func (h *Handler) handleBRPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'BRPOP' command")
	}
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i])
	}
	timeout, err := strconv.Atoi(string(args[len(args)-1]))
	if err != nil {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	state.blocking.Store(true)
	key, value, err := h.Db.BRPOPBlocking(state.ctx, keys, timeout)
	state.blocking.Store(false)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NilArray{}
	}
	if key == "" {
		return proto.NilArray{}
	}
	return &proto.Array{Args: [][]byte{[]byte(key), []byte(value)}}
}

// handleBRPOPLPUSH 实现 BRPOPLPUSH 命令
func (h *Handler) handleBRPOPLPUSH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'BRPOPLPUSH' command")
	}
	source, destination := string(args[0]), string(args[1])
	timeout, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	state.blocking.Store(true)
	value, err := h.Db.BRPOPLPUSHBlocking(state.ctx, source, destination, timeout)
	state.blocking.Store(false)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	if value == "" {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString([]byte(value))

	// Hash命令
}

// handleLMPOP 实现 LMPOP 命令
func (h *Handler) handleLMPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'LMPOP' command")
	}
	numKeys, kErr := strconv.Atoi(string(args[0]))
	if kErr != nil || numKeys < 1 || 1+numKeys+1 > len(args) {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	modifier := strings.ToUpper(string(args[1+numKeys]))
	if modifier != "LEFT" && modifier != "RIGHT" {
		return proto.NewError("ERR syntax error")
	}
	count := 1
	if len(args) >= 3+numKeys {
		if strings.ToUpper(string(args[2+numKeys])) == "COUNT" {
			if len(args) < 4+numKeys {
				return proto.NewError("ERR syntax error")
			}
			c, cErr := strconv.Atoi(string(args[3+numKeys]))
			if cErr != nil || c < 1 {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			count = c
		}
	}
	key, elements, err := h.Db.LMPop(keys, modifier, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if key == "" || len(elements) == 0 {
		return proto.NilArray{}
	}
	elemArgs := make([][]byte, len(elements))
	for i, e := range elements {
		elemArgs[i] = []byte(e)
	}
	return &proto.NestedArray{
		Elems: []proto.RESP{
			proto.NewBulkString([]byte(key)),
			&proto.Array{Args: elemArgs},
		},
	}
}
