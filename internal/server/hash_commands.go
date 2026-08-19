package server

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleHSET 实现 HSET 命令
func (h *Handler) handleHSET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'HSET' command")
	}
	key := string(args[0])
	h.markDirtyKeys(state, key)
	count := 0
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		field, value := string(args[i]), args[i+1]
		if err := h.Db.HSet(key, field, string(value)); err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return wrapLogError(err)
		}
		count++
	}
	return proto.NewInteger(int64(count))
}

// handleHGET 实现 HGET 命令
func (h *Handler) handleHGET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'HGET' command")
	}
	key, field := string(args[0]), string(args[1])
	value, err := h.Db.HGet(key, field)
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if err != nil || value == nil {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewBulkString(value)
}

// handleHDEL 实现 HDEL 命令
func (h *Handler) handleHDEL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'HDEL' command")
	}
	key := string(args[0])
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.HDel(key, fields...)
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if err != nil {
		return wrapLogError(err)
	}
	return proto.NewInteger(int64(count))
}

// handleHLEN 实现 HLEN 命令
func (h *Handler) handleHLEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'HLEN' command")
	}
	key := string(args[0])
	length, err := h.Db.HLen(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewInteger(0)
	}
	// #nosec G115 - length is bounded by practical data size limits
	return proto.NewInteger(int64(length))
}

// handleHGETALL 实现 HGETALL 命令
func (h *Handler) handleHGETALL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'HGETALL' command")
	}
	key := string(args[0])
	data, err := h.Db.HGetAll(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, 0, len(data)*2)
	// 稳定字段顺序：map 遍历无序，客户端比较数组需要确定性输出
	// （与 XCLAIM "stable field order" 惯例一致）
	fields := make([]string, 0, len(data))
	for field := range data {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		results = append(results, []byte(field), data[field])
	}
	return &proto.Array{Args: results}
}

// handleHEXISTS 实现 HEXISTS 命令
func (h *Handler) handleHEXISTS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'HEXISTS' command")
	}
	key, field := string(args[0]), string(args[1])
	exists, err := h.Db.HExists(key, field)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewInteger(0)
	}
	return proto.NewInteger(int64(boolToInt(exists)))
}

// handleHKEYS 实现 HKEYS 命令
func (h *Handler) handleHKEYS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'HKEYS' command")
	}
	key := string(args[0])
	keys, err := h.Db.HKeys(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, len(keys))
	for i, k := range keys {
		results[i] = []byte(k)
	}
	return &proto.Array{Args: results}
}

// handleHVALS 实现 HVALS 命令
func (h *Handler) handleHVALS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'HVALS' command")
	}
	key := string(args[0])
	values, err := h.Db.HVals(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, len(values))
	copy(results, values)
	return &proto.Array{Args: results}
}

// handleHMSET 实现 HMSET 命令
func (h *Handler) handleHMSET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 || len(args)%2 == 0 {
		return proto.NewError("ERR wrong number of arguments for 'HMSET' command")
	}
	key := string(args[0])
	h.markDirtyKeys(state, key)
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		field, value := string(args[i]), string(args[i+1])
		if err := h.Db.HSet(key, field, value); err != nil {
			return wrapStoreError(err)
		}
	}
	return proto.OK
}

// handleHMGET 实现 HMGET 命令
func (h *Handler) handleHMGET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'HMGET' command")
	}
	key := string(args[0])
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i])
	}
	values, err := h.Db.HMGet(key, fields...)
	if err != nil {
		return wrapStoreError(err)
	}
	results := make([][]byte, len(values))
	for i, v := range values {
		if v == nil {
			results[i] = nil
		} else {
			results[i] = v
		}
	}
	return &proto.Array{Args: results}
}

// handleHSETNX 实现 HSETNX 命令
func (h *Handler) handleHSETNX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'HSETNX' command")
	}
	key, field, value := string(args[0]), string(args[1]), string(args[2])
	h.markDirtyKeys(state, key)
	success, err := h.Db.HSetNX(key, field, value)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleHINCRBY 实现 HINCRBY 命令
func (h *Handler) handleHINCRBY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'HINCRBY' command")
	}
	key, field := string(args[0]), string(args[1])
	increment, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	value, err := h.Db.HIncrBy(key, field, increment)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(value)
}

// handleHINCRBYFLOAT 实现 HINCRBYFLOAT 命令
func (h *Handler) handleHINCRBYFLOAT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'HINCRBYFLOAT' command")
	}
	key, field := string(args[0]), string(args[1])
	increment, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	h.markDirtyKeys(state, key)
	value, err := h.Db.HIncrByFloat(key, field, increment)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewBulkString([]byte(fmt.Sprintf("%.10g", value)))
}

// handleHSTRLEN 实现 HSTRLEN 命令
func (h *Handler) handleHSTRLEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'HSTRLEN' command")
	}
	key, field := string(args[0]), string(args[1])
	length, err := h.Db.HStrLen(key, field)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewInteger(0)
	}
	// #nosec G115 - length is bounded by practical data size limits
	return proto.NewInteger(int64(length))
}

// handleHRANDFIELD 实现 HRANDFIELD 命令
func (h *Handler) handleHRANDFIELD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'HRANDFIELD' command")
	}
	key := string(args[0])
	count := 1
	withValues := false
	// 解析可选参数: HRANDFIELD key [count [WITHVALUES]]
	if len(args) >= 2 {
		// 第二个参数可能是 count 或 WITHVALUES
		secondArg := strings.ToUpper(string(args[1]))
		if secondArg != "WITHVALUES" {
			// 是 count
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			count = c
		}
	}
	// 检查是否有 WITHVALUES 选项
	for i := 1; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "WITHVALUES" {
			withValues = true
		}
	}
	fields, values, err := h.Db.HRandField(key, count, withValues)
	if err != nil {
		return wrapStoreError(err)
	}
	// 构建响应
	if withValues {
		// 返回字段和值的交替数组
		result := make([][]byte, 0, len(fields)*2)
		for i, field := range fields {
			result = append(result, []byte(field))
			result = append(result, []byte(values[i]))
		}
		return &proto.Array{Args: result}
	}
	// 只返回字段
	result := make([][]byte, len(fields))
	for i, field := range fields {
		result[i] = []byte(field)
	}
	return &proto.Array{Args: result}

	// Set命令
}

// handleHRANDMEMBER 实现 HRANDMEMBER 命令
func (h *Handler) handleHRANDMEMBER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'HRANDMEMBER' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	count := 1 // default: return 1 field when no count specified
	withValues := false
	countSpecified := false
	if len(args) >= 2 {
		// Check if args[1] is WITHVALUES (no count)
		if strings.EqualFold(string(args[1]), "WITHVALUES") {
			withValues = true
		} else {
			c, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			count = c
			countSpecified = true
		}
	}
	for i := 2; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "WITHVALUES") {
			withValues = true
		}
	}
	// HRANDFIELD key 0 → empty array (Redis semantics)
	if countSpecified && count == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	entries, err := h.Db.HRandMember(key, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if len(entries) == 0 {
		if !countSpecified {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return &proto.Array{Args: [][]byte{}}
	}
	if !countSpecified && !withValues {
		return proto.NewBulkString([]byte(entries[0].Field))
	}
	if !countSpecified && withValues {
		return &proto.Array{Args: [][]byte{
			[]byte(entries[0].Field),
			entries[0].Value,
		}}
	}
	if withValues {
		result := make([][]byte, 0, len(entries)*2)
		for _, e := range entries {
			result = append(result, []byte(e.Field), e.Value)
		}
		return &proto.Array{Args: result}
	}
	result := make([][]byte, len(entries))
	for i, e := range entries {
		result[i] = []byte(e.Field)
	}
	return &proto.Array{Args: result}
}
