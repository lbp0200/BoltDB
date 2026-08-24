package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleSETBIT 实现 SETBIT 命令
func (h *Handler) handleSETBIT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'SETBIT' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	offset, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	bit, err := strconv.ParseUint(string(args[2]), 10, 8)
	if err != nil || (bit != 0 && bit != 1) {
		return proto.NewError("ERR bit is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	newBit, err := h.Db.SetBit(key, int(offset), int(bit))
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(newBit))
}

// handleGETBIT 实现 GETBIT 命令
func (h *Handler) handleGETBIT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'GETBIT' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	offset, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	bit, err := h.Db.GetBit(key, int(offset))
	if err != nil {
		return wrapStoreError(err)
	}
	if bit == 0 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewInteger(int64(bit))
}

// handleBITCOUNT 实现 BITCOUNT 命令
func (h *Handler) handleBITCOUNT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'BITCOUNT' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	// BITCOUNT key [start end [BYTE | BIT]]
	// 检查 BITCOUNT key BYTE|BIT（缺少 start/end）→ 语法错误
	if len(args) == 2 {
		u := strings.ToUpper(string(args[1]))
		if u == "BYTE" || u == "BIT" {
			return proto.NewError("ERR syntax error")
		}
	}
	start := 0
	end := -1
	unit := "BYTE"
	if len(args) >= 3 {
		s, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		start = s
		e, err := strconv.Atoi(string(args[2]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		end = e
	}
	// 可选 BYTE/BIT 单位（Redis 7 语义）
	if len(args) >= 4 {
		u := strings.ToUpper(string(args[3]))
		if u != "BYTE" && u != "BIT" {
			return proto.NewError("ERR syntax error")
		}
		unit = u
	}
	count, err := h.Db.BitCountWithUnit(key, start, end, unit)
	if err != nil {
		return wrapStoreError(err)
	}
	if count == 0 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewInteger(int64(count))
}

// handleBITOP 实现 BITOP 命令
func (h *Handler) handleBITOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BITOP operation destkey key [key ...]
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'BITOP' command")
	}
	operation := strings.ToUpper(string(args[0]))
	destKey := string(args[1])
	sourceKeys := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		sourceKeys[i-2] = string(args[i])
	}
	// 验证操作类型
	if operation != "AND" && operation != "OR" && operation != "XOR" && operation != "NOT" {
		return proto.NewError("ERR syntax error")
	}
	// NOT 只能有一个源键
	if operation == "NOT" && len(sourceKeys) != 1 {
		return proto.NewError("ERR BITOP NOT must be called with exactly one source key")
	}
	allKeys := append([]string{destKey}, sourceKeys...)
	if resp := h.checkAndHandleMultiKeyRedirect(allKeys); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, destKey)
	length, err := h.Db.BitOp(operation, destKey, sourceKeys...)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(length))
}

// handleBITFIELD 实现 BITFIELD 命令
func (h *Handler) handleBITFIELD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BITFIELD key [GET type offset | SET type offset value | INCRBY type offset increment] ...
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'BITFIELD' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	operations := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		operations = append(operations, string(args[i]))
	}
	results, err := h.Db.BitField(key, operations)
	if err != nil {
		return wrapStoreError(err)
	}
	for _, r := range results {
		if v, ok := r.(int64); ok {
			if v == 0 {
				if exists, err := h.Db.Exists(key); err == nil && !exists {
					h.recordKeyspaceMiss()
				} else if err == nil && exists {
					h.recordKeyspaceHit()
				}
			} else {
				h.recordKeyspaceHit()
			}
		} else {
			h.recordKeyspaceHit()
		}
	}
	// Single operation returns integer, multiple operations return array
	if len(results) == 1 {
		switch v := results[0].(type) {
		case int64:
			return proto.NewInteger(v)
		case []interface{}:
			// Overflow case
			return proto.NewError(fmt.Sprintf("ERR overflow: %v", v))
		}
	}
	// Convert results to RESP array for multiple operations
	respArgs := make([][]byte, len(results))
	for i, r := range results {
		switch v := r.(type) {
		case int64:
			respArgs[i] = []byte(strconv.FormatInt(v, 10))
		case []interface{}:
			respArgs[i] = []byte(fmt.Sprintf("%v:%v", v[0], v[1]))
		}
	}
	return &proto.Array{Args: respArgs}
}

// handleBITFIELD_RO 实现 BITFIELD_RO 命令（只读版本，仅支持 GET）
func (h *Handler) handleBITFIELD_RO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BITFIELD_RO key [GET type offset ...]
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'BITFIELD_RO' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	// Validate that all operations are GET
	operations := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		op := strings.ToUpper(string(args[i]))
		if op != "GET" {
			return proto.NewError("ERR BITFIELD_RO only supports the GET subcommand")
		}
		operations = append(operations, string(args[i]))
		// Include the type and offset args
		if i+2 < len(args) {
			operations = append(operations, string(args[i+1]), string(args[i+2]))
			i += 2
		}
	}
	results, err := h.Db.BitField(key, operations)
	if err != nil {
		return wrapStoreError(err)
	}
	for _, r := range results {
		if v, ok := r.(int64); ok {
			if v == 0 {
				if exists, err := h.Db.Exists(key); err == nil && !exists {
					h.recordKeyspaceMiss()
				} else if err == nil && exists {
					h.recordKeyspaceHit()
				}
			} else {
				h.recordKeyspaceHit()
			}
		} else {
			h.recordKeyspaceHit()
		}
	}
	// Single operation returns integer, multiple operations return array
	if len(results) == 1 {
		if v, ok := results[0].(int64); ok {
			return proto.NewInteger(v)
		}
	}
	// Convert results to RESP array
	respArgs := make([][]byte, len(results))
	for i, r := range results {
		switch v := r.(type) {
		case int64:
			respArgs[i] = []byte(strconv.FormatInt(v, 10))
		case []interface{}:
			respArgs[i] = []byte(fmt.Sprintf("%v:%v", v[0], v[1]))
		}
	}
	return &proto.Array{Args: respArgs}
}

// handleBITPOS 实现 BITPOS 命令
func (h *Handler) handleBITPOS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BITPOS key bit [start [end [BYTE | BIT]]]
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'BITPOS' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	bit, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	start, end := 0, -1
	unit := "BYTE"
	if len(args) >= 3 {
		start, err = strconv.Atoi(string(args[2]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
	}
	if len(args) >= 4 {
		end, err = strconv.Atoi(string(args[3]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
	}
	// 可选 BYTE/BIT 单位（Redis 7 语义）
	if len(args) >= 5 {
		u := strings.ToUpper(string(args[4]))
		if u != "BYTE" && u != "BIT" {
			return proto.NewError("ERR syntax error")
		}
		unit = u
	}
	pos, err := h.Db.BitPosWithUnit(key, bit, start, end, unit)
	if err != nil {
		return wrapStoreError(err)
	}
	if pos == -1 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewInteger(int64(pos))
}

// handleBITLEN 实现 BITLEN 命令
func (h *Handler) handleBITLEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BITLEN key
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'BITLEN' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	length, err := h.Db.BitLen(key)
	if err != nil {
		return wrapStoreError(err)
	}
	if length == 0 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewInteger(int64(length))
}

// handlePFADD 实现 PFADD 命令
func (h *Handler) handlePFADD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PFADD' command")
	}
	key := string(args[0])
	// 检查集群重定向
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	elements := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		elements[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	changed, err := h.Db.PFAdd(key, elements...)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(changed)
}

// handlePFCOUNT 实现 PFCOUNT 命令
func (h *Handler) handlePFCOUNT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PFCOUNT' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	// 检查集群重定向
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	count, err := h.Db.PFCount(keys...)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(count)
}

// handlePFMERGE 实现 PFMERGE 命令
func (h *Handler) handlePFMERGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'PFMERGE' command")
	}
	destKey := string(args[0])
	sourceKeys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		sourceKeys[i-1] = string(args[i])
	}
	// 检查集群重定向
	if resp := h.checkAndHandleRedirect(state, destKey); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, destKey)
	err := h.Db.PFMerge(destKey, sourceKeys...)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handlePFINFO 实现 PFINFO 命令
func (h *Handler) handlePFINFO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PFINFO' command")
	}
	key := string(args[0])
	// 检查集群重定向
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	info, err := h.Db.PFInfo(key)
	if err != nil {
		return wrapStoreError(err)
	}
	// 返回数组格式: [key1, value1, key2, value2, ...]
	result := make([][]byte, 0, len(info)*2)
	for k, v := range info {
		result = append(result, []byte(k), []byte(strconv.FormatInt(v, 10)))
	}
	return &proto.Array{Args: result}
}
