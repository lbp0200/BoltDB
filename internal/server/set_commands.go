package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleSADD 实现 SADD 命令
func (h *Handler) handleSADD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SADD' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.SAdd(key, members...)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleSREM 实现 SREM 命令
func (h *Handler) handleSREM(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SREM' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.SRem(key, members...)
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if err != nil {
		return wrapLogError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleSCARD 实现 SCARD 命令
func (h *Handler) handleSCARD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SCARD' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	count, err := h.Db.SCard(key)
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if err != nil {
		return proto.NewInteger(0)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleSISMEMBER 实现 SISMEMBER 命令
func (h *Handler) handleSISMEMBER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SISMEMBER' command")
	}
	key, member := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	exists, err := h.Db.SIsMember(key, member)
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if err != nil {
		return proto.NewInteger(0)
	}
	return proto.NewInteger(int64(boolToInt(exists)))
}

// handleSMEMBERS 实现 SMEMBERS 命令
func (h *Handler) handleSMEMBERS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SMEMBERS' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	members, err := h.Db.SMembers(key)
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if err != nil {
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m)
	}
	return &proto.Array{Args: results}
}

// handleSPOP 实现 SPOP 命令
func (h *Handler) handleSPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SPOP' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}

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
		h.markDirtyKeys(state, key)
		// Single propagation path: SREM of actual members. processRequest
		// excludes SPOP via shouldPropagateCommand to avoid double-prop
		// (SREM + raw SPOP) which would drop extra members on the slave.
		if h.Replication != nil && h.Replication.IsMaster() {
			propArgs := make([][]byte, 2, 2+len(members))
			propArgs[0] = []byte("SREM")
			propArgs[1] = args[0]
			for _, m := range members {
				propArgs = append(propArgs, []byte(m))
			}
			h.Replication.PropagateCommand(propArgs)
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
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	h.markDirtyKeys(state, key)
	// See SPOP-N path: handler-only SREM propagation (not raw SPOP).
	if h.Replication != nil && h.Replication.IsMaster() {
		h.Replication.PropagateCommand([][]byte{[]byte("SREM"), args[0], []byte(member)})
	}
	return proto.NewBulkString([]byte(member))
}

// handleSRANDMEMBER 实现 SRANDMEMBER 命令
func (h *Handler) handleSRANDMEMBER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	if len(args) == 1 {
		// SRANDMEMBER key - return single member
		member, err := h.Db.SRandMember(key)
		if err != nil {
			return wrapStoreError(err)
		}
		if member == "" {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return proto.NewBulkString([]byte(member))
	}
	// SRANDMEMBER key count - return array of members
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	members, err := h.Db.SRandMemberN(key, count)
	if err != nil {
		return wrapStoreError(err)
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m)
	}
	return &proto.Array{Args: results}
}

// handleSMOVE 实现 SMOVE 命令
func (h *Handler) handleSMOVE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'SMOVE' command")
	}
	source, destination, member := string(args[0]), string(args[1]), string(args[2])
	if resp := h.checkAndHandleMultiKeyRedirect([]string{source, destination}); resp != nil {
		return resp
	}
	success, err := h.Db.SMove(source, destination, member)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(int64(boolToInt(success)))
}

// handleSINTER 实现 SINTER 命令
func (h *Handler) handleSINTER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SINTER' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	members, err := h.Db.SInter(keys...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m)
	}
	return &proto.Array{Args: results}
}

// handleSUNION 实现 SUNION 命令
func (h *Handler) handleSUNION(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SUNION' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	members, err := h.Db.SUnion(keys...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m)
	}
	return &proto.Array{Args: results}
}

// handleSDIFF 实现 SDIFF 命令
func (h *Handler) handleSDIFF(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SDIFF' command")
	}
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	members, err := h.Db.SDiff(keys...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return &proto.Array{Args: [][]byte{}}
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m)
	}
	return &proto.Array{Args: results}
}

// handleSINTERSTORE 实现 SINTERSTORE 命令
func (h *Handler) handleSINTERSTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SINTERSTORE' command")
	}
	destination := string(args[0])
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, destination)
	count, err := h.Db.SInterStore(destination, keys...)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleSMISMEMBER 实现 SMISMEMBER 命令
func (h *Handler) handleSMISMEMBER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SMISMEMBER' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	results, err := h.Db.SMIsMember(key, members...)
	if err != nil {
		return wrapStoreError(err)
	}
	elems := make([]proto.RESP, len(results))
	for i, v := range results {
		elems[i] = proto.Integer(v)
	}
	return &proto.NestedArray{Elems: elems}
}

// handleSINTERCARD 实现 SINTERCARD 命令
func (h *Handler) handleSINTERCARD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SINTERCARD' command")
	}
	numkeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numkeys < 1 || numkeys > len(args)-1 {
		return proto.NewError("ERR wrong number of arguments for 'SINTERCARD' command")
	}
	sinterKeys := make([]string, numkeys)
	for i := 0; i < numkeys; i++ {
		sinterKeys[i] = string(args[i+1])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(sinterKeys); resp != nil {
		return resp
	}
	// 可选 LIMIT n：统计到该值即提前停止（Redis 语义）
	var limit int64
	if numkeys+2 < len(args) && strings.ToUpper(string(args[numkeys+1])) == "LIMIT" {
		l, err := strconv.ParseInt(string(args[numkeys+2]), 10, 64)
		if err != nil || l < 0 {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		limit = l
	}
	count, err := h.Db.SInterCardWithLimit(limit, sinterKeys...)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(count)
}

// handleSUNIONSTORE 实现 SUNIONSTORE 命令
func (h *Handler) handleSUNIONSTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SUNIONSTORE' command")
	}
	destination := string(args[0])
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, destination)
	count, err := h.Db.SUnionStore(destination, keys...)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleSDIFFSTORE 实现 SDIFFSTORE 命令
func (h *Handler) handleSDIFFSTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SDIFFSTORE' command")
	}
	destination := string(args[0])
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, destination)
	count, err := h.Db.SDiffStore(destination, keys...)
	if err != nil {
		return wrapStoreError(err)
	}
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleSSCAN 实现 SSCAN 命令
func (h *Handler) handleSSCAN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// SSCAN key cursor [MATCH pattern] [COUNT count]
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'SSCAN' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	pattern := ""
	count := 10
	// Parse optional MATCH and COUNT
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			if opt == "MATCH" && i+1 < len(args) {
				pattern = string(args[i+1])
				i++
			} else if opt == "COUNT" && i+1 < len(args) {
				count, err = strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				i++
			}
		}
	}
	result, err := h.Db.SScan(key, cursor, pattern, count)
	if err != nil {
		return wrapStoreError(err)
	}
	// 返回格式: [cursor, [member1, member2, ...]]
	memberElems := make([]proto.RESP, len(result.Members))
	for i, m := range result.Members {
		memberElems[i] = proto.NewBulkString([]byte(m))
	}
	return &proto.NestedArray{
		Elems: []proto.RESP{
			proto.NewBulkString([]byte(strconv.FormatUint(result.Cursor, 10))),
			&proto.NestedArray{Elems: memberElems},
		},
	}

	// ==================== HSCAN ====================
}

// handleHSCAN 实现 HSCAN 命令
func (h *Handler) handleHSCAN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'HSCAN' command")
	}
	hscanKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, hscanKey); resp != nil {
		return resp
	}
	hscanCursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	hscanPattern := ""
	hscanCount := 10
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			if opt == "MATCH" && i+1 < len(args) {
				hscanPattern = string(args[i+1])
				i++
			} else if opt == "COUNT" && i+1 < len(args) {
				hscanCount, err = strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				i++
			}
		}
	}
	hscanResult, err := h.Db.HScan(hscanKey, hscanCursor, hscanPattern, hscanCount)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	fieldElems := make([]proto.RESP, 0, len(hscanResult.Fields)*2)
	for fieldName, fieldVal := range hscanResult.Fields {
		fieldElems = append(fieldElems, proto.NewBulkString([]byte(fieldName)))
		fieldElems = append(fieldElems, proto.NewBulkString(fieldVal))
	}
	return &proto.NestedArray{
		Elems: []proto.RESP{
			proto.NewBulkString([]byte(strconv.FormatUint(hscanResult.Cursor, 10))),
			&proto.NestedArray{Elems: fieldElems},
		},
	}

	// SortedSet命令 - 由于代码太长，这里只实现主要命令
}
