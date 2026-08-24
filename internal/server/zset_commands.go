package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleZINTER 实现 ZINTER 命令（Redis 7.0+）
func (h *Handler) handleZINTER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZINTER' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	if numKeys < 1 {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	weights := []float64{}
	aggregate := "SUM"
	withScores := false
	i := 1 + numKeys
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "WEIGHTS":
			if i+numKeys >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			weights = make([]float64, numKeys)
			for w := 0; w < numKeys; w++ {
				f, err := strconv.ParseFloat(string(args[i+1+w]), 64)
				if err != nil {
					return proto.NewError("ERR value is not a float")
				}
				weights[w] = f
			}
			i += numKeys + 1
		case "AGGREGATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[i+1]))
			if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
				return proto.NewError("ERR syntax error")
			}
			i += 2
		case "WITHSCORES":
			withScores = true
			i++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", args[i]))
		}
	}
	members, err := h.Db.ZInter(keys, weights, aggregate)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if len(members) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	if withScores {
		return zrangeWithScoresReply(state, members)
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m.Member)
	}
	return &proto.Array{Args: result}
}

// zrangeWithScoresReply 组装 WITHSCORES 响应：RESP3 为 array of [member,
// Double]（Redis 8 RESP3 wire 语义），RESP2 为扁平 [member, score, ...]。
func zrangeWithScoresReply(state *connState, members []store.ZSetMember) proto.RESP {
	if state.respVersion == 3 {
		elems := make([]proto.RESP, 0, len(members))
		for _, m := range members {
			elems = append(elems, &proto.NestedArray{Elems: []proto.RESP{
				proto.NewBulkString([]byte(m.Member)),
				&proto.Double{Value: m.Score},
			}})
		}
		return &proto.NestedArray{Elems: elems}
	}
	results := make([][]byte, 0, len(members)*2)
	for _, m := range members {
		results = append(results, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
	}
	return &proto.Array{Args: results}
}

// handleZINTERCARD 实现 ZINTERCARD 命令（Redis 7.0+），返回交集基数
func (h *Handler) handleZINTERCARD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// ZINTERCARD numkeys key [key ...] [LIMIT limit]
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZINTERCARD' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys < 1 {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	if 1+numKeys > len(args) {
		return proto.NewError("ERR wrong number of arguments for 'ZINTERCARD' command")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	var limit int64
	i := 1 + numKeys
	for i < len(args) {
		if strings.ToUpper(string(args[i])) == "LIMIT" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			limit, err = strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || limit < 0 {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			i += 2
		} else {
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", args[i]))
		}
	}
	count, err := h.Db.ZInterCard(keys, limit)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(count)
}

// handleZUNION 实现 ZUNION 命令（Redis 7.0+）
func (h *Handler) handleZUNION(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZUNION' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	if numKeys < 1 {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	weights := []float64{}
	aggregate := "SUM"
	withScores := false
	i := 1 + numKeys
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "WEIGHTS":
			if i+numKeys >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			weights = make([]float64, numKeys)
			for w := 0; w < numKeys; w++ {
				f, err := strconv.ParseFloat(string(args[i+1+w]), 64)
				if err != nil {
					return proto.NewError("ERR value is not a float")
				}
				weights[w] = f
			}
			i += numKeys + 1
		case "AGGREGATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[i+1]))
			if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
				return proto.NewError("ERR syntax error")
			}
			i += 2
		case "WITHSCORES":
			withScores = true
			i++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", args[i]))
		}
	}
	members, err := h.Db.ZUnion(keys, weights, aggregate)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if len(members) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	if withScores {
		return zrangeWithScoresReply(state, members)
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m.Member)
	}
	return &proto.Array{Args: result}
}

// handleZADD 实现 ZADD 命令
func (h *Handler) handleZADD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZADD' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	var opts store.ZAddOptions
	members := make([]store.ZSetMember, 0)

	// Parse options: NX XX GT LT CH INCR (only one of NX/XX, GT/LT allowed)
	i := 1
parseOpts:
	for i < len(args)-1 {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			opts.NX = true
			i++
		case "XX":
			opts.XX = true
			i++
		case "GT":
			opts.GT = true
			i++
		case "LT":
			opts.LT = true
			i++
		case "CH":
			opts.CH = true
			i++
		case "INCR":
			opts.INCR = true
			i++
		default:
			break parseOpts
		}
	}
	if opts.NX && opts.XX {
		return proto.NewError("ERR XX and NX options at the same time are not compatible")
	}
	if opts.GT && opts.LT {
		return proto.NewError("ERR GT, LT, and/or NX options at the same time are not compatible")
	}
	if opts.INCR && len(args)-i > 2 {
		return proto.NewError("ERR INCR option supports a single increment-element pair")
	}

	for ; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		score, err := strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			return proto.NewError("ERR value is not a valid float")
		}
		member := string(args[i+1])
		members = append(members, store.ZSetMember{Member: member, Score: score})
	}
	if len(members) == 0 {
		return proto.NewError("ERR wrong number of arguments for 'ZADD' command")
	}

	h.markDirtyKeys(state, key)
	changed, err := h.Db.ZAddWithOptions(key, opts, members)
	if err != nil {
		return wrapStoreError(err)
	}
	// INCR 模式返回新 score（BulkString），其余返回变更计数
	if opts.INCR {
		if changed == 0 {
			// 未变更（NX 命中或 XX 不命中）：返回 nil
			return proto.NewBulkString(nil)
		}
		// 读取实际 score（INCR 只有一个成员）
		score, _, sErr := h.Db.ZScore(key, members[0].Member)
		if sErr != nil {
			return wrapStoreError(sErr)
		}
		return proto.NewBulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
	}
	return proto.NewInteger(changed)
}

// handleZREM 实现 ZREM 命令
func (h *Handler) handleZREM(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZREM' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, key)
	count := 0
	for i := 1; i < len(args); i++ {
		member := string(args[i])
		deleted, err := h.Db.ZRem(key, member)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			if errors.Is(err, store.ErrMemberNotFound) {
				continue
			}
			return wrapLogError(err)
		}
		count += int(deleted)
	}
	return proto.NewInteger(int64(count))
}

// handleZREMRANGEBYRANK 实现 ZREMRANGEBYRANK 命令
func (h *Handler) handleZREMRANGEBYRANK(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYRANK' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.ZRemRangeByRank(key, start, stop)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(count)
}

// handleZREMRANGEBYSCORE 实现 ZREMRANGEBYSCORE 命令
func (h *Handler) handleZREMRANGEBYSCORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYSCORE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	min, minExclusive, err := parseScoreExclusive(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	max, maxExclusive, err := parseScoreExclusive(string(args[2]))
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.ZRemRangeByScore(key, min, max, minExclusive, maxExclusive)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(count)
}

// handleZPOPMAX 实现 ZPOPMAX 命令
func (h *Handler) handleZPOPMAX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'ZPOPMAX' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	count := 1
	if len(args) >= 2 {
		c, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		count = c
	}
	h.markDirtyKeys(state, key)
	members, err := h.Db.ZPopMax(key, count)
	if err != nil {
		return wrapStoreError(err)
	}
	// 返回 member 和 score 的交替数组
	result := make([][]byte, 0, len(members)*2)
	for _, m := range members {
		result = append(result, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
	}
	return &proto.Array{Args: result}
}

// handleZPOPMIN 实现 ZPOPMIN 命令
func (h *Handler) handleZPOPMIN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'ZPOPMIN' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	count := 1
	if len(args) >= 2 {
		c, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		count = c
	}
	h.markDirtyKeys(state, key)
	members, err := h.Db.ZPopMin(key, count)
	if err != nil {
		return wrapStoreError(err)
	}
	// 返回 member 和 score 的交替数组
	result := make([][]byte, 0, len(members)*2)
	for _, m := range members {
		result = append(result, []byte(m.Member), []byte(fmt.Sprintf("%.10g", m.Score)))
	}
	return &proto.Array{Args: result}
}

// handleBZPOPMAX 实现 BZPOPMAX 命令
func (h *Handler) handleBZPOPMAX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'BZPOPMAX' command")
	}
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	timeout, err := strconv.Atoi(string(args[len(args)-1]))
	if err != nil {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	state.blocking.Store(true)
	key, member, err := h.Db.BZPopMaxBlocking(state.blockCtx(), keys, timeout)
	state.blocking.Store(false)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return h.nilArrayOrNull(state)
	}
	if key == "" {
		return h.nilArrayOrNull(state)
	}
	return &proto.Array{Args: [][]byte{[]byte(key), []byte(member.Member), []byte(fmt.Sprintf("%.10g", member.Score))}}
}

// handleBZPOPMIN 实现 BZPOPMIN 命令
func (h *Handler) handleBZPOPMIN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'BZPOPMIN' command")
	}
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	timeout, err := strconv.Atoi(string(args[len(args)-1]))
	if err != nil {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	state.blocking.Store(true)
	key, member, err := h.Db.BZPopMinBlocking(state.blockCtx(), keys, timeout)
	state.blocking.Store(false)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return h.nilArrayOrNull(state)
	}
	if key == "" {
		return h.nilArrayOrNull(state)
	}
	return &proto.Array{Args: [][]byte{[]byte(key), []byte(member.Member), []byte(fmt.Sprintf("%.10g", member.Score))}}
}

// handleZCARD 实现 ZCARD 命令
func (h *Handler) handleZCARD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'ZCARD' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	count, err := h.Db.ZCard(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
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

// handleZSCORE 实现 ZSCORE 命令
func (h *Handler) handleZSCORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZSCORE' command")
	}
	key, member := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	score, exists, err := h.Db.ZScore(key, member)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if !exists {
		h.recordKeyspaceMiss()
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	h.recordKeyspaceHit()
	// RESP3: score is a Double type (',' prefix); redis-py 8 returns float
	// only for the double, bytes otherwise.
	if state.respVersion == 3 {
		return &proto.Double{Value: score}
	}
	return proto.NewBulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
}

// handleZRANK 实现 ZRANK 命令
func (h *Handler) handleZRANK(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	h.incrementCmdCounter("ZRANK")
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZRANK' command")
	}
	key, member := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	rank, err := h.Db.ZRank(key, member)
	if err != nil {
		return wrapStoreError(err)
	}
	if rank < 0 {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewInteger(rank)
}

// handleZREVRANK 实现 ZREVRANK 命令
func (h *Handler) handleZREVRANK(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	h.incrementCmdCounter("ZREVRANK")
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZREVRANK' command")
	}
	key, member := string(args[0]), string(args[1])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	rank, err := h.Db.ZRevRank(key, member)
	if err != nil {
		return wrapStoreError(err)
	}
	if rank < 0 {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return proto.NewInteger(rank)
}

// handleZCOUNT 实现 ZCOUNT 命令
func (h *Handler) handleZCOUNT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZCOUNT' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	minScore, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	maxScore, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	count, err := h.Db.ZCount(key, minScore, maxScore)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			h.recordKeyspaceMiss()
			return proto.NewInteger(0)
		}
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
	return proto.NewInteger(count)
}

// handleZMSCORE 实现 ZMSCORE 命令
func (h *Handler) handleZMSCORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZMSCORE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i])
	}
	scores, err := h.Db.ZMScore(key, members...)
	if err != nil {
		return wrapStoreError(err)
	}
	// Check if key exists at all — Redis returns nil for non-existent key
	card, _ := h.Db.ZCard(key)
	if card == 0 {
		return proto.NewBulkString(nil)
	}

	// Build response: nil for missing members, string score for found members
	results := make([][]byte, len(scores))
	for i, s := range scores {
		if s == 0.0 {
			// Check if this member actually exists (0.0 could be a valid score)
			_, found, _ := h.Db.ZScore(key, members[i])
			if !found {
				results[i] = nil // nil bytes = nil BulkString in RESP
			} else {
				results[i] = []byte(strconv.FormatFloat(s, 'f', -1, 64))
			}
		} else {
			results[i] = []byte(strconv.FormatFloat(s, 'f', -1, 64))
		}
	}
	// RESP3: array of Double/Null — scores use the ',' prefix and missing
	// members the '_' Null type (redis-py 8 blocks on RESP2 '$-1' and
	// returns bytes instead of floats without the double prefix).
	if state.respVersion == 3 {
		elems := make([]proto.RESP, len(results))
		for i, v := range results {
			if v == nil {
				elems[i] = &proto.Null{}
			} else {
				score, _ := strconv.ParseFloat(string(v), 64)
				elems[i] = &proto.Double{Value: score}
			}
		}
		return &proto.NestedArray{Elems: elems}
	}
	return &proto.Array{Args: results}
}

// handleZRANGE 实现 ZRANGE 命令
func (h *Handler) handleZRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	h.incrementCmdCounter("ZRANGE")
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZRANGE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	withScores := false
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "WITHSCORES" {
			withScores = true
		}
	}
	members, err := h.Db.ZRange(key, start, stop)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			h.recordKeyspaceMiss()
			return &proto.Array{Args: [][]byte{}}
		}
		return wrapStoreError(err)
	}
	if len(members) == 0 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	if withScores {
		// RESP3: array of [member, Double] pairs; RESP2: flat [m, s, m, s].
		if state.respVersion == 3 {
			elems := make([]proto.RESP, 0, len(members))
			for _, m := range members {
				elems = append(elems, &proto.NestedArray{Elems: []proto.RESP{
					proto.NewBulkString([]byte(m.Member)),
					&proto.Double{Value: m.Score},
				}})
			}
			return &proto.NestedArray{Elems: elems}
		}
		results := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			results = append(results, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return &proto.Array{Args: results}
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m.Member)
	}
	return &proto.Array{Args: results}
}

// handleZREVRANGE 实现 ZREVRANGE 命令
func (h *Handler) handleZREVRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZREVRANGE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	withScores := false
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "WITHSCORES" {
			withScores = true
		}
	}
	members, err := h.Db.ZRevRange(key, start, stop)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			h.recordKeyspaceMiss()
			return &proto.Array{Args: [][]byte{}}
		}
		return wrapStoreError(err)
	}
	if len(members) == 0 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	if withScores {
		vals := make([]store.ZSetMember, len(members))
		for i, m := range members {
			vals[i] = *m
		}
		return zrangeWithScoresReply(state, vals)
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m.Member)
	}
	return &proto.Array{Args: results}
}

// handleZRANGEBYSCORE 实现 ZRANGEBYSCORE 命令
func (h *Handler) handleZRANGEBYSCORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZRANGEBYSCORE' command")
	}
	zsetName := string(args[0])
	if resp := h.checkAndHandleRedirect(state, zsetName); resp != nil {
		return resp
	}
	minScore, minExclusive, err := parseScoreExclusive(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	maxScore, maxExclusive, err := parseScoreExclusive(string(args[2]))
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	offset := 0
	count := -1
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		if opt == "LIMIT" && i+2 < len(args) {
			offset, err = strconv.Atoi(string(args[i+1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count, err = strconv.Atoi(string(args[i+2]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			break
		}
	}
	members, err := h.Db.ZRangeByScore(zsetName, minScore, maxScore, offset, count, minExclusive, maxExclusive)
	if err != nil {
		return wrapStoreError(err)
	}
	if len(members) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	withScores := false
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "WITHSCORES" {
			withScores = true
			break
		}
	}
	if withScores {
		return zrangeWithScoresReply(state, members)
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m.Member)
	}
	return &proto.Array{Args: results}
}

// handleZREVRANGEBYSCORE 实现 ZREVRANGEBYSCORE 命令
func (h *Handler) handleZREVRANGEBYSCORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZREVRANGEBYSCORE' command")
	}
	zsetName := string(args[0])
	if resp := h.checkAndHandleRedirect(state, zsetName); resp != nil {
		return resp
	}
	maxScore, maxExclusive, err := parseScoreExclusive(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	minScore, minExclusive, err := parseScoreExclusive(string(args[2]))
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	offset := 0
	count := -1
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		if opt == "LIMIT" && i+2 < len(args) {
			offset, err = strconv.Atoi(string(args[i+1]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count, err = strconv.Atoi(string(args[i+2]))
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			break
		}
	}
	members, err := h.Db.ZRevRangeByScore(zsetName, maxScore, minScore, offset, count, minExclusive, maxExclusive)
	if err != nil {
		return wrapStoreError(err)
	}
	if len(members) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	withScores := false
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "WITHSCORES" {
			withScores = true
			break
		}
	}
	if withScores {
		return zrangeWithScoresReply(state, members)
	}
	results := make([][]byte, len(members))
	for i, m := range members {
		results[i] = []byte(m.Member)
	}
	return &proto.Array{Args: results}
}

// handleZINCRBY 实现 ZINCRBY 命令
func (h *Handler) handleZINCRBY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZINCRBY' command")
	}
	key, member := string(args[0]), string(args[2])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	delta, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return proto.NewError("ERR value is not a valid float")
	}
	h.markDirtyKeys(state, key)
	newScore, err := h.Db.ZIncrBy(key, member, delta)
	if err != nil {
		return wrapStoreError(err)
	}
	// RESP3: score is a Double type (',' prefix); redis-py 8 returns float
	// only for the double, bytes otherwise.
	if state.respVersion == 3 {
		return &proto.Double{Value: newScore}
	}
	return proto.NewBulkString([]byte(strconv.FormatFloat(newScore, 'f', -1, 64)))
}

// handleZRANDMEMBER 实现 ZRANDMEMBER 命令
func (h *Handler) handleZRANDMEMBER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'ZRANDMEMBER' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	count := 0
	withScores := false
	if len(args) >= 2 {
		c, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		count = c
	}
	for i := 2; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "WITHSCORES") {
			withScores = true
		}
	}
	members, err := h.Db.ZRandMember(key, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if len(members) == 0 {
		if count == 0 {
			if state.respVersion == 3 {
				return &proto.Null{}
			}
			return proto.NewBulkString(nil)
		}
		return &proto.Array{Args: [][]byte{}}
	}
	if count == 0 && !withScores {
		return proto.NewBulkString([]byte(members[0].Member))
	}
	if count == 0 && withScores {
		return &proto.Array{Args: [][]byte{
			[]byte(members[0].Member),
			[]byte(strconv.FormatFloat(members[0].Score, 'f', -1, 64)),
		}}
	}
	if withScores {
		return zrangeWithScoresReply(state, members)
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m.Member)
	}
	return &proto.Array{Args: result}
}

// handleZMPOP 实现 ZMPOP 命令
func (h *Handler) handleZMPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZMPOP' command")
	}
	numKeys, kErr := strconv.Atoi(string(args[0]))
	if kErr != nil || numKeys < 1 || 1+numKeys+1 > len(args) {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	modifier := strings.ToUpper(string(args[1+numKeys]))
	if modifier != "MIN" && modifier != "MAX" {
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
	key, members, err := h.Db.ZMPop(keys, modifier, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if key == "" || len(members) == 0 {
		return h.nilArrayOrNull(state)
	}
	result := make([][]byte, 0, 1+len(members)*2)
	result = append(result, []byte(key))
	for _, m := range members {
		result = append(result, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
	}
	return &proto.Array{Args: result}
}

// handleBZMPOP 实现 BZMPOP 命令，阻塞式从多个排序集合弹出成员
func (h *Handler) handleBZMPOP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'BZMPOP' command")
	}
	timeout, tErr := strconv.Atoi(string(args[0]))
	if tErr != nil || timeout < 0 {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	numKeys, kErr := strconv.Atoi(string(args[1]))
	if kErr != nil || numKeys < 1 || 2+numKeys+1 > len(args) {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	modifier := strings.ToUpper(string(args[2+numKeys]))
	if modifier != "MIN" && modifier != "MAX" {
		return proto.NewError("ERR syntax error")
	}
	count := 1
	if len(args) >= 4+numKeys {
		if strings.ToUpper(string(args[3+numKeys])) == "COUNT" {
			if len(args) < 5+numKeys {
				return proto.NewError("ERR syntax error")
			}
			c, cErr := strconv.Atoi(string(args[4+numKeys]))
			if cErr != nil || c < 1 {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			count = c
		}
	}
	state.blocking.Store(true)
	key, members, err := h.Db.BZMPopBlocking(state.blockCtx(), keys, modifier, count, timeout)
	state.blocking.Store(false)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return h.nilArrayOrNull(state)
	}
	if key == "" || len(members) == 0 {
		return h.nilArrayOrNull(state)
	}
	result := make([][]byte, 0, 1+len(members)*2)
	result = append(result, []byte(key))
	for _, m := range members {
		result = append(result, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
	}
	return &proto.Array{Args: result}
}

// handleZUNIONSTORE 实现 ZUNIONSTORE 命令
func (h *Handler) handleZUNIONSTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZUNIONSTORE' command")
	}
	destination := string(args[0])
	// 解析参数: ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	weights := []float64{}
	aggregate := "SUM"
	// 解析可选参数
	i := 2 + numKeys
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "WEIGHTS":
			if i+numKeys >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			weights = make([]float64, numKeys)
			for j := 0; j < numKeys; j++ {
				w, err := strconv.ParseFloat(string(args[i+1+j]), 64)
				if err != nil {
					return proto.NewError("ERR weight is not a float")
				}
				weights[j] = w
			}
			i += 1 + numKeys
		case "AGGREGATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[i+1]))
			if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
				return proto.NewError("ERR syntax error")
			}
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}
	h.markDirtyKeys(state, destination)
	count, err := h.Db.ZUnionStore(destination, keys, weights, aggregate)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(count)
}

// handleZINTERSTORE 实现 ZINTERSTORE 命令
func (h *Handler) handleZINTERSTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZINTERSTORE' command")
	}
	destination := string(args[0])
	// 解析参数
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	weights := []float64{}
	aggregate := "SUM"
	// 解析可选参数
	i := 2 + numKeys
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "WEIGHTS":
			if i+numKeys >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			weights = make([]float64, numKeys)
			for j := 0; j < numKeys; j++ {
				w, err := strconv.ParseFloat(string(args[i+1+j]), 64)
				if err != nil {
					return proto.NewError("ERR weight is not a float")
				}
				weights[j] = w
			}
			i += 1 + numKeys
		case "AGGREGATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[i+1]))
			if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
				return proto.NewError("ERR syntax error")
			}
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}
	h.markDirtyKeys(state, destination)
	count, err := h.Db.ZInterStore(destination, keys, weights, aggregate)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(count)
}

// handleZDIFFSTORE 实现 ZDIFFSTORE 命令
func (h *Handler) handleZDIFFSTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZDIFFSTORE' command")
	}
	destination := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	if numKeys < 1 {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	h.markDirtyKeys(state, destination)
	count, err := h.Db.ZDiffStore(destination, keys)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(count)
}

// handleZDIFF 实现 ZDIFF 命令
func (h *Handler) handleZDIFF(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZDIFF' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	if numKeys < 1 {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	withScores := false
	for i := 1 + numKeys; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "WITHSCORES") {
			withScores = true
		}
	}
	members, err := h.Db.ZDiff(keys)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if len(members) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	if withScores {
		return zrangeWithScoresReply(state, members)
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m.Member)
	}
	return &proto.Array{Args: result}
}

// handleZLEXCOUNT 实现 ZLEXCOUNT 命令
func (h *Handler) handleZLEXCOUNT(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// ZLEXCOUNT key min max
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZLEXCOUNT' command")
	}
	zSetName := string(args[0])
	if resp := h.checkAndHandleRedirect(state, zSetName); resp != nil {
		return resp
	}
	min := string(args[1])
	max := string(args[2])
	count, err := h.Db.ZLexCount(zSetName, min, max)
	if err != nil {
		return wrapStoreError(err)
	}
	if count == 0 {
		if exists, err := h.Db.Exists(zSetName); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	return proto.NewInteger(count)
}

// handleZRANGEBYLEX 实现 ZRANGEBYLEX 命令
func (h *Handler) handleZRANGEBYLEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// ZRANGEBYLEX key min max [LIMIT offset count]
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZRANGEBYLEX' command")
	}
	zSetName := string(args[0])
	if resp := h.checkAndHandleRedirect(state, zSetName); resp != nil {
		return resp
	}
	min := string(args[1])
	max := string(args[2])
	offset := 0
	count := -1
	var err error
	// Parse optional LIMIT
	if len(args) > 3 {
		for i := 3; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			if opt == "LIMIT" && i+2 < len(args) {
				offset, err = strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count, err = strconv.Atoi(string(args[i+2]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				i += 2
			}
		}
	}
	members, err := h.Db.ZRangeByLex(zSetName, min, max, offset, count)
	if err != nil {
		return wrapStoreError(err)
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m)
	}
	return &proto.Array{Args: result}
}

// handleZREVRANGEBYLEX 实现 ZREVRANGEBYLEX 命令
func (h *Handler) handleZREVRANGEBYLEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// ZREVRANGEBYLEX key max min [LIMIT offset count]
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZREVRANGEBYLEX' command")
	}
	zSetName := string(args[0])
	if resp := h.checkAndHandleRedirect(state, zSetName); resp != nil {
		return resp
	}
	max := string(args[1])
	min := string(args[2])
	offset := 0
	count := -1
	var err error
	// Parse optional LIMIT
	if len(args) > 3 {
		for i := 3; i < len(args); i++ {
			opt := strings.ToUpper(string(args[i]))
			if opt == "LIMIT" && i+2 < len(args) {
				offset, err = strconv.Atoi(string(args[i+1]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				count, err = strconv.Atoi(string(args[i+2]))
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				i += 2
			}
		}
	}
	members, err := h.Db.ZRevRangeByLex(zSetName, max, min, offset, count)
	if err != nil {
		return wrapStoreError(err)
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m)
	}
	return &proto.Array{Args: result}
}

// handleZREMRANGEBYLEX 实现 ZREMRANGEBYLEX 命令
func (h *Handler) handleZREMRANGEBYLEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// ZREMRANGEBYLEX key min max
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'ZREMRANGEBYLEX' command")
	}
	zSetName := string(args[0])
	if resp := h.checkAndHandleRedirect(state, zSetName); resp != nil {
		return resp
	}
	min := string(args[1])
	max := string(args[2])
	h.markDirtyKeys(state, zSetName)
	removed, err := h.Db.ZRemRangeByLex(zSetName, min, max)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(removed)
}

// handleZSCAN 实现 ZSCAN 命令
func (h *Handler) handleZSCAN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// ZSCAN key cursor [MATCH pattern] [COUNT count]
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZSCAN' command")
	}
	zSetName := string(args[0])
	if resp := h.checkAndHandleRedirect(state, zSetName); resp != nil {
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
	result, err := h.Db.ZScan(zSetName, cursor, pattern, count)
	if err != nil {
		return wrapStoreError(err)
	}
	if len(result.Members) == 0 {
		if exists, err := h.Db.Exists(zSetName); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	// 返回格式: [cursor, [member1, score1, member2, score2, ...]]
	membersArray := make([][]byte, len(result.Members)*2)
	for i, m := range result.Members {
		membersArray[i*2] = []byte(m.Member)
		membersArray[i*2+1] = []byte(fmt.Sprintf("%.10g", m.Score))
	}
	return &proto.NestedArray{
		Elems: []proto.RESP{
			proto.Integer(result.Cursor),
			&proto.Array{Args: membersArray},
		},
	}
}

// handleZRANGESTORE 实现 ZRANGESTORE 命令
func (h *Handler) handleZRANGESTORE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// ZRANGESTORE dstkey srckey min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'ZRANGESTORE' command")
	}
	dstKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, dstKey); resp != nil {
		return resp
	}
	srcKey := string(args[1])
	min := string(args[2])
	max := string(args[3])

	// Parse options
	byScore := false
	byLex := false
	rev := false
	var limitOffset, limitCount int64 = 0, -1

	i := 4
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
				return proto.NewError("ERR syntax error")
			}
			offset, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid LIMIT offset")
			}
			count, err := strconv.ParseInt(string(args[i+2]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid LIMIT count")
			}
			limitOffset = offset
			limitCount = count
			i += 3
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	// Parse min/max as float64 only if BYSCORE
	var minScore, maxScore float64
	var err error
	if byScore {
		minScore, err = strconv.ParseFloat(min, 64)
		if err != nil {
			return proto.NewError("ERR min value is not a float")
		}
		maxScore, err = strconv.ParseFloat(max, 64)
		if err != nil {
			return proto.NewError("ERR max value is not a float")
		}
	}

	// Determine the range operation to use
	var members []store.ZSetMember

	if byLex {
		lexMembers, lexErr := h.Db.ZRangeByLex(srcKey, min, max, int(limitOffset), int(limitCount))
		if lexErr != nil {
			return wrapStoreError(lexErr)
		}
		// Convert []string to []ZSetMember (score=0 for all)
		members = make([]store.ZSetMember, len(lexMembers))
		for i, m := range lexMembers {
			members[i] = store.ZSetMember{Member: m, Score: 0}
		}
	} else if byScore {
		members, err = h.Db.ZRangeByScore(srcKey, minScore, maxScore, int(limitOffset), int(limitCount), false, false)
		if err != nil {
			return wrapStoreError(err)
		}
	} else {
		// Default: treat min/max as ranks (integers)
		start, err := strconv.ParseInt(min, 10, 64)
		if err != nil {
			return proto.NewError("ERR min value is not an integer")
		}
		stop, err := strconv.ParseInt(max, 10, 64)
		if err != nil {
			return proto.NewError("ERR max value is not an integer")
		}
		// With REV, swap start and stop (range becomes [stop, start])
		if rev {
			start, stop = stop, start
		}
		ptrMembers, rangeErr := h.Db.ZRange(srcKey, start, stop)
		if rangeErr != nil {
			return wrapStoreError(rangeErr)
		}
		// Apply LIMIT for rank-based range
		if limitCount >= 0 && int64(len(ptrMembers)) > limitOffset {
			if limitCount == 0 || limitOffset+int64(limitCount) > int64(len(ptrMembers)) {
				ptrMembers = ptrMembers[limitOffset:]
			} else {
				ptrMembers = ptrMembers[limitOffset : limitOffset+int64(limitCount)]
			}
		}
		// Convert []*ZSetMember to []ZSetMember
		members = make([]store.ZSetMember, len(ptrMembers))
		for i, m := range ptrMembers {
			members[i] = store.ZSetMember{Member: m.Member, Score: m.Score}
		}
	}

	// Apply REV for BYSCORE and BYLEX (reverse the result)
	// Note: For rank-based ranges, REV is handled by swapping start/stop above
	if rev && (byScore || byLex) {
		for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
			members[i], members[j] = members[j], members[i]
		}
	}

	// Delete destination if it exists
	h.markDirtyKeys(state, dstKey)
	if _, err := h.Db.Del(dstKey); err != nil {
		return wrapStoreError(err)
	}

	// Add members to destination
	if len(members) > 0 {
		err = h.Db.ZAdd(dstKey, members)
		if err != nil {
			return wrapStoreError(err)
		}
	}

	// ZRANGESTORE always returns the count of elements stored
	return proto.NewInteger(int64(len(members)))

	// Pub/Sub命令
}
