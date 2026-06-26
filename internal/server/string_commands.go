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
