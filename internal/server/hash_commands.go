package server

import (
	"errors"
	"fmt"

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
			return proto.NewError(fmt.Sprintf("ERR %v", err))
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
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(int64(count))
}
