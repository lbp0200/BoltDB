package server

import (
	"errors"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"strconv"
	"strings"
)

// handleJSON_SET 实现 JSON.SET 命令
func (h *Handler) handleJSON_SET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.SET' command")
	}
	key, path := string(args[0]), string(args[1])
	value := string(args[2])
	nx, xx := false, false
	// Parse optional NX/XX arguments
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		}
	}
	h.markDirtyKeys(state, key)
	result, err := h.Db.JSONSet(key, path, value, nx, xx)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewSimpleString(result)
}

// handleJSON_GET 实现 JSON.GET 命令
func (h *Handler) handleJSON_GET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.GET' command")
	}
	key := string(args[0])
	paths := make([]string, 0)
	for i := 1; i < len(args); i++ {
		paths = append(paths, string(args[i]))
	}
	result, err := h.Db.JSONGet(key, paths...)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
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
	if len(result) == 1 {
		return proto.NewBulkString([]byte(result[0]))
	}
	// Multiple paths
	arr := make([][]byte, len(result))
	for i, v := range result {
		arr[i] = []byte(v)
	}
	return &proto.Array{Args: arr}
}

// handleJSON_DEL 实现 JSON.DEL 命令
func (h *Handler) handleJSON_DEL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.DEL' command")
	}
	key := string(args[0])
	paths := make([]string, 0)
	for i := 1; i < len(args); i++ {
		paths = append(paths, string(args[i]))
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.JSONDel(key, paths...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(count)
}

// handleJSON_TYPE 实现 JSON.TYPE 命令
func (h *Handler) handleJSON_TYPE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.TYPE' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) >= 2 {
		path = string(args[1])
	}
	result, err := h.Db.JSONType(key, path)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
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
	return proto.NewBulkString([]byte(result))
}

// handleJSON_MGET 实现 JSON.MGET 命令
func (h *Handler) handleJSON_MGET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.MGET' command")
	}
	path := string(args[len(args)-1])
	keys := make([]string, 0)
	for i := 0; i < len(args)-1; i++ {
		keys = append(keys, string(args[i]))
	}
	result, err := h.Db.JSONMGet(path, keys...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	arr := make([][]byte, len(result))
	for i, v := range result {
		if v == "" {
			arr[i] = nil
		} else {
			arr[i] = []byte(v)
		}
	}
	return &proto.Array{Args: arr}
}

// handleJSON_ARRAPPEND 实现 JSON.ARRAPPEND 命令
func (h *Handler) handleJSON_ARRAPPEND(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.ARRAPPEND' command")
	}
	key, path := string(args[0]), string(args[1])
	values := make([]string, 0)
	for i := 2; i < len(args); i++ {
		values = append(values, string(args[i]))
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.JSONArrAppend(key, path, values...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(count)
}

// handleJSON_ARRLEN 实现 JSON.ARRLEN 命令
func (h *Handler) handleJSON_ARRLEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.ARRLEN' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) >= 2 {
		path = string(args[1])
	}
	count, err := h.Db.JSONArrLen(key, path)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(count)
}

// handleJSON_OBJKEYS 实现 JSON.OBJKEYS 命令
func (h *Handler) handleJSON_OBJKEYS(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.OBJKEYS' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) >= 2 {
		path = string(args[1])
	}
	keys, err := h.Db.JSONObjKeys(key, path)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	arr := make([][]byte, len(keys))
	for i, k := range keys {
		arr[i] = []byte(k)
	}
	return &proto.Array{Args: arr}
}

// handleJSON_NUMINCRBY 实现 JSON.NUMINCRBY 命令
func (h *Handler) handleJSON_NUMINCRBY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.NUMINCRBY' command")
	}
	key, path := string(args[0]), string(args[1])
	increment, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR increment must be a valid number")
	}
	h.markDirtyKeys(state, key)
	result, err := h.Db.JSONNumIncrBy(key, path, increment)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewBulkString([]byte(strconv.FormatFloat(result, 'f', -1, 64)))
}

// handleJSON_NUMMULTBY 实现 JSON.NUMMULTBY 命令
func (h *Handler) handleJSON_NUMMULTBY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.NUMMULTBY' command")
	}
	key, path := string(args[0]), string(args[1])
	multiplier, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR multiplier must be a valid number")
	}
	h.markDirtyKeys(state, key)
	result, err := h.Db.JSONNumMultBy(key, path, multiplier)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewBulkString([]byte(strconv.FormatFloat(result, 'f', -1, 64)))
}

// handleJSON_CLEAR 实现 JSON.CLEAR 命令
func (h *Handler) handleJSON_CLEAR(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.CLEAR' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) >= 2 {
		path = string(args[1])
	}
	h.markDirtyKeys(state, key)
	count, err := h.Db.JSONClear(key, path)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(count)
}

// handleJSON_DEBUG 实现 JSON.DEBUG 命令
func (h *Handler) handleJSON_DEBUG(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'JSON.DEBUG' command")
	}
	subCmd := strings.ToUpper(string(args[0]))
	if subCmd != "MEMORY" {
		return proto.NewError("ERR syntax error")
	}
	key := string(args[1])
	path := "$"
	if len(args) >= 3 {
		path = string(args[2])
	}
	memory, err := h.Db.JSONDebugMemory(key, path)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
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
	return proto.NewInteger(memory)

	// ==================== Time Series ====================
}
