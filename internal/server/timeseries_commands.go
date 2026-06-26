package server

import (
	"errors"
	"fmt"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"strconv"
	"strings"
	"time"
)

// handleTS_CREATE 实现 TS.CREATE 命令
func (h *Handler) handleTS_CREATE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TS.CREATE' command")
	}
	key := string(args[0])
	opts := store.TSCreateOptions{}
	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "RETENTION":
			i++
			if i >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			retention, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid RETENTION value")
			}
			opts.Retention = retention
		case "ENCODING":
			i++
			if i >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			opts.Encoding = string(args[i])
		case "DUPLICATE_POLICY":
			i++
			if i >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			opts.DuplicatePolicy = string(args[i])
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
		i++
	}
	if err := h.Db.TSCreate(key, opts); err != nil {
		return wrapStoreError(err)
	}
	return proto.OK
}

// handleTS_ADD 实现 TS.ADD 命令
func (h *Handler) handleTS_ADD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'TS.ADD' command")
	}
	key := string(args[0])
	var timestamp int64
	if string(args[1]) == "*" {
		timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	} else {
		var err error
		timestamp, err = strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR invalid timestamp")
		}
	}
	value, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return proto.NewError("ERR invalid value")
	}
	opts := store.TSAddOptions{}
	if len(args) > 3 {
		opt := strings.ToUpper(string(args[3]))
		if opt == "ON_DUPLICATE" && len(args) > 4 {
			opts.OnDuplicate = string(args[4])
		}
	}
	h.markDirtyKeys(state, key)
	ts, err := h.Db.TSAdd(key, timestamp, value, opts)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(ts)
}

// handleTS_GET 实现 TS.GET 命令
func (h *Handler) handleTS_GET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TS.GET' command")
	}
	key := string(args[0])
	dp, err := h.Db.TSGet(key)
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
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	// Return as array: [timestamp, value]
	return &proto.Array{
		Args: [][]byte{
			[]byte(strconv.FormatInt(dp.Timestamp, 10)),
			[]byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)),
		},
	}
}

// handleTS_RANGE 实现 TS.RANGE 命令
func (h *Handler) handleTS_RANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'TS.RANGE' command")
	}
	key := string(args[0])
	start := string(args[1])
	stop := string(args[2])
	count := int64(-1)
	if len(args) > 3 {
		opt := strings.ToUpper(string(args[3]))
		if opt == "COUNT" && len(args) > 4 {
			c, err := strconv.ParseInt(string(args[4]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid COUNT value")
			}
			count = c
		}
	}
	results, err := h.Db.TSRange(key, start, stop, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	arr := make([][]byte, 0, len(results)*2)
	for _, dp := range results {
		arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
		arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
	}
	return &proto.Array{Args: arr}
}

// handleTS_DEL 实现 TS.DEL 命令
func (h *Handler) handleTS_DEL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'TS.DEL' command")
	}
	key := string(args[0])
	start := string(args[1])
	stop := string(args[2])
	h.markDirtyKeys(state, key)
	deleted, err := h.Db.TSDel(key, start, stop)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(deleted)
}

// handleTS_INFO 实现 TS.INFO 命令
func (h *Handler) handleTS_INFO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TS.INFO' command")
	}
	key := string(args[0])
	info, err := h.Db.TSInfo(key)
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
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	// Return as array of key-value pairs
	return &proto.Array{
		Args: [][]byte{
			[]byte("totalSamples"), []byte(strconv.FormatInt(info.TotalSamples, 10)),
			[]byte("memoryUsage"), []byte(strconv.FormatInt(info.MemoryUsage, 10)),
			[]byte("firstTimestamp"), []byte(strconv.FormatInt(info.FirstTimestamp, 10)),
			[]byte("lastTimestamp"), []byte(strconv.FormatInt(info.LastTimestamp, 10)),
			[]byte("retentionTime"), []byte(strconv.FormatInt(info.RetentionTime, 10)),
			[]byte("encoding"), []byte(info.Encoding),
			[]byte("chunkCount"), []byte(strconv.FormatInt(info.ChunkCount, 10)),
		},
	}
}

// handleTS_LEN 实现 TS.LEN 命令
func (h *Handler) handleTS_LEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TS.LEN' command")
	}
	key := string(args[0])
	length, err := h.Db.TSLen(key)
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
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(length)
}

// handleTS_MGET 实现 TS.MGET 命令
func (h *Handler) handleTS_MGET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'TS.MGET' command")
	}
	filter := string(args[0])
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	results, err := h.Db.TSMGet(filter, keys...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	arr := make([][]byte, 0, len(results)*2)
	for _, dp := range results {
		if dp == nil {
			arr = append(arr, []byte{})
			arr = append(arr, []byte{})
		} else {
			arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
			arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
		}
	}
	return &proto.Array{Args: arr}
}
