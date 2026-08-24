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
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
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
	h.markDirtyKeys(state, key)
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
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
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
	i := 3
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "ON_DUPLICATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			opts.OnDuplicate = string(args[i+1])
			i += 2
		case "RETENTION":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			r, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || r < 0 {
				return proto.NewError("ERR invalid RETENTION value")
			}
			opts.Retention = r
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR unsupported option '%s'", string(args[i])))
		}
	}
	h.markDirtyKeys(state, key)
	ts, err := h.Db.TSAdd(key, timestamp, value, opts)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(ts)
}

// handleTS_GET 实现 TS.GET 命令
func (h *Handler) handleTS_GET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TS.GET' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	dp, err := h.Db.TSGet(key)
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
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
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
		return wrapLogError(err)
	}
	if len(results) == 0 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
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
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	start := string(args[1])
	stop := string(args[2])
	h.markDirtyKeys(state, key)
	deleted, err := h.Db.TSDel(key, start, stop)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(deleted)
}

// handleTS_INFO 实现 TS.INFO 命令
func (h *Handler) handleTS_INFO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TS.INFO' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	info, err := h.Db.TSInfo(key)
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
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
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
		return wrapLogError(err)
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
	if resp := h.checkAndHandleMultiKeyRedirect(keys); resp != nil {
		return resp
	}
	results, err := h.Db.TSMGet(filter, keys...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	for _, dp := range results {
		if dp == nil {
			h.recordKeyspaceMiss()
		} else {
			h.recordKeyspaceHit()
		}
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

// handleTS_REVRANGE 实现 TS.REVRANGE 命令
func (h *Handler) handleTS_REVRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'TS.REVRANGE' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
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
	results, err := h.Db.TSRevRange(key, start, stop, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	if len(results) == 0 {
		if exists, err := h.Db.Exists(key); err == nil && !exists {
			h.recordKeyspaceMiss()
		} else if err == nil && exists {
			h.recordKeyspaceHit()
		}
	} else {
		h.recordKeyspaceHit()
	}
	arr := make([][]byte, 0, len(results)*2)
	for _, dp := range results {
		arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
		arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
	}
	return &proto.Array{Args: arr}
}

// handleTS_MRANGE 实现 TS.MRANGE 命令
func (h *Handler) handleTS_MRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'TS.MRANGE' command")
	}
	start := string(args[0])
	stop := string(args[1])
	filterArg := strings.ToUpper(string(args[2]))
	if filterArg != "FILTER" {
		return proto.NewError("ERR syntax error")
	}
	filters := []string{}
	count := int64(-1)
	i := 3
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		if opt == "COUNT" && i+1 < len(args) {
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid COUNT value")
			}
			count = c
			i += 2
		} else {
			filters = append(filters, string(args[i]))
			i++
		}
	}
	keys, err := h.Db.TSQueryIndex(filters)
	if err != nil {
		return wrapLogError(err)
	}
	results, err := h.Db.TSMRange(strings.Join(filters, ","), keys, start, stop, count)
	if err != nil {
		return wrapLogError(err)
	}
	if len(results) == 0 {
		for _, k := range keys {
			if exists, err := h.Db.Exists(k); err == nil && !exists {
				h.recordKeyspaceMiss()
			} else if err == nil && exists {
				h.recordKeyspaceHit()
			}
		}
		if len(keys) == 0 {
			h.recordKeyspaceMiss()
		}
	} else {
		for range results {
			h.recordKeyspaceHit()
		}
	}
	respElems := make([]proto.RESP, len(results))
	for i, r := range results {
		keyName := r[0].(string)
		dps := r[1].([]store.TimeSeriesDataPoint)
		arr := make([][]byte, 0, len(dps)*2)
		for _, dp := range dps {
			arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
			arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
		}
		respElems[i] = &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(keyName)),
				&proto.Array{Args: arr},
			},
		}
	}
	return &proto.NestedArray{Elems: respElems}
}

// handleTS_MREVRANGE 实现 TS.MREVRANGE 命令
func (h *Handler) handleTS_MREVRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'TS.MREVRANGE' command")
	}
	start := string(args[0])
	stop := string(args[1])
	filterArg := strings.ToUpper(string(args[2]))
	if filterArg != "FILTER" {
		return proto.NewError("ERR syntax error")
	}
	filters := []string{}
	count := int64(-1)
	i := 3
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		if opt == "COUNT" && i+1 < len(args) {
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid COUNT value")
			}
			count = c
			i += 2
		} else {
			filters = append(filters, string(args[i]))
			i++
		}
	}
	keys, err := h.Db.TSQueryIndex(filters)
	if err != nil {
		return wrapLogError(err)
	}
	var results [][]interface{}
	for _, key := range keys {
		dps, err := h.Db.TSRevRange(key, start, stop, count)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrWrongType) {
				continue
			}
			return wrapLogError(err)
		}
		if len(dps) > 0 {
			results = append(results, []interface{}{key, dps})
		}
	}
	if len(results) == 0 {
		for _, k := range keys {
			if exists, err := h.Db.Exists(k); err == nil && !exists {
				h.recordKeyspaceMiss()
			} else if err == nil && exists {
				h.recordKeyspaceHit()
			}
		}
		if len(keys) == 0 {
			h.recordKeyspaceMiss()
		}
	} else {
		for range results {
			h.recordKeyspaceHit()
		}
	}
	respElems := make([]proto.RESP, len(results))
	for i, r := range results {
		keyName := r[0].(string)
		dps := r[1].([]store.TimeSeriesDataPoint)
		arr := make([][]byte, 0, len(dps)*2)
		for _, dp := range dps {
			arr = append(arr, []byte(strconv.FormatInt(dp.Timestamp, 10)))
			arr = append(arr, []byte(strconv.FormatFloat(dp.Value, 'f', -1, 64)))
		}
		respElems[i] = &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(keyName)),
				&proto.Array{Args: arr},
			},
		}
	}
	return &proto.NestedArray{Elems: respElems}
}

// handleTS_QUERYINDEX 实现 TS.QUERYINDEX 命令
func (h *Handler) handleTS_QUERYINDEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'TS.QUERYINDEX' command")
	}
	filters := make([]string, len(args))
	for i, arg := range args {
		filters[i] = string(arg)
	}
	keys, err := h.Db.TSQueryIndex(filters)
	if err != nil {
		return wrapLogError(err)
	}
	arr := make([][]byte, len(keys))
	for i, key := range keys {
		arr[i] = []byte(key)
	}
	return &proto.Array{Args: arr}
}

// handleTS_MADD 实现 TS.MADD 命令
func (h *Handler) handleTS_MADD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 || len(args)%3 != 0 {
		return proto.NewError("ERR wrong number of arguments for 'TS.MADD' command")
	}
	results := make([]proto.RESP, len(args)/3)
	// Collect all keys for cluster routing
	maddKeys := make([]string, 0, len(args)/3)
	for i := 0; i < len(args); i += 3 {
		maddKeys = append(maddKeys, string(args[i]))
	}
	if resp := h.checkAndHandleMultiKeyRedirect(maddKeys); resp != nil {
		return resp
	}
	for i := 0; i < len(args); i += 3 {
		key := string(args[i])
		var timestamp int64
		if string(args[i+1]) == "*" {
			timestamp = time.Now().UnixNano() / int64(time.Millisecond)
			// Canonicalize * for replication (same pattern as XADD).
			args[i+1] = []byte(strconv.FormatInt(timestamp, 10))
		} else {
			var err error
			timestamp, err = strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid timestamp")
			}
		}
		value, err := strconv.ParseFloat(string(args[i+2]), 64)
		if err != nil {
			return proto.NewError("ERR invalid value")
		}
		h.markDirtyKeys(state, key)
		ts, err := h.Db.TSAdd(key, timestamp, value, store.TSAddOptions{})
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return wrapLogError(err)
		}
		results[i/3] = proto.NewInteger(ts)
	}
	return &proto.NestedArray{Elems: results}
}

// handleTS_INCRBY 实现 TS.INCRBY 命令
func (h *Handler) handleTS_INCRBY(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'TS.INCRBY' command")
	}
	key := string(args[0])
	if resp := h.checkAndHandleRedirect(state, key); resp != nil {
		return resp
	}
	value, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return proto.NewError("ERR invalid value")
	}
	var timestamp int64
	opts := store.TSAddOptions{}
	i := 2
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "TIMESTAMP":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			timestamp, err = strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR invalid timestamp")
			}
			i += 2
		case "ON_DUPLICATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			opts.OnDuplicate = string(args[i+1])
			i += 2
		case "RETENTION":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			r, rErr := strconv.ParseInt(string(args[i+1]), 10, 64)
			if rErr != nil || r < 0 {
				return proto.NewError("ERR invalid RETENTION value")
			}
			opts.Retention = r
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR unsupported option '%s'", string(args[i])))
		}
	}
	h.markDirtyKeys(state, key)
	ts, err := h.Db.TSIncrBy(key, timestamp, value, opts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return proto.NewError("ERR the key does not exist")
		}
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapLogError(err)
	}
	return proto.NewInteger(ts)
}

// handleTS_CREATERULE 实现 TS.CREATERULE 命令
func (h *Handler) handleTS_CREATERULE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 5 {
		return proto.NewError("ERR wrong number of arguments for 'TS.CREATERULE' command")
	}
	sourceKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, sourceKey); resp != nil {
		return resp
	}
	destKey := string(args[1])
	aggArg := strings.ToUpper(string(args[2]))
	if aggArg != "AGGREGATION" {
		return proto.NewError("ERR syntax error")
	}
	aggregator := string(args[3])
	bucketDuration, err := strconv.ParseInt(string(args[4]), 10, 64)
	if err != nil {
		return proto.NewError("ERR invalid bucket duration")
	}
	h.markDirtyKeys(state, sourceKey, destKey)
	if err := h.Db.TSAddRule(sourceKey, destKey, aggregator, bucketDuration); err != nil {
		return wrapStoreError(err)
	}
	return proto.NewSimpleString("OK")
}

// handleTS_DELETERULE 实现 TS.DELETERULE 命令
func (h *Handler) handleTS_DELETERULE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 5 {
		return proto.NewError("ERR wrong number of arguments for 'TS.DELETERULE' command")
	}
	sourceKey := string(args[0])
	if resp := h.checkAndHandleRedirect(state, sourceKey); resp != nil {
		return resp
	}
	destKey := string(args[1])
	aggArg := strings.ToUpper(string(args[2]))
	if aggArg != "AGGREGATION" {
		return proto.NewError("ERR syntax error")
	}
	aggregator := string(args[3])
	bucketDuration, err := strconv.ParseInt(string(args[4]), 10, 64)
	if err != nil {
		return proto.NewError("ERR invalid bucket duration")
	}
	h.markDirtyKeys(state, sourceKey, destKey)
	if err := h.Db.TSDelRule(sourceKey, destKey, aggregator, bucketDuration); err != nil {
		return wrapStoreError(err)
	}
	return proto.NewSimpleString("OK")
}
