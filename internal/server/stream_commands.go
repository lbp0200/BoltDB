package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleXADD 实现 XADD 命令
func (h *Handler) handleXADD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'XADD' command")
	}
	key := string(args[0])
	var opts store.StreamXAddOptions
	var id string
	var fields = make(map[string]string)

	// Parse options
	i := 1
	for i < len(args)-2 && string(args[i])[0] == '-' {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "MAXLEN":
			if i+1 >= len(args)-2 {
				return proto.NewError("ERR syntax error")
			}
			maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			opts.MaxLen = maxlen
			i += 2
		case "MINID":
			if i+1 >= len(args)-2 {
				return proto.NewError("ERR syntax error")
			}
			opts.MinID = string(args[i+1])
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	// ID or field name
	idPos := i
	id = string(args[i])
	if id == "*" || (len(id) > 0 && id[0] == '-') {
		// It's the ID (* or an option), skip it
		i++
	} else {
		// It's the ID
		i++
	}

	// Remaining args are field-value pairs
	for i < len(args) {
		field := string(args[i])
		if i+1 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		value := string(args[i+1])
		fields[field] = value
		i += 2
	}

	h.markDirtyKeys(state, key)
	resultID, err := h.Db.XAdd(key, opts, id, fields)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	if h.Replication != nil && h.Replication.IsMaster() && id == "*" {
		args[idPos] = []byte(resultID)
	}
	return proto.NewBulkString([]byte(resultID))
}

// handleXLEN 实现 XLEN 命令
func (h *Handler) handleXLEN(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'XLEN' command")
	}
	key := string(args[0])
	length, err := h.Db.XLen(key)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(length)
}

// handleXREAD 实现 XREAD 命令
func (h *Handler) handleXREAD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	var count int64 = 0
	var block int64 = -1

	// Parse options
	i := 0
	if i < len(args) && strings.ToUpper(string(args[i])) == "COUNT" {
		if i+1 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		count = c
		i += 2
	}
	if i < len(args) && strings.ToUpper(string(args[i])) == "BLOCK" {
		if i+1 >= len(args) {
			return proto.NewError("ERR syntax error")
		}
		b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return proto.NewError("ERR value is not an integer")
		}
		block = b
		i += 2
	}

	// Check for STREAMS
	if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
		return proto.NewError("ERR syntax error, missing STREAMS keyword")
	}
	i++

	// Parse stream IDs
	// Format: key1 id1 key2 id2 ...
	remaining := len(args) - i
	if remaining < 2 || remaining%2 != 0 {
		return proto.NewError(fmt.Sprintf("ERR syntax error: remaining=%d, i=%d, len(args)=%d", remaining, i, len(args)))
	}
	numStreams := remaining / 2
	streamKeys := make([]string, numStreams)
	streamIDs := make([]string, numStreams)
	for j := 0; j < numStreams; j++ {
		streamKeys[j] = string(args[i+j*2])
		streamIDs[j] = string(args[i+j*2+1])
	}

	// Combine keys and IDs
	allArgs := make([]string, 0)
	for j := 0; j < numStreams; j++ {
		allArgs = append(allArgs, streamKeys[j])
		allArgs = append(allArgs, streamIDs[j])
	}

	if block >= 0 {
		state.blocking.Store(true)
	}
	results, err := h.Db.XRead(state.ctx, count, block, allArgs...)
	if block >= 0 {
		state.blocking.Store(false)
	}
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}

	// Format response
	if len(results) == 0 {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	streamResults := make([]proto.RESP, 0, len(results))
	for _, streamMap := range results {
		for streamKey, entries := range streamMap {
			entriesResp := make([]proto.RESP, 0, len(entries))
			for _, entry := range entries {
				fieldsResp := make([]proto.RESP, 0, len(entry.Fields)*2)
				for k, v := range entry.Fields {
					fieldsResp = append(fieldsResp,
						proto.NewBulkString([]byte(k)),
						proto.NewBulkString([]byte(v)),
					)
				}
				entriesResp = append(entriesResp, &proto.NestedArray{Elems: []proto.RESP{
					proto.NewBulkString([]byte(entry.ID)),
					&proto.NestedArray{Elems: fieldsResp},
				}})
			}
			streamResults = append(streamResults, &proto.NestedArray{Elems: []proto.RESP{
				proto.NewBulkString([]byte(streamKey)),
				&proto.NestedArray{Elems: entriesResp},
			}})
		}
	}
	return &proto.NestedArray{Elems: streamResults}
}

// handleXRANGE 实现 XRANGE 命令
func (h *Handler) handleXRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'XRANGE' command")
	}
	key := string(args[0])
	start := string(args[1])
	stop := string(args[2])
	count := int64(0)

	// Parse COUNT option
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "COUNT" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			break
		}
	}

	entries, err := h.Db.XRange(key, start, stop, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}

	// XRANGE returns [[entryID, [field, value, ...]], ...]
	// go-redis parses nested arrays as flat when structure is [id, [fields...]]
	// Format expected by test: [[id1, field1, value1], [id2, field2, value2]]
	if len(entries) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	var resultElems []proto.RESP
	for _, entry := range entries {
		var fieldElems []proto.RESP
		for k, v := range entry.Fields {
			bsK := proto.BulkString(k)
			bsV := proto.BulkString(v)
			fieldElems = append(fieldElems, &bsK, &bsV)
		}
		bsID := proto.BulkString(entry.ID)
		// Entry: [id, [field, value, ...]]
		resultElems = append(resultElems, &proto.NestedArray{
			Elems: []proto.RESP{
				&bsID,
				&proto.NestedArray{Elems: fieldElems},
			},
		})
	}
	return &proto.NestedArray{Elems: resultElems}
}

// handleXREVRANGE 实现 XREVRANGE 命令
func (h *Handler) handleXREVRANGE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'XREVRANGE' command")
	}
	key := string(args[0])
	start := string(args[1])
	stop := string(args[2])
	count := int64(0)

	// Parse COUNT option
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "COUNT" {
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			break
		}
	}

	entries, err := h.Db.XRevRange(key, start, stop, count)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}

	// XREVRANGE returns [[entryID, [field, value, ...]], ...] (reverse order)
	if len(entries) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	var resultElems []proto.RESP
	for _, entry := range entries {
		var fieldElems []proto.RESP
		for k, v := range entry.Fields {
			bsK := proto.BulkString(k)
			bsV := proto.BulkString(v)
			fieldElems = append(fieldElems, &bsK, &bsV)
		}
		bsID := proto.BulkString(entry.ID)
		resultElems = append(resultElems, &proto.NestedArray{
			Elems: []proto.RESP{
				&bsID,
				&proto.NestedArray{Elems: fieldElems},
			},
		})
	}
	return &proto.NestedArray{Elems: resultElems}
}

// handleXDEL 实现 XDEL 命令
func (h *Handler) handleXDEL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'XDEL' command")
	}
	key := string(args[0])
	ids := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		ids[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	deleted, err := h.Db.XDel(key, ids...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(deleted)
}

// handleXACK 实现 XACK 命令
func (h *Handler) handleXACK(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 3 {
		return proto.NewError("ERR wrong number of arguments for 'XACK' command")
	}
	key := string(args[0])
	group := string(args[1])
	ids := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		ids[i-2] = string(args[i])
	}
	acknowledged, err := h.Db.XAck(key, group, ids...)
	if err != nil {
		return wrapStoreError(err)
	}
	return proto.NewInteger(acknowledged)
}

// handleXGROUP 实现 XGROUP 命令
func (h *Handler) handleXGROUP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'XGROUP' command")
	}
	subcommand := strings.ToUpper(string(args[0]))

	switch subcommand {
	case "CREATE":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'XGROUP CREATE' command")
		}
		key := string(args[1])
		group := string(args[2])
		startID := string(args[3])
		h.markDirtyKeys(state, key)
		err := h.Db.XGroupCreate(key, group, startID)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK
	case "DESTROY":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XGROUP DESTROY' command")
		}
		key := string(args[1])
		group := string(args[2])
		h.markDirtyKeys(state, key)
		err := h.Db.XGroupDestroy(key, group)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(1)
	case "SETID":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'XGROUP SETID' command")
		}
		key := string(args[1])
		group := string(args[2])
		id := string(args[3])
		h.markDirtyKeys(state, key)
		err := h.Db.XGroupSetID(key, group, id)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.OK
	case "DELCONSUMER":
		if len(args) < 4 {
			return proto.NewError("ERR wrong number of arguments for 'XGROUP DELCONSUMER' command")
		}
		key := string(args[1])
		group := string(args[2])
		consumer := string(args[3])
		h.markDirtyKeys(state, key)
		removed, err := h.Db.XGroupDelConsumer(key, group, consumer)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		return proto.NewInteger(removed)
	default:
		return proto.NewError("ERR syntax error")
	}
}

// handleXREADGROUP 实现 XREADGROUP 命令
func (h *Handler) handleXREADGROUP(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	var count int64 = 0
	var block int64 = -1
	var group, consumer string

	// Find GROUP keyword first
	groupIdx := -1
	for i := 0; i < len(args); i++ {
		if strings.ToUpper(string(args[i])) == "GROUP" {
			groupIdx = i
			break
		}
	}
	if groupIdx < 0 {
		return proto.NewError("ERR syntax error, missing GROUP keyword")
	}

	// Parse options before GROUP
	i := 0
	for i < groupIdx {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "COUNT":
			if i+1 >= groupIdx {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			i += 2
		case "BLOCK":
			if i+1 >= groupIdx {
				return proto.NewError("ERR syntax error")
			}
			b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			block = b
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	// Parse group and consumer
	if groupIdx+2 >= len(args) {
		return proto.NewError("ERR syntax error")
	}
	group = string(args[groupIdx+1])
	consumer = string(args[groupIdx+2])
	i = groupIdx + 3

	// Parse options (COUNT, BLOCK) after group/consumer
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		if opt == "STREAMS" {
			break
		}
		switch opt {
		case "COUNT":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			c, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			count = c
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			b, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			block = b
			i += 2
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
		}
	}

	// Check for STREAMS
	if i >= len(args) || strings.ToUpper(string(args[i])) != "STREAMS" {
		return proto.NewError("ERR syntax error, missing STREAMS keyword")
	}
	i++

	// Parse stream IDs
	remaining := len(args) - i
	if remaining < 2 || remaining%2 != 0 {
		return proto.NewError(fmt.Sprintf("ERR syntax error: remaining=%d, i=%d, len(args)=%d", remaining, i, len(args)))
	}
	numStreams := remaining / 2
	streamKeys := make([]string, numStreams)
	streamIDs := make([]string, numStreams)
	for j := 0; j < numStreams; j++ {
		streamKeys[j] = string(args[i+j*2])
		streamIDs[j] = string(args[i+j*2+1])
	}

	if block >= 0 {
		state.blocking.Store(true)
	}
	results, err := h.Db.XReadGroup(h.Ctx, group, consumer, count, block, streamKeys...)
	if block >= 0 {
		state.blocking.Store(false)
	}
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}

	// Format response - XREADGROUP returns [[stream, [[entry1], [entry2], ...]], ...]
	var response []proto.RESP
	for _, streamMap := range results {
		for streamKey, entries := range streamMap {
			var entryArrayElems []proto.RESP
			for _, entry := range entries {
				var fieldElems []proto.RESP
				for k, v := range entry.Fields {
					bsK := proto.BulkString(k)
					bsV := proto.BulkString(v)
					fieldElems = append(fieldElems, &bsK, &bsV)
				}
				bsID := proto.BulkString(entry.ID)
				entryArrayElems = append(entryArrayElems, &proto.NestedArray{
					Elems: []proto.RESP{&bsID, &proto.NestedArray{Elems: fieldElems}},
				})
			}
			bsKey := proto.BulkString(streamKey)
			response = append(response, &proto.NestedArray{
				Elems: []proto.RESP{
					&bsKey,
					&proto.NestedArray{Elems: entryArrayElems},
				},
			})
		}
	}
	if len(response) == 0 {
		if state.respVersion == 3 {
			return &proto.Null{}
		}
		return proto.NewBulkString(nil)
	}
	return &proto.NestedArray{Elems: response}
}

// handleXCLAIM 实现 XCLAIM 命令
func (h *Handler) handleXCLAIM(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 5 {
		return proto.NewError("ERR wrong number of arguments for 'XCLAIM' command")
	}
	key := string(args[0])
	group := string(args[1])
	consumer := string(args[2])
	minIdleTime, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	ids := make([]string, len(args)-4)
	for i := 4; i < len(args); i++ {
		ids[i-4] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	claimed, err := h.Db.XClaim(key, group, consumer, minIdleTime, ids...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	result := make([][]byte, len(claimed))
	for i, id := range claimed {
		result[i] = []byte(id)
	}
	return &proto.Array{Args: result}
}

// handleXAUTOCLAIM 实现 XAUTOCLAIM 命令
func (h *Handler) handleXAUTOCLAIM(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 5 {
		return proto.NewError("ERR wrong number of arguments for 'XAUTOCLAIM' command")
	}
	key := string(args[0])
	group := string(args[1])
	consumer := string(args[2])
	minIdleTime, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	start := string(args[4])

	// Parse options
	opts := store.XAutoClaimOptions{Count: 100, JustID: false}
	i := 5
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "COUNT":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			count, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			opts.Count = count
			i += 2
		case "JUSTID":
			opts.JustID = true
			i++
		default:
			return proto.NewError("ERR syntax error")
		}
	}

	h.markDirtyKeys(state, key)
	result, err := h.Db.XAutoClaim(key, group, consumer, minIdleTime, start, opts)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}

	if opts.JustID {
		entries := make([]proto.RESP, len(result.ClaimedIDs))
		for i, id := range result.ClaimedIDs {
			entries[i] = proto.NewBulkString([]byte(id))
		}
		return &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(result.NextID)),
				&proto.NestedArray{Elems: entries},
			},
		}
	}

	entries := make([]proto.RESP, 0, len(result.Messages))
	for _, msg := range result.Messages {
		fields := make([]proto.RESP, 0, len(msg.Fields)*2)
		for k, v := range msg.Fields {
			fields = append(fields, proto.NewBulkString([]byte(k)), proto.NewBulkString([]byte(v)))
		}
		entries = append(entries, &proto.NestedArray{
			Elems: []proto.RESP{
				proto.NewBulkString([]byte(msg.ID)),
				&proto.NestedArray{Elems: fields},
			},
		})
	}
	return &proto.NestedArray{
		Elems: []proto.RESP{
			proto.NewBulkString([]byte(result.NextID)),
			&proto.NestedArray{Elems: entries},
		},
	}
}

// handleXPENDING 实现 XPENDING 命令
func (h *Handler) handleXPENDING(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'XPENDING' command")
	}
	key := string(args[0])
	group := string(args[1])
	entries, err := h.Db.XPending(key, group)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	response := make([]proto.RESP, 0)

	count := len(entries)
	response = append(response, proto.Integer(count))

	var minID, maxID string
	for _, e := range entries {
		if minID == "" || e.ID < minID {
			minID = e.ID
		}
		if maxID == "" || e.ID > maxID {
			maxID = e.ID
		}
	}
	response = append(response, proto.NewBulkString([]byte(minID)))
	response = append(response, proto.NewBulkString([]byte(maxID)))

	var entriesArray []proto.RESP
	for _, e := range entries {
		entryArray := []proto.RESP{
			proto.NewBulkString([]byte(e.ID)),
			proto.NewBulkString([]byte(e.Consumer)),
			proto.Integer(e.DeliveryCount),
			proto.Integer(e.LastDelivery),
		}
		entriesArray = append(entriesArray, &proto.NestedArray{Elems: entryArray})
	}
	response = append(response, &proto.NestedArray{Elems: entriesArray})

	return &proto.NestedArray{Elems: response}
}

// handleXINFO 实现 XINFO 命令
func (h *Handler) handleXINFO(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'XINFO' command")
	}
	subcommand := strings.ToUpper(string(args[0]))

	switch subcommand {
	case "HELP":
		response := [][]byte{
			[]byte("XINFO <subcommand> [<arg> ...]"),
			[]byte("Returns information about streams and consumer groups."),
			[]byte(""),
			[]byte("XINFO STREAM <key> [FULL]"),
			[]byte("  -- Returns information about a stream."),
			[]byte(""),
			[]byte("XINFO GROUPS <key>"),
			[]byte("  -- Returns the consumer groups of a stream."),
			[]byte(""),
			[]byte("XINFO CONSUMERS <key> <group>"),
			[]byte("  -- Returns the consumers of a consumer group."),
			[]byte(""),
			[]byte("XINFO STREAM <key> FULL [COUNT <count>]"),
			[]byte("  -- Returns full information about a stream including entries."),
		}
		return &proto.Array{Args: response}
	case "STREAM":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XINFO STREAM' command")
		}
		key := string(args[1])
		info, err := h.Db.XInfo(key)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		groupsCount := int64(0)
		if info.Groups != nil {
			groupsCount = int64(len(info.Groups))
		}
		response := [][]byte{
			[]byte("length"),
			[]byte(strconv.FormatInt(info.Length, 10)),
			[]byte("radix-tree-keys"),
			[]byte(strconv.FormatInt(info.RadixTreeKeys, 10)),
			[]byte("radix-tree-nodes"),
			[]byte(strconv.FormatInt(info.RadixTreeNodes, 10)),
			[]byte("last-generated-id"),
			[]byte(info.LastID),
			[]byte("max-deleted-entry-id"),
			[]byte(info.MaxDeletedID),
			[]byte("entries-added"),
			[]byte(strconv.FormatInt(info.Length, 10)),
			[]byte("groups"),
			[]byte(strconv.FormatInt(groupsCount, 10)),
		}
		return &proto.Array{Args: response}
	case "GROUPS":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'XINFO GROUPS' command")
		}
		key := string(args[1])
		groups, err := h.Db.XInfoGroups(key)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		var response []proto.RESP
		for _, g := range groups {
			groupInfo := []proto.RESP{
				proto.NewBulkString([]byte("name")),
				proto.NewBulkString([]byte(g.Name)),
				proto.NewBulkString([]byte("consumers")),
				proto.NewBulkString([]byte(strconv.Itoa(len(g.Consumers)))),
				proto.NewBulkString([]byte("pending")),
				proto.NewBulkString([]byte(strconv.Itoa(len(g.Pending)))),
			}
			response = append(response, &proto.NestedArray{Elems: groupInfo})
		}
		return &proto.NestedArray{Elems: response}
	case "CONSUMERS":
		if len(args) < 3 {
			return proto.NewError("ERR wrong number of arguments for 'XINFO CONSUMERS' command")
		}
		key := string(args[1])
		group := string(args[2])
		consumers, err := h.Db.XInfoConsumers(key, group)
		if err != nil {
			if errors.Is(err, store.ErrWrongType) {
				return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			return proto.NewError(fmt.Sprintf("ERR %v", err))
		}
		var response []proto.RESP
		for _, c := range consumers {
			consumerInfo := []proto.RESP{
				proto.NewBulkString([]byte("name")),
				proto.NewBulkString([]byte(c.Name)),
				proto.NewBulkString([]byte("seen")),
				proto.NewBulkString([]byte(strconv.FormatInt(c.LastSeen, 10))),
			}
			response = append(response, &proto.NestedArray{Elems: consumerInfo})
		}
		return &proto.NestedArray{Elems: response}
	default:
		return proto.NewError("ERR syntax error")
	}
}

// handleXTRIM 实现 XTRIM 命令
func (h *Handler) handleXTRIM(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'XTRIM' command")
	}
	key := string(args[0])
	var maxLen int64 = 0
	var minID string

	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "MAXLEN":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			nextArg := strings.ToUpper(string(args[i+1]))
			if nextArg == "~" {
				if i+2 >= len(args) {
					return proto.NewError("ERR syntax error")
				}
				maxlen, err := strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				maxLen = maxlen
				i += 3
			} else {
				maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return proto.NewError("ERR value is not an integer")
				}
				maxLen = maxlen
				i += 2
			}
		case "MINID":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			minID = string(args[i+1])
			i += 2
		case "~":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			maxlen, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer")
			}
			maxLen = maxlen
			i += 2
		default:
			if _, err := strconv.ParseInt(opt, 10, 64); err == nil {
				maxLen, _ = strconv.ParseInt(opt, 10, 64)
				i++
			} else {
				return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", opt))
			}
		}
	}

	h.markDirtyKeys(state, key)
	trimmed, err := h.Db.XTrim(key, maxLen, minID)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(trimmed)
}

// handleXACKDEL 实现 XACKDEL 命令（Redis 8.2+），确认并条件删除流条目
// XACKDEL key group [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
func (h *Handler) handleXACKDEL(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'XACKDEL' command")
	}
	key := string(args[0])
	group := string(args[1])

	// Parse optional mode: KEEPREF | DELREF | ACKED
	mode := "KEEPREF"
	nextArg := strings.ToUpper(string(args[2]))
	if nextArg == "KEEPREF" || nextArg == "DELREF" || nextArg == "ACKED" {
		mode = nextArg
		// IDS keyword expected next
		if len(args) < 6 {
			return proto.NewError("ERR wrong number of arguments for 'XACKDEL' command")
		}
		if strings.ToUpper(string(args[3])) != "IDS" {
			return proto.NewError("ERR syntax error")
		}
		// Parse numids and ids
		numIDs, err := strconv.Atoi(string(args[4]))
		if err != nil || numIDs < 1 {
			return proto.NewError("ERR value is not an integer or out of range")
		}
		if 5+numIDs > len(args) {
			return proto.NewError("ERR wrong number of arguments for 'XACKDEL' command")
		}
		ids := make([]string, numIDs)
		for i := 0; i < numIDs; i++ {
			ids[i] = string(args[5+i])
		}
		return h.executeXACKDEL(state, key, group, mode, ids)
	}

	// Without mode keyword: args[2] should be "IDS"
	if nextArg != "IDS" {
		return proto.NewError("ERR syntax error")
	}
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'XACKDEL' command")
	}
	numIDs, err := strconv.Atoi(string(args[3]))
	if err != nil || numIDs < 1 {
		return proto.NewError("ERR value is not an integer or out of range")
	}
	if 4+numIDs > len(args) {
		return proto.NewError("ERR wrong number of arguments for 'XACKDEL' command")
	}
	ids := make([]string, numIDs)
	for i := 0; i < numIDs; i++ {
		ids[i] = string(args[4+i])
	}
	return h.executeXACKDEL(state, key, group, mode, ids)
}

// executeXACKDEL 执行 XACKDEL 的核心逻辑
func (h *Handler) executeXACKDEL(state *connState, key, group, mode string, ids []string) proto.RESP {
	h.markDirtyKeys(state, key)

	// Step 1: Acknowledge in the consumer group
	acknowledged, err := h.Db.XAck(key, group, ids...)
	if err != nil {
		return wrapStoreError(err)
	}
	_ = acknowledged // we use per-id results instead

	// Step 2: Delete entries based on mode
	results := make([]proto.RESP, len(ids))
	switch mode {
	case "KEEPREF":
		// Delete from stream, keep PEL refs in other groups
		for i, id := range ids {
			deleted, err := h.Db.XDel(key, id)
			if err != nil {
				return wrapStoreError(err)
			}
			if deleted > 0 {
				results[i] = proto.NewInteger(1)
			} else {
				results[i] = proto.NewInteger(-1)
			}
		}
	case "DELREF":
		// Delete from stream + remove PEL refs from ALL groups
		for i, id := range ids {
			deleted, err := h.Db.XDel(key, id)
			if err != nil {
				return wrapStoreError(err)
			}
			if deleted > 0 {
				results[i] = proto.NewInteger(1)
			} else {
				// Even if entry is gone, remove dangling PEL refs
				if refErr := h.Db.XAckDelRemoveRefs(key, id); refErr != nil {
					logger.Logger.Warn().Err(refErr).Str("key", key).Str("id", id).
						Msg("XACKDEL: failed to remove dangling PEL refs")
				}
				results[i] = proto.NewInteger(1)
			}
		}
	case "ACKED":
		// Only delete if ALL consumer groups have acknowledged
		for i, id := range ids {
			allAcked, err := h.Db.XIsAckedByAllGroups(key, id)
			if err != nil {
				return wrapStoreError(err)
			}
			if allAcked {
				deleted, err := h.Db.XDel(key, id)
				if err != nil {
					return wrapStoreError(err)
				}
				if deleted > 0 {
					results[i] = proto.NewInteger(1)
				} else {
					results[i] = proto.NewInteger(-1)
				}
			} else {
				results[i] = proto.NewInteger(2) // acked but not deleted
			}
		}
	}
	return &proto.NestedArray{Elems: results}
}

// handleXDELEX 实现 XDELEX 命令，删除流条目（功能等同 XDEL）
func (h *Handler) handleXDELEX(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'XDELEX' command")
	}
	key := string(args[0])
	ids := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		ids[i-1] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	deleted, err := h.Db.XDel(key, ids...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	return proto.NewInteger(deleted)
}

// handleXNACK 实现 XNACK 命令，释放 PEL 消息重新投递
// XNACK key group consumer id [id ...]
func (h *Handler) handleXNACK(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'XNACK' command")
	}
	key := string(args[0])
	group := string(args[1])
	consumer := string(args[2])
	ids := make([]string, len(args)-3)
	for i := 3; i < len(args); i++ {
		ids[i-3] = string(args[i])
	}
	h.markDirtyKeys(state, key)
	released, err := h.Db.XNack(key, group, consumer, ids...)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapStoreError(err)
	}
	return proto.NewInteger(released)
}

// handleXSETID 实现 XSETID 命令（内部复制命令）
// XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
func (h *Handler) handleXSETID(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'XSETID' command")
	}
	key := string(args[0])
	lastID := string(args[1])

	// Parse optional arguments
	var entriesAdded int64 = -1
	var maxDeletedID string
	i := 2
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "ENTRIESADDED":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			var err error
			entriesAdded, err = strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return proto.NewError("ERR value is not an integer or out of range")
			}
			i += 2
		case "MAXDELETEDID":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			maxDeletedID = string(args[i+1])
			i += 2
		default:
			return proto.NewError("ERR syntax error")
		}
	}

	h.markDirtyKeys(state, key)
	err := h.Db.XSetID(key, lastID, entriesAdded, maxDeletedID)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return wrapStoreError(err)
	}
	return proto.NewSimpleString("OK")
}

// handleXCFGSET 实现 XCFGSET 命令（IDMP 配置参数，内部命令）
func (h *Handler) handleXCFGSET(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// XCFGSET is an internal Redis command; accept and return OK for compatibility
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'XCFGSET' command")
	}
	return proto.NewSimpleString("OK")
}
